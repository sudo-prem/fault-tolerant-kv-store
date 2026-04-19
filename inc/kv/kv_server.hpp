#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "kv.grpc.pb.h"
#include "rafty/raft.hpp"

namespace kv {

    class KvServer : public kvpb::KvService::Service {
    public:
        explicit KvServer(rafty::Raft &raft) : raft_(raft) {}
        ~KvServer() = default;

        void on_apply(const rafty::ApplyResult &result) {
            if(!result.valid) { return; }

            std::shared_ptr<Pending> pending;
            ApplyOutcome pending_outcome = timeout_outcome();

            auto op = deserialize_op(result.data);
            if(!op.has_value()) {
                {
                    std::lock_guard<std::mutex> lk(mu_);
                    auto it = pending_by_index_.find(result.index);
                    if(it != pending_by_index_.end()) {
                        pending = it->second;
                        pending_by_index_.erase(it);
                    }
                }
                complete_pending(std::move(pending), pending_outcome);
                return;
            }

            kvpb::KvStatus status = kvpb::KV_SUCCESS;
            std::string value;
            bool is_dup = false;

            {
                std::lock_guard<std::mutex> lk(mu_);

                auto dedup_it = dedup_.find(op->client_id);
                if(dedup_it != dedup_.end()) {
                    auto cached_it = dedup_it->second.by_seq.find(op->seq_num);
                    if(cached_it != dedup_it->second.by_seq.end()) {
                        // Exact duplicate RPC: return cached result without re-executing.
                        is_dup = true;
                        status = cached_it->second.status;
                        value = cached_it->second.value;
                    }
                }

                if(!is_dup && dedup_it != dedup_.end() && op->seq_num < dedup_it->second.max_seq) {
                    // Old/superseded request: do not re-execute an already-seen sequence.
                    is_dup = true;
                    auto latest_it = dedup_it->second.by_seq.find(dedup_it->second.max_seq);
                    if(latest_it != dedup_it->second.by_seq.end()) {
                        status = latest_it->second.status;
                        value = latest_it->second.value;
                    }
                }

                if(!is_dup) {
                    if(op->type == OpType::Put) {
                        store_[op->key] = op->value;
                    } else if(op->type == OpType::Append) {
                        store_[op->key] += op->value;
                    } else {
                        auto it = store_.find(op->key);
                        value = (it == store_.end()) ? "" : it->second;
                    }

                    auto &history = dedup_[op->client_id];
                    history.by_seq[op->seq_num] = ClientCache{
                        .status = status,
                        .value = value,
                    };
                    history.seq_order.push_back(op->seq_num);
                    if(op->seq_num > history.max_seq) { history.max_seq = op->seq_num; }
                    trim_client_history_locked(history);
                }

                // Record applied results so waiters arriving after apply still resolve.
                applied_by_index_[result.index] = AppliedEntry{
                    .command = result.data,
                    .status = status,
                    .value = value,
                };
                applied_order_.push_back(result.index);
                while(applied_order_.size() > kAppliedHistoryLimit) {
                    const uint64_t old_index = applied_order_.front();
                    applied_order_.pop_front();
                    applied_by_index_.erase(old_index);
                }

                auto pending_it = pending_by_index_.find(result.index);
                if(pending_it != pending_by_index_.end()) {
                    pending = pending_it->second;
                    pending_by_index_.erase(pending_it);
                    if(pending->expected_command == result.data) {
                        pending_outcome = ApplyOutcome{ .status = status, .value = value };
                    }
                }
            }

            complete_pending(std::move(pending), pending_outcome);
        }

        grpc::Status
        Put(grpc::ServerContext *context, const kvpb::PutRequest *request, kvpb::KvResponse *response) override {
            (void)context;

            if(!raft_.get_state().is_leader) {
                response->set_status(kvpb::KV_NOTLEADER);
                return grpc::Status::OK;
            }

            auto cached = check_cached_duplicate(request->client_id(), request->seq_num());
            if(cached.has_value()) {
                response->set_status(cached->status);
                return grpc::Status::OK;
            }

            const std::string cmd
                = serialize_op(OpType::Put, request->key(), request->value(), request->client_id(), request->seq_num());
            auto proposal = raft_.propose(cmd);
            if(!proposal.is_leader || proposal.index == 0) {
                response->set_status(kvpb::KV_NOTLEADER);
                return grpc::Status::OK;
            }

            auto applied = wait_for_apply(proposal.index, cmd);
            response->set_status(applied.status);
            return grpc::Status::OK;
        }

        grpc::Status
        Get(grpc::ServerContext *context, const kvpb::GetRequest *request, kvpb::GetResponse *response) override {
            (void)context;

            if(!raft_.get_state().is_leader) {
                response->set_status(kvpb::KV_NOTLEADER);
                response->set_value("");
                return grpc::Status::OK;
            }

            auto cached = check_cached_duplicate(request->client_id(), request->seq_num());
            if(cached.has_value()) {
                response->set_status(cached->status);
                response->set_value(cached->value);
                return grpc::Status::OK;
            }

            const std::string cmd
                = serialize_op(OpType::Get, request->key(), "", request->client_id(), request->seq_num());
            auto proposal = raft_.propose(cmd);
            if(!proposal.is_leader || proposal.index == 0) {
                response->set_status(kvpb::KV_NOTLEADER);
                response->set_value("");
                return grpc::Status::OK;
            }

            auto applied = wait_for_apply(proposal.index, cmd);
            response->set_status(applied.status);
            response->set_value(applied.value);
            return grpc::Status::OK;
        }

        grpc::Status
        Append(grpc::ServerContext *context, const kvpb::AppendRequest *request, kvpb::KvResponse *response) override {
            (void)context;

            if(!raft_.get_state().is_leader) {
                response->set_status(kvpb::KV_NOTLEADER);
                return grpc::Status::OK;
            }

            auto cached = check_cached_duplicate(request->client_id(), request->seq_num());
            if(cached.has_value()) {
                response->set_status(cached->status);
                return grpc::Status::OK;
            }

            const std::string cmd = serialize_op(OpType::Append, request->key(), request->value(), request->client_id(),
                                                 request->seq_num());
            auto proposal = raft_.propose(cmd);
            if(!proposal.is_leader || proposal.index == 0) {
                response->set_status(kvpb::KV_NOTLEADER);
                return grpc::Status::OK;
            }

            auto applied = wait_for_apply(proposal.index, cmd);
            response->set_status(applied.status);
            return grpc::Status::OK;
        }

    private:
        enum class OpType : uint8_t { Put = 0, Get = 1, Append = 2 };

        struct Op {
            OpType type;
            uint64_t client_id;
            uint64_t seq_num;
            std::string key;
            std::string value;
        };

        struct ClientCache {
            kvpb::KvStatus status;
            std::string value;
        };

        struct ClientHistory {
            uint64_t max_seq = 0;
            std::unordered_map<uint64_t, ClientCache> by_seq;
            std::deque<uint64_t> seq_order;
        };

        struct ApplyOutcome {
            kvpb::KvStatus status = kvpb::KV_TIMEOUT;
            std::string value;
        };

        struct Pending {
            std::string expected_command;
            std::promise<ApplyOutcome> promise;
        };

        struct AppliedEntry {
            std::string command;
            kvpb::KvStatus status = kvpb::KV_TIMEOUT;
            std::string value;
        };

        static std::string serialize_op(OpType type, const std::string &key, const std::string &value,
                                        uint64_t client_id, uint64_t seq_num) {
            // Format:
            // <type>\n<client_id>\n<seq_num>\n<key_len>\n<value_len>\n<key><value>
            std::string out;
            out.reserve(64 + key.size() + value.size());
            out.append(std::to_string(static_cast<uint8_t>(type)));
            out.push_back('\n');
            out.append(std::to_string(client_id));
            out.push_back('\n');
            out.append(std::to_string(seq_num));
            out.push_back('\n');
            out.append(std::to_string(key.size()));
            out.push_back('\n');
            out.append(std::to_string(value.size()));
            out.push_back('\n');
            out.append(key);
            out.append(value);
            return out;
        }

        static std::optional<Op> deserialize_op(const std::string &data) {
            size_t pos = 0;
            const auto parse_line = [&](size_t &cursor, std::string &line) -> bool {
                const size_t nl = data.find('\n', cursor);
                if(nl == std::string::npos) { return false; }
                line = data.substr(cursor, nl - cursor);
                cursor = nl + 1;
                return true;
            };

            std::string line;
            if(!parse_line(pos, line)) { return std::nullopt; }
            int raw_type = -1;
            {
                std::istringstream iss(line);
                if(!(iss >> raw_type)) { return std::nullopt; }
            }
            OpType type;
            if(raw_type == 0) {
                type = OpType::Put;
            } else if(raw_type == 1) {
                type = OpType::Get;
            } else if(raw_type == 2) {
                type = OpType::Append;
            } else {
                return std::nullopt;
            }

            if(!parse_line(pos, line)) { return std::nullopt; }
            uint64_t client_id = 0;
            {
                std::istringstream iss(line);
                if(!(iss >> client_id)) { return std::nullopt; }
            }

            if(!parse_line(pos, line)) { return std::nullopt; }
            uint64_t seq_num = 0;
            {
                std::istringstream iss(line);
                if(!(iss >> seq_num)) { return std::nullopt; }
            }

            if(!parse_line(pos, line)) { return std::nullopt; }
            size_t key_len = 0;
            {
                std::istringstream iss(line);
                if(!(iss >> key_len)) { return std::nullopt; }
            }

            if(!parse_line(pos, line)) { return std::nullopt; }
            size_t value_len = 0;
            {
                std::istringstream iss(line);
                if(!(iss >> value_len)) { return std::nullopt; }
            }

            if(pos + key_len + value_len > data.size()) { return std::nullopt; }

            return Op{
                .type = type,
                .client_id = client_id,
                .seq_num = seq_num,
                .key = data.substr(pos, key_len),
                .value = data.substr(pos + key_len, value_len),
            };
        }

        static ApplyOutcome timeout_outcome() { return ApplyOutcome{ .status = kvpb::KV_TIMEOUT, .value = "" }; }

        static void complete_pending(std::shared_ptr<Pending> pending, ApplyOutcome outcome) {
            if(!pending) { return; }
            try {
                pending->promise.set_value(std::move(outcome));
            } catch(const std::future_error &) {
                // Waiter may have already timed out and abandoned this pending request.
            }
        }

        static void trim_client_history_locked(ClientHistory &history) {
            while(history.seq_order.size() > kDedupHistoryPerClient) {
                const uint64_t oldest = history.seq_order.front();
                history.seq_order.pop_front();
                if(oldest == history.max_seq) {
                    history.seq_order.push_front(oldest);
                    break;
                }
                history.by_seq.erase(oldest);
            }
        }

        std::optional<ClientCache> check_cached_duplicate(uint64_t client_id, uint64_t seq_num) {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = dedup_.find(client_id);
            if(it != dedup_.end()) {
                auto cached_it = it->second.by_seq.find(seq_num);
                if(cached_it != it->second.by_seq.end()) { return cached_it->second; }

                if(seq_num < it->second.max_seq) {
                    auto latest_it = it->second.by_seq.find(it->second.max_seq);
                    if(latest_it != it->second.by_seq.end()) { return latest_it->second; }
                }
            }
            return std::nullopt;
        }

        ApplyOutcome wait_for_apply(uint64_t index, const std::string &command) {
            std::shared_ptr<Pending> pending;
            std::future<ApplyOutcome> future;
            {
                std::lock_guard<std::mutex> lk(mu_);

                auto applied_it = applied_by_index_.find(index);
                if(applied_it != applied_by_index_.end()) {
                    if(applied_it->second.command == command) {
                        return ApplyOutcome{
                            .status = applied_it->second.status,
                            .value = applied_it->second.value,
                        };
                    }
                    return timeout_outcome();
                }

                pending = std::make_shared<Pending>();
                pending->expected_command = command;
                future = pending->promise.get_future();
                pending_by_index_[index] = pending;
            }

            if(future.wait_for(kRpcWaitTimeout) == std::future_status::ready) { return future.get(); }

            std::lock_guard<std::mutex> lk(mu_);
            auto pending_it = pending_by_index_.find(index);
            if(pending_it != pending_by_index_.end() && pending_it->second == pending) {
                pending_by_index_.erase(pending_it);
            }
            return timeout_outcome();
        }

        rafty::Raft &raft_;
        std::mutex mu_;

        std::unordered_map<std::string, std::string> store_;
        std::unordered_map<uint64_t, ClientHistory> dedup_;
        std::unordered_map<uint64_t, std::shared_ptr<Pending>> pending_by_index_;
        std::unordered_map<uint64_t, AppliedEntry> applied_by_index_;
        std::deque<uint64_t> applied_order_;

        static constexpr std::chrono::milliseconds kRpcWaitTimeout{ 4500 };
        static constexpr size_t kAppliedHistoryLimit = 2048;
        static constexpr size_t kDedupHistoryPerClient = 64;
    };

} // namespace kv
