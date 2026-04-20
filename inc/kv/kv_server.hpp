#pragma once

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
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

            std::shared_ptr<Pending> pending_to_notify;

            auto op = deserialize_op(result.data);
            if(!op.has_value()) {
                {
                    std::lock_guard<std::mutex> lk(mu_);
                    auto it = pending_by_index_.find(result.index);
                    if(it != pending_by_index_.end()) {
                        pending_to_notify = it->second;
                        pending_to_notify->finished = true;
                        pending_to_notify->matched = false;
                        pending_by_index_.erase(it);
                    }
                    last_applied_index_ = std::max(last_applied_index_, result.index);
                }
                if(pending_to_notify) { pending_to_notify->cv.notify_one(); }
                apply_progress_cv_.notify_all();
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

                if(!is_dup) {
                    if(op->type == OpType::Put) {
                        store_[op->key] = op->value;
                    } else if(op->type == OpType::Append) {
                        store_[op->key] += op->value;
                    } else {
                        auto it = store_.find(op->key);
                        value = (it == store_.end()) ? "" : it->second;
                    }

                    cache_client_result_locked(op->client_id, op->seq_num, status, value);
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

                last_applied_index_ = std::max(last_applied_index_, result.index);

                auto pending_it = pending_by_index_.find(result.index);
                if(pending_it != pending_by_index_.end()) {
                    pending_to_notify = pending_it->second;
                    pending_to_notify->finished = true;
                    pending_to_notify->matched = (pending_to_notify->expected_command == result.data);
                    pending_to_notify->status = status;
                    pending_to_notify->value = value;
                    pending_by_index_.erase(pending_it);
                }
            }

            if(pending_to_notify) { pending_to_notify->cv.notify_one(); }
            apply_progress_cv_.notify_all();
        }

        grpc::Status
        Put(grpc::ServerContext *context, const kvpb::PutRequest *request, kvpb::KvResponse *response) override {
            (void)context;

            auto cached = check_cached_duplicate(request->client_id(), request->seq_num());
            if(cached.has_value()) {
                response->set_status(cached->status);
                return grpc::Status::OK;
            }

            if(!raft_.get_state().is_leader) {
                response->set_status(kvpb::KV_NOTLEADER);
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

            auto cached = check_cached_duplicate(request->client_id(), request->seq_num());
            if(cached.has_value()) {
                response->set_status(cached->status);
                response->set_value(cached->value);
                return grpc::Status::OK;
            }

            if(!raft_.get_state().is_leader) {
                response->set_status(kvpb::KV_NOTLEADER);
                response->set_value("");
                return grpc::Status::OK;
            }

            auto fast_read = try_linearizable_local_get(request->key(), request->client_id(), request->seq_num());
            if(fast_read.has_value()) {
                response->set_status(fast_read->status);
                response->set_value(fast_read->value);
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

            auto cached = check_cached_duplicate(request->client_id(), request->seq_num());
            if(cached.has_value()) {
                response->set_status(cached->status);
                return grpc::Status::OK;
            }

            if(!raft_.get_state().is_leader) {
                response->set_status(kvpb::KV_NOTLEADER);
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
            std::unordered_map<uint64_t, ClientCache> by_seq;
        };

        struct Pending {
            std::string expected_command;
            bool finished = false;
            bool matched = false;
            kvpb::KvStatus status = kvpb::KV_TIMEOUT;
            std::string value;
            std::condition_variable cv;
        };

        struct ApplyOutcome {
            kvpb::KvStatus status = kvpb::KV_TIMEOUT;
            std::string value;
        };

        struct AppliedEntry {
            std::string command;
            kvpb::KvStatus status = kvpb::KV_TIMEOUT;
            std::string value;
        };

        static void append_u64(std::string &out, uint64_t value) {
            for(int shift = 56; shift >= 0; shift -= 8) { out.push_back(static_cast<char>((value >> shift) & 0xff)); }
        }

        static bool read_u64(const std::string &data, size_t &pos, uint64_t &value) {
            if(pos + sizeof(uint64_t) > data.size()) { return false; }

            value = 0;
            for(size_t i = 0; i < sizeof(uint64_t); ++i) {
                value = (value << 8) | static_cast<unsigned char>(data[pos]);
                ++pos;
            }
            return true;
        }

        static std::string serialize_op(OpType type, const std::string &key, const std::string &value,
                                        uint64_t client_id, uint64_t seq_num) {
            std::string out;
            out.reserve(1 + (sizeof(uint64_t) * 4) + key.size() + value.size());
            out.push_back(static_cast<char>(type));
            append_u64(out, client_id);
            append_u64(out, seq_num);
            append_u64(out, key.size());
            append_u64(out, value.size());
            out.append(key);
            out.append(value);
            return out;
        }

        static std::optional<Op> deserialize_op(const std::string &data) {
            constexpr size_t kHeaderSize = 1 + (sizeof(uint64_t) * 4);
            if(data.size() < kHeaderSize) { return std::nullopt; }

            size_t pos = 0;
            const uint8_t raw_type = static_cast<uint8_t>(data[pos]);
            ++pos;

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

            uint64_t client_id = 0;
            if(!read_u64(data, pos, client_id)) { return std::nullopt; }

            uint64_t seq_num = 0;
            if(!read_u64(data, pos, seq_num)) { return std::nullopt; }

            uint64_t key_len = 0;
            if(!read_u64(data, pos, key_len)) { return std::nullopt; }

            uint64_t value_len = 0;
            if(!read_u64(data, pos, value_len)) { return std::nullopt; }

            if(key_len > data.size() - pos) { return std::nullopt; }
            pos += static_cast<size_t>(key_len);
            if(value_len > data.size() - pos) { return std::nullopt; }

            const size_t key_pos = pos - static_cast<size_t>(key_len);
            const size_t value_pos = pos;
            if(value_pos + static_cast<size_t>(value_len) != data.size()) { return std::nullopt; }

            return Op{
                .type = type,
                .client_id = client_id,
                .seq_num = seq_num,
                .key = data.substr(key_pos, static_cast<size_t>(key_len)),
                .value = data.substr(value_pos, static_cast<size_t>(value_len)),
            };
        }

        void cache_client_result_locked(uint64_t client_id, uint64_t seq_num, kvpb::KvStatus status,
                                        const std::string &value) {
            auto &history = dedup_[client_id];
            history.by_seq[seq_num] = ClientCache{
                .status = status,
                .value = value,
            };
        }

        std::optional<ClientCache> check_cached_duplicate(uint64_t client_id, uint64_t seq_num) {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = dedup_.find(client_id);
            if(it != dedup_.end()) {
                auto cached_it = it->second.by_seq.find(seq_num);
                if(cached_it != it->second.by_seq.end()) { return cached_it->second; }
            }
            return std::nullopt;
        }

        bool wait_until_applied(uint64_t index, std::unique_lock<std::mutex> &lk,
                                std::chrono::steady_clock::time_point deadline) {
            while(last_applied_index_ < index) {
                if(apply_progress_cv_.wait_until(lk, deadline) == std::cv_status::timeout) { return false; }
            }
            return true;
        }

        std::optional<ApplyOutcome>
        try_linearizable_local_get(const std::string &key, uint64_t client_id, uint64_t seq_num) {
            if(!raft_.has_committed_current_term_entry()) { return std::nullopt; }

            auto read_index = raft_.linearizable_read_index(kReadConfirmTimeout);
            if(!read_index.has_value()) {
                if(!raft_.get_state().is_leader) { return ApplyOutcome{ .status = kvpb::KV_NOTLEADER, .value = "" }; }
                return std::nullopt;
            }

            std::unique_lock<std::mutex> lk(mu_);
            const auto deadline = std::chrono::steady_clock::now() + kRpcWaitTimeout;
            if(!wait_until_applied(*read_index, lk, deadline)) {
                return ApplyOutcome{ .status = kvpb::KV_TIMEOUT, .value = "" };
            }

            const auto it = store_.find(key);
            const std::string value = (it == store_.end()) ? "" : it->second;
            cache_client_result_locked(client_id, seq_num, kvpb::KV_SUCCESS, value);
            return ApplyOutcome{ .status = kvpb::KV_SUCCESS, .value = value };
        }

        ApplyOutcome wait_for_apply(uint64_t index, const std::string &command) {
            std::unique_lock<std::mutex> lk(mu_);

            auto applied_it = applied_by_index_.find(index);
            if(applied_it != applied_by_index_.end()) {
                if(applied_it->second.command == command) {
                    return ApplyOutcome{
                        .status = applied_it->second.status,
                        .value = applied_it->second.value,
                    };
                }
                return ApplyOutcome{ .status = kvpb::KV_TIMEOUT, .value = "" };
            }

            auto pending = std::make_shared<Pending>();
            pending->expected_command = command;
            pending_by_index_[index] = pending;

            const auto deadline = std::chrono::steady_clock::now() + kRpcWaitTimeout;
            while(!pending->finished) {
                if(pending->cv.wait_until(lk, deadline) == std::cv_status::timeout) {
                    auto pending_it = pending_by_index_.find(index);
                    if(pending_it != pending_by_index_.end() && pending_it->second == pending) {
                        pending_by_index_.erase(pending_it);
                    }
                    return ApplyOutcome{ .status = kvpb::KV_TIMEOUT, .value = "" };
                }
            }

            ApplyOutcome out;
            if(!pending->matched) {
                out.status = kvpb::KV_TIMEOUT;
                out.value = "";
            } else {
                out.status = pending->status;
                out.value = pending->value;
            }

            auto pending_it = pending_by_index_.find(index);
            if(pending_it != pending_by_index_.end() && pending_it->second == pending) {
                pending_by_index_.erase(pending_it);
            }
            return out;
        }

        rafty::Raft &raft_;
        std::mutex mu_;

        std::unordered_map<std::string, std::string> store_;
        std::unordered_map<uint64_t, ClientHistory> dedup_;
        std::unordered_map<uint64_t, std::shared_ptr<Pending>> pending_by_index_;
        std::unordered_map<uint64_t, AppliedEntry> applied_by_index_;
        std::deque<uint64_t> applied_order_;
        uint64_t last_applied_index_ = 0;
        std::condition_variable apply_progress_cv_;

        static constexpr std::chrono::milliseconds kRpcWaitTimeout{ 4500 };
        static constexpr std::chrono::milliseconds kReadConfirmTimeout{ 100 };
        static constexpr size_t kAppliedHistoryLimit = 2048;
    };

} // namespace kv
