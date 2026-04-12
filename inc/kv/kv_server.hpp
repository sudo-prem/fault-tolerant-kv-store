#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
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
    if (!result.valid) {
      return;
    }

    auto op = deserialize_op(result.data);
    if (!op.has_value()) {
      std::lock_guard<std::mutex> lk(mu_);
      auto it = pending_by_index_.find(result.index);
      if (it != pending_by_index_.end()) {
        it->second.finished = true;
        it->second.matched = false;
        cv_.notify_all();
      }
      return;
    }

    kvpb::KvStatus status = kvpb::KV_SUCCESS;
    std::string value;
    bool is_dup = false;

    {
      std::lock_guard<std::mutex> lk(mu_);

      auto dedup_it = dedup_.find(op->client_id);
      if (dedup_it != dedup_.end() && op->seq_num <= dedup_it->second.seq_num) {
        // Duplicate RPC: return cached result without re-executing.
        is_dup = true;
        status = dedup_it->second.status;
        value = dedup_it->second.value;
      }

      if (!is_dup) {
        if (op->type == OpType::Put) {
          store_[op->key] = op->value;
        } else if (op->type == OpType::Append) {
          store_[op->key] += op->value;
        } else {
          auto it = store_.find(op->key);
          value = (it == store_.end()) ? "" : it->second;
        }

        dedup_[op->client_id] = ClientCache{
            .seq_num = op->seq_num,
            .status = status,
            .value = value,
        };
      }

      // Record applied results so waiters arriving after apply still resolve.
      applied_by_index_[result.index] = AppliedEntry{
          .command = result.data,
          .status = status,
          .value = value,
      };
      applied_order_.push_back(result.index);
      while (applied_order_.size() > kAppliedHistoryLimit) {
        const uint64_t old_index = applied_order_.front();
        applied_order_.pop_front();
        applied_by_index_.erase(old_index);
      }

      auto pending_it = pending_by_index_.find(result.index);
      if (pending_it != pending_by_index_.end()) {
        pending_it->second.finished = true;
        pending_it->second.matched =
            (pending_it->second.expected_command == result.data);
        pending_it->second.status = status;
        pending_it->second.value = value;
        cv_.notify_all();
      }
    }
  }

  grpc::Status Put(grpc::ServerContext *context,
                   const kvpb::PutRequest *request,
                   kvpb::KvResponse *response) override {
    (void)context;

    if (!raft_.get_state().is_leader) {
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    auto cached = check_cached_duplicate(request->client_id(), request->seq_num());
    if (cached.has_value()) {
      response->set_status(cached->status);
      return grpc::Status::OK;
    }

    const std::string cmd = serialize_op(
        OpType::Put, request->key(), request->value(), request->client_id(),
        request->seq_num());
    auto proposal = raft_.propose(cmd);
    if (!proposal.is_leader || proposal.index == 0) {
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    auto applied = wait_for_apply(proposal.index, cmd);
    response->set_status(applied.status);
    return grpc::Status::OK;
  }

  grpc::Status Get(grpc::ServerContext *context,
                   const kvpb::GetRequest *request,
                   kvpb::GetResponse *response) override {
    (void)context;

    if (!raft_.get_state().is_leader) {
      response->set_status(kvpb::KV_NOTLEADER);
      response->set_value("");
      return grpc::Status::OK;
    }

    auto cached = check_cached_duplicate(request->client_id(), request->seq_num());
    if (cached.has_value()) {
      response->set_status(cached->status);
      response->set_value(cached->value);
      return grpc::Status::OK;
    }

    const std::string cmd = serialize_op(OpType::Get, request->key(), "",
                                         request->client_id(), request->seq_num());
    auto proposal = raft_.propose(cmd);
    if (!proposal.is_leader || proposal.index == 0) {
      response->set_status(kvpb::KV_NOTLEADER);
      response->set_value("");
      return grpc::Status::OK;
    }

    auto applied = wait_for_apply(proposal.index, cmd);
    response->set_status(applied.status);
    response->set_value(applied.value);
    return grpc::Status::OK;
  }

  grpc::Status Append(grpc::ServerContext *context,
                      const kvpb::AppendRequest *request,
                      kvpb::KvResponse *response) override {
    (void)context;

    if (!raft_.get_state().is_leader) {
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    auto cached = check_cached_duplicate(request->client_id(), request->seq_num());
    if (cached.has_value()) {
      response->set_status(cached->status);
      return grpc::Status::OK;
    }

    const std::string cmd = serialize_op(
        OpType::Append, request->key(), request->value(), request->client_id(),
        request->seq_num());
    auto proposal = raft_.propose(cmd);
    if (!proposal.is_leader || proposal.index == 0) {
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
    uint64_t seq_num;
    kvpb::KvStatus status;
    std::string value;
  };

  struct Pending {
    std::string expected_command;
    bool finished = false;
    bool matched = false;
    kvpb::KvStatus status = kvpb::KV_TIMEOUT;
    std::string value;
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

  static std::string serialize_op(OpType type, const std::string &key,
                                  const std::string &value, uint64_t client_id,
                                  uint64_t seq_num) {
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
      if (nl == std::string::npos) {
        return false;
      }
      line = data.substr(cursor, nl - cursor);
      cursor = nl + 1;
      return true;
    };

    std::string line;
    if (!parse_line(pos, line)) {
      return std::nullopt;
    }
    int raw_type = -1;
    {
      std::istringstream iss(line);
      if (!(iss >> raw_type)) {
        return std::nullopt;
      }
    }
    OpType type;
    if (raw_type == 0) {
      type = OpType::Put;
    } else if (raw_type == 1) {
      type = OpType::Get;
    } else if (raw_type == 2) {
      type = OpType::Append;
    } else {
      return std::nullopt;
    }

    if (!parse_line(pos, line)) {
      return std::nullopt;
    }
    uint64_t client_id = 0;
    {
      std::istringstream iss(line);
      if (!(iss >> client_id)) {
        return std::nullopt;
      }
    }

    if (!parse_line(pos, line)) {
      return std::nullopt;
    }
    uint64_t seq_num = 0;
    {
      std::istringstream iss(line);
      if (!(iss >> seq_num)) {
        return std::nullopt;
      }
    }

    if (!parse_line(pos, line)) {
      return std::nullopt;
    }
    size_t key_len = 0;
    {
      std::istringstream iss(line);
      if (!(iss >> key_len)) {
        return std::nullopt;
      }
    }

    if (!parse_line(pos, line)) {
      return std::nullopt;
    }
    size_t value_len = 0;
    {
      std::istringstream iss(line);
      if (!(iss >> value_len)) {
        return std::nullopt;
      }
    }

    if (pos + key_len + value_len > data.size()) {
      return std::nullopt;
    }

    return Op{
        .type = type,
        .client_id = client_id,
        .seq_num = seq_num,
        .key = data.substr(pos, key_len),
        .value = data.substr(pos + key_len, value_len),
    };
  }

  std::optional<ClientCache> check_cached_duplicate(uint64_t client_id,
                                                    uint64_t seq_num) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = dedup_.find(client_id);
    if (it != dedup_.end() && seq_num <= it->second.seq_num) {
      return it->second;
    }
    return std::nullopt;
  }

  ApplyOutcome wait_for_apply(uint64_t index, const std::string &command) {
    std::unique_lock<std::mutex> lk(mu_);

    auto applied_it = applied_by_index_.find(index);
    if (applied_it != applied_by_index_.end()) {
      if (applied_it->second.command == command) {
        return ApplyOutcome{
            .status = applied_it->second.status,
            .value = applied_it->second.value,
        };
      }
      return ApplyOutcome{.status = kvpb::KV_TIMEOUT, .value = ""};
    }

    auto &pending = pending_by_index_[index];
    pending.expected_command = command;
    pending.finished = false;
    pending.matched = false;
    pending.status = kvpb::KV_TIMEOUT;
    pending.value.clear();

    const auto deadline = std::chrono::steady_clock::now() + kRpcWaitTimeout;
    while (!pending.finished) {
      if (cv_.wait_until(lk, deadline) == std::cv_status::timeout) {
        pending_by_index_.erase(index);
        return ApplyOutcome{.status = kvpb::KV_TIMEOUT, .value = ""};
      }
    }

    ApplyOutcome out;
    if (!pending.matched) {
      out.status = kvpb::KV_TIMEOUT;
      out.value = "";
    } else {
      out.status = pending.status;
      out.value = pending.value;
    }
    pending_by_index_.erase(index);
    return out;
  }

  rafty::Raft &raft_;
  std::mutex mu_;
  std::condition_variable cv_;

  std::unordered_map<std::string, std::string> store_;
  std::unordered_map<uint64_t, ClientCache> dedup_;
  std::unordered_map<uint64_t, Pending> pending_by_index_;
  std::unordered_map<uint64_t, AppliedEntry> applied_by_index_;
  std::deque<uint64_t> applied_order_;

  static constexpr std::chrono::milliseconds kRpcWaitTimeout{4500};
  static constexpr size_t kAppliedHistoryLimit = 2048;
};

} // namespace kv
