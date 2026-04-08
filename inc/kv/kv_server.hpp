#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "kv.grpc.pb.h"
#include "rafty/raft.hpp"

namespace kv {

class KvServer : public kvpb::KvService::Service {
public:
  explicit KvServer(rafty::Raft &raft) : raft_(raft) {
    // TODO (lab 3): initialize your data structures here.
    // You may want to start background threads, set up condition variables, etc.
  }

  ~KvServer() {
    // TODO (lab 3): clean up any background threads.
  }

  // -----------------------------------------------------------------
  // on_apply is called by the node wrapper each time Raft commits a
  // log entry. The ApplyResult contains the index and data of the
  // committed entry (the same string you passed to raft_.propose()).
  //
  // You should:
  //   1. Deserialize the operation from result.data
  //   2. Apply it to your in-memory key/value map
  //   3. Handle RIFL duplicate detection
  //   4. Notify the waiting RPC handler that its operation has committed
  // -----------------------------------------------------------------
  void on_apply(const rafty::ApplyResult &result) {
    // TODO (lab 3): implement this.
    (void)result;
  }

  // -----------------------------------------------------------------
  // gRPC handlers for client operations.
  //
  // Each handler should:
  //   1. Check if this node is the Raft leader (if not, return KV_NOTLEADER)
  //   2. Serialize the operation into a string
  //   3. Call raft_.propose(serialized_op) to submit to Raft
  //   4. Wait for on_apply() to process the committed entry at the
  //      returned index
  //   5. Return the result to the client
  //
  // Use RIFL (client_id + seq_num) to detect and handle duplicate
  // requests, just like Lab 0b.
  // -----------------------------------------------------------------

  grpc::Status Put(grpc::ServerContext *context,
                   const kvpb::PutRequest *request,
                   kvpb::KvResponse *response) override {
    // TODO (lab 3): implement
    (void)context;
    (void)request;
    response->set_status(kvpb::KV_TIMEOUT);
    return grpc::Status::OK;
  }

  grpc::Status Get(grpc::ServerContext *context,
                   const kvpb::GetRequest *request,
                   kvpb::GetResponse *response) override {
    // TODO (lab 3): implement
    (void)context;
    (void)request;
    response->set_status(kvpb::KV_TIMEOUT);
    return grpc::Status::OK;
  }

  grpc::Status Append(grpc::ServerContext *context,
                      const kvpb::AppendRequest *request,
                      kvpb::KvResponse *response) override {
    // TODO (lab 3): implement
    (void)context;
    (void)request;
    response->set_status(kvpb::KV_TIMEOUT);
    return grpc::Status::OK;
  }

private:
  rafty::Raft &raft_;

  // TODO (lab 3): add your state here. Consider:
  //
  // - std::unordered_map<std::string, std::string> store_;
  //       The in-memory key/value map.
  //
  // - RIFL tables: track (client_id -> highest seq_num) and cache the
  //   response for the last operation per client, so that duplicate
  //   requests return the cached result instead of re-executing.
  //
  // - A notification mechanism (e.g., std::condition_variable or
  //   std::promise/std::future per pending request) so that RPC handler
  //   threads can wait for their specific log entry to be committed
  //   and applied via on_apply().
  //
  // - std::mutex for protecting shared state.
};

} // namespace kv
