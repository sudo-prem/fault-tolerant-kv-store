#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"
#include "toolings/msg_queue.hpp"

// it will pick up correct header
// when you generate the grpc proto files
#include "raft.grpc.pb.h"

using namespace toolings;

namespace rafty {
    using RaftServiceStub = std::unique_ptr<raftpb::RaftService::Stub>;
    using grpc::Server;

    class Raft {
    public:
        Raft(const Config &config, MessageQueue<ApplyResult> &ready);
        ~Raft();

        // WARN: do not modify the signature
        // TODO: implement `run`, `propose` and `get_state`
        void run();                                      /* lab 1 */
        ProposalResult propose(const std::string &data); /* lab 1 */
        State get_state() const;                         /* lab 2 */

        // lab3: sync propose
        ProposalResult propose_sync(const std::string &data);

        // WARN: do not modify the signature
        void start_server();
        void stop_server();
        void connect_peers();
        bool is_dead() const;
        void kill();

    private:
        // WARN: do not modify `create_context` and `apply`.

        // invoke `create_context` when creating context for rpc call.
        // args: the id of which raft instance the RPC will go to.
        std::unique_ptr<grpc::ClientContext> create_context(uint64_t to) const;
        void apply(const ApplyResult &result);

    protected:
        // WARN: do not modify `mtx` and `logger`.
        mutable std::mutex mtx;
        std::unique_ptr<rafty::utils::logger> logger;

    private:
        // WARN: do not modify the declaration of
        // `id`, `listening_addr`, `peer_addrs`,
        // `dead`, `ready_queue`, `peers_`, and `server_`.
        uint64_t id;
        std::string listening_addr;
        std::map<uint64_t, std::string> peer_addrs;

        std::atomic<bool> dead;
        MessageQueue<ApplyResult> &ready_queue;

        std::unordered_map<uint64_t, RaftServiceStub> peers_;
        std::unique_ptr<raftpb::RaftService::Service> service_;
        std::unique_ptr<Server> server_;
    };
} // namespace rafty

#include "rafty/impl/raft.ipp" // IWYU pragma: keep
