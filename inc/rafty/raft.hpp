#pragma once

#include <cstdint>
#include <functional>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
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
        grpc::Status handle_append_entries_rpc(const raftpb::AppendEntriesRequest *request, raftpb::AppendEntriesReply *reply);
        grpc::Status handle_request_vote_rpc(const raftpb::RequestVoteRequest *request, raftpb::RequestVoteReply *reply);

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
        enum class Role {
            Follower,
            Candidate,
            Leader,
        };

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

        std::atomic<bool> started_{ false };
        std::thread ticker_;

        Role role_ = Role::Follower;
        uint64_t current_term_ = 0;
        std::optional<uint64_t> voted_for_ = std::nullopt;

        std::chrono::milliseconds heartbeat_interval_{ 120 };
        std::chrono::milliseconds election_timeout_min_{ 450 };
        std::chrono::milliseconds election_timeout_max_{ 900 };

        std::chrono::steady_clock::time_point next_heartbeat_at_{};
        std::chrono::steady_clock::time_point election_deadline_{};
        uint64_t votes_granted_in_term_ = 0;
        bool election_needs_vote_requests_ = false;
        uint64_t pending_vote_request_term_ = 0;

        std::chrono::milliseconds random_election_timeout() const;
        void reset_election_deadline_locked();
        void start_election_locked();
        void become_leader_locked();
        void become_follower_locked(uint64_t new_term);
        void send_request_votes_once(uint64_t term);
        void send_heartbeats_once();
        void ticker_loop();
    };
} // namespace rafty

#include "rafty/impl/raft.ipp" // IWYU pragma: keep
