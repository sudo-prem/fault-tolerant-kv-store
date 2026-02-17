#include "common/utils/rand_gen.hpp"
#include "rafty/raft.hpp"
#include <algorithm>
#include <chrono>
#include <thread>
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif

namespace rafty {
    using grpc::ServerBuilder;
    using grpc::ServerContext;
    using grpc::Status;
    using grpc::experimental::ClientInterceptorFactoryInterface;
    using grpc::experimental::CreateCustomChannelWithInterceptors;

    namespace {
        class RaftRpcHandler final : public raftpb::RaftService::Service {
        public:
            explicit RaftRpcHandler(Raft *raft) : raft_(raft) {}

            Status AppendEntries(ServerContext * /*context*/, const raftpb::AppendEntriesRequest *request,
                                 raftpb::AppendEntriesReply *reply) override {
                return this->raft_->handle_append_entries_rpc(request, reply);
            }

            Status RequestVote(ServerContext * /*context*/, const raftpb::RequestVoteRequest *request,
                               raftpb::RequestVoteReply *reply) override {
                return this->raft_->handle_request_vote_rpc(request, reply);
            }

        private:
            Raft *raft_;
        };
    } // namespace

    Raft::Raft(const Config &config, MessageQueue<ApplyResult> &ready)
        : logger(utils::logger::get_logger(config.id)), id(config.id), listening_addr(config.addr),
          peer_addrs(config.peer_addrs), dead(false), ready_queue(ready) {
        this->service_ = std::make_unique<RaftRpcHandler>(this);
        std::lock_guard<std::mutex> lk(this->mtx);
        this->role_ = Role::Follower;
        this->current_term_ = 0;
        this->voted_for_.reset();
        this->next_heartbeat_at_ = std::chrono::steady_clock::now() + this->heartbeat_interval_;
        this->reset_election_deadline_locked();
    }

    Raft::~Raft() {
        this->dead.store(true);
        if(this->ticker_.joinable()) { this->ticker_.join(); }
        this->stop_server();
    }

    void Raft::run() {
        if(this->dead.load()) { return; }
        if(this->started_.exchange(true)) { return; }

        logger->info("Raft node {} starting main loop", this->id);
        this->ticker_ = std::thread([this] { this->ticker_loop(); });
    }

    State Raft::get_state() const {
        std::lock_guard<std::mutex> lk(this->mtx);
        return State{
            .term = this->current_term_,
            .is_leader = this->role_ == Role::Leader,
        };
    }

    ProposalResult Raft::propose(const std::string &data) {
        // TODO: lab 2
    }

    ProposalResult Raft::propose_sync(const std::string &data) {
        // TODO: lab 3
    }

    std::chrono::milliseconds Raft::random_election_timeout() const {
        const auto min_ms = std::min(this->election_timeout_min_.count(), this->election_timeout_max_.count());
        const auto max_ms = std::max(this->election_timeout_min_.count(), this->election_timeout_max_.count());
        const auto width = static_cast<uint64_t>(max_ms - min_ms + 1);
        const auto jitter = static_cast<int64_t>(utils::RandGen::get_instance().intn(width));
        return std::chrono::milliseconds(min_ms + jitter);
    }

    void Raft::reset_election_deadline_locked() {
        this->election_deadline_ = std::chrono::steady_clock::now() + this->random_election_timeout();
    }

    void Raft::become_follower_locked(uint64_t new_term) {
        this->role_ = Role::Follower;
        this->current_term_ = std::max(this->current_term_, new_term);
        this->voted_for_.reset();
        this->reset_election_deadline_locked();
    }

    void Raft::become_leader_locked() {
        this->role_ = Role::Leader;
        this->next_heartbeat_at_ = std::chrono::steady_clock::now();
        logger->info("Node {} became leader at term {}", this->id, this->current_term_);
    }

    void Raft::start_election_locked() {
        this->role_ = Role::Candidate;
        this->current_term_ += 1;
        this->voted_for_ = this->id;
        this->votes_granted_in_term_ = 1;
        this->reset_election_deadline_locked();
        logger->info("Node {} started election for term {}", this->id, this->current_term_);

        this->pending_vote_request_term_ = this->current_term_;
        this->election_needs_vote_requests_ = true;

        if(this->votes_granted_in_term_ > this->peer_addrs.size() / 2) {
            this->become_leader_locked();
            this->election_needs_vote_requests_ = false;
        }
    }

    grpc::Status Raft::handle_append_entries_rpc(const raftpb::AppendEntriesRequest *request,
                                                 raftpb::AppendEntriesReply *reply) {
        std::lock_guard<std::mutex> lk(this->mtx);
        const uint64_t req_term = request->term();

        if(req_term < this->current_term_) {
            reply->set_term(this->current_term_);
            reply->set_success(false);
            return grpc::Status::OK;
        }

        if(req_term > this->current_term_) {
            this->become_follower_locked(req_term);
        } else {
            this->role_ = Role::Follower;
            this->reset_election_deadline_locked();
        }

        reply->set_term(this->current_term_);
        reply->set_success(true);
        return grpc::Status::OK;
    }

    grpc::Status Raft::handle_request_vote_rpc(const raftpb::RequestVoteRequest *request, raftpb::RequestVoteReply *reply) {
        std::lock_guard<std::mutex> lk(this->mtx);
        const uint64_t req_term = request->term();

        if(req_term < this->current_term_) {
            reply->set_term(this->current_term_);
            reply->set_vote_granted(false);
            return grpc::Status::OK;
        }

        if(req_term > this->current_term_) {
            this->become_follower_locked(req_term);
        }

        constexpr uint64_t local_last_log_term = 0;
        constexpr uint64_t local_last_log_index = 0;
        const bool candidate_up_to_date =
            (request->last_log_term() > local_last_log_term) ||
            (request->last_log_term() == local_last_log_term && request->last_log_index() >= local_last_log_index);

        const bool can_vote_for_candidate = !this->voted_for_.has_value() || this->voted_for_.value() == request->candidate_id();
        const bool grant_vote = can_vote_for_candidate && candidate_up_to_date;

        if(grant_vote) {
            this->voted_for_ = request->candidate_id();
            this->reset_election_deadline_locked();
        }

        reply->set_term(this->current_term_);
        reply->set_vote_granted(grant_vote);
        return grpc::Status::OK;
    }

    void Raft::send_request_votes_once(uint64_t term) {
        raftpb::RequestVoteRequest req;
        req.set_term(term);
        req.set_candidate_id(this->id);
        req.set_last_log_index(0);
        req.set_last_log_term(0);

        for(const auto &[peer_id, _] : this->peer_addrs) {
            auto stub_it = this->peers_.find(peer_id);
            if(stub_it == this->peers_.end()) {
                continue;
            }

            raftpb::RequestVoteReply reply;
            auto context = this->create_context(peer_id);
            context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));
            grpc::Status status = stub_it->second->RequestVote(&*context, req, &reply);
            if(!status.ok()) {
                continue;
            }

            std::lock_guard<std::mutex> lk(this->mtx);
            if(reply.term() > this->current_term_) {
                this->become_follower_locked(reply.term());
                this->election_needs_vote_requests_ = false;
                continue;
            }

            // Ignore stale replies from prior terms or after role changes.
            if(this->role_ != Role::Candidate || this->current_term_ != term) {
                continue;
            }

            if(reply.vote_granted()) {
                this->votes_granted_in_term_ += 1;
                if(this->votes_granted_in_term_ > this->peer_addrs.size() / 2) {
                    this->become_leader_locked();
                    this->election_needs_vote_requests_ = false;
                }
            }
        }
    }

    void Raft::send_heartbeats_once() {
        uint64_t term = 0;
        {
            std::lock_guard<std::mutex> lk(this->mtx);
            if(this->role_ != Role::Leader || this->dead.load()) { return; }
            term = this->current_term_;
        }

        for(const auto &[peer_id, _] : this->peer_addrs) {
            auto stub_it = this->peers_.find(peer_id);
            if(stub_it == this->peers_.end()) { continue; }

            raftpb::AppendEntriesRequest req;
            req.set_term(term);
            req.set_leader_id(this->id);
            req.set_prev_log_index(0);
            req.set_prev_log_term(0);
            req.set_leader_commit(0);

            raftpb::AppendEntriesReply reply;
            auto context = this->create_context(peer_id);
            grpc::Status status = stub_it->second->AppendEntries(&*context, req, &reply);
            if(!status.ok()) { continue; }

            if(reply.term() > term) {
                std::lock_guard<std::mutex> lk(this->mtx);
                if(reply.term() > this->current_term_) {
                    logger->info("Node {} stepping down due to higher term {}", this->id, reply.term());
                    this->become_follower_locked(reply.term());
                }
            }
        }
    }

    void Raft::ticker_loop() {
        while(!this->dead.load()) {
            bool should_send_heartbeat = false;
            bool should_request_votes = false;
            uint64_t vote_request_term = 0;
            {
                std::lock_guard<std::mutex> lk(this->mtx);
                const auto now = std::chrono::steady_clock::now();

                if(this->role_ == Role::Leader) {
                    if(now >= this->next_heartbeat_at_) {
                        should_send_heartbeat = true;
                        this->next_heartbeat_at_ = now + this->heartbeat_interval_;
                    }
                } else if(now >= this->election_deadline_) {
                    this->start_election_locked();
                }

                if(this->election_needs_vote_requests_ && this->role_ == Role::Candidate &&
                   this->pending_vote_request_term_ == this->current_term_) {
                    should_request_votes = true;
                    vote_request_term = this->current_term_;
                    this->election_needs_vote_requests_ = false;
                }
            }

            if(should_send_heartbeat) { this->send_heartbeats_once(); }
            if(should_request_votes) { this->send_request_votes_once(vote_request_term); }

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

} // namespace rafty
