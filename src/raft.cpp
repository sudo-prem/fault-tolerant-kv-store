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

        this->log_.clear();
        this->log_.push_back(LogEntry{.index = 0, .term = 0, .data = ""});
        this->commit_index_ = 0;
        this->last_applied_ = 0;
        this->next_index_.clear();
        this->match_index_.clear();

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
        std::unique_lock<std::mutex> lk(this->mtx);
        const bool leader = (this->role_ == Role::Leader) && !this->dead.load();
        ProposalResult result{
            .index = 0,
            .term = this->current_term_,
            .is_leader = leader,
        };

        if(!leader) {
            return result;
        }

        const uint64_t index = this->last_log_index_locked() + 1;
        const uint64_t term = this->current_term_;
        this->log_.push_back(LogEntry{.index = index, .term = term, .data = data});

        // Kick replication soon, do not wait for commit
        this->next_heartbeat_at_ = std::chrono::steady_clock::now();

        result.index = index;
        result.term = term;

        // Single node optimization: commit immediately
        if(this->quorum_size() == 1 && this->commit_index_ < index && index < this->log_.size() && this->log_[index].term == this->current_term_) {
            this->commit_index_ = index;
            auto applies = this->collect_newly_committed_applies_locked();
            lk.unlock();
            for(const auto &a : applies) {
                this->apply(a);
            }
            return result;
        }

        return result;
    }

    ProposalResult Raft::propose_sync(const std::string &data) {
        // TODO: lab 3
    }

    uint64_t Raft::last_log_index_locked() const {
        if(this->log_.empty()) {
            return 0;
        }
        return this->log_.back().index;
    }

    uint64_t Raft::last_log_term_locked() const {
        if(this->log_.empty()) {
            return 0;
        }
        return this->log_.back().term;
    }

    raftpb::Entry Raft::to_proto_entry(const LogEntry &e) const {
        raftpb::Entry out;
        out.set_index(e.index);
        out.set_term(e.term);
        out.set_command(e.data);
        return out;
    }

    uint64_t Raft::majority_match_index_locked() const {
        std::vector<uint64_t> matches;
        matches.reserve(this->peer_addrs.size() + 1);

        // leader itself always has its full log
        matches.push_back(this->last_log_index_locked());

        for(const auto &[peer_id, _] : this->peer_addrs) {
            auto it = this->match_index_.find(peer_id);
            matches.push_back(it == this->match_index_.end() ? 0 : it->second);
        }

        std::sort(matches.begin(), matches.end());
        const uint64_t q = this->quorum_size();
        if(q == 0 || matches.empty() || q > matches.size()) {
            return 0;
        }
        return matches[matches.size() - static_cast<size_t>(q)];
    }

    std::vector<ApplyResult> Raft::collect_newly_committed_applies_locked() {
        std::vector<ApplyResult> out;
        while(this->last_applied_ < this->commit_index_) {
            this->last_applied_ += 1;
            if(this->last_applied_ >= this->log_.size()) {
                break;
            }
            const auto &e = this->log_[this->last_applied_];
            out.push_back(ApplyResult{.valid = true, .data = e.data, .index = e.index});
        }
        return out;
    }

    std::chrono::milliseconds Raft::random_election_timeout() const {
        const auto min_ms = std::min(this->election_timeout_min_.count(), this->election_timeout_max_.count());
        const auto max_ms = std::max(this->election_timeout_min_.count(), this->election_timeout_max_.count());
        const auto width = static_cast<uint64_t>(max_ms - min_ms + 1);
        const auto jitter = static_cast<int64_t>(utils::RandGen::get_instance().intn(width));
        return std::chrono::milliseconds(min_ms + jitter);
    }

    uint64_t Raft::quorum_size() const {
        const uint64_t cluster_size = static_cast<uint64_t>(this->peer_addrs.size()) + 1;
        return (cluster_size / 2) + 1;
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

        this->next_index_.clear();
        this->match_index_.clear();
        const uint64_t next = this->last_log_index_locked() + 1;
        for(const auto &[peer_id, _] : this->peer_addrs) {
            this->next_index_[peer_id] = next;
            this->match_index_[peer_id] = 0;
        }

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

        if(this->votes_granted_in_term_ >= this->quorum_size()) {
            this->become_leader_locked();
            this->election_needs_vote_requests_ = false;
        }
    }

    grpc::Status Raft::handle_append_entries_rpc(const raftpb::AppendEntriesRequest *request,
                                                 raftpb::AppendEntriesReply *reply) {
        std::unique_lock<std::mutex> lk(this->mtx);
        const uint64_t req_term = request->term();

        if(req_term < this->current_term_) {
            reply->set_term(this->current_term_);
            reply->set_success(false);
            return grpc::Status::OK;
        }

        if(req_term > this->current_term_) {
            this->become_follower_locked(req_term);
        } else {
            if(this->role_ != Role::Follower) {
                this->role_ = Role::Follower;
            }
        }

        // Valid leader contact for current term; prevent election
        this->reset_election_deadline_locked();

        const uint64_t prev_index = request->prev_log_index();
        const uint64_t prev_term = request->prev_log_term();

        // Reply false if log doesn't contain an entry at prev_log_index whose term matches prev_log_term
        if(prev_index >= this->log_.size()) {
            reply->set_term(this->current_term_);
            reply->set_success(false);
            return grpc::Status::OK;
        }
        if(this->log_[prev_index].term != prev_term) {
            reply->set_term(this->current_term_);
            reply->set_success(false);
            return grpc::Status::OK;
        }

        // If an existing entry conflicts with a new onem same index but different term
        // delete the existing entry and all that follow it, then append new entries
        uint64_t idx = prev_index;
        for(const auto &incoming : request->entries()) {
            idx += 1;
            const uint64_t in_term = incoming.term();
            const std::string in_data = incoming.command();

            if(idx < this->log_.size()) {
                if(this->log_[idx].term != in_term) {
                    this->log_.resize(idx);
                }
            }

            if(idx == this->log_.size()) {
                this->log_.push_back(LogEntry{.index = idx, .term = in_term, .data = in_data});
            }
        }

        // Advance commit index
        if(request->leader_commit() > this->commit_index_) {
            this->commit_index_ = std::min<uint64_t>(request->leader_commit(), this->last_log_index_locked());
        }

        reply->set_term(this->current_term_);
        reply->set_success(true);

        auto applies = this->collect_newly_committed_applies_locked();
        lk.unlock();
        for(const auto &a : applies) {
            this->apply(a);
        }
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

        const uint64_t local_last_log_term = this->last_log_term_locked();
        const uint64_t local_last_log_index = this->last_log_index_locked();
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
        {
            std::lock_guard<std::mutex> lk(this->mtx);
            req.set_last_log_index(this->last_log_index_locked());
            req.set_last_log_term(this->last_log_term_locked());
        }

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

            // Ignore stale replies from prior terms or after role changes
            if(this->role_ != Role::Candidate || this->current_term_ != term) {
                continue;
            }

            if(reply.vote_granted()) {
                this->votes_granted_in_term_ += 1;
                if(this->votes_granted_in_term_ >= this->quorum_size()) {
                    this->become_leader_locked();
                    this->election_needs_vote_requests_ = false;
                }
            }
        }
    }

    void Raft::send_heartbeats_once() {
        struct AppendPlan {
            uint64_t peer_id;
            uint64_t term;
            uint64_t leader_commit;
            uint64_t prev_log_index;
            uint64_t prev_log_term;
            std::vector<LogEntry> entries;
        };

        constexpr size_t kMaxEntriesPerAppend = 1; // keep RPC size bounded

        for(const auto &[peer_id, _] : this->peer_addrs) {
            auto stub_it = this->peers_.find(peer_id);
            if(stub_it == this->peers_.end()) {
                continue;
            }

            AppendPlan plan;
            {
                std::lock_guard<std::mutex> lk(this->mtx);
                if(this->role_ != Role::Leader || this->dead.load()) {
                    return;
                }

                const uint64_t last_index = this->last_log_index_locked();
                auto it = this->next_index_.find(peer_id);
                uint64_t next_index = (it == this->next_index_.end()) ? (last_index + 1) : it->second;

                // Clamp next_index to a valid range
                if(next_index < 1) {
                    next_index = 1;
                }
                if(next_index > last_index + 1) {
                    next_index = last_index + 1;
                }

                const uint64_t prev_index = next_index - 1;
                const uint64_t prev_term = (prev_index < this->log_.size()) ? this->log_[prev_index].term : 0;

                plan.peer_id = peer_id;
                plan.term = this->current_term_;
                plan.leader_commit = this->commit_index_;
                plan.prev_log_index = prev_index;
                plan.prev_log_term = prev_term;

                if(next_index <= last_index) {
                    const uint64_t end = std::min<uint64_t>(last_index, next_index + static_cast<uint64_t>(kMaxEntriesPerAppend) - 1);
                    plan.entries.reserve(static_cast<size_t>(end - next_index + 1));
                    for(uint64_t idx = next_index; idx <= end; idx++) {
                        if(idx < this->log_.size()) {
                            plan.entries.push_back(this->log_[idx]);
                        }
                    }
                }
            }

            raftpb::AppendEntriesRequest req;
            req.set_term(plan.term);
            req.set_leader_id(this->id);
            req.set_prev_log_index(plan.prev_log_index);
            req.set_prev_log_term(plan.prev_log_term);
            req.set_leader_commit(plan.leader_commit);
            for(const auto &e : plan.entries) {
                *req.add_entries() = this->to_proto_entry(e);
            }

            raftpb::AppendEntriesReply reply;
            auto context = this->create_context(peer_id);
            context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));
            grpc::Status status = stub_it->second->AppendEntries(&*context, req, &reply);
            if(!status.ok()) {
                continue;
            }

            if(reply.term() > plan.term) {
                std::lock_guard<std::mutex> lk(this->mtx);
                if(reply.term() > this->current_term_) {
                    logger->info("Node {} stepping down due to higher term {}", this->id, reply.term());
                    this->become_follower_locked(reply.term());
                }
                continue;
            }

            if(!reply.success()) {
                std::lock_guard<std::mutex> lk(this->mtx);
                if(this->role_ == Role::Leader && this->current_term_ == plan.term) {
                    // Back up nextIndex and retry later.
                    this->next_index_[peer_id] = std::max<uint64_t>(1, plan.prev_log_index);
                }
                continue;
            }

            std::vector<ApplyResult> applies;
            {
                std::lock_guard<std::mutex> lk(this->mtx);
                if(this->role_ != Role::Leader || this->current_term_ != plan.term) {
                    continue;
                }

                // Update follower progress - works for both empty and non-empty appends
                const uint64_t advanced = plan.prev_log_index + static_cast<uint64_t>(plan.entries.size());
                auto cur_match = this->match_index_.find(peer_id);
                uint64_t prev_match = (cur_match == this->match_index_.end()) ? 0 : cur_match->second;
                const uint64_t new_match = std::max(prev_match, advanced);
                this->match_index_[peer_id] = new_match;
                this->next_index_[peer_id] = new_match + 1;

                // Leader commit rule: commit N if a majority have replicated it and log[N].term == currentTerm
                const uint64_t candidate = this->majority_match_index_locked();
                if(candidate > this->commit_index_ && candidate < this->log_.size() && this->log_[candidate].term == this->current_term_) {
                    this->commit_index_ = candidate;
                    // Propagate updated leaderCommit promptly
                    this->next_heartbeat_at_ = std::chrono::steady_clock::now();
                }

                applies = this->collect_newly_committed_applies_locked();
            }
            for(const auto &a : applies) {
                this->apply(a);
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
