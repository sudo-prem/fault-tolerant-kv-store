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
        const std::chrono::milliseconds kReadLeaseDuration{ 180 };
    }

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
        this->log_.push_back(LogEntry{ .index = 0, .term = 0, .data = "" });
        this->commit_index_ = 0;
        this->last_applied_ = 0;
        this->next_index_.clear();
        this->match_index_.clear();
        this->last_append_success_at_.clear();
        this->replication_epoch_ = 0;

        this->next_heartbeat_at_ = std::chrono::steady_clock::now() + this->heartbeat_interval_;
        this->reset_election_deadline_locked();
    }

    Raft::~Raft() {
        this->dead.store(true);
        this->ticker_cv_.notify_all();
        this->replication_cv_.notify_all();
        if(this->ticker_.joinable()) { this->ticker_.join(); }
        for(auto &worker : this->replication_workers_) {
            if(worker.joinable()) { worker.join(); }
        }
        this->stop_server();
    }

    void Raft::run() {
        if(this->dead.load()) { return; }
        if(this->started_.exchange(true)) { return; }

        logger->info("Raft node {} starting main loop", this->id);
        this->replication_workers_.reserve(this->peer_addrs.size());
        for(const auto &[peer_id, _] : this->peer_addrs) {
            this->replication_workers_.emplace_back([this, peer_id] { this->replication_loop(peer_id); });
        }
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

        if(!leader) { return result; }

        const uint64_t index = this->last_log_index_locked() + 1;
        const uint64_t term = this->current_term_;
        this->log_.push_back(LogEntry{ .index = index, .term = term, .data = data });

        this->notify_replication_locked();

        result.index = index;
        result.term = term;

        // Single node optimization: commit immediately
        if(this->quorum_size() == 1 && this->commit_index_ < index && index < this->log_.size()
           && this->log_[index].term == this->current_term_) {
            this->commit_index_ = index;
            auto applies = this->collect_newly_committed_applies_locked();
            lk.unlock();
            for(const auto &a : applies) { this->apply(a); }
            return result;
        }

        return result;
    }

    ProposalResult Raft::propose_sync(const std::string &data) {
        auto proposed = this->propose(data);
        if(!proposed.is_leader || proposed.index == 0) { return proposed; }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(4500);
        while(!this->dead.load()) {
            {
                std::lock_guard<std::mutex> lk(this->mtx);

                // Entry disappeared or was overwritten before commit.
                if(proposed.index >= this->log_.size() || this->log_[proposed.index].term != proposed.term) {
                    return ProposalResult{
                        .index = 0,
                        .term = this->current_term_,
                        .is_leader = false,
                    };
                }

                if(this->commit_index_ >= proposed.index) { return proposed; }
            }

            if(std::chrono::steady_clock::now() >= deadline) { break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }

        std::lock_guard<std::mutex> lk(this->mtx);
        return ProposalResult{
            .index = 0,
            .term = this->current_term_,
            .is_leader = false,
        };
    }

    bool Raft::confirm_leadership(std::chrono::milliseconds timeout) {
        std::vector<uint64_t> peers;
        std::vector<uint64_t> successful_contacts;
        uint64_t term = 0;
        {
            std::lock_guard<std::mutex> lk(this->mtx);
            if(this->role_ != Role::Leader || this->dead.load()) { return false; }
            term = this->current_term_;
            peers.reserve(this->peer_addrs.size());
            for(const auto &[peer_id, _] : this->peer_addrs) { peers.push_back(peer_id); }
        }

        uint64_t grants = 1; // self vote
        const uint64_t quorum = this->quorum_size();
        if(grants >= quorum) { return true; }

        const auto deadline = std::chrono::steady_clock::now() + timeout;
        for(const auto peer_id : peers) {
            if(grants >= quorum) { break; }

            const auto now = std::chrono::steady_clock::now();
            if(now >= deadline) { break; }

            auto stub_it = this->peers_.find(peer_id);
            if(stub_it == this->peers_.end()) { continue; }

            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            const auto rpc_timeout = std::min<std::chrono::milliseconds>(remaining, std::chrono::milliseconds(30));
            if(rpc_timeout <= std::chrono::milliseconds::zero()) { break; }

            raftpb::AppendEntriesRequest req;
            req.set_term(term);
            req.set_leader_id(this->id);
            req.set_prev_log_index(0);
            req.set_prev_log_term(0);
            req.set_leader_commit(0);

            raftpb::AppendEntriesReply reply;
            auto context = this->create_context(peer_id);
            context->set_deadline(std::chrono::system_clock::now() + rpc_timeout);
            grpc::Status status = stub_it->second->AppendEntries(&*context, req, &reply);
            if(!status.ok()) { continue; }

            if(reply.term() > term) {
                std::lock_guard<std::mutex> lk(this->mtx);
                if(reply.term() > this->current_term_) { this->become_follower_locked(reply.term()); }
                return false;
            }

            if(reply.success()) {
                grants += 1;
                successful_contacts.push_back(peer_id);
            }
        }

        if(grants < quorum) { return false; }

        std::lock_guard<std::mutex> lk(this->mtx);
        if(this->role_ != Role::Leader || this->current_term_ != term || this->dead.load()) { return false; }

        const auto now = std::chrono::steady_clock::now();
        for(const auto peer_id : successful_contacts) { this->last_append_success_at_[peer_id] = now; }
        return true;
    }

    bool Raft::has_committed_current_term_entry() const {
        std::lock_guard<std::mutex> lk(this->mtx);
        return this->role_ == Role::Leader && !this->dead.load() && this->commit_index_ > 0
               && this->commit_index_ < this->log_.size()
               && this->log_[this->commit_index_].term == this->current_term_;
    }

    std::optional<uint64_t> Raft::linearizable_read_index(std::chrono::milliseconds timeout) {
        {
            std::lock_guard<std::mutex> lk(this->mtx);
            if(this->role_ != Role::Leader || this->dead.load()) { return std::nullopt; }
            if(this->commit_index_ == 0 || this->commit_index_ >= this->log_.size()) { return std::nullopt; }
            if(this->log_[this->commit_index_].term != this->current_term_) { return std::nullopt; }
            if(this->has_quorum_recent_contact_locked(std::chrono::steady_clock::now(), kReadLeaseDuration)) {
                return this->commit_index_;
            }
        }

        if(!this->confirm_leadership(timeout)) { return std::nullopt; }

        std::lock_guard<std::mutex> lk(this->mtx);
        if(this->role_ != Role::Leader || this->dead.load()) { return std::nullopt; }
        if(this->commit_index_ == 0 || this->commit_index_ >= this->log_.size()) { return std::nullopt; }
        if(this->log_[this->commit_index_].term != this->current_term_) { return std::nullopt; }
        return this->commit_index_;
    }

    uint64_t Raft::last_log_index_locked() const {
        if(this->log_.empty()) { return 0; }
        return this->log_.back().index;
    }

    uint64_t Raft::last_log_term_locked() const {
        if(this->log_.empty()) { return 0; }
        return this->log_.back().term;
    }

    bool Raft::has_quorum_recent_contact_locked(std::chrono::steady_clock::time_point now,
                                                std::chrono::milliseconds lease_duration) const {
        uint64_t grants = 1; // leader itself
        const uint64_t quorum = this->quorum_size();
        if(grants >= quorum) { return true; }

        for(const auto &[peer_id, _] : this->peer_addrs) {
            auto it = this->last_append_success_at_.find(peer_id);
            if(it == this->last_append_success_at_.end()) { continue; }
            if(now - it->second <= lease_duration) {
                grants += 1;
                if(grants >= quorum) { return true; }
            }
        }
        return false;
    }

    void Raft::notify_replication_locked() {
        this->replication_epoch_ += 1;
        this->replication_cv_.notify_all();
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
        if(q == 0 || matches.empty() || q > matches.size()) { return 0; }
        return matches[matches.size() - static_cast<size_t>(q)];
    }

    std::vector<ApplyResult> Raft::collect_newly_committed_applies_locked() {
        std::vector<ApplyResult> out;
        while(this->last_applied_ < this->commit_index_) {
            this->last_applied_ += 1;
            if(this->last_applied_ >= this->log_.size()) { break; }
            const auto &e = this->log_[this->last_applied_];
            out.push_back(ApplyResult{ .valid = true, .data = e.data, .index = e.index });
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
        this->last_append_success_at_.clear();
        this->reset_election_deadline_locked();
    }

    void Raft::become_leader_locked() {
        this->role_ = Role::Leader;
        this->next_heartbeat_at_ = std::chrono::steady_clock::now() + this->heartbeat_interval_;

        this->next_index_.clear();
        this->match_index_.clear();
        this->last_append_success_at_.clear();
        const uint64_t next = this->last_log_index_locked() + 1;
        for(const auto &[peer_id, _] : this->peer_addrs) {
            this->next_index_[peer_id] = next;
            this->match_index_[peer_id] = 0;
        }

        this->notify_replication_locked();

        logger->info("Node {} became leader at term {}", this->id, this->current_term_);
    }

    void Raft::start_election_locked() {
        this->role_ = Role::Candidate;
        this->current_term_ += 1;
        this->voted_for_ = this->id;
        this->votes_granted_in_term_ = 1;
        this->last_append_success_at_.clear();
        this->reset_election_deadline_locked();
        logger->info("Node {} started election for term {}", this->id, this->current_term_);

        this->pending_vote_request_term_ = this->current_term_;
        this->election_needs_vote_requests_ = true;

        if(this->votes_granted_in_term_ >= this->quorum_size()) {
            this->become_leader_locked();
            this->election_needs_vote_requests_ = false;
        }
    }

    grpc::Status
    Raft::handle_append_entries_rpc(const raftpb::AppendEntriesRequest *request, raftpb::AppendEntriesReply *reply) {
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
            if(this->role_ != Role::Follower) { this->role_ = Role::Follower; }
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
                if(this->log_[idx].term != in_term) { this->log_.resize(idx); }
            }

            if(idx == this->log_.size()) {
                this->log_.push_back(LogEntry{ .index = idx, .term = in_term, .data = in_data });
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
        for(const auto &a : applies) { this->apply(a); }
        return grpc::Status::OK;
    }

    grpc::Status
    Raft::handle_request_vote_rpc(const raftpb::RequestVoteRequest *request, raftpb::RequestVoteReply *reply) {
        std::lock_guard<std::mutex> lk(this->mtx);
        const uint64_t req_term = request->term();

        if(req_term < this->current_term_) {
            reply->set_term(this->current_term_);
            reply->set_vote_granted(false);
            return grpc::Status::OK;
        }

        if(req_term > this->current_term_) { this->become_follower_locked(req_term); }

        const uint64_t local_last_log_term = this->last_log_term_locked();
        const uint64_t local_last_log_index = this->last_log_index_locked();
        const bool candidate_up_to_date
            = (request->last_log_term() > local_last_log_term)
              || (request->last_log_term() == local_last_log_term && request->last_log_index() >= local_last_log_index);

        const bool can_vote_for_candidate
            = !this->voted_for_.has_value() || this->voted_for_.value() == request->candidate_id();
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
            if(stub_it == this->peers_.end()) { continue; }

            raftpb::RequestVoteReply reply;
            auto context = this->create_context(peer_id);
            context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));
            grpc::Status status = stub_it->second->RequestVote(&*context, req, &reply);
            if(!status.ok()) { continue; }

            std::lock_guard<std::mutex> lk(this->mtx);
            if(reply.term() > this->current_term_) {
                this->become_follower_locked(reply.term());
                this->election_needs_vote_requests_ = false;
                continue;
            }

            // Ignore stale replies from prior terms or after role changes
            if(this->role_ != Role::Candidate || this->current_term_ != term) { continue; }

            if(reply.vote_granted()) {
                this->votes_granted_in_term_ += 1;
                if(this->votes_granted_in_term_ >= this->quorum_size()) {
                    this->become_leader_locked();
                    this->election_needs_vote_requests_ = false;
                    break;
                }
            }
        }
    }

    void Raft::replication_loop(uint64_t peer_id) {
        struct AppendPlan {
            raftpb::RaftService::Stub *stub = nullptr;
            uint64_t term = 0;
            uint64_t leader_commit = 0;
            uint64_t prev_log_index = 0;
            uint64_t prev_log_term = 0;
            std::vector<LogEntry> entries;
        };

        constexpr size_t kMaxEntriesPerAppend = 64;

        raftpb::RaftService::Stub *stub = nullptr;
        {
            auto stub_it = this->peers_.find(peer_id);
            if(stub_it == this->peers_.end()) { return; }
            stub = stub_it->second.get();
        }

        uint64_t seen_epoch = 0;
        while(!this->dead.load()) {
            {
                std::unique_lock<std::mutex> lk(this->mtx);
                this->replication_cv_.wait(lk, [&] {
                    return this->dead.load() || (this->role_ == Role::Leader && this->replication_epoch_ != seen_epoch);
                });
                if(this->dead.load()) { return; }
                seen_epoch = this->replication_epoch_;
            }

            while(!this->dead.load()) {
                AppendPlan plan;
                {
                    std::lock_guard<std::mutex> lk(this->mtx);
                    if(this->role_ != Role::Leader || this->dead.load()) { break; }

                    const uint64_t last_index = this->last_log_index_locked();
                    auto it = this->next_index_.find(peer_id);
                    uint64_t next_index = (it == this->next_index_.end()) ? (last_index + 1) : it->second;
                    if(next_index < 1) { next_index = 1; }
                    if(next_index > last_index + 1) { next_index = last_index + 1; }

                    const uint64_t prev_index = next_index - 1;
                    const uint64_t prev_term = (prev_index < this->log_.size()) ? this->log_[prev_index].term : 0;

                    plan.stub = stub;
                    plan.term = this->current_term_;
                    plan.leader_commit = this->commit_index_;
                    plan.prev_log_index = prev_index;
                    plan.prev_log_term = prev_term;

                    if(next_index <= last_index) {
                        const uint64_t end = std::min<uint64_t>(
                            last_index, next_index + static_cast<uint64_t>(kMaxEntriesPerAppend) - 1);
                        plan.entries.reserve(static_cast<size_t>(end - next_index + 1));
                        for(uint64_t idx = next_index; idx <= end; ++idx) {
                            if(idx < this->log_.size()) { plan.entries.push_back(this->log_[idx]); }
                        }
                    }
                }

                raftpb::AppendEntriesRequest req;
                req.set_term(plan.term);
                req.set_leader_id(this->id);
                req.set_prev_log_index(plan.prev_log_index);
                req.set_prev_log_term(plan.prev_log_term);
                req.set_leader_commit(plan.leader_commit);
                for(const auto &entry : plan.entries) { *req.add_entries() = this->to_proto_entry(entry); }

                raftpb::AppendEntriesReply reply;
                auto context = this->create_context(peer_id);
                context->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));
                grpc::Status status = plan.stub->AppendEntries(&*context, req, &reply);

                std::vector<ApplyResult> applies;
                bool retry_now = false;
                {
                    std::lock_guard<std::mutex> lk(this->mtx);
                    if(this->dead.load()) { return; }

                    if(status.ok() && reply.term() > plan.term) {
                        if(reply.term() > this->current_term_) { this->become_follower_locked(reply.term()); }
                        break;
                    }

                    if(this->role_ != Role::Leader || this->current_term_ != plan.term) { break; }

                    if(!status.ok()) { break; }

                    if(!reply.success()) {
                        const uint64_t cur = this->next_index_.contains(peer_id) ? this->next_index_[peer_id]
                                                                                 : (plan.prev_log_index + 1);
                        const uint64_t step = std::max<uint64_t>(1, (cur - 1) / 2);
                        this->next_index_[peer_id] = std::max<uint64_t>(1, cur - step);
                        retry_now = true;
                    } else {
                        this->last_append_success_at_[peer_id] = std::chrono::steady_clock::now();

                        const uint64_t advanced = plan.prev_log_index + static_cast<uint64_t>(plan.entries.size());
                        auto cur_match = this->match_index_.find(peer_id);
                        uint64_t prev_match = (cur_match == this->match_index_.end()) ? 0 : cur_match->second;
                        const uint64_t new_match = std::max(prev_match, advanced);
                        this->match_index_[peer_id] = new_match;
                        this->next_index_[peer_id] = new_match + 1;

                        const uint64_t candidate = this->majority_match_index_locked();
                        if(candidate > this->commit_index_ && candidate < this->log_.size()
                           && this->log_[candidate].term == this->current_term_) {
                            this->commit_index_ = candidate;
                            this->notify_replication_locked();
                        }

                        applies = this->collect_newly_committed_applies_locked();

                        const uint64_t last_index = this->last_log_index_locked();
                        retry_now = this->next_index_[peer_id] <= last_index || this->commit_index_ > plan.leader_commit
                                    || this->replication_epoch_ != seen_epoch;
                        seen_epoch = this->replication_epoch_;
                    }
                }

                for(const auto &apply_result : applies) { this->apply(apply_result); }
                if(!retry_now) { break; }
            }
        }
    }

    void Raft::ticker_loop() {
        while(!this->dead.load()) {
            bool should_request_votes = false;
            uint64_t vote_request_term = 0;
            {
                std::lock_guard<std::mutex> lk(this->mtx);
                const auto now = std::chrono::steady_clock::now();

                if(this->role_ == Role::Leader) {
                    if(now >= this->next_heartbeat_at_) {
                        this->next_heartbeat_at_ = now + this->heartbeat_interval_;
                        this->notify_replication_locked();
                    }
                } else if(now >= this->election_deadline_) {
                    this->start_election_locked();
                }

                if(this->election_needs_vote_requests_ && this->role_ == Role::Candidate
                   && this->pending_vote_request_term_ == this->current_term_) {
                    should_request_votes = true;
                    vote_request_term = this->current_term_;
                    this->election_needs_vote_requests_ = false;
                }
            }

            if(should_request_votes) { this->send_request_votes_once(vote_request_term); }

            std::unique_lock<std::mutex> lk(this->mtx);
            if(this->dead.load()) { break; }

            auto wake_at = this->role_ == Role::Leader ? this->next_heartbeat_at_ : this->election_deadline_;
            if(wake_at <= std::chrono::steady_clock::now()) { continue; }

            this->ticker_cv_.wait_until(lk, wake_at, [this] {
                return this->dead.load() || (this->election_needs_vote_requests_ && this->role_ == Role::Candidate);
            });
        }
    }

} // namespace rafty
