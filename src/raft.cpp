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

            Status AppendEntries(ServerContext * /*context*/, const raftpb::AppendEntriesRequest * /*request*/,
                                 raftpb::AppendEntriesReply * /*reply*/) override {
                (void)this->raft_;
                return Status::OK;
            }

            Status RequestVote(ServerContext * /*context*/, const raftpb::RequestVoteRequest * /*request*/,
                               raftpb::RequestVoteReply * /*reply*/) override {
                (void)this->raft_;
                return Status::OK;
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
        // TODO: lab 1
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
        this->reset_election_deadline_locked();
        logger->info("Node {} started election for term {}", this->id, this->current_term_);

        // Part B only: bootstrap single-node cluster leadership.
        if(this->peer_addrs.empty()) { this->become_leader_locked(); }
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
            }

            if(should_send_heartbeat) { this->send_heartbeats_once(); }

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

} // namespace rafty
