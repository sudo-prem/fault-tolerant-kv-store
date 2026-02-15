#include "common/utils/rand_gen.hpp"
#include "rafty/raft.hpp"
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
        // TODO: add more field if desired
        // TODO: finish it
    }

    Raft::~Raft() { this->stop_server(); }

    void Raft::run() {
        // TODO: kick off the raft instance
        // Note: this function should be non-blocking

        // lab 1
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

    // TODO: add more functions if desired.

} // namespace rafty
