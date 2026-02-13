#include "common/utils/rand_gen.hpp"
#include "rafty/raft.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif

namespace rafty {
    using grpc::ServerBuilder;
    using grpc::ServerContext;
    using grpc::experimental::ClientInterceptorFactoryInterface;
    using grpc::experimental::CreateCustomChannelWithInterceptors;

    Raft::Raft(const Config &config, MessageQueue<ApplyResult> &ready)
        : logger(utils::logger::get_logger(config.id)), id(config.id), listening_addr(config.addr),
          peer_addrs(config.peer_addrs), dead(false), ready_queue(ready) {
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
