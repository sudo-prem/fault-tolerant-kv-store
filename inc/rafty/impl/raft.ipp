#pragma once

#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "common/utils/net_intercepter.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif
#include "rafty/raft.hpp"

namespace rafty {
    using grpc::ServerBuilder;
    using grpc::ServerContext;
    using grpc::experimental::ClientInterceptorFactoryInterface;
    using grpc::experimental::CreateCustomChannelWithInterceptors;

    inline void Raft::start_server() {
        grpc::EnableDefaultHealthCheckService(false);
        grpc::reflection::InitProtoReflectionServerBuilderPlugin();

        ServerBuilder builder;
        builder.AddListeningPort(listening_addr, grpc::InsecureServerCredentials());

#ifdef TRACING
        builder.experimental().SetInterceptorCreators(tracing::CreateServerTracingInterceptors());
#endif

        builder.RegisterService(service_.get());

        auto server = builder.BuildAndStart();
        logger->info("Raft server {} listening on {}", id, listening_addr);

        server_ = std::move(server);

        std::thread([this] { server_->Wait(); }).detach();
    }

    inline void Raft::stop_server() {
        if(server_) { server_->Shutdown(); }
    }

    inline void Raft::connect_peers() {
        grpc::ChannelArguments args;
        // Set the maximum backoff time for reconnection attempts (e.g., 200ms)
        args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 200); // 1 second max backoff
        // Set the minimum backoff time for reconnection attempts (e.g., 50ms)
        args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 50); // 100ms min backoff
        // Set the initial backoff time for reconnection attempts (e.g., 50ms)
        args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS,
                    50); // 100ms initial backoff

        for(const auto &peer_addr : peer_addrs) {
            logger->info("Connecting to peer {} at {}", peer_addr.first, peer_addr.second);
            std::vector<std::unique_ptr<ClientInterceptorFactoryInterface>> interceptor_creators;
            interceptor_creators.emplace_back(std::make_unique<ByteCountingInterceptorFactory>());
            interceptor_creators.emplace_back(std::make_unique<NetInterceptorFactory>());
#ifdef TRACING
            interceptor_creators.push_back(std::make_unique<tracing::TracingClientInterceptorFactory>());
#endif
            auto channel = CreateCustomChannelWithInterceptors(peer_addr.second, grpc::InsecureChannelCredentials(),
                                                               args, std::move(interceptor_creators));
            auto stub = raftpb::RaftService::NewStub(std::move(channel));
            peers_[peer_addr.first] = std::move(stub);
        }
    }

    inline bool Raft::is_dead() const { return dead.load(); }

    inline void Raft::kill() {
        dead.store(true);
        ticker_cv_.notify_all();
        replication_cv_.notify_all();
    }

    inline std::unique_ptr<grpc::ClientContext> Raft::create_context(uint64_t to) const {
        auto context = std::make_unique<grpc::ClientContext>();
        context->AddMetadata("from", std::to_string(id));
        context->AddMetadata("to", std::to_string(to));
        return context;
    }

    inline void Raft::apply(const ApplyResult &result) { ready_queue.enqueue(result); }

} // namespace rafty
