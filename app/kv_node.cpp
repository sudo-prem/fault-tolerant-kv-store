#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <csignal>
#include <poll.h>
#include <unistd.h>

#include <ddb/integration.hpp>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"

#include "common/config.hpp"
#include "common/logger.hpp"
#include "common/utils/net_intercepter.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif
#include "toolings/msg_queue.hpp"
#include "toolings/raft_wrapper.hpp"

#include "kv/kv_server.hpp"

// KV port = Raft port + KV_PORT_OFFSET
static constexpr uint64_t KV_PORT_OFFSET = 1000;

// ---------------------------------------------------------------------------
// KvNodeWrapper extends RaftWrapper to add a KV gRPC service.
//
// Architecture:
//   - Raft gRPC server on port P       (inter-node consensus RPCs)
//   - Tester gRPC server on tester port (test infrastructure control)
//   - KV gRPC server on port P+1000    (client-facing Put/Get/Append)
//
// When Raft commits a log entry, the Apply handler (overridden here)
// forwards it to KvServer::on_apply() AND streams it to the tester.
// ---------------------------------------------------------------------------
class KvNodeWrapper : public toolings::RaftWrapper {
public:
  KvNodeWrapper(const std::string &ctrl_addr, uint64_t t_node_port,
                uint64_t kv_port, const rafty::Config &config,
                toolings::MessageQueue<rafty::ApplyResult> &ready)
      : toolings::RaftWrapper(ctrl_addr, t_node_port, config, ready),
        kv_ready_(ready), node_id_(config.id) {
    kv_server_ = std::make_unique<kv::KvServer>(
        static_cast<rafty::Raft &>(*this));
    start_kv_server(kv_port);
  }

  KvNodeWrapper(uint64_t kv_port, const rafty::Config &config,
                toolings::MessageQueue<rafty::ApplyResult> &ready)
      : toolings::RaftWrapper(config, ready), kv_ready_(ready),
        node_id_(config.id) {
    kv_server_ = std::make_unique<kv::KvServer>(
        static_cast<rafty::Raft &>(*this));
    start_kv_server(kv_port);
  }

  ~KvNodeWrapper() {
    if (kv_grpc_server_) {
      kv_grpc_server_->Shutdown();
    }
  }

  // Override Apply to intercept committed Raft entries.
  // Each committed entry is forwarded to KvServer::on_apply() (for KV state)
  // and also streamed to the tester (for test infrastructure visibility).
  grpc::Status
  Apply(grpc::ServerContext *context, const google::protobuf::Empty *request,
        grpc::ServerWriter<testerpb::ApplyResult> *writer) override {
    (void)context;
    (void)request;

    while (!this->is_dead()) {
      auto result = kv_ready_.dequeue();
      if (this->is_dead())
        break;
      if (result.valid) {
        // Forward to KvServer for KV state application
        kv_server_->on_apply(result);

        // Also stream to tester for test infrastructure
        testerpb::ApplyResult r;
        r.set_valid(result.valid);
        r.set_data(result.data);
        r.set_index(result.index);
        r.set_id(node_id_);
        writer->Write(r);
      }
    }
    return grpc::Status::OK;
  }

private:
  void start_kv_server(uint64_t port) {
    grpc::EnableDefaultHealthCheckService(false);

    grpc::ServerBuilder builder;
    std::string addr = "0.0.0.0:" + std::to_string(port);
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(kv_server_.get());
    kv_grpc_server_ = builder.BuildAndStart();
    std::cout << "KV server listening on " << addr << std::endl;
    std::thread([this] { kv_grpc_server_->Wait(); }).detach();
  }

  toolings::MessageQueue<rafty::ApplyResult> &kv_ready_;
  uint64_t node_id_;
  std::unique_ptr<kv::KvServer> kv_server_;
  std::unique_ptr<grpc::Server> kv_grpc_server_;
};

// ---------------------------------------------------------------------------
// Command-line flags (same as raft_node, reused)
// ---------------------------------------------------------------------------

struct PeerAddrs {
  std::map<uint64_t, std::string> values;
};

bool AbslParseFlag(absl::string_view text, PeerAddrs *out, std::string *error) {
  if (text.empty()) {
    return true;
  }
  std::vector<std::string> parts = absl::StrSplit(text, ',');
  for (const auto &part : parts) {
    std::vector<std::string> id_addr = absl::StrSplit(part, '+');
    if (id_addr.size() != 2) {
      *error = "Invalid peer address: " + part;
      return false;
    }
    uint64_t id;
    if (!absl::SimpleAtoi(id_addr[0], &id)) {
      *error = "Invalid peer id: " + id_addr[0];
      return false;
    }
    out->values[id] = id_addr[1];
  }
  return true;
}

std::string AbslUnparseFlag(const PeerAddrs &flag) {
  std::vector<std::string> parts;
  for (const auto &[id, addr] : flag.values) {
    parts.emplace_back(std::to_string(id) + "+" + addr);
  }
  if (parts.empty()) {
    return "";
  }
  return absl::StrJoin(parts, ",");
}

ABSL_FLAG(uint16_t, id, 0, "id for the current node");
ABSL_FLAG(uint16_t, port, 50051, "Raft server port");
ABSL_FLAG(PeerAddrs, peers, PeerAddrs(), "a list of peer addresses");
ABSL_FLAG(int, verbosity, 1, "Verbosity level: 0 (silent), 1 (file only), 2 (all)");
ABSL_FLAG(int, fail_type, 0, "Failure Type: 0 (disconnection), 1 (partition)");

// tester flags
ABSL_FLAG(bool, enable_ctrl, false, "Enable test controller");
ABSL_FLAG(std::string, ctrl_addr, "localhost:55000", "Test controller address");
ABSL_FLAG(uint64_t, node_tester_port, 55001, "Tester node port");

// DDB flags
ABSL_FLAG(bool, ddb, false, "Enable DDB");
ABSL_FLAG(std::string, ddb_host_ip, "127.0.0.1", "DDB host IP");
ABSL_FLAG(bool, wait_for_attach, true, "Wait for DDB attach");

static std::unique_ptr<KvNodeWrapper> node = nullptr;
std::atomic<bool> keep_running(true);

void handle_sigint(int sig) {
  (void)sig;
  std::cout << "\nCaught SIGINT, cleaning up..." << std::endl;
  if (node) {
    node->kill();
    node->stop_server();
    node.reset();
  }
  keep_running.store(false, std::memory_order_seq_cst);
}

int setup_signal() {
  struct sigaction sa;
  sa.sa_handler = handle_sigint;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  if (sigaction(SIGINT, &sa, NULL) == -1) {
    perror("sigaction");
    return 1;
  }
  return 0;
}

void prepare_logging(uint64_t id) {
  int verbosity = absl::GetFlag(FLAGS_verbosity);
  auto logger_name = std::format("rafty_node_{}", static_cast<uint64_t>(id));
  if (verbosity < 1) {
    rafty::utils::disable_console_logging(spdlog::get(logger_name), true);
  } else if (verbosity < 2) {
    rafty::utils::disable_console_logging(spdlog::get(logger_name), false);
  }
}

void prepare_interceptor(const std::set<std::string> &world) {
  rafty::NetInterceptor::setup_rank(world);
  int failure = absl::GetFlag(FLAGS_fail_type);
  if (failure == 0) {
    rafty::NetInterceptor::set_type(
        rafty::NetInterceptionType::NETWORK_FAILURE);
  } else if (failure == 1) {
    rafty::NetInterceptor::set_type(
        rafty::NetInterceptionType::NETWORK_PARTITION);
  } else {
    rafty::NetInterceptor::set_type(
        rafty::NetInterceptionType::NETWORK_FAILURE);
  }
}

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);

  auto id = absl::GetFlag(FLAGS_id);
  std::string node_alias = "kv_node_" + std::to_string(id);
  auto port = absl::GetFlag(FLAGS_port);
  uint64_t kv_port = port + KV_PORT_OFFSET;

  // optionally enable DDB
  if (absl::GetFlag(FLAGS_ddb)) {
    auto cfg = DDB::Config::get_default(absl::GetFlag(FLAGS_ddb_host_ip))
                   .with_alias(node_alias)
                   .with_logical_group(node_alias);
    cfg.wait_for_attach = absl::GetFlag(FLAGS_wait_for_attach);
    auto connector = DDB::DDBConnector(cfg);
    connector.init();
  }

  if (setup_signal() != 0) {
    return 1;
  }

  rafty::utils::init_logger();

#ifdef TRACING
  tracing::InitOtelInfra(node_alias);
#endif

  std::map<uint64_t, std::string> peers;
  if (!absl::GetFlag(FLAGS_peers).values.empty()) {
    peers = absl::GetFlag(FLAGS_peers).values;
  }

  auto addr = "0.0.0.0:" + std::to_string(port);
  rafty::Config config = {.id = id, .addr = addr, .peer_addrs = peers};

  std::set<std::string> world;
  world.insert(std::to_string(id));

  std::cout << "KV node (id=" << id << ") Raft on " << addr
            << ", KV on port " << kv_port << ", peers:" << std::endl;
  for (const auto &[peer_id, peer_addr] : peers) {
    std::cout << "\t" << peer_id << " " << peer_addr << std::endl;
    world.insert(std::to_string(peer_id));
  }

  auto enable_tester = absl::GetFlag(FLAGS_enable_ctrl);
  toolings::MessageQueue<rafty::ApplyResult> ready;

  node = [&]() {
    if (enable_tester) {
      auto ctrl_addr = absl::GetFlag(FLAGS_ctrl_addr);
      auto node_tester_port = absl::GetFlag(FLAGS_node_tester_port);
      return std::make_unique<KvNodeWrapper>(ctrl_addr, node_tester_port,
                                             kv_port, config, ready);
    }
    return std::make_unique<KvNodeWrapper>(kv_port, config, ready);
  }();

  prepare_interceptor(world);
  prepare_logging(id);

  node->start_server();

  if (enable_tester) {
    node->start_svr_loop();
  } else {
    // Interactive mode — block until SIGINT
    std::cout << "KV node running. Press Ctrl+C to stop." << std::endl;
    while (keep_running.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  node.reset();
  return 0;
}
