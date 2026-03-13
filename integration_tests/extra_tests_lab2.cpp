#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include <ddb/integration.hpp>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "common/logger.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_config.hpp"
#include "toolings/test_ctrl.hpp"

using namespace toolings;

const std::string NODE_APP_PATH = "../app/raft_node";

ABSL_FLAG(
    int, tester_verb, 1,
    "Tester verbosity level: 0 (silent), 1 (tester message (file sink only)), "
    "2 (all tester messages, file sink + console sink)");
ABSL_FLAG(int, raft_node_verb, 0,
          "Raft node verbosity level: 0 (silent), 1 (raft message (file sink "
          "only)), 2 "
          "(all raft node messages, file sink + console sink)");

ABSL_FLAG(bool, ddb, false, "Enable DDB for debugging.");
ABSL_FLAG(std::string, ddb_host_ip, "127.0.0.1",
          "Host IP of this machine for DDB to connect to.");
ABSL_FLAG(bool, wait_for_attach, true,
          "Wait for DDB debugger to attach before proceeding.");
ABSL_FLAG(bool, ddb_app_wrapper, true,
          "Use DDB app runner wrapper to start raft node (supports PET).");

static DDBConfig ddb_conf;

static constexpr std::chrono::milliseconds TIMEOUT(1000);

class OutputSuppressor {
public:
  OutputSuppressor() = default;
  ~OutputSuppressor() = default;

  void Enable() {
    devnull_fd = open("/dev/null", O_WRONLY);
    if (devnull_fd == -1) {
      throw std::runtime_error("Failed to open /dev/null");
    }
    saved_stdout_fd = dup(STDOUT_FILENO);
    if (saved_stdout_fd == -1) {
      throw std::runtime_error("Failed to duplicate stdout file descriptor");
    }
    if (dup2(devnull_fd, STDOUT_FILENO) == -1) {
      throw std::runtime_error("Failed to redirect stdout to /dev/null");
    }
  }

  void Disable() {
    if (dup2(saved_stdout_fd, STDOUT_FILENO) == -1) {
      std::cerr << "Failed to restore stdout" << std::endl;
    }
    close(devnull_fd);
    close(saved_stdout_fd);
  }

private:
  int devnull_fd;
  int saved_stdout_fd;
};

void cleanup_raft_node_processes() {
  std::system("pkill -9 raft_node 2>/dev/null");
}

class RaftTest : public ::testing::Test {
protected:
  void SetUp() override {
    this->tester_verb = absl::GetFlag(FLAGS_tester_verb);
    this->raft_node_verb = absl::GetFlag(FLAGS_raft_node_verb);
  }

  void TearDown() override {
    if (this->logger) {
      this->logger->flush();
    }
    cleanup_raft_node_processes();
  }

  void init_logger(const std::string &name) {
    auto logger_name = name.empty() ? "raft_test" : "raft_test_" + name;
    this->logger = spdlog::get(logger_name);
    if (!this->logger) {
      this->logger = spdlog::basic_logger_mt(
          logger_name, std::format("logs/{}.log", logger_name), true);
    }
    if (tester_verb < 1) {
      rafty::utils::disable_console_logging(this->logger, true);
    } else if (tester_verb < 2) {
      rafty::utils::disable_console_logging(this->logger, false);
    }
  }

  std::shared_ptr<spdlog::logger> logger = nullptr;
  int tester_verb = 0;
  int raft_node_verb = 0;
  OutputSuppressor suppressor;
};

TEST_F(RaftTest, Figure8UncommittedExtraB) {
  this->init_logger("Figure8UncommittedExtraB");
  auto servers = 5;
  auto local_confs = toolings::ConfigGen::gen_local_instances(servers, 50051);
  auto r_confs = toolings::ConfigGen::gen_raft_configs(local_confs);
  toolings::MultiprocTestConfig cfg(r_confs, NODE_APP_PATH, this->logger,
                                    ddb_conf, 0, this->raft_node_verb);

  try {
    cfg.begin();

    logger->info("Step 1: Initial agreement");
    auto one1 = cfg.one("101", servers, false);
    ASSERT_TRUE(one1.has_value());

    auto l1 = cfg.check_one_leader();
    ASSERT_TRUE(l1.has_value());

    std::vector<uint64_t> l1_group;
    l1_group.push_back(*l1);
    auto others = cfg.pick_n_servers(4, *l1);
    l1_group.push_back(others[0]);

    std::set<uint64_t> l2_group;
    for (int i = 1; i < 4; i++) l2_group.insert(others[i]);

    for (auto id : l2_group) cfg.disconnect(id);

    logger->info("Step 3: Propose '102' to isolated Leader 1");
    cfg.propose(*l1, "102");
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    logger->info("Step 4: Swap partitions");
    for (auto id : l1_group) cfg.disconnect(id);
    for (auto id : l2_group) cfg.reconnect(id);

    auto l2 = cfg.check_one_leader();
    ASSERT_TRUE(l2.has_value());

    logger->info("Step 6: Isolate new Leader 2 and propose '103'");
    std::vector<uint64_t> l2_followers;
    for (auto id : l2_group) {
        if (id != *l2) l2_followers.push_back(id);
    }
    cfg.disconnect(l2_followers[0]);
    cfg.disconnect(l2_followers[1]);

    cfg.propose(*l2, "103");
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    logger->info("Step 7: Form new majority with L1, F1, and one old follower");
    cfg.disconnect(*l2);
    for (auto id : l1_group) cfg.reconnect(id);
    cfg.reconnect(l2_followers[0]);

    auto l3 = cfg.check_one_leader();
    ASSERT_TRUE(l3.has_value());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    logger->info("Step 9: Verifying old term entry is NOT committed despite majority");
    auto cr = cfg.n_committed(2);
    ASSERT_EQ(cr.num, 0) << "DANGER: Old term entry incorrectly committed without an entry from the current term!";

    logger->info("Step 10: Proposing new entry to commit everything");
    cfg.propose(*l3, "104");

    auto comm = cfg.wait(3, 3);
    ASSERT_TRUE(comm.has_value());
    ASSERT_EQ(*comm, "104");

    cr = cfg.n_committed(2);
    ASSERT_GE(cr.num, 3u) << "Old term entry failed to commit indirectly";

    cfg.reconnect_all();
    auto final_agree = cfg.one("105", servers, true);
    ASSERT_TRUE(final_agree.has_value());

  } catch (const std::exception &e) {
    logger->error("Exception: {}", e.what());
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(RaftTest, PartitionB) {
  this->init_logger("PartitionB");
  uint64_t servers = 5;
  auto local_confs = toolings::ConfigGen::gen_local_instances(servers, 50051);
  auto r_confs = toolings::ConfigGen::gen_raft_configs(local_confs);
  toolings::MultiprocTestConfig cfg(r_confs, NODE_APP_PATH, this->logger,
                                    ddb_conf, 1, this->raft_node_verb);

  try {
    cfg.begin();

    auto one1 = cfg.one("initial", servers, false);
    ASSERT_TRUE(one1.has_value());

    int iters = 8;
    for (int i = 0; i < iters; i++) {
      logger->info("=== Figure8 iteration {} ===", i);

      auto leader = cfg.check_one_leader();
      ASSERT_TRUE(leader.has_value());

      auto data = "fig8_iter" + std::to_string(i);
      cfg.propose(leader.value(), data);

      std::this_thread::sleep_for(std::chrono::milliseconds(50));

      auto companion = cfg.pick_n_servers(1, leader.value())[0];
      cfg.disconnect(leader.value());
      cfg.disconnect(companion);

      std::this_thread::sleep_for(TIMEOUT);

      for (uint64_t node = 0; node < servers; node++) {
        cfg.propose(node, "fig8_new_" + std::to_string(i) + "_" + std::to_string(node));
      }
      std::this_thread::sleep_for(TIMEOUT);

      cfg.reconnect(leader.value());
      cfg.reconnect(companion);

      std::this_thread::sleep_for(TIMEOUT);
    }

    auto final_r = cfg.one("figure8_final", servers, true);
    ASSERT_TRUE(final_r.has_value());

  } catch (const std::exception &e) {
    logger->error("Exception: {}", e.what());
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(RaftTest, StaleLeaderDivergenceB) {
  this->init_logger("StaleLeaderDivergenceB");
  uint64_t servers = 5;
  auto local_confs = toolings::ConfigGen::gen_local_instances(servers, 50051);
  auto r_confs = toolings::ConfigGen::gen_raft_configs(local_confs);
  toolings::MultiprocTestConfig cfg(r_confs, NODE_APP_PATH, this->logger,
                                    ddb_conf, 1, this->raft_node_verb);

  try {
    cfg.begin();

    auto one1 = cfg.one("init", servers, false);
    ASSERT_TRUE(one1.has_value());

    auto leader1_opt = cfg.check_one_leader();
    ASSERT_TRUE(leader1_opt.has_value());
    auto leader1 = leader1_opt.value();
    logger->info("leader1 = {}", leader1);

    for (int i = 0; i < 30; i++) {
      cfg.propose(leader1, "stale_" + std::to_string(i));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    cfg.disconnect(leader1);

    std::this_thread::sleep_for(TIMEOUT);

    auto leader2_opt = cfg.check_one_leader();
    ASSERT_TRUE(leader2_opt.has_value());
    auto leader2 = leader2_opt.value();
    ASSERT_NE(leader1, leader2);
    logger->info("leader2 = {}", leader2);

    for (int i = 0; i < 30; i++) {
      auto r = cfg.one("valid_" + std::to_string(i), 4, true);
      ASSERT_TRUE(r.has_value());
    }

    cfg.disconnect(leader2);

    logger->info("Reconnecting stale leader1 = {}", leader1);
    cfg.reconnect(leader1);

    std::this_thread::sleep_for(TIMEOUT);

    auto r = cfg.one("after_rejoin", 4, true);
    ASSERT_TRUE(r.has_value());

    cfg.reconnect(leader2);
    std::this_thread::sleep_for(TIMEOUT);

    auto final_r = cfg.one("final_all", servers, true);
    ASSERT_TRUE(final_r.has_value());

  } catch (const std::exception &e) {
    logger->error("Exception: {}", e.what());
    FAIL() << "Exception: " << e.what();
  }
}

// ============================================================================

static pid_t pgid = 0;

void signal_handler(int signal) {
  if (signal == SIGINT) {
    std::cout << "Caught SIGINT (Ctrl+C), cleaning up..." << std::endl;
    if (::kill(-pgid, SIGKILL) == 0) {
    } else {
      std::perror("Failed to kill process");
      std::exit(1);
    }
    exit(0);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  ddb_conf = {
      .enable_ddb = absl::GetFlag(FLAGS_ddb),
      .ddb_host_ip = absl::GetFlag(FLAGS_ddb_host_ip),
      .wait_for_attach = absl::GetFlag(FLAGS_wait_for_attach),
      .ddb_app_wrapper = absl::GetFlag(FLAGS_ddb_app_wrapper),
  };

  if (absl::GetFlag(FLAGS_ddb)) {
    std::string app_alias = "extra_tests_lab2_app";
    auto cfg = DDB::Config::get_default(absl::GetFlag(FLAGS_ddb_host_ip))
                   .with_alias(app_alias)
                   .with_logical_group(app_alias);
    cfg.wait_for_attach = absl::GetFlag(FLAGS_wait_for_attach);
    auto connector = DDB::DDBConnector(cfg);
    connector.init();
  }

  rafty::utils::init_logger();

  pgid = getpid();
  struct sigaction sigIntHandler;
  sigIntHandler.sa_handler = signal_handler;
  sigemptyset(&sigIntHandler.sa_mask);
  sigIntHandler.sa_flags = 0;
  sigaction(SIGINT, &sigIntHandler, nullptr);

  return RUN_ALL_TESTS();
}
