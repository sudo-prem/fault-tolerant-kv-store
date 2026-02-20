
#include <signal.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <math.h>
#include <memory>
#include <optional>
#include <ranges>
#include <set>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>

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

// used by DDB
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

  std::string current_test_name() const {
    const auto* info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    return info ? info->name() : "";
  }

  std::shared_ptr<spdlog::logger> logger = nullptr;
  int tester_verb = 0;
  int raft_node_verb = 0;
  OutputSuppressor suppressor;
};

TEST_F(RaftTest, 9366994833_SymmetricEvenPartitionA) {
  this->init_logger("9366994833_SymmetricEvenPartitionA");
  auto local_confs = ConfigGen::gen_local_instances(4, 50051);
  auto r_confs = ConfigGen::gen_raft_configs(local_confs);
  toolings::MultiprocTestConfig cfg(
      r_confs, NODE_APP_PATH, this->logger,
      ddb_conf, 1, this->raft_node_verb);

  try {
    cfg.begin();
    auto initial_leader = cfg.check_one_leader();
    logger->info("Initial leader: {}", *initial_leader);
    logger->info("Creating 2-2 symmetric partition");
    auto leader_id = *initial_leader;
    cfg.disconnect((leader_id + 1) % 4);
    cfg.disconnect(leader_id);
    std::this_thread::sleep_for(5 * TIMEOUT);
    cfg.check_no_leader();
    logger->info("Reconnecting all nodes");

    cfg.reconnect_all();

    std::this_thread::sleep_for(3 * TIMEOUT);

    auto final_leader = cfg.check_one_leader();
    ASSERT_TRUE(final_leader.has_value());
    logger->info("Final leader after recovery: {}", *final_leader);

    auto final_term = cfg.check_terms();
    ASSERT_TRUE(final_term.has_value());
    logger->info("Final unanimous term: {}", *final_term);

  } catch (const std::exception &e) {
    logger->error("Exception: {}", e.what());
    FAIL() << "Exception: " << e.what();
  }
}


TEST_F(RaftTest, 2159570081_StrictNoReelectionOnReconnectA) {
  this->init_logger("2159570081_StrictNoReelectionOnReconnectA");
  auto servers = 3;
  auto local_confs = toolings::ConfigGen::gen_local_instances(servers, 50051);
  auto r_confs = toolings::ConfigGen::gen_raft_configs(local_confs);
  toolings::MultiprocTestConfig cfg(r_confs, NODE_APP_PATH, this->logger,
                                    ddb_conf, 0, this->raft_node_verb);

  try {
    cfg.begin();

    auto leader1 = cfg.check_one_leader();
    EXPECT_TRUE(leader1.has_value()) << "No leader found!";
    auto term1 = cfg.check_terms();
    EXPECT_TRUE(term1.has_value()) << "Failed to get initial term";

    logger->info("Disconnect leader - force reelection");
    cfg.disconnect(*leader1);
    cfg.check_one_leader();
    logger->info("Disconnect leader finished - should done election.");

    logger->info("Reconnect leader - should have no reelection");
    auto term2 = cfg.check_terms();
    EXPECT_TRUE(term2.has_value()) << "Failed to get term before reconnect";

    cfg.reconnect(*leader1);
    auto leader2 = cfg.check_one_leader();
    EXPECT_TRUE(leader2.has_value()) << "No leader found!";

    auto term3 = cfg.check_terms();
    EXPECT_TRUE(term3.has_value()) << "Failed to get term after reconnect";

    ASSERT_EQ(*term2, *term3)
        << "Term changed after reconnect: " << *term2 << " -> " << *term3
        << ". Unnecessary re-election detected!";

    logger->info("Reconnect leader finished - should have no election.");

    logger->info("Disconnect two servers - should have no leader.");
    cfg.disconnect((*leader2 + 1) % servers);
    cfg.disconnect(*leader2);
    std::this_thread::sleep_for(2 * TIMEOUT);
    cfg.check_no_leader();
    logger->info("Disconnect two servers finished - should have no leader.");

    logger->info("Reconnect two servers - should trigger one election.");
    cfg.reconnect((*leader2 + 1) % servers);
    cfg.check_one_leader();
    logger->info("Reconnect two servers finished - should trigger one election.");

    cfg.reconnect(*leader2);
    cfg.check_one_leader();
  } catch (const std::exception &e) {
    logger->error("Exception: {}", e.what());
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(RaftTest, 2714827242_LeaderStepdownOnHigherTermA) {
    this->init_logger("LeaderStepdownA");
    auto local_confs = ConfigGen::gen_local_instances(3, 50500);
    auto r_confs = ConfigGen::gen_raft_configs(local_confs);
    MultiprocTestConfig cfg(r_confs, NODE_APP_PATH, this->logger, ddb_conf, 0, this->raft_node_verb);

    try {
        cfg.begin();

        auto leader = cfg.check_one_leader();
        ASSERT_TRUE(leader.has_value()) << "No leader found initially";

        // Disconnect the current leader to allow followers to elect a new leader
        cfg.disconnect(leader.value());
        std::this_thread::sleep_for(std::chrono::seconds(2));

        auto new_leader = cfg.check_one_leader();
        ASSERT_TRUE(new_leader.has_value()) << "No leader elected after leader disconnect";
        ASSERT_NE(new_leader.value(), leader.value()) << "Leader did not step down naturally";

        // Reconnect old leader and check it steps down
        cfg.reconnect(leader.value());
        std::this_thread::sleep_for(std::chrono::seconds(1));

        auto final_leader = cfg.check_one_leader();
        ASSERT_TRUE(final_leader.has_value()) << "No leader after old leader reconnect";
        ASSERT_EQ(final_leader.value(), new_leader.value()) << "Old leader became leader again incorrectly";
    } catch (const std::exception &e) {
        FAIL() << "Exception: " << e.what();
    }
}

TEST_F(RaftTest, 6584218762_StabilityNoChurnA) {
  this->init_logger("6584218762_StabilityNoChurnA");
  auto local_confs = ConfigGen::gen_local_instances(5, 50061);
  auto r_confs = ConfigGen::gen_raft_configs(local_confs);
  toolings::MultiprocTestConfig cfg(r_confs, NODE_APP_PATH, this->logger,
                                    ddb_conf, 0, this->raft_node_verb);

  try {
    cfg.begin();

    auto leader1 = cfg.check_one_leader();
    ASSERT_TRUE(leader1.has_value());
    auto term1 = cfg.check_terms();
    ASSERT_TRUE(term1.has_value());

    std::this_thread::sleep_for(3 * TIMEOUT);

    auto leader2 = cfg.check_one_leader();
    auto term2 = cfg.check_terms();
    ASSERT_TRUE(leader2.has_value());
    ASSERT_TRUE(term2.has_value());
    ASSERT_EQ(term1.value(), term2.value()) << "term changed with no failures";
    ASSERT_EQ(leader1.value(), leader2.value()) << "leader changed with no failures";
  } catch (const std::exception &e) {
    logger->error("Exception: {}", e.what());
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(RaftTest, 1998937054_8861801385_MajorityRetainsLeaderA) {
  this->init_logger("1998937054_8861801385_MajorityRetainsLeaderA");
  auto servers = 5;
  auto local_confs = ConfigGen::gen_local_instances(servers, 50051);
  auto r_confs = ConfigGen::gen_raft_configs(local_confs);
  MultiprocTestConfig cfg(r_confs, NODE_APP_PATH, this->logger,
                          ddb_conf, 0, this->raft_node_verb);

  try {
    cfg.begin();

    auto leader1 = cfg.check_one_leader();
    ASSERT_TRUE(leader1.has_value());

    auto term1 = cfg.check_terms();
    ASSERT_TRUE(term1.has_value());

    // Disconnect only 2 followers (still majority alive)
    auto ids = cfg.pick_n_servers(2, leader1);
    for (auto id : ids)
      cfg.disconnect(id);

    // Wait longer than election timeout
    std::this_thread::sleep_for(2 * TIMEOUT);

    // Leader should still exist
    auto leader2 = cfg.check_one_leader();
    ASSERT_TRUE(leader2.has_value());

    // Should be same leader
    ASSERT_EQ(*leader1, *leader2)
        << "Leader changed despite majority being the same";

    auto term2 = cfg.check_terms();
    ASSERT_TRUE(term2.has_value());

    // Term should not increase
    ASSERT_EQ(*term1, *term2)
        << "Term changed despite majority being the same";

  } catch (const std::exception &e) {
    FAIL() << e.what();
  }
}

TEST_F(RaftTest, NetworkPartitionA) {
  this->init_logger("boyuany_NetworkPartitionA");

  auto servers = 5;
  auto local_confs = ConfigGen::gen_local_instances(servers, 50051);
  auto r_confs = ConfigGen::gen_raft_configs(local_confs);

  toolings::MultiprocTestConfig cfg(r_confs, NODE_APP_PATH,this->logger,
                                    ddb_conf, 1,this->raft_node_verb);

  try {
    cfg.begin();

    auto initial_term = cfg.check_terms();
    ASSERT_TRUE(initial_term.has_value());

    for (int i = 0; i < 8; i++) {
      auto ids = cfg.pick_n_servers(2);
      for (auto id : ids)
        cfg.disconnect(id);
      // This should not cause an election
      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      for (auto id : ids)
        cfg.reconnect(id);

      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    auto final_term = cfg.check_terms();
    ASSERT_TRUE(final_term.has_value());

    auto growth = final_term.value() - initial_term.value();

    ASSERT_TRUE(growth <= 12)
        << "Term Too much, Raft should tolerate network partitions within timeout";

    cfg.check_one_leader();

  } catch (const std::exception &e) {
    logger->error("Exception: {}", e.what());
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(RaftTest, ctli_DisconnectFollowersA) {
  this->init_logger("ctli_DisconnectFollowersA");
  auto local_confs = ConfigGen::gen_local_instances(5, 50051);
  auto r_confs = ConfigGen::gen_raft_configs(local_confs);
  toolings::MultiprocTestConfig cfg(r_confs, NODE_APP_PATH, this->logger,
                                    ddb_conf, 0, this->raft_node_verb);

  try {
    cfg.begin();

    auto leader = cfg.check_one_leader();
    auto term = cfg.check_terms();
    ASSERT_TRUE(leader.has_value()) << "No leader found!";
    ASSERT_TRUE(term.has_value()) << "Failed to get terms after first election";
    ASSERT_TRUE(term.value() >= 1)
        << "term is " << term.value() << ", but should be at least 1";

    auto iters = 5;
    for (int i = 0; i < iters; i++) {
      std::set<uint64_t> leader_set{leader.value()};
      auto ids = cfg.pick_n_servers(2, leader_set);
      for (auto id : ids) {
        cfg.disconnect(id);
        logger->info("Disconnect server {}", id);
      }

      // current leader should still be alive
      auto cur_leader = cfg.check_one_leader();
      auto cur_term = cfg.check_terms();
      ASSERT_TRUE(cur_leader.has_value()) << "No leader found!";
      ASSERT_TRUE(cur_leader.value() == leader.value()) << "Leader changed!";
      ASSERT_TRUE(cur_term.has_value()) << "Failed to get terms";
      ASSERT_TRUE(cur_term.value() == term.value())
          << "term is " << cur_term.value() << ", but should be "
          << term.value();

      for (auto id : ids) {
        cfg.reconnect(id);
        logger->info("Reconnect server {}", id);
      }

      // Leader might change after reconnection
      leader = cfg.check_one_leader();
      term = cfg.check_terms();
    }

    cfg.check_one_leader();
  } catch (const std::exception &e) {
    logger->error("Exception: {}", e.what());
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(RaftTest, 5854395979_4945749684_UniqueLeaderOverTimeA) {
  this->init_logger("UniqueLeaderOverTimeA");

  auto local_confs = ConfigGen::gen_local_instances(3, 50251);
  auto r_confs = ConfigGen::gen_raft_configs(local_confs);
  toolings::MultiprocTestConfig cfg(r_confs, NODE_APP_PATH, this->logger,
                                   ddb_conf, 0, this->raft_node_verb);

  try {
    cfg.begin();

    logger->info("Waiting for initial leader election...");
    cfg.check_one_leader();

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    auto base_term = cfg.check_terms();
    ASSERT_TRUE(base_term.has_value()) << "Failed to read terms after election";
    ASSERT_GE(base_term.value(), 1u) << "Term should be >= 1";

    for (int i = 0; i < 20; i++) {
      cfg.check_one_leader();

      auto t = cfg.check_terms();
      ASSERT_TRUE(t.has_value()) << "Failed to read terms during sampling loop";

      ASSERT_EQ(base_term.value(), t.value())
          << "Term drift during stable sampling loop (likely leader churn). "
          << "base=" << base_term.value() << " now=" << t.value();

      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

  } catch (const std::exception &e) {
    logger->error("Exception: {}", e.what());
    FAIL() << "Exception: " << e.what();
  }
}

static pid_t pgid;

static void signal_handler(int signum) {
  killpg(pgid, SIGKILL);
  _exit(1);
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
    std::string app_alias = "raft_test_app";
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
