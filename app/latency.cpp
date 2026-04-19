#include <algorithm>
#include <chrono>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <numeric>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "common/logger.hpp"
#include "kv/kv_client.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"

namespace {

    constexpr uint64_t kNumNodes = 3;
    constexpr uint64_t kRaftBasePort = 50050;
    constexpr uint64_t kKvPortOffset = 1000;
    constexpr uint64_t kTesterPortBase = 56001;
    constexpr uint64_t kCtrlPort = 56000;
    constexpr int kNumOperations = 1000;
    constexpr std::chrono::seconds kClusterWarmup{ 2 };

    struct LatencyStats {
        double avg_ms = 0.0;
        double p50_ms = 0.0;
        double p99_ms = 0.0;
    };

    double percentile_from_sorted(const std::vector<double> &sorted, double percentile) {
        if(sorted.empty()) { return 0.0; }

        const size_t idx = static_cast<size_t>((percentile / 100.0) * static_cast<double>(sorted.size() - 1));
        return sorted[idx];
    }

    LatencyStats compute_stats(std::vector<double> latencies_ms) {
        LatencyStats stats;
        if(latencies_ms.empty()) { return stats; }

        std::sort(latencies_ms.begin(), latencies_ms.end());
        const double sum = std::accumulate(latencies_ms.begin(), latencies_ms.end(), 0.0);

        stats.avg_ms = sum / static_cast<double>(latencies_ms.size());
        stats.p50_ms = percentile_from_sorted(latencies_ms, 50.0);
        stats.p99_ms = percentile_from_sorted(latencies_ms, 99.0);
        return stats;
    }

    std::string resolve_kv_node_binary() {
        const std::array<std::string, 4> candidates = {
            "./kv_node",
            "./build/app/kv_node",
            "./app/kv_node",
            "../app/kv_node",
        };

        for(const auto &candidate : candidates) {
            if(::access(candidate.c_str(), X_OK) == 0) { return candidate; }
        }
        return "./kv_node";
    }

} // namespace

int main() {
    rafty::utils::init_logger();

    const std::string logger_name = "kv_latency_bench";
    auto logger = spdlog::get(logger_name);
    if(!logger) { logger = spdlog::basic_logger_mt(logger_name, "logs/" + logger_name + ".log", true); }

    std::vector<rafty::Config> configs;
    std::unordered_map<uint64_t, uint64_t> node_tester_ports;
    std::vector<std::string> kv_addrs;
    kv_addrs.reserve(kNumNodes);

    const auto instances = toolings::ConfigGen::gen_local_instances(kNumNodes, kRaftBasePort);
    uint64_t tester_port = kTesterPortBase;
    for(const auto &inst : instances) {
        std::map<uint64_t, std::string> peer_addrs;
        for(const auto &peer : instances) {
            if(peer.id == inst.id) { continue; }
            peer_addrs[peer.id] = peer.external_addr;
        }

        configs.push_back(rafty::Config{
            .id = inst.id,
            .addr = inst.listening_addr,
            .peer_addrs = peer_addrs,
        });
        node_tester_ports[inst.id] = tester_port++;
        kv_addrs.push_back("localhost:" + std::to_string(inst.port + kKvPortOffset));
    }

    const std::string kv_node_bin = resolve_kv_node_binary();
    if(::access(kv_node_bin.c_str(), X_OK) != 0) {
        std::cerr << "Cannot find executable kv_node binary in ./kv_node, ./build/app/kv_node, ./app/kv_node, "
                  << "or ../app/kv_node\n";
        return EXIT_FAILURE;
    }

    const std::string ctrl_addr = "0.0.0.0:" + std::to_string(kCtrlPort);
    auto ctrl = std::make_unique<toolings::RaftTestCtrl>(configs, node_tester_ports, kv_node_bin, ctrl_addr,
                                                         /*fail_type=*/0, /*verbosity=*/0, logger);

    ctrl->register_applier_handler([](testerpb::ApplyResult msg) { (void)msg; });

    ctrl->run();
    std::this_thread::sleep_for(kClusterWarmup);

    kv::KvClient client(kv_addrs);
    std::vector<double> latencies_ms;
    latencies_ms.reserve(kNumOperations);

    uint64_t failed_ops = 0;
    for(int i = 0; i < kNumOperations; ++i) {
        const std::string key = "key_" + std::to_string(i);
        const std::string value = "value_" + std::to_string(i);

        const auto start = std::chrono::steady_clock::now();
        const auto status = client.put(key, value);
        const auto end = std::chrono::steady_clock::now();

        const auto latency = std::chrono::duration<double, std::milli>(end - start).count();
        latencies_ms.push_back(latency);

        if(status != kvpb::KV_SUCCESS) { ++failed_ops; }
    }

    const LatencyStats stats = compute_stats(latencies_ms);

    std::cout << "######################################\n";
    std::cout << "#      latAvg       latP50      latP99\n";
    std::cout << "#        (ms)         (ms)        (ms)\n";
    std::cout << "--------------------------------------\n";
    std::cout << std::fixed << std::setprecision(2) << std::setw(12) << stats.avg_ms << std::setw(13) << stats.p50_ms
              << std::setw(12) << stats.p99_ms << "\n";

    ctrl->kill();
    ctrl.reset();

    if(failed_ops > 0) {
        std::cerr << "Latency benchmark failed: " << failed_ops << " out of " << kNumOperations
                  << " requests did not return KV_SUCCESS\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
