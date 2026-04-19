#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
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

    constexpr uint64_t kDefaultNumNodes = 3;
    constexpr uint64_t kKvPortOffset = 1000;
    constexpr uint64_t kOpsPerClient = 1000;
    constexpr uint64_t kKeyspaceSize = 1000;
    constexpr uint64_t kInitialRaftPort = 50050;
    constexpr uint64_t kInitialCtrlPort = 57000;
    constexpr uint64_t kInitialTesterPort = 57001;
    constexpr uint64_t kPortStridePerRound = 20;
    constexpr std::chrono::seconds kClusterWarmup{ 2 };
    constexpr std::chrono::seconds kPostKillDelay{ 1 };
    constexpr std::chrono::seconds kProgressPrintInterval{ 5 };

    struct LatencyStats {
        double avg_ms = 0.0;
        double p50_ms = 0.0;
        double p90_ms = 0.0;
        double p99_ms = 0.0;
    };

    struct RoundResult {
        uint64_t client_count = 0;
        uint64_t total_ops = 0;
        uint64_t success_ops = 0;
        LatencyStats latency;
        double throughput_ops_sec = 0.0;
    };

    struct ClusterContext {
        std::unique_ptr<toolings::RaftTestCtrl> ctrl;
        std::vector<std::string> kv_addrs;
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
        stats.p90_ms = percentile_from_sorted(latencies_ms, 90.0);
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

    ClusterContext start_cluster(uint64_t round_idx, uint64_t num_nodes, const std::string &kv_node_bin,
                                 std::shared_ptr<spdlog::logger> logger) {
        ClusterContext cluster;
        cluster.kv_addrs.reserve(num_nodes);

        const uint64_t raft_base_port = kInitialRaftPort + round_idx * kPortStridePerRound;
        const uint64_t ctrl_port = kInitialCtrlPort + round_idx * kPortStridePerRound;
        const uint64_t tester_port_base = kInitialTesterPort + round_idx * kPortStridePerRound;

        std::vector<rafty::Config> configs;
        std::unordered_map<uint64_t, uint64_t> node_tester_ports;

        const auto instances = toolings::ConfigGen::gen_local_instances(num_nodes, raft_base_port);
        uint64_t tester_port = tester_port_base;
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
            cluster.kv_addrs.push_back("localhost:" + std::to_string(inst.port + kKvPortOffset));
        }

        const std::string ctrl_addr = "0.0.0.0:" + std::to_string(ctrl_port);
        cluster.ctrl = std::make_unique<toolings::RaftTestCtrl>(configs, node_tester_ports, kv_node_bin, ctrl_addr,
                                                                /*fail_type=*/0, /*verbosity=*/0, logger);

        cluster.ctrl->register_applier_handler([](testerpb::ApplyResult result) { (void)result; });

        cluster.ctrl->run();
        std::this_thread::sleep_for(kClusterWarmup);
        return cluster;
    }

    bool prepopulate_keyspace(const std::vector<std::string> &kv_addrs) {
        kv::KvClient client(kv_addrs);
        for(uint64_t i = 1; i <= kKeyspaceSize; ++i) {
            const std::string key = "key_" + std::to_string(i);
            const std::string value = "init_" + std::to_string(i);
            const auto status = client.put(key, value);
            if(status != kvpb::KV_SUCCESS) { return false; }

            if(i % 200 == 0 || i == kKeyspaceSize) {
                std::cout << "  prepopulate progress: " << i << "/" << kKeyspaceSize << std::endl;
            }
        }
        return true;
    }

    RoundResult run_round(uint64_t client_count, int put_ratio, const std::vector<std::string> &kv_addrs) {
        RoundResult result;
        result.client_count = client_count;
        result.total_ops = client_count * kOpsPerClient;

        std::vector<std::thread> workers;
        workers.reserve(client_count);

        std::vector<std::vector<double>> per_client_latencies(client_count);
        std::atomic<uint64_t> success_ops{ 0 };
        std::atomic<uint64_t> completed_clients{ 0 };

        std::mutex start_mtx;
        std::condition_variable start_cv;
        uint64_t ready_clients = 0;
        bool start_now = false;

        std::mutex done_mtx;
        std::condition_variable done_cv;

        for(uint64_t c = 0; c < client_count; ++c) {
            workers.emplace_back([&, c] {
                kv::KvClient client(kv_addrs);
                auto &local_latencies = per_client_latencies[c];
                local_latencies.reserve(kOpsPerClient);

                const auto thread_seed
                    = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count())
                      ^ (c * 0x9e3779b97f4a7c15ULL);
                std::mt19937_64 rng(thread_seed);
                std::uniform_int_distribution<int> op_dist(1, 100);
                std::uniform_int_distribution<uint64_t> key_dist(1, kKeyspaceSize);

                {
                    std::unique_lock<std::mutex> lk(start_mtx);
                    ++ready_clients;
                    if(ready_clients == client_count) { start_cv.notify_one(); }
                    start_cv.wait(lk, [&] { return start_now; });
                }

                uint64_t local_success = 0;
                for(uint64_t i = 0; i < kOpsPerClient; ++i) {
                    const bool do_put = (op_dist(rng) <= put_ratio);
                    const uint64_t key_id = key_dist(rng);
                    const std::string key = "key_" + std::to_string(key_id);

                    auto start = std::chrono::steady_clock::now();
                    kvpb::KvStatus status = kvpb::KV_TIMEOUT;

                    if(do_put) {
                        const std::string value = "v_" + std::to_string(c) + "_" + std::to_string(i);
                        status = client.put(key, value);
                    } else {
                        status = client.get(key).first;
                    }

                    auto end = std::chrono::steady_clock::now();
                    local_latencies.push_back(std::chrono::duration<double, std::milli>(end - start).count());

                    if(status == kvpb::KV_SUCCESS) { ++local_success; }
                }

                success_ops.fetch_add(local_success, std::memory_order_relaxed);
                completed_clients.fetch_add(1, std::memory_order_relaxed);
                done_cv.notify_all();
            });
        }

        std::chrono::steady_clock::time_point bench_start;
        {
            std::unique_lock<std::mutex> lk(start_mtx);
            start_cv.wait(lk, [&] { return ready_clients == client_count; });
            bench_start = std::chrono::steady_clock::now();
            start_now = true;
        }
        start_cv.notify_all();

        {
            std::unique_lock<std::mutex> lk(done_mtx);
            while(completed_clients.load(std::memory_order_relaxed) < client_count) {
                done_cv.wait_for(lk, kProgressPrintInterval);
                const uint64_t done = completed_clients.load(std::memory_order_relaxed);
                std::cout << "  workload progress: " << done << "/" << client_count << " clients finished" << std::endl;
            }
        }

        for(auto &worker : workers) {
            if(worker.joinable()) { worker.join(); }
        }
        const auto bench_end = std::chrono::steady_clock::now();

        std::vector<double> all_latencies;
        all_latencies.reserve(result.total_ops);
        for(const auto &latencies : per_client_latencies) {
            all_latencies.insert(all_latencies.end(), latencies.begin(), latencies.end());
        }

        result.success_ops = success_ops.load(std::memory_order_relaxed);
        result.latency = compute_stats(all_latencies);

        const double elapsed_sec = std::chrono::duration<double>(bench_end - bench_start).count();
        result.throughput_ops_sec = elapsed_sec > 0.0 ? static_cast<double>(result.success_ops) / elapsed_sec : 0.0;

        return result;
    }

    void write_result_header(std::ostream &os) {
        os << "##################################################################################\n";
        os << "# clientCount      latAvg       latP50       latP90       latP99        throughput\n";
        os << "#                    (ms)         (ms)         (ms)         (ms)         (ops/sec)\n";
        os << "----------------------------------------------------------------------------------\n";
    }

    void write_result_row(std::ostream &os, const RoundResult &result) {
        os << std::fixed << std::setprecision(2) << std::setw(12) << result.client_count << std::setw(12)
           << result.latency.avg_ms << std::setw(12) << result.latency.p50_ms << std::setw(12) << result.latency.p90_ms
           << std::setw(12) << result.latency.p99_ms << std::setw(18) << result.throughput_ops_sec << "\n";
    }

    bool round_has_failures(const RoundResult &result) { return result.success_ops != result.total_ops; }

} // namespace

int main(int argc, char **argv) {
    if(argc < 3 || argc > 5) {
        std::cerr << "Usage: ./tput <MaxClientCount> <PutRatio> [NumNodes] [ResultFile]\n";
        return 1;
    }

    int max_client_count = 0;
    int put_ratio = 0;
    uint64_t num_nodes = kDefaultNumNodes;
    std::string result_path = "result.txt";
    try {
        max_client_count = std::stoi(argv[1]);
        put_ratio = std::stoi(argv[2]);

        if(argc >= 4) { num_nodes = static_cast<uint64_t>(std::stoull(argv[3])); }
        if(argc >= 5) { result_path = argv[4]; }
    } catch(const std::exception &) {
        std::cerr << "Invalid arguments. MaxClientCount/PutRatio/NumNodes must be numeric.\n";
        return 1;
    }

    if(max_client_count < 1) {
        std::cerr << "MaxClientCount must be >= 1.\n";
        return 1;
    }
    if(put_ratio < 0 || put_ratio > 100) {
        std::cerr << "PutRatio must be between 0 and 100.\n";
        return 1;
    }
    if(num_nodes < 3) {
        std::cerr << "NumNodes must be >= 3.\n";
        return 1;
    }

    rafty::utils::init_logger();
    const std::string logger_name = "kv_tput_bench";
    auto logger = spdlog::get(logger_name);
    if(!logger) { logger = spdlog::basic_logger_mt(logger_name, "logs/" + logger_name + ".log", true); }

    const std::string kv_node_bin = resolve_kv_node_binary();
    if(::access(kv_node_bin.c_str(), X_OK) != 0) {
        std::cerr << "Cannot find executable kv_node binary in ./kv_node, ./build/app/kv_node, ./app/kv_node, "
                  << "or ../app/kv_node\n";
        return EXIT_FAILURE;
    }

    std::ofstream result_file(result_path, std::ios::out | std::ios::trunc);
    if(!result_file.is_open()) {
        std::cerr << "Failed to open " << result_path << " for writing.\n";
        return 1;
    }

    std::cout << "Benchmark config: nodes=" << num_nodes << ", putRatio=" << put_ratio
              << ", maxClients=" << max_client_count << std::endl;

    write_result_header(result_file);
    write_result_header(std::cout);

    bool saw_any_round_failure = false;
    uint64_t round_idx = 0;
    for(uint64_t client_count = 1; client_count <= static_cast<uint64_t>(max_client_count);
        client_count <<= 1, ++round_idx) {
        std::cout << "Running round with " << client_count << " clients..." << std::endl;

        std::cout << "  starting cluster..." << std::endl;
        auto cluster = start_cluster(round_idx, num_nodes, kv_node_bin, logger);
        std::cout << "  cluster ready, prepopulating keyspace..." << std::endl;
        const bool prepopulate_ok = prepopulate_keyspace(cluster.kv_addrs);
        if(!prepopulate_ok) {
            std::cerr << "Pre-population failed in round with " << client_count << " clients. Aborting benchmark.\n";
            if(cluster.ctrl) {
                cluster.ctrl->kill();
                cluster.ctrl.reset();
            }
            return 1;
        }

        std::cout << "  prepopulation complete, running workload..." << std::endl;

        const RoundResult result = run_round(client_count, put_ratio, cluster.kv_addrs);
        std::cout << "  workload complete, writing results..." << std::endl;
        write_result_row(result_file, result);
        write_result_row(std::cout, result);
        result_file.flush();

        if(round_has_failures(result)) {
            const uint64_t failed_ops = result.total_ops - result.success_ops;
            std::cerr << "Round with " << client_count << " clients had " << failed_ops << " non-success operations ("
                      << result.success_ops << "/" << result.total_ops << " succeeded).\n";
            saw_any_round_failure = true;
        }

        if(cluster.ctrl) {
            cluster.ctrl->kill();
            cluster.ctrl.reset();
        }
        std::this_thread::sleep_for(kPostKillDelay);

        if(client_count > static_cast<uint64_t>(max_client_count) / 2) { break; }
    }

    std::cout << "Results written to " << result_path << std::endl;
    return saw_any_round_failure ? EXIT_FAILURE : EXIT_SUCCESS;
}
