# RaftKV: Linearizable Distributed Key/Value Store

A replicated, linearizable key/value service built in C++20 on top of the Raft consensus algorithm. The service runs across independent server processes, replicates operations through a majority quorum, and continues serving requests after leader changes as long as a majority of replicas can communicate.

Clients can `Put`, `Get`, and `Append` values through a gRPC API. The client library discovers the active leader through retries, while the servers use request identifiers and cached results to prevent retried operations from executing twice.

## Features

- Replicated in-memory key/value state backed by Raft
- Linearizable `Put`, `Get`, and `Append` operations
- Automatic leader rotation and retry logic in the client
- Per-RPC deadlines and explicit success, not-leader, and timeout statuses
- Exactly-once-style duplicate suppression using client IDs and sequence numbers
- Proposal-to-commit coordination between gRPC handlers and the Raft apply path
- Read-index fast path with quorum confirmation for linearizable local reads
- Concurrent request processing and synchronized state-machine application
- Recovery across leader failure, follower disconnection, and network partitions
- Latency and throughput benchmark programs with percentile reporting
- Optional OpenTelemetry tracing and structured spdlog output

## Operations

| Operation | Behavior |
| --- | --- |
| `Put(key, value)` | Creates or replaces the value for `key` |
| `Get(key)` | Returns the current value, or an empty string if the key is absent |
| `Append(key, value)` | Appends to the existing value, or creates the key if absent |

The API returns `KV_SUCCESS`, `KV_NOTLEADER`, or `KV_TIMEOUT`, allowing clients to distinguish successful operations from leader changes and quorum loss.

## Architecture

Each server process hosts two services:

- A Raft service on port `P` for consensus traffic between replicas
- A client-facing KV service on port `P + 1000`

A replicated write follows this path:

1. The KV handler checks its duplicate-result cache and verifies leadership.
2. The operation is serialized and proposed to the local Raft instance.
3. Raft replicates the command and commits it after majority acknowledgement.
4. The apply path updates the local key/value map in log order.
5. The waiting RPC handler receives the applied result and responds to the client.

Each client instance generates a random 64-bit client ID and monotonically increasing sequence numbers. Servers cache results by `(client_id, sequence_number)`, so a timeout and retry cannot apply the same operation twice.

`Get` uses a quorum-confirmed read index when possible, avoiding a full log entry while ensuring an isolated former leader cannot return stale data.

## Technology

- C++20
- gRPC and Protocol Buffers
- Raft consensus
- CMake
- spdlog
- GoogleTest
- Python and matplotlib for benchmark plots
- OpenTelemetry instrumentation (optional)

## Prerequisites

The provided scripts target Ubuntu 22.04 or 24.04. The project expects:

- CMake 3.22.1 or newer
- GCC/G++ 13 or newer
- gRPC and Protocol Buffers installed under `$HOME/.local`
- Git submodules for spdlog, GoogleTest, and the tracing integration

Initialize the dependencies from the repository root:

```bash
git submodule update --init --recursive
./setup.sh
```

If gRPC is not already installed in `$HOME/.local`, the provided Ubuntu helper installs the required gRPC fork and Paho MQTT dependency:

```bash
./scripts/install_gcc-13.sh  # Only if GCC 13 is unavailable
./scripts/install_grpc.sh
```

The installation scripts use `sudo` for system packages. `install_grpc.sh` also recreates `$HOME/grpc` before installing into `$HOME/.local`.

## Build

From the repository root:

```bash
cmake -S . -B build -DTRACING=OFF -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

Application binaries are generated under `build/app/`.

## Run the Service

The included latency runner is the quickest end-to-end demonstration. It starts a local three-replica cluster, issues 1,000 sequential `Put` requests, prints average/p50/p99 latency, and shuts the cluster down:

```bash
cd build/app
./latency
```

The reusable client API is defined in `inc/kv/kv_client.hpp`. Given the three default Raft ports `50050` through `50052`, clients connect to the corresponding KV addresses:

```cpp
std::vector<std::string> addresses = {
    "localhost:51050",
    "localhost:51051",
    "localhost:51052",
};

kv::KvClient client(addresses);
client.put("user:42", "Ada");
client.append("user:42", " Lovelace");
auto [status, value] = client.get("user:42");
```

## Benchmarks

Run the throughput benchmark with a maximum client count and a percentage of writes. The benchmark doubles concurrency from one client up to the requested maximum, pre-populates 1,000 keys, and records average/p50/p90/p99 latency and throughput:

```bash
cd build/app
./tput 64 50
```

Optional arguments select the replica count and output file:

```bash
./tput 64 10 5 result-5-replicas.txt
```

Generate a latency-throughput plot from a result file:

```bash
python3 ../../bench/lat-tput.py \
  --input result.txt \
  --output ../../bench/lat-tput.png
```

The plotting script requires matplotlib. Existing benchmark measurements and plots are available under `bench-results/` and `bench/`.

## Tests

The KV integration suite covers basic operations, multiple keys, concurrent requests, append linearizability, leader failure, and stale-read prevention during partitions:

```bash
cd build/integration_tests
./kv_test
```

The underlying Raft election and replication suite is also included:

```bash
./raft_test
```

## Repository Layout

| Path | Contents |
| --- | --- |
| `inc/kv/kv_server.hpp` | KV RPC handlers, state machine, deduplication, and apply coordination |
| `inc/kv/kv_client.hpp` | Retrying client with leader rotation and request IDs |
| `inc/rafty/` and `src/` | Raft consensus implementation |
| `proto/kv.proto` | Client-facing KV service definition |
| `proto/raft.proto` | Inter-replica consensus protocol |
| `app/kv_node.cpp` | Combined Raft and KV server process |
| `app/latency.cpp` | Sequential unloaded-latency benchmark |
| `app/tput.cpp` | Concurrent mixed-workload throughput benchmark |
| `bench/` | Benchmark plotting utility and generated charts |
| `integration_tests/` | Raft and KV failure-scenario tests |

## Scope

Data and duplicate-result caches are held in memory. Persistent storage, crash recovery, snapshots/log compaction, authentication, encryption, and dynamic cluster membership are outside the current scope.
