<p align="center">
  <h1 align="center">⚡ CELRIX</h1>
  <p align="center"><strong>High-Performance In-Memory Cache Database</strong></p>
  <p align="center">5-10x Faster Redis Alternative • Lock-Free Architecture • AI-Enhanced Features</p>
</p>

<p align="center">
  <a href="#features"><img src="https://img.shields.io/badge/Status-In%20Development-yellow?style=flat-square" alt="Status"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue?style=flat-square" alt="License"></a>
  <a href="#"><img src="https://img.shields.io/badge/Language-Rust-orange?style=flat-square" alt="Language"></a>
</p>

---

## 🎯 Overview

**CELRIX** is a next-generation in-memory cache database built from the ground up for maximum throughput and minimal latency. Designed as a drop-in Redis replacement, CELRIX achieves breakthrough performance through innovative lock-free architecture and a custom binary protocol.

### Key Metrics

| Metric | Target |
|--------|--------|
| **Throughput** | 1M+ ops/sec |
| **P99 Latency** | <5ms |
| **Protocol Efficiency** | 22-byte frames (vs Redis RESP 35+ bytes) |

---

## ✨ Features

### Core Cache (Phase 1)
- ⚡ **VCP Protocol** - Custom binary protocol with 22-byte frames
- 🔑 **Basic Commands** - PING, GET, SET, DEL, EXISTS
- ⏰ **TTL Support** - Automatic key expiration with background cleaner
- 📊 **Metrics** - Operations counters and latency measurement
- 🖥️ **CLI Client** - Built-in test client

### High Concurrency (Phase 2)
- 🧵 **Multi-core Workers** - One acceptor + N worker executors pinned to cores
- 🔓 **Lock-Free Data Structures** - RCU HashMap for zero-contention reads
- 📬 **MPMC Queues** - Bounded command routing between tasks
- 🚫 **Zero-Allocation Hot Path** - Pooled buffers and pre-allocated frames

### Advanced Performance (Phase 3)
- 🔌 **Connection Multiplexing** - Handle thousands of concurrent connections
- 📦 **Request Pipelining** - Batch multiple commands per round-trip
- 🔢 **Extended Commands** - SCAN, MGET/MSET, INCR/DECR
- 🧹 **Smart Eviction** - LRU/LFU policies with configurable memory limits

### AI & Vector Add-ons (Phase 4)
- 🧠 **Embedding Store** - Store and retrieve vector embeddings
- ⚡ **SIMD Acceleration** - AVX2-optimized similarity search
- 🔍 **Semantic Caching** - Query by similarity with configurable thresholds
- 🔗 **External Integration** - HTTP/gRPC hooks for LLM/embedding services

### Production Ready (Phase 5)
- 💾 **Persistence** - RDB-style snapshots and AOF write-ahead logging
- 📈 **Observability** - Prometheus metrics, detailed tracing
- ⚙️ **Admin API** - Hot configuration, policy changes, health monitoring
- 🔒 **Battle-Tested** - Fuzzing, chaos testing, load benchmarks

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CELRIX Server                            │
├─────────────────────────────────────────────────────────────────┤
│  ┌───────────┐    ┌─────────────┐    ┌─────────────────────┐   │
│  │  Network  │───▶│  VCP Parser │───▶│   Command Router    │   │
│  │  (Tokio)  │    │  (22-byte)  │    │   (MPMC Queue)      │   │
│  └───────────┘    └─────────────┘    └──────────┬──────────┘   │
│                                                  │              │
│  ┌──────────────────────────────────────────────▼──────────┐   │
│  │                   Worker Pool (N cores)                  │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐       │   │
│  │  │Worker 0 │ │Worker 1 │ │Worker 2 │ │Worker N │       │   │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘       │   │
│  └───────┼───────────┼───────────┼───────────┼─────────────┘   │
│          │           │           │           │                  │
│  ┌───────▼───────────▼───────────▼───────────▼─────────────┐   │
│  │              Lock-Free RCU HashMap                       │   │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐           │   │
│  │  │Shard 0 │ │Shard 1 │ │Shard 2 │ │Shard N │           │   │
│  │  └────────┘ └────────┘ └────────┘ └────────┘           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │ TTL Cleaner │  │  Persistence │  │  Vector Store (AI)   │   │
│  │ (Background)│  │  (RDB/AOF)   │  │  (SIMD Accelerated)  │   │
│  └─────────────┘  └──────────────┘  └──────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔧 VCP Protocol (Velocity Cache Protocol)

CELRIX uses a custom binary protocol optimized for performance:

### Frame Format

```
┌──────────────────────────────────────────────────────────────┐
│                      VCP Frame (22 bytes header)             │
├──────────┬──────────┬──────────┬──────────┬─────────────────┤
│  Magic   │ Version  │  OpCode  │  Flags   │  Payload Len    │
│ (4 bytes)│ (1 byte) │ (1 byte) │ (2 bytes)│   (4 bytes)     │
├──────────┴──────────┴──────────┴──────────┴─────────────────┤
│  Request ID (8 bytes)  │  Reserved (2 bytes)                │
├─────────────────────────────────────────────────────────────┤
│                     Payload (variable)                       │
│               (length-prefixed arguments)                    │
└─────────────────────────────────────────────────────────────┘
```

### Supported Operations

| OpCode | Command | Description |
|--------|---------|-------------|
| 0x01 | PING | Health check |
| 0x02 | GET | Retrieve value by key |
| 0x03 | SET | Store key-value pair |
| 0x04 | DEL | Delete key |
| 0x05 | EXISTS | Check key existence |
| 0x10 | MGET | Multi-get |
| 0x11 | MSET | Multi-set |
| 0x20 | INCR | Atomic increment |
| 0x21 | DECR | Atomic decrement |
| 0x30 | SCAN | Iterate keyspace |
| 0x40 | VSIM | Vector similarity search |

---

## 🚀 Quick Start

### Prerequisites

- Rust 1.75+ (with cargo)
- Linux/macOS (Windows support planned)

### Build from Source

```bash
# Clone the repository
git clone https://github.com/YASSERRMD/celrix.git
cd celrix

# Build release binary
cargo build --release

# Run the server
./target/release/celrix-server --port 6380

# In another terminal, use the CLI client
./target/release/celrix-cli --host 127.0.0.1 --port 6380
```

### Basic Usage

```bash
# Connect to CELRIX
celrix-cli> PING
PONG

# Set a value (with optional TTL in seconds)
celrix-cli> SET mykey "Hello, CELRIX!" 3600
OK

# Get a value
celrix-cli> GET mykey
"Hello, CELRIX!"

# Check existence
celrix-cli> EXISTS mykey
1

# Delete a key
celrix-cli> DEL mykey
1
```

---

## 📊 Benchmarks

*Coming soon - benchmarks comparing CELRIX against Redis, KeyDB, and Dragonfly*

### Planned Test Scenarios

- **Throughput**: Single-node ops/sec with varying payload sizes
- **Latency**: P50/P95/P99 under different load patterns
- **Scalability**: Performance vs core count
- **Memory Efficiency**: Overhead per key-value pair

---

## 🛠️ Configuration

```toml
# celrix.toml

[server]
bind = "0.0.0.0"
port = 6380
workers = 0  # 0 = auto-detect CPU cores

[memory]
max_memory = "8GB"
eviction_policy = "lru"  # lru, lfu, random, none

[persistence]
enabled = true
snapshot_interval = 300  # seconds
aof_enabled = true
aof_fsync = "everysec"  # always, everysec, no

[networking]
tcp_keepalive = 300
max_connections = 10000
pipeline_limit = 1000

[ai]
enabled = false
embedding_dim = 1536
similarity_threshold = 0.85
```

---

## 🗺️ Roadmap

- [x] **Phase 0**: Project Setup & Documentation
- [ ] **Phase 1**: Minimal Core Cache (VCP + Basic Commands)
- [ ] **Phase 2**: Concurrency & Lock-Free Architecture
- [ ] **Phase 3**: Advanced Performance Features
- [ ] **Phase 4**: AI & Vector Capabilities
- [ ] **Phase 5**: Production Reliability

---

## 🤝 Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

### Development Setup

```bash
# Clone and setup
git clone https://github.com/YASSERRMD/celrix.git
cd celrix

# Run tests
cargo test

# Run benchmarks
cargo bench

# Check formatting
cargo fmt --check

# Run clippy
cargo clippy -- -D warnings
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- Inspired by Redis, KeyDB, and Dragonfly
- Built with [Tokio](https://tokio.rs/) for async I/O
- Uses [crossbeam](https://github.com/crossbeam-rs/crossbeam) for lock-free data structures

---

<p align="center">
  <sub>Built with ❤️ and Rust</sub>
</p>
