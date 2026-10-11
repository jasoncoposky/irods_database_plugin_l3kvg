# iRODS L3KVG Database Plugin

A high-performance, distributed database plugin for iRODS 5+ based on the L3KVG Actor-Model Property Graph Engine. This plugin replaces traditional relational SQL databases (PostgreSQL/MySQL/Oracle) with a shared-nothing distributed graph fabric powered by the **Bifurcated Native Graph Substrate**.

---

## ⚡ Live Performance: L3KVG vs. PostgreSQL 16

Empirical head-to-head performance evaluation executing identical iRODS macro-workloads in a dual-cluster Ubuntu 24.04 LTS containerized environment across 3 complete iterations of the **1K Scale Tier** (1,000 data objects, 50 collections, depth 2, 300 queries per pass):

| Workload | PostgreSQL Mean (ms) | PostgreSQL P95 (ms) | L3KVG Mean (ms) | L3KVG P95 (ms) | Speedup Factor | Superior Backend |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **`imeta qu` (AVU Exact Match)** | 144.36 ms | 155.35 ms | **86.67 ms** | **93.18 ms** | **1.67x** | 🏆 **L3KVG** |
| **`imeta add` (Metadata Ingestion)** | 148.85 ms | 165.83 ms | **90.09 ms** | **100.39 ms** | **1.65x** | 🏆 **L3KVG** |
| **Branching Join (`iquest` obj stat)** | 148.57 ms | 159.95 ms | **98.51 ms** | **105.85 ms** | **1.51x** | 🏆 **L3KVG** |
| **`imkdir` (Tree Hierarchy Ingestion)** | 169.07 ms | 187.25 ms | **119.34 ms** | **133.42 ms** | **1.42x** | 🏆 **L3KVG** |
| **Overall Catalog Queries** | 147.91 ms | 160.42 ms | **110.60 ms** | **151.45 ms** | **1.34x** | 🏆 **L3KVG** |
| **Wildcard Scan (`iquest like`)** | 150.81 ms | 165.84 ms | **146.61 ms** | **159.70 ms** | **1.03x** | 🏆 **L3KVG** |
| **Bulk Registration (`iput -b -r`)** | **22.83 ms** | **23.22 ms** | 38.94 ms | 40.46 ms | 0.59x | PostgreSQL |
| **Hierarchical Listing (`ils -r`)** | **352.82 ms** | **396.86 ms** | 462.54 ms | 514.50 ms | 0.76x | PostgreSQL |
| **Cleanup (`irm -rf` & `irmtrash`)** | 12,788 ms | 13,237 ms | 12,983 ms | 13,753 ms | 0.98x | Parity (~1.0x) |

*Full percentile distributions, raw datasets, and footprint profiling are recorded in [`benchmarks/benchmark_results_bifurcated_substrate_1k.json`](benchmarks/benchmark_results_bifurcated_substrate_1k.json).*

### Why L3KVG Dominates Graph & Metadata Operations:
1. **Zero Relational Join Overhead:** Multi-hop branching queries (e.g. resolving a `DataObject`'s replicas via `HAS_REPLICA` while joining parent `Collection` metadata via `CONTAINS`) execute in **98.51 ms vs 148.57 ms** (**1.51x speedup**). Instead of executing costly SQL hash/merge joins across `R_DATA_MAIN`, `R_RESC_MAIN`, and `R_COLL_MAIN`, the plugin resolves edges in-memory in **< 18.4 ns / hop**.
2. **Native Property Graph Metadata:** AVU additions (`imeta add`) and queries (`imeta qu`) are **1.65x–1.67x faster**. Properties reside directly on entity vertices and secondary index keys, bypassing relational link tables (`R_META_MAIN`, `R_OBJT_METAMAP`).
3. **Hardware-Sympathetic Ingestion:** Directory tree creation (`imkdir`) reaches **8.38 ops/sec vs 5.91 ops/sec** (**1.42x speedup**) via lock-free actor thread isolation and compact WAL appending.

---

## 🏛️ Architectural Innovations

### 1. Bifurcated Native Graph Substrate
The plugin interfaces with L3KVG's bifurcated graph substrate, separating topological edge traversal from heavy document storage:
- **`PackedEdge` (24 bytes):** Natural 8-byte alignment containing `target_id` (64-bit), `label_id` (16-bit dictionary token), `flags` (16-bit), and `weight` (32-bit float).
- **`VertexRecord` (32 bytes):** Direct small-degree inlining for vertices with $\le 1$ edge, eliminating pointer indirections for leaf nodes.
- **Slotted `AdjacencyPage` (4,096 bytes):** Dynamic 4KB cache-aligned pages holding up to 170 packed edges per page, chained dynamically for high-fanout supernodes.
- **SIMD Predicate Filter Engine (`SIMDFilter`):** Branchless AVX2 vectorized evaluation (`_mm256_i32gather_epi32`, `_mm256_cmp_ps`) capable of scanning 1,000 edges in ~473 ns.
- **Compact Binary Edge WAL (`BinaryEdgeWAL`):** 32-byte binary mutation structs appended directly to disk with 64KB user-space buffering and 64KB chunked sequential crash recovery.

### 2. Multi-Mutation IPC Batching
Collapses 15–20 synchronous roundtrips per registered data object into **1 single atomic batch**:
- Aggregates node payloads, containment edges (`CONTAINS`), replica pointers (`HAS_REPLICA`), and ACL tokens (`HAS_ACCESS`) into a columnar `MutationBatch`.
- Transmitted over ZeroMQ using opcode `"B"`.
- Applied by the server engine in a single sequential pass with all-or-nothing consistency.

### 3. Pure-Binary Zero-Copy Protocol (`lite3cpp::Buffer`)
All inter-process communication (IPC) and on-disk payloads use native binary `lite3cpp::Buffer`:
- Zero string serialization or JSON parsing on the hot path.
- In-place field updates without re-allocating memory.
- Move semantics across the legacy iRODS C-API boundary.

### 4. GenQuery2 Compiler & GenQuery1 Bridge
- **`Gq2ToL3kvgCompiler`:** Translates iRODS GenQuery2 AST syntax trees directly into L3KVG fluent traversal pipelines. Supports aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`), `DISTINCT`, boolean trees (`AND`/`OR`/`NOT`), and multi-hop schema traversals.
- **GenQuery1 Bridge:** Provides seamless backward compatibility for legacy icommands (`ils`, `imeta`, `iquest`, `irm`) by translating legacy GenQuery1 column tokens (`COL_COLL_NAME`, `COL_DATA_NAME`, `COL_META_DATA_ATTR_NAME`) into GenQuery2 AST equivalents.

---

## 🧩 Implemented Functionality

The L3KVG plugin provides an exhaustive implementation of the iRODS database interface:

### 1. Identity & Group Management
- **Users:** Full lifecycle (registration, modification, deletion). Supports privilege levels and authentication.
- **Groups:** Membership managed via `MEMBER_OF` edges. Supports `add_user_to_group` and `remove_user_from_group`.
- **Authorization:** `check_auth` and `check_auth_credentials` implemented using graph-native lookups.

### 2. Collection & Data Object Lifecycle
- **Collections:** Full registration, renaming, modification, and deletion. Hierarchy managed via `CONTAINS` edges.
- **Data Objects:** Full registration, renaming, move, and deletion. Automated cascading deletion of replicas.
- **Path Resolution:** Integrated Proxy Indexing for O(1) resolution of full paths to graph Snowflake IDs.

### 3. Replica & Resource Management
- **Replicas:** Registration and unregistration. Linked to data objects via `HAS_REPLICA` and to resources via `STAYING_AT`.
- **Resources:** Support for flat and hierarchical topologies.
- **Hierarchies:** `add_child_resource`, `remove_child_resource`, and automated hierarchy path reconstruction (`get_hierarchy_for_resc`).
- **Monitoring:** Integrated server load tracking and resource object counts.

### 4. Metadata (AVUs)
- **Comprehensive Ops:** `add`, `delete`, `modify`, `copy`, and `set` metadata for both Data Objects and Collections.
- **Graph Mapping:** Entities are linked to AVU nodes via `ANNOTATED_WITH` edges, allowing for shared metadata nodes and efficient attribute-based discovery.

### 5. Access Control (ACLs)
- **Permission Model:** Mapped to graph-native edges (`HAS_ACCESS` and `FOR_OBJECT`).
- **Granular Checks:** `check_permission` and `check_permission_to_modify_data_object` support user and group-based authorization via recursive traversal.

### 6. System Operations
- **Quotas:** Physical and Logical (Collection-based) quota management.
- **Sequences:** Distributed sequence management for legacy numeric IDs.
- **Tokens:** Registration and deletion of system-level tokens.
- **Grid Config:** Set and get global grid configuration values.

---

## ⚙️ Configuration

The plugin is configured via the `plugin_configuration` section in the iRODS `server_config.json`:

```json
{
    "plugin_configuration": {
        "database": {
            "postgres": {
                "technology": "postgres",
                "uses_odbc": false,
                "plugin_specific_configuration": {
                    "db_path": "/var/lib/irods/l3kvg_db",
                    "node_id": 1,
                    "cluster_id": 521,
                    "zmq_endpoint": "tcp://127.0.0.1:5556",
                    "federation": [
                        {
                            "name": "eu-west",
                            "id": 100,
                            "endpoint": "tcp://10.0.1.5:5556"
                        }
                    ]
                }
            }
        }
    }
}
```

- **`db_path`**: Local path for the L3KV storage file/directory and binary WAL.
- **`node_id`**: Unique node ID for Snowflake ID generation and HLC timestamps.
- **`cluster_id`**: Unique cluster identifier for cross-zone federation.
- **`zmq_endpoint`**: The endpoint for the local `l3kvg_server` ZeroMQ ROUTER daemon.
- **`federation`**: List of remote zones and endpoints to connect to for horizontal scaling.

---

## 🛠️ Building & Testing

### Prerequisites
- C++20 compatible compiler (Clang 13+ or GCC 11+)
- CMake 3.20+
- ZeroMQ (`libzmq3-dev`, `cppzmq-dev`)
- OpenSSL (`libssl-dev`)
- iRODS 5.1.0 development headers and runtime packages

### Build
```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Run Unit & Integration Tests
```bash
cd build
# Run GoogleTest suites
./test_plugin_genquery2
./test_plugin_data
./test_plugin_collections
./test_plugin_metadata
./test_plugin_acls
./test_gq1_bridge
```

### Run Comparative Benchmark Harness
```bash
# Smoke test (small tier)
python3 benchmarks/bench_catalog_comparison.py --tiers test_small --backends both --dry-run

# 1K Macro Benchmark
python3 benchmarks/bench_catalog_comparison.py --tiers 1k --backends both --iterations 3 --output benchmarks/benchmark_results.json
```

---

## 📄 License

See [LICENSE](LICENSE) for details.
