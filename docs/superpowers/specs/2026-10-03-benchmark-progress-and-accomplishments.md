# Progress & Accomplishments: iRODS L3KVG vs PostgreSQL 16 Benchmark

This document tracks all project milestones, empirical benchmarks, engine optimizations, and architectural findings across the comparative evaluation of the **iRODS L3KVG graph catalog plugin** against **PostgreSQL 16**.

---

## 1. Executive Summary & Current Status

- **Automated Harness & Infrastructure:** Complete dual-stack testing environment provisioned on Ubuntu 24.04 via Docker containers. Comprehensive statistical benchmark framework with sub-millisecond monotonic timer accuracy, percentile rankings ($p_{50}, p_{95}, p_{99}$), and in-container streaming workers deployed.
- **Completed Scale Tiers:**
  - **Tier 1 (1,000 objects, 50 collections, depth 2):** 3 full iterations completed on both PostgreSQL 16 and L3KVG. Baselines committed to git (`3932052`).
  - **Tier 2 (10,000 objects, 200 collections, depth 5):** 3 full iterations completed on both backends. Preserved in `benchmark_full_results.json`.
  - **Tier 3 (50,000 objects, 1,000 collections, depth 10):** PostgreSQL completed end-to-end. L3KVG is currently at **36,608 / 50,000 objects registered (73.2% complete)**, progressing at ~2.9 ops/sec.
- **Engine Optimization Delivered:** Eliminated CPU-wasting sleep/yield polling in L3KV shard queues by introducing `moodycamel::BlockingConcurrentQueue` with event-driven wakeup. Rebuilt and live-deployed to the cluster (`4e9d7e4`, `0e4f5be`).
- **Memory & Storage Profiling:** Uncovered key drivers for L3KVG's memory scaling (~13.2 KB/object) and disk usage (7.5 GB vs PG's 86 MB), establishing an actionable optimization roadmap.

---

## 2. Key Accomplishments & Milestones

### Milestone 1: Automated Comparative Harness & Environment Staging
- **Dual-Stack Orchestration:** Standardized containerized deployments for both L3KVG (`ubuntu-2404-l3kvg-irods-catalog-provider-1`) and PostgreSQL 16 (`test-postgres-bench-irods-catalog-provider-1`) using `irods_testing_environment`.
- **Harness Framework:** Created [`benchmarks/bench_catalog_comparison.py`](file:///home/darkfell/dev/irods_database_plugin_l3kvg/benchmarks/bench_catalog_comparison.py) with 33 verified unit tests in [`benchmarks/test_bench_harness.py`](file:///home/darkfell/dev/irods_database_plugin_l3kvg/benchmarks/test_bench_harness.py).
- **In-Container Streaming Workers:** Eliminated host-to-container fork/exec spawn overhead by generating single-process streaming Python workers that communicate over container pipes.
- **Native Bulk Registration:** Integrated `iput -b -r` for 10K and 50K tiers to evaluate iRODS bulk protocol ingestion against catalog backends.

### Milestone 2: Core Engine Concurrency Optimization
- **Identified Polling Bottleneck:** Shard processing workers in `L3KV/src/engine/store.hpp` were executing `std::this_thread::sleep_for(std::chrono::microseconds(200))` and `std::this_thread::yield()` when queues were idle, creating artificial latency floors and wasting CPU cycles.
- **Architectural Solution:** Migrated the shard ingestion pipeline to `moodycamel::BlockingConcurrentQueue` with `wait_dequeue_timed(50ms)`:
  - Instantaneous wakeup upon enqueue (zero latency overhead on incoming requests).
  - Event-driven blocking when queues are empty.
  - Safe shutdown guarantees on engine destruction.
- **Verification & Deployment:** Passed full regression test suites (`test_store`, `test_wal`, `bench_engine_threaded`, `test_repro_imkdir`, `test_gq1_bridge`), rebuilt `libl3kvg.so` and `l3kvg_server`, and hot-reloaded the active cluster without catalog corruption.

### Milestone 3: Deep Memory Footprint & Storage Profiling
- **Database On-Disk Footprint:**
  - **PostgreSQL 16:** **86 MB** total on-disk directory size (`/var/lib/postgresql/data`) after 50,000 objects.
  - **L3KVG:** **7.5 GB** database size (`/var/lib/irods/l3kvg_db`).
  - *Root Cause:* L3KV uses append-only WAL logs and soft-deletion tombstones; deleted items are never reclaimed without an explicit background compaction pass.
- **Memory Scaling Profile:**
  - **PostgreSQL 16:** In-memory RSS remained bounded at **~28–35 MB** regardless of object scale.
  - **L3KVG Server:** Scales linearly at **~13.2 KB per object** (~60 MB @ 1K, ~170 MB @ 10K, ~470 MB @ 36.6K, ~650 MB projected @ 50K).
  - *Root Causes Identified:*
    1. **Key Multiplication:** Every logical object generates 18–22 distinct keys (node + replica + indexes + forward/reverse containment edges + ACLs).
    2. **Static Buffer Allocation:** `Blob(cap = 1024)` allocates 1,024 bytes minimum even for 2-byte edge payloads (`"{}"`).
    3. **Node Overhead:** `std::map<std::string, ...>` red-black tree nodes introduce ~200 bytes of pointer and string heap overhead per key.
    4. **Tombstone Persistence:** Deletions replace values with empty payloads rather than removing keys from the internal map.

### Milestone 4: Graph Key Modeling & Snowflake ID Architectural Clarity
- **Bidirectional Graph Indexing:** Documented why key-value graph stores require both `e:out:{src}:{label}:{dst}` and `e:in:{dst}:{label}:{src}` keys:
  - A key-value store only indexes keys in 1D lexicographical order from the leading prefix.
  - Forward keys enable $O(\log N + K)$ scans for folder contents (`src -> dst`).
  - Reverse keys enable $O(\log N)$ lookups for parent resolution and permission checks (`dst <- src`).
  - Without the reverse key, finding a parent from a child object would require an $O(N)$ full database scan across millions of keys.
- **Snowflake ID Demarcation:** Clarified that 64-bit Snowflake IDs (`[16-bit cluster][48-bit hash]`) resolve **federated cluster routing** and distributed uniqueness without coordination, but cannot eliminate bidirectional edge indexing because an edge links two distinct node IDs.

---

## 3. Empirical Performance Summary

| Workload | Tier 1 (1K) PG | Tier 1 (1K) L3KVG | Speedup (T1) | Tier 2 (10K) PG | Tier 2 (10K) L3KVG | Speedup (T2) | Tier 3 (50K) PG | Tier 3 (50K) L3KVG (In Prog) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`imkdir` (mean)** | 3.9 ms | 283.2 ms | **0.01×** | 166.7 ms (bulk) | 260.8 ms | **0.64×** | 166.7 ms | Completed (1,000 colls) |
| **Registration (`iput -b`)** | 208.9 ms | 398.8 ms | **0.52×** | 22.2 ms/obj (45.0 ops/s) | 197.1 ms/obj (5.07 ops/s) | **0.11×** | 23.2 ms/obj (43.1 ops/s) | ~2.9–3.2 ops/s (**73.2% complete**) |
| **Traversal (`ils -r`)** | 339.0 ms | 2,372.8 ms | **0.14×** | 956.1 ms | 26,904.9 ms (26.9s) | **0.04×** | 4,250.8 ms | Pending ingestion |
| **Metadata (`imeta add`)** | 134.4 ms | 185.0 ms | **0.73×** | 136.2 ms | 184.2 ms | **0.74×** | 143.4 ms | Pending ingestion |
| **GenQuery (Branching Join)**| 150.5 ms | 322.9 ms | **0.47×** | 142.5 ms | 4,674.3 ms (4.67s) | **0.03×** | 142.5 ms | Pending ingestion |
| **GenQuery (Wildcard Scan)** | 162.6 ms | 1,846.2 ms | **0.09×** | 243.7 ms | 637,958 ms (**10.6 min**) | **0.00×** | 425.6 ms | Pending ingestion |
| **Cleanup (`irm -rf`)** | 1,029.8 ms | 1,019.2 ms | **1.01×** | 158.4 s | 207.2 s | **0.76×** | 820.9 s | Pending ingestion |

---

## 4. Work in Flight & Next Steps

1. **Complete 50K Ingestion (L3KVG):**
   - 36,608 / 50,000 objects registered (13,392 remaining $\approx$ 70 minutes).
2. **Execute Downstream 50K Workloads:**
   - Recursive listing (`ils -r`), metadata addition sample, and GenQuery evaluation (~15 minutes).
3. **Capture Final Footprints & Metrics:**
   - Execute `measure_footprint()` for memory RSS and database disk usage across all tiers.
   - Commit complete multi-tier dataset (`1k`, `10k`, `50k`, `footprint`) to git.
4. **Compile Final Comparative Performance Report (Task 6):**
   - Synthesize scaling curves, latency percentiles, Mermaid diagrams, and architectural recommendations into [`docs/superpowers/specs/2026-10-02-l3kvg-vs-relational-performance-report.md`](file:///home/darkfell/dev/irods_database_plugin_l3kvg/docs/superpowers/specs/2026-10-02-l3kvg-vs-relational-performance-report.md).
