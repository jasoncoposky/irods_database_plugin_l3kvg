# iRODS L3KVG Database Plugin: Legacy Compatibility & Production Readiness Plan

## Objective
To mature the L3KVG database plugin from an experimental graph-backend prototype into a production-ready system. This requires closing the gap between the legacy GenQuery1 (GQ1) expectations of the iRODS server (which relies on relational columns and `lexical_cast` conversions) and the GenQuery2 (GQ2) native graph capabilities of the plugin. The ultimate goal is **100% comprehensive support** for all legacy GenQuery1 operations.

## Background & Motivation
Currently, standard iRODS tools like `ils` crash with `INVALID_LEXICAL_CAST: SYS_INTERNAL_ERR`. This occurs because the legacy iRODS server requests a predefined set of columns (including IDs, mode bits, and resource metadata) and crashes if the database plugin returns empty strings instead of valid integers. While the core graph traversal engine (L3KV) and the GQ2 compiler are functional, the GQ1 emulation bridge requires strict schema alignment to prevent these server-side panics.

## Phased Implementation Plan

### Phase 1: Core Filesystem Viability (Unblocking `ils`, `iput`, `iget`)
1. **Core Schema Audit**: Systematically map the critical legacy columns used by core `icommands` (Data, Collection, Resource, User, Zone) against the `COLUMN_MAP` in `gq2_compiler.cpp`.
2. **Type-Aware Packing**: Enhance `gq1_bridge.cpp` to correctly identify integer vs. string columns. Instead of returning `\0` for missing integer fields (which causes `lexical_cast` to crash), the bridge will inject `"0"` or `"-1"`.
3. **C-String Buffer Alignment**: Fix the buffer allocation in `pack_gq1_results` to ensure `sqlResult->len` accurately reflects the packed string length or properly padded buffers, preventing trailing garbage during server post-processing.
4. **Verification**: Successfully run `ils -l` and `ils -L` without server crashes.

### Phase 2: Comprehensive Path & Starting Node Resolution
1. **Advanced Graph Walking**: Enhance the `resolve_path` method in `catalog_facade.cpp` to accurately resolve Collections, Data Objects, and Replicas by traversing the graph hierarchy.
2. **Context-Aware Synthesis**: Update `synthesize_gq2_ast` to smartly choose the starting node based on the most specific filter (e.g., if a query filters by `DATA_ID`, use that as the starting node instead of walking the path from the Zone root).

### Phase 3: Comprehensive Legacy Coverage (100% Mapping)
1. **Metadata & ACLs**: Implement GQ1 translation for AVUs (`COL_META_DATA_ATTR_NAME`, etc.) and permissions (`COL_DATA_ACCESS_NAME`), mapping them to Graph properties and edges.
2. **Advanced Subsystems**: Systematically map the remaining `rodsGenQuery.h` indices covering Tickets, Quotas, Rules, and Audit trails into corresponding Graph structures.
3. **Verification**: Successfully run `imeta ls`, `ils -A`, and ensure all edge-case GQ1 queries map correctly to GQ2 traversals.

### Phase 4: Hardening & Distributed Testing
1. **Asynchronous Consistency**: Replace blocking `client_->put_node_async(…).get()` calls with proper transactional or batch commits within the plugin to reduce latency during bulk operations.
2. **Cluster Mode Testing**: Spin up a multi-node L3KVG cluster and verify that the plugin can resolve paths and execute queries when collections and data objects reside on different physical graph shards.

## Alternatives Considered
* **Abandoning GQ1**: We could force all clients to use GenQuery2. While cleaner, this would break 20 years of existing iRODS scripts, UIs, and `icommands`. The emulation bridge is deemed a necessary "compatibility tax."
* **Full SQL Parser**: Writing a full SQL parser inside the plugin to handle raw GQ1 strings. This is unnecessarily complex, as the integer-based `genQueryInp_t` structure provides enough semantic meaning to synthesize an AST.

## Verification & Testing
* The primary metric of success is the passing of the standard iRODS test suite (`python3 run_tests.py --run_specific_test test_ils`).
* Load testing will be performed using `engine_bench` to ensure the bridge overhead does not negate the performance benefits of the graph engine.