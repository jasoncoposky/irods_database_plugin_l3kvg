# iRODS L3KVG Database Plugin

A high-performance, distributed database plugin for iRODS 5+ based on the L3KVG Actor-Model Property Graph Engine. This plugin replaces traditional relational SQL databases with a shared-nothing distributed graph fabric.

## Architectural Pillars

This implementation is 100% compliant with the L3KVG architectural whitepaper, focusing on the following six core pillars:

1. **Concurrency & Actor-Model:** Eliminates OS-level file locking by utilizing a sharded actor-model message queue. Mutations are isolated to specific hardware threads, ensuring lock-free concurrency.
2. **Distributed Catalog:** Supports transparent horizontal scaling using ZeroMQ, Consistent Hashing, and Hybrid Logical Clocks (HLC). Every iRODS server can act as a shard owner in a shared-nothing topology.
3. **Fluent Querying:** Features a native GenQuery2-to-Graph compiler (`Gq2ToL3kvgCompiler`) that translates iRODS GenQuery2 AST nodes directly into L3KVG fluent traversal pipelines, rendering relational SQL generation obsolete.
4. **Zero-Copy Performance:** Eradicates the "Copy Tax" by utilizing `std::string_view`, PMR, and Move Semantics. Data flows from legacy iRODS structs into the graph engine without intermediate heap allocations or memory copies.
5. **PIMPL Facade Isolation:** Enforces a strict boundary between the legacy iRODS C-API and the modern C++20 engine. The PIMPL (Pointer to Implementation) pattern prevents dependency bleeding and ensures high-speed compilation.
6. **Unified Principal Model:** Leverages graph edges (`MEMBER_OF`) to unify Users and Groups. Permission resolution is performed via graph traversals, allowing for complex, nested membership and rapid authorization checks.

## Implemented Functionality

The L3KVG plugin provides an exhaustive implementation of the iRODS database interface:

### 1. Identity & Group Management
- **Users:** Full lifecycle (registration, modification, deletion). Supports privilege levels and passwords.
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
- **Tokens:** registration and deletion of system-level tokens.
- **Grid Config:** Set and get global grid configuration values.

### 7. GenQuery2 Integration
- **Direct AST Translation:** Native compiler translates GenQuery2 Select ASTs into graph-native JSON DSL.
- **Advanced Features:** Supports aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`), `DISTINCT` flag, complex `OR/AND` logic, and multi-hop traversals across the catalog schema.

## Configuration

The plugin is configured via the `plugin_configuration` section in the iRODS `server_config.json`:

```json
{
    "plugin_configuration": {
        "database": {
            "l3kvg": {
                "plugin_specific_configuration": {
                    "db_path": "/var/lib/irods/catalog.l3kvg",
                    "node_id": 1,
                    "zmq_endpoint": "tcp://127.0.0.1:5555",
                    "federation": [
                        {
                            "name": "eu-west",
                            "id": 100,
                            "endpoint": "tcp://10.0.1.5:5555"
                        }
                    ]
                }
            }
        }
    }
}
```

- **`db_path`**: Local path for the L3KV graph store and WAL.
- **`node_id`**: Unique ID for this server within the cluster.
- **`zmq_endpoint`**: The endpoint for the local ZMQ server.
- **`federation`**: List of remote zones to connect to for horizontal scaling.

## Building & Testing

### Requirements
- C++20 compatible compiler (Clang 13+ or GCC 11+)
- CMake 3.20+
- ZeroMQ (libzmq)

### Build
```bash
mkdir build
cd build
cmake ..
make -j$(nproc)
```

### Run Tests
```bash
cd build
# Run all tests
for f in test_*; do [ -x "$f" ] && ./$f; done
```

## Implementation Details

- **`src/db_plugin.cpp`**: Native iRODS database plugin entry point.
- **`src/catalog/catalog_facade.cpp`**: Orchestrates graph operations and Snowflake ID resolution.
- **`src/compiler/gq2_compiler.cpp`**: Translates iRODS GenQuery2 AST to L3KVG traversal pipelines.
- **`include/irods/catalog/binary_key.hpp`**: Handles Snowflake ID generation and binary packing for indices.
