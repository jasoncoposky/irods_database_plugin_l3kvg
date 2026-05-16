# L3KVG Smart Client DB Plugin Design

## Goal
To build a high-performance, federation-ready iRODS database plugin that acts as a thin "Smart Client" to a standalone L3KVG Graph Cluster, leveraging zero-copy BSON buffers and hardware-level data locality.

## Architecture
The design shifts the iRODS database layer from an embedded model to a distributed client-server model. Each `irodsAgent` process hosts a stateless Smart Client that manages routing and communication with an L3KVG cluster.

### Key Components
1.  **Binary Composite Key:** A packed byte structure `[4-byte ZoneHash][1-byte Type][8-byte ObjectID]`.
    *   **Locality:** Groups all metadata for a specific Zone and Entity Type (e.g., all DataObjects in `tempZone`) contiguously on disk.
    *   **Performance:** Enables hardware-level prefix scans and ultra-fast range queries.
2.  **Smart Routing (Consistent Hashing):** Uses the `ClusterResolver` to map the ZoneHash to a specific physical node in the L3KVG cluster, enabling deterministic routing and linear scaling.
3.  **Zero-Copy BSON Pipeline:** iRODS internal structures are serialized directly into `lite3::Buffer` objects. These buffers are wrapped by ZeroMQ messages and sent over the wire without intermediate memory copies.
4.  **Predicate Push-down:** Filtering logic and prefix scans are executed on the L3KVG server nodes. The client sends the query AST or BSON filters, and receives only the relevant result set.
5.  **Federated Awareness:** The `ZoneHash` prefix allows the client to naturally route queries across federated boundaries, following edges between local and remote nodes.

## Technical Stack
*   **Transport:** ZeroMQ (IPC for local, TCP for cluster).
*   **Serialization:** `lite3::Buffer` (BSON-native, zero-copy).
*   **Hashing:** XXH3 for Zone and Type identifiers.
*   **Concurrency:** Stateless client model to support iRODS' fork-per-agent architecture without shared memory exhaustion.

## Implementation Phases
### Phase 1: Key & Buffer Unification
*   Implement the `BinaryCompositeKey` generator.
*   Refactor `CatalogFacade` to use `lite3::Buffer` exclusively for all internal iRODS types.

### Phase 2: Standalone L3KVG Server
*   Implement a minimal `l3kvg-server` daemon that hosts the `l3kvg::Engine`.
*   Establish the ZeroMQ command listener (PUT, GET, SCAN, QUERY).

### Phase 3: Smart Client Plugin
*   Replace the embedded `l3kvg::Engine` in the DB Plugin with `RemoteL3KVClient`.
*   Implement `ClusterResolver` integration for multi-node routing.
*   Verify performance with contiguous prefix scans for `ils`.
