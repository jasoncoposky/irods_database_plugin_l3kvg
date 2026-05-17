# Phase 3: Unified Mesh Federation Integration

## Objective
Implement the iRODS Database Plugin side of the "Unified Mesh" federation architecture. In this design, the underlying L3KVG engine handles all cross-cluster routing and traversal suspension. The DB Plugin acts as a Smart Client that constructs Snowflake IDs and establishes "Anchor" nodes for remote zones, presenting a single, global namespace to the iRODS server.

## 1. Engine-Level Federation Handoff
Remove the `FederationResolver` logic from the iRODS plugin. The plugin will no longer manage ZMQ endpoints or routing decisions for remote clusters.
*   **Action:** The plugin's `init()` method will pass the federation topology (read from `server_config.json`) directly to the local L3KVG engine during connection setup.
*   **Result:** All `RemoteL3KVClient` operations (put, get, match) are sent *only* to the local cluster, which natively routes requests across the mesh using the top 16 bits of the Snowflake ID.

## 2. Snowflake ID Generation (Deterministic)
The plugin must reliably map string-based `zone_name` to a 16-bit Cluster ID to ensure consistent 64-bit ID generation across the grid.
*   **Implementation:** `uint16_t cluster_id = XXH3_32bits(zone_name) & 0xFFFF;`
*   Every entity (User, DataObject, Collection) registered by the plugin will use this generated `cluster_id` in its Snowflake ID.

## 3. Remote Zone Anchoring (Bootstrapping)
When a foreign zone is federated, the iRODS plugin must "surface" it to the local namespace.
*   **Action:** Modify `bootstrap_catalog()` (or a new `bootstrap_federation()` method) to create a "Proxy Zone Node" for each federated peer.
*   **Process:**
    1.  Calculate the foreign zone's Snowflake ID: `id = (foreign_cluster_id << 48) | hash(foreign_zone_name)`.
    2.  Write a minimal BSON node to the *local* cluster representing this foreign zone (e.g., `{"n": "eu-west", "t": "remote"}`).
    3.  Create index proxy nodes pointing to this Snowflake ID.
*   **Benefit:** Queries like `ils /` or GenQuery `SELECT ZONE_NAME` will find the remote zone locally, allowing traversals to begin, which the engine will subsequently suspend and route.

## 4. Cross-Zone Edges & GenQuery
Ensure the plugin uses the engine's capability for cross-zone edge creation and traversal.
*   **Edge Creation:** When creating an ACL spanning zones, the plugin calculates the Snowflake IDs of both the local User and the remote DataObject and issues a standard `add_edge` command to the local engine.
*   **GenQuery Ast:** The `Gq2ToL3kvgCompiler` remains unaware of federation. It compiles standard Cypher-style ASTs. The local L3KVG engine handles the execution, scattering the query across the mesh and returning the unified result set to the plugin.
