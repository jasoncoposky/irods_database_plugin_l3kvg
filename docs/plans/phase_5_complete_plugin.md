# Phase 5: Complete DB Plugin Implementation

## Objective
Implement all remaining iRODS database plugin operations as a Smart Client using the consolidated L3KVG ZeroMQ protocol. Align the ID generation with the new `l3kvg::FederationID` standard. Ensure comprehensive test coverage for data, metadata, and federation operations.

## 1. ID Generation Alignment
*   Remove the custom `SnowflakeID` structure from `binary_key.hpp`.
*   Adopt `l3kvg::FederationID::pack(cluster_id, local_hash)` to generate globally unique 64-bit Node IDs.
*   The `local_hash` will remain a 48-bit `xxHash` of the entity type and iRODS numerical ID.

## 2. Operation Implementation (CatalogFacade)
Implement the missing logic for the remaining iRODS operations using `l3kvg::RemoteL3KVClient`.

### Data Objects & Replicas
*   `db_reg_data_obj_op`: Create DataObject node.
*   `db_reg_replica_op`: Create Replica node and link to DataObject (`HAS_REPLICA`) and Resource (`STAYING_AT`).
*   `db_rename_object_op`: Update `n` (name) property and Proxy Index node.
*   `db_move_object_op`: Update Collection relationship (`CONTAINS`).
*   `db_unreg_replica_op`: Delete Replica node.

### Collections
*   `db_reg_coll_op`: Create Collection node and link to Parent Collection or Zone.
*   `db_rename_coll_op`: Update Collection name and Proxy Index node.
*   `db_del_coll_op`: Delete Collection node (assuming cascading deletes in L3KVG or handling orphans).

### Resources
*   `db_reg_resc_op`: Create Resource node.
*   `db_mod_resc_op`: Update Resource properties.
*   `db_del_resc_op`: Delete Resource node.

### Metadata (AVU)
*   `db_add_avu_metadata_op`: Create AVU node and link (`ANNOTATED_WITH`).
*   `db_del_avu_metadata_op`: Remove link.
*   `db_mod_avu_metadata_op`: Update AVU properties.

## 3. Test Suites
Develop specific mock test suites to verify ZMQ payloads for all implemented operations.

*   `test_plugin_data.cpp`: Validate Data Object and Replica registration, moves, and renames.
*   `test_plugin_collections.cpp`: Validate Collection hierarchy registration.
*   `test_plugin_metadata.cpp`: Validate AVU creation and edge linking.

## 4. Federated Anchors
*   Ensure `bootstrap_federation` continues to create "Proxy Zone Nodes" in the local cluster so that traversals can hit the anchor and be delegated by the L3KVG Engine's `FederationResolver`.
