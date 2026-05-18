# Phase 6: Access Control & Group Membership

## Objective
Fully implement and verify the iRODS Access Control List (ACL) model within the Federated Smart Client. This involves translating legacy string-based permissions operations into structured graph edges (`HAS_ACCESS`, `FOR_OBJECT`, `MEMBER_OF`) and ensuring group-based inheritance is supported.

## 1. Name-to-ID Resolution
The iRODS `db_mod_access_control_op` provides string identifiers (username, zone, object path). The Smart Client must resolve these to Snowflake IDs before mutating the graph.
*   **Action:** Implement a `resolve_id_from_index` helper in `CatalogFacade`.
*   **Logic:** The helper will issue an asynchronous `GET` request to the L3KVG server for the specific Proxy Index Node (e.g., `idx:user:tempZone:alice`) and parse the returning BSON to extract the true 64-bit Snowflake ID.

## 2. Group Membership Edges
Groups in iRODS are represented as standard `User` entities with a specific type (`rodsgroup`).
*   **Action:** Fully implement `add_user_to_group` and `remove_user_from_group`.
*   **Logic:** Issue an `add_edge` request to create a `MEMBER_OF` relationship from the `User` node to the `Group` node.

## 3. ACL Edge Creation
*   **Action:** Update `set_access` to use the resolved Snowflake IDs.
*   **Logic:**
    *   Create an `Access` node representing the permission level (e.g., `read`, `own`).
    *   Create a `HAS_ACCESS` edge from the User/Group to the `Access` node.
    *   Create a `FOR_OBJECT` edge from the `Access` node to the Target (DataObject/Collection).

## 4. Test Verification (`test_plugin_acls.cpp`)
Construct a new GTest suite against the `MockL3KVGServer` to rigorously verify the ACL model.
*   **Direct Access:** Verify that setting permissions for a user creates the exact correct graph structure (`User -> Access -> DataObject`).
*   **Group Access:** Verify that adding a user to a group creates the `MEMBER_OF` edge, and setting permissions for the group routes the ACL correctly.
*   **Resolution Verification:** Ensure the plugin correctly fetches from the Proxy Index before executing the mutation.
