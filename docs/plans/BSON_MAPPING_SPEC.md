# iRODS to L3KVG BSON Mapping Specification

## Overview
This document defines the exhaustive mapping of iRODS internal data types to the L3KVG Graph BSON format. It adheres to the design principles of **Snowflake-style 64-bit Routing**, **Zero-Copy Serialization**, and **Federated Awareness**.

## 1. Snowflake 64-bit ID Structure
All Node IDs in L3KVG are 64-bit integers (`uint64_t`).

- **Bits 48-63 (16 bits):** Cluster / Zone ID (Supports up to 65,536 federated clusters).
- **Bits 0-47 (48 bits):** Local Node Hash (Deterministic `xxHash` of the local UUID or ID).

### Routing Logic
1.  **Federation:** `id >> 48`. If it matches the local cluster ID, execution is local.
2.  **Sharding:** `id & 0x0000FFFFFFFFFFFF` is hashed against the consistent hash ring.

---

## 2. Entity Type Enumeration
While the ID is a 48-bit hash, the BSON payload contains an explicit type identifier to support typed filtering.

| Type Code | Identifier | iRODS Equivalent |
| :--- | :--- | :--- |
| `0x01` | **Zone** | `zoneInfo_t` |
| `0x02` | **User** | `userInfo_t`, `administration::user` |
| `0x03` | **Collection** | `collInfo_t` |
| `0x04` | **DataObject** | `dataObjInfo_t` (Logical) |
| `0x05` | **Replica** | `dataObjInfo_t` (Physical) |
| `0x06` | **Resource** | `administration::resource_info` |
| `0x07` | **AVU** | `keyValPair_t` (Metadata) |
| `0x08` | **Access** | ACLs / Permissions |
| `0x09` | **Rule** | Delayed Execution / Rule Base |
| `0x0A` | **Token** | Namespace Tokens |
| `0x0B` | **Quota** | Usage Quotas |
| `0x0C` | **Audit** | Audit Logs |
| `0x0D` | **Ticket** | Ticket-based Access |

---

## 3. BSON Entity Mappings (Compact Format)

### 3.1 Zone (`0x01`)
```json
{
  "n": "string",  // name
  "t": "string",  // type
  "c": "string",  // connection
  "m": "string"   // comment
}
```
*   **Edges:**
    *   `OUT [HAS_USER] -> User`
    *   `OUT [HAS_ROOT_COLL] -> Collection`
    *   `OUT [HAS_RESC] -> Resource`

### 3.2 User / Group (`0x02`)
```json
{
  "n": "string",  // userName
  "z": "string",  // rodsZone
  "t": "string",  // userType
  "p": 123,       // priv_level
  "d": "string",  // userDN
  "ct": "string", // createTime
  "mt": "string"  // modifyTime
}
```
*   **Edges:**
    *   `OUT [MEMBER_OF] -> User(Group)`
    *   `IN [HAS_USER] -> Zone`

### 3.3 Collection (`0x03`)
```json
{
  "n": "string",  // collName
  "o": "string",  // ownerName
  "z": "string",  // ownerZone
  "i": "string",  // inheritance
  "t": "string",  // collType
  "ct": "string", // createTime
  "mt": "string"  // modifyTime
}
```
*   **Edges:**
    *   `IN [CONTAINS] -> Collection(Parent)`
    *   `OUT [CONTAINS] -> Collection / DataObject`
    *   `IN [HAS_ROOT_COLL] -> Zone`

### 3.4 DataObject (`0x04`)
```json
{
  "n": "string",  // objPath
  "o": "string",  // dataOwnerName
  "z": "string",  // dataOwnerZone
  "s": 12345,     // dataSize (Logical)
  "ct": "string", // createTime
  "mt": "string"  // modifyTime
}
```
*   **Edges:**
    *   `IN [CONTAINS] -> Collection`
    *   `OUT [HAS_REPLICA] -> Replica`

### 3.5 Replica (`0x05`)
```json
{
  "rn": 0,        // replNum
  "p": "string",  // filePath
  "h": "string",  // rescHier
  "s": 1,         // replStatus
  "st": "string", // statusString
  "cs": "string", // checksum
  "mt": "string", // modifyTime
  "at": "string"  // accessTime
}
```
*   **Edges:**
    *   `IN [HAS_REPLICA] -> DataObject`
    *   `OUT [STAYING_AT] -> Resource`

### 3.6 Resource (`0x06`)
```json
{
  "n": "string",  // name
  "t": "string",  // type
  "l": "string",  // location (host)
  "v": "string",  // vault_path
  "c": "string",  // context
  "f": 12345,     // free_space
  "s": 1,         // status
  "m": "string",  // comments
  "ct": "string", // createTime
  "mt": "string"  // modifyTime
}
```
*   **Edges:**
    *   `IN [STAYING_AT] -> Replica`
    *   `OUT [CHILD_OF] -> Resource(Parent)`
    *   `IN [HAS_RESC] -> Zone`

### 3.7 AVU (Metadata) (`0x07`)
```json
{
  "a": "string",  // attribute
  "v": "string",  // value
  "u": "string"   // units
}
```
*   **Edges:**
    *   `IN [ANNOTATED_WITH] -> Entity(DataObject, Collection, User, Resource)`

### 3.8 Access (ACL) (`0x08`)
```json
{
  "l": "string",  // accessLevel
  "n": "string"   // tokenNamespace
}
```
*   **Edges:**
    *   `IN [HAS_ACCESS] -> User`
    *   `OUT [FOR_OBJECT] -> Entity`

---

## 4. Secondary Index Map (Proxy Nodes)
Proxy nodes are created to resolve strings (like paths) to Snowflake IDs. They are stored as standard nodes with a single field `id` containing the 64-bit Snowflake ID.

| Proxy Key | Pattern | Target |
| :--- | :--- | :--- |
| `idx:data:[Path]` | `[ZoneID][0x04][XXH3_48(objPath)]` | `DataObject` |
| `idx:coll:[Path]` | `[ZoneID][0x03][XXH3_48(collName)]` | `Collection` |
| `idx:user:[Name]` | `[ZoneID][0x02][XXH3_48(userName)]` | `User` |
| `idx:resc:[Name]` | `[ZoneID][0x06][XXH3_48(rescName)]` | `Resource` |
