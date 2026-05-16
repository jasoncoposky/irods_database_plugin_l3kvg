# iRODS to L3KVG BSON Mapping Specification

## Overview
This document defines the exhaustive mapping of iRODS internal data types to the L3KVG Graph BSON format. It adheres to the design principles of **Binary Composite Keys**, **Zero-Copy Serialization**, and **Federated Awareness**.

## 1. Binary Composite Key Structure
All primary keys in the KV store follow a fixed-length 13-byte structure:
`[4-byte ZoneHash][1-byte EntityType][8-byte ObjectID]`

*   **ZoneHash:** XXH3 (32-bit) of the Zone Name (e.g., `tempZone`).
*   **EntityType:** Enumerated byte identifier.
*   **ObjectID:** The native iRODS numeric identifier.

### Entity Type Enumeration
| Type | Identifier | iRODS Equivalent |
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

## 2. BSON Entity Mappings

### 2.1 Zone (`0x01`)
*   **BSON Structure:**
    ```json
    {
      "n": "string",  // name
      "t": "string",  // type
      "c": "string",  // connection
      "m": "string",  // comment
      "ct": "string", // createTime
      "mt": "string"  // modifyTime
    }
    ```

### 2.2 User / Group (`0x02`)
*   **BSON Structure:**
    ```json
    {
      "n": "string",  // userName
      "z": "string",  // rodsZone
      "t": "string",  // userType
      "p": 123,       // priv_level
      "d": "string",  // userDN
      "i": "string",  // userInfo
      "m": "string",  // userComment
      "ct": "string", // createTime
      "mt": "string"  // modifyTime
    }
    ```
*   **Edges:**
    *   `OUT [MEMBER_OF] -> User(Group)`

### 2.3 Collection (`0x03`)
*   **BSON Structure:**
    ```json
    {
      "n": "string",  // collName
      "o": "string",  // ownerName
      "z": "string",  // ownerZone
      "i": "string",  // inheritance
      "t": "string",  // collType
      "c1": "string", // info1 (Mounted path/etc)
      "c2": "string", // info2
      "m": "string",  // comments
      "ct": "string", // createTime
      "mt": "string"  // modifyTime
    }
    ```
*   **Edges:**
    *   `IN [CONTAINS] -> Collection(Parent)`

### 2.4 DataObject (`0x04`)
*   **BSON Structure:**
    ```json
    {
      "n": "string",  // objPath
      "o": "string",  // dataOwnerName
      "z": "string",  // dataOwnerZone
      "t": "string",  // dataType
      "s": 12345,     // dataSize (Logical)
      "st": "string", // dataStatus
      "m": "string",  // dataComments
      "ct": "string", // createTime
      "mt": "string"  // modifyTime
    }
    ```
*   **Edges:**
    *   `IN [CONTAINS] -> Collection`

### 2.5 Replica (`0x05`)
*   **ObjectID:** The Logical `dataId` (Stored with `replNum` in BSON to handle collisions).
*   **BSON Structure:**
    ```json
    {
      "rn": 0,        // replNum
      "p": "string",  // filePath
      "h": "string",  // rescHier
      "s": 1,         // replStatus (0=Stale, 1=Good)
      "st": "string", // statusString
      "cs": "string", // checksum
      "mt": "string", // modifyTime
      "at": "string"  // accessTime
    }
    ```
*   **Edges:**
    *   `IN [HAS_REPLICA] -> DataObject`
    *   `OUT [STAYING_AT] -> Resource`

### 2.6 Resource (`0x06`)
*   **BSON Structure:**
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
    *   `OUT [CHILD_OF] -> Resource(Parent)`

### 2.7 AVU (Metadata) (`0x07`)
*   **ObjectID:** XXH3(Attribute + Value + Units).
*   **BSON Structure:**
    ```json
    {
      "a": "string",  // attribute
      "v": "string",  // value
      "u": "string"   // units
    }
    ```
*   **Edges:**
    *   `IN [ANNOTATED_WITH] -> Entity(DataObject, Collection, User, Resource)`

### 2.8 Access (ACL) (`0x08`)
*   **ObjectID:** XXH3(UserId + TargetId).
*   **BSON Structure:**
    ```json
    {
      "l": "string",  // accessLevel
      "n": "string"   // tokenNamespace
    }
    ```
*   **Edges:**
    *   `IN [HAS_ACCESS] -> User`
    *   `OUT [FOR_OBJECT] -> Entity`

### 2.9 Rule / Delayed Exec (`0x09`)
*   **BSON Structure:**
    ```json
    {
      "n": "string",  // name
      "u": "string",  // userName
      "a": "string",  // address
      "t": "string",  // execTime
      "f": "string",  // frequency
      "p": 123,       // priority
      "s": "string",  // status
      "c": "string"   // context
    }
    ```

### 2.10 Token (`0x0A`)
*   **BSON Structure:**
    ```json
    {
      "ns": "string", // namespace
      "n": "string",  // name
      "v": "string",  // value
      "v2": "string", // value2
      "v3": "string", // value3
      "m": "string"   // comment
    }
    ```

### 2.11 Quota (`0x0B`)
*   **BSON Structure:**
    ```json
    {
      "u": "string",  // userId/userName
      "r": "string",  // rescId/rescName
      "l": 12345,     // limit
      "o": 12345      // over
    }
    ```

---

## 3. Secondary Index Map (Direct Hashed Keys)
Proxy nodes are created for fast lookups by path or name. Proxy nodes contain a single field `id` pointing to the 13-byte primary key.

| Index Key (BSON Key) | Pattern | Target |
| :--- | :--- | :--- |
| `p:data:[Path]` | `[ZoneHash][0x04][XXH3(objPath)]` | `DataObject` |
| `p:coll:[Path]` | `[ZoneHash][0x03][XXH3(collName)]` | `Collection` |
| `p:user:[Name]` | `[ZoneHash][0x02][XXH3(userName)]` | `User` |
| `p:resc:[Name]` | `[ZoneHash][0x06][XXH3(rescName)]` | `Resource` |
