# iRODS L3KVG Database Plugin: iCommand Coverage Status

This document tracks the compatibility and test coverage of all standard iRODS `icommands` against the L3KVG property-graph database catalog plugin. 

## Objectives
- Achieve **100% legacy compatibility** for all standard `icommands`.
- Ensure all GenQuery1 (GQ1) and GenQuery2 (GQ2) catalog operations correctly translate to graph queries without causing server-side lexical cast panics or database inconsistencies.

---

## iCommand Coverage & Status

| Phase | iCommand | Category | Status | Notes / Test Failures |
|---|---|---|---|---|
| **Phase 1** | `iinit` | Session | **Passing** | Works for admins and regular users. |
| | `iexit` | Session | **Passing** | Clears session credentials. |
| | `ienv` | Session | **Passing** | Prints environment details. |
| | `ipasswd` | Session | **Untested** | Changes user password. |
| | `iuserinfo` | Session | **Untested** | Displays user details. |
| | `ils` | Directory | **Partially Working** | `ils` and `ils -l` pass; `ils -A` fails on collection ACL printing due to projection mapping issues. |
| | `icd` | Directory | **Passing** | Changes current working collection. |
| | `ipwd` | Directory | **Passing** | Prints current collection path. |
| | `itree` | Directory | **Untested** | Shows graphical tree view of collections. |
| **Phase 2** | `iput` | CRUD | **Partially Working** | Standard upload works, but metadata persistence issues have been observed in complex sequences. |
| | `iget` | CRUD | **Partially Working** | Downloads data objects, dependent on proper replica resolution. |
| | `icp` | CRUD | **Partially Working** | Copies objects. Fails if source replica metadata resolution has gaps. |
| | `imv` | CRUD | **Untested** | Moves/renames objects. |
| | `irm` | CRUD | **Partially Working** | Removes data objects. |
| | `irmdir` | CRUD | **Untested** | Removes collections. |
| | `imkdir` | CRUD | **Partially Working** | Works for admin users; regular users previously encountered `SYS_NO_API_PRIV` permission validation blocks. |
| | `itouch` | CRUD | **Untested** | Updates timestamps or creates empty data objects. |
| **Phase 3** | `ilsresc` | Resource | **Untested** | Lists storage resources. |
| | `iphymv` | Resource | **Untested** | Physical move of data replicas. |
| | `irepl` | Resource | **Untested** | Replicates data objects across resources. |
| | `itrim` | Resource | **Untested** | Trims data replicas. |
| | `ireg` | Resource | **Untested** | Registers physical files into iRODS. |
| | `iunreg` | Resource | **Untested** | Unregisters files from catalog. |
| **Phase 4** | `imeta` | Metadata | **Untested** | Manages AVUs on data, collections, resources, and users. |
| | `ichmod` | ACLs | **Partially Working** | Sets permissions. Internal mappings between permission names (e.g. `own`) and integer values (e.g. `1200`) require verification. |
| | `isysmeta` | Metadata | **Untested** | Manipulates system metadata. |
| | `iquota` | Quotas | **Untested** | Manages data quotas. |
| **Phase 5** | `iquest` | Query | **Untested** | General query interface. |
| | `iquery` | Query | **Untested** | GenQuery2 query tool. |
| **Phase 6** | `iadmin` | Admin | **Partially Working** | Admin commands (`mkuser`, `moduser`, `rmuser`, `mkresc`, `rmresc`) are tested as part of test suite setups. |
| | `igroupadmin`| Admin | **Untested** | Manages group memberships. |
| | `izonereport` | Admin | **Untested** | Generates zone configuration reports. |
| | `irule` | Workflow | **Untested** | Submits and executes rules. |
| | `iticket` | Tickets | **Untested** | Manages ticket-based access controls. |
| | `ibun` | Archive | **Untested** | Handles structured file bundles. |
| | `imcoll` | Archive | **Untested** | Mounts structured collections. |
| | `istream` | CRUD | **Untested** | Streams data objects. |
| | `iscan` | Audit | **Untested** | Scans local filesystem vs catalog. |
| | `ifsck` | Audit | **Untested** | File system consistency checks. |
| | `ips` | Process | **Untested** | Shows active agent processes. |
| | `ichksum` | Hash | **Untested** | Computes or verifies data checksums. |
| **Phase 7** | `iqstat` | Queue | **Untested** | Displays rule execution queue. |
| | `iqmod` | Queue | **Untested** | Modifies queued rules. |
| | `iqdel` | Queue | **Untested** | Deletes queued rules. |

---

## Active Triage & Investigation Logs

### 1. `ils -A` Collection ACL Failure
- **Error**: `remote addresses: <IP> ERROR: printCollAcl: getSqlResultByInx for COL_COLL_USER_NAME failed`
- **Root Cause**: In `gq1_bridge.cpp`, the select projections loop uses `pure_inx = inx` instead of stripping out bitwise sort flags `ORDER_BY` and `ORDER_BY_DESC`. When iRODS executes sorting on collection ACL queries (such as ordering by user name/zone), the bridge gets `COL_COLL_USER_NAME | ORDER_BY` and fails to map the column, returning an empty column name which defaults to `DATA_ID` (DataObject ID).
- **Resolution**: Update `src/gq1_bridge.cpp` to correctly apply `pure_inx = inx & ~ORDER_BY & ~ORDER_BY_DESC` in the projections loop.

### 2. Standard User `imkdir` Permission Block (`SYS_NO_API_PRIV`)
- **Error**: Creating a collection as a regular user (e.g. `alice`) failed with `SYS_NO_API_PRIV`.
- **Root Cause**: The catalog facade or GenQuery permission checking layer needs to verify permissions by checking the path hierarchy and matching against the User's ACLs. If the parent path resolution fails or has null-safety gaps, it defaults to denying access.
- **Resolution**: Under active investigation. Verify parent collection inheritance and user group membership resolution in the graph traversals.
