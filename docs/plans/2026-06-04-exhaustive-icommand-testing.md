# [Exhaustive iCommand Testing] Implementation Plan

**Goal:** Systematically verify all 50+ iRODS icommands against the L3KVG database plugin to ensure 100% functional parity with legacy catalog providers.

## Phase 1: Identity, Session & Navigation
Verify that users can authenticate, manage environments, and traverse the collection hierarchy.
- **Commands:** `iinit`, `iexit`, `ienv`, `ipasswd`, `iuserinfo`, `ils`, `icd`, `ipwd`, `itree`.
- **Test Cases:**
    - Login with valid/invalid passwords.
    - Password updates (native auth).
    - Recursive listing (`ils -r`) with deep hierarchies.
    - Path resolution across zones.

## Phase 2: Core Data Operations (CRUD)
Verify the most critical path: uploading, downloading, and moving data.
- **Commands:** `iput`, `iget`, `icp`, `imv`, `irm`, `irmdir`, `imkdir`, `itouch`.
- **Test Cases:**
    - Single file and recursive directory uploads.
    - Overwrites and checksum verification during put.
    - Moving files between collections (metadata updates).
    - Garbage collection (trash bin functionality).

## Phase 3: Resource & Replica Management
Verify that L3KVG correctly tracks physical paths and resource hierarchies.
- **Commands:** `ilsresc`, `iphymv`, `irepl`, `itrim`, `ireg`, `iunreg`.
- **Test Cases:**
    - Replica creation and deletion.
    - Moving replicas between resources (physical path updates).
    - Registering existing physical files into the catalog.
    - Trimming old replicas based on count/age.

## Phase 4: Metadata, ACLs & Quotas
Verify that graph-based metadata and permission edges are robust.
- **Commands:** `imeta`, `ichmod`, `isysmeta`, `iquota`.
- **Test Cases:**
    - AVU operations (add, rm, mod, set, cp) on all entity types.
    - Permission inheritance on nested collections.
    - Group-based permissions.
    - Quota enforcement and usage calculation.

## Phase 5: Query & Search
Verify that the GenQuery bridge handles all legacy query patterns.
- **Commands:** `iquest`, `iquery`.
- **Test Cases:**
    - Complex joins (e.g., User -> Access -> Collection -> DataObject).
    - Aggregate functions (COUNT, SUM).
    - Case-insensitive searches and wildcards.
    - Specific queries (alias execution).

## Phase 6: Admin & System Specialized
Verify maintenance and high-level grid operations.
- **Commands:** `iadmin`, `igroupadmin`, `izonereport`, `irule`, `iticket`, `ibun`, `imcoll`, `istream`, `iscan`, `ifsck`, `ips`, `ichksum`.
- **Test Cases:**
    - Bulk registration via `ibun`.
    - Ticket-based access for anonymous/external users.
    - Delay rule queue management (`iqstat`, etc.).
    - Rule execution and metadata-driven policies.

## Phase 7: Delayed Rules & Queue
Verify the asynchronous rule engine interactions.
- **Commands:** `iqstat`, `iqmod`, `iqdel`.
- **Test Cases:**
    - Scheduling delayed rules.
    - Modifying rule execution times in the DB.
    - Locking/unlocking rule records.

---

## Execution Strategy
1.  **Automated Regression:** Use the existing `run_plugin_tests.py` to trigger the official Python test suites for each command.
2.  **TDD Reproductions:** For any command that fails, create a minimal C++ unit test in the plugin to reproduce the DB-level mismatch.
3.  **Gap Analysis:** Audit the GenQuery bridge logs for every "0 rows" or "error" response during the full run.
