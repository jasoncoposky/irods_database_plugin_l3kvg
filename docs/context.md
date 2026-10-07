# iRODS L3KVG Database Plugin Development Context

This document captures the historical efforts, resolved issues, and outstanding roadmap for the development and QA of the L3KVG property-graph database plugin.

---

## 1. Executive Summary

The **L3KVG Database Plugin** integrates a property-graph catalog engine into the iRODS 5.0 architecture, replacing legacy relational databases (PostgreSQL/MySQL/Oracle) with a property-graph DB catalog using ZeroMQ catalog plugin APIs. 

To validate legacys functional parity, an exhaustive QA testing sweep is underway against the 50+ standard iRODS `icommands` using the official `irods_testing_environment` Docker topologies.

---

## 2. Completed Efforts & Bug Fixes

### 1. GenQuery1 Emulation Bridge Sorting Modifiers (`gq1_bridge.cpp`)
- **Issue**: Running `ils -A` (list ACLs of a collection) resulted in an infinite pagination loop. The iRODS agent hung, spun CPU at 100%, and wrote gigabytes of logs (reaching 10+ GB in a few minutes) with increasing query offsets (e.g. `offset: 1166080`).
- **Root Cause**: In legacy GenQuery1, select columns with requested sorting have `ORDER_BY` or `ORDER_BY_DESC` bitwise modifiers applied directly to the column index (e.g. `COL_COLL_USER_NAME | ORDER_BY`). The bridge projections loop in `synthesize_gq2_ast` did not strip these modifiers before checking against mapped columns. This caused the column names to evaluate as empty strings, which defaulted to dummy `DATA_ID` projections. Because the client was returned data object IDs instead of user names/zones, it could never find the requested permission row and kept paging forever.
- **Resolution**: Patched `src/gq1_bridge.cpp` to strip `ORDER_BY` and `ORDER_BY_DESC` flags from column indices in the projections loop, restoring correct mapping to `User.n` and `User.z`.

### 2. CMake Prefix & Packaging Paths (`CMakeLists.txt`)
- **Issue**: The test hook failed with `l3kvg_server: No such file or directory` or `bootstrap_l3kvg: No such file or directory`.
- **Root Cause**: The plugin CMake project was configured with default settings, which set the installation prefix to `/usr/local` (installing executables to `/usr/local/bin`). However, the test hooks and iRODS setup scripts inside the test containers explicitly execute `/usr/bin/l3kvg_server` and `/usr/bin/bootstrap_l3kvg`.
- **Resolution**: Reconfigured CMake with `-DCMAKE_INSTALL_PREFIX=/usr`. All built executables are now packaged and installed into `/usr/bin` within the final Debian package.

### 3. Collection Inheritance Member Typo (`db_plugin.cpp`)
- **Issue**: Compilation failed with an error stating `collInfo_t` had no member named `collInheritable`.
- **Root Cause**: A typo in `src/db_plugin.cpp` attempted to access `_info->collInheritable` instead of the standard `_info->collInheritance` defined in `collInfo_t` (under `objInfo.h`).
- **Resolution**: Changed `collInheritable` to `collInheritance`.

### 4. Progress Tracking Documentation
- **Resolution**: Created `docs/icommand_coverage_status.md` to track legacy compatibility status, groupings, and active triage logs for all 7 phases of standard `icommands`.

---

## 3. Outstanding Roadmap

### Phase 1: Identity, Session & Navigation
- **Goal**: Fully verify Phase 1 commands (`iinit`, `iexit`, `ienv`, `ipasswd`, `iuserinfo`, `ils`, `icd`, `ipwd`, `itree`).
- **Next Steps**: Monitor the current `test_ils` run. Once `ils -A` passes, Phase 1 is officially complete.

### Phase 2: Core Data Operations (CRUD)
- **Goal**: Verify CRUD operations (`iput`, `iget`, `icp`, `imv`, `irm`, `irmdir`, `imkdir`, `itouch`).
- **Next Steps**:
  1. Trigger test suites for `iput` and `iget`.
  2. Investigate the standard user `imkdir` permission validation issue. Previously, executing `imkdir` as a non-admin user (`alice`) resulted in a `SYS_NO_API_PRIV` exception, suggesting a gap in user role/group validation or hierarchy path check.

### Phase 3 to 7: Specialized Commands
- **Goal**: Systematically progress testing through the remaining command categories:
  - **Phase 3**: Resource and Replica management (`ilsresc`, `iphymv`, `irepl`, `itrim`, `ireg`, `iunreg`).
  - **Phase 4**: Metadata, ACLs, and Quotas (`imeta`, `ichmod`, `isysmeta`, `iquota`).
  - **Phase 5**: GenQuery1/2 Querying and search (`iquest`, `iquery`).
  - **Phase 6**: Grid Administration and Specialized system commands (`iadmin`, `igroupadmin`, `izonereport`, `irule`, `iticket`, `ibun`, `imcoll`, `istream`, `iscan`, `ifsck`, `ips`, `ichksum`).
  - **Phase 7**: Queue and Delay Rules (`iqstat`, `iqmod`, `iqdel`).
