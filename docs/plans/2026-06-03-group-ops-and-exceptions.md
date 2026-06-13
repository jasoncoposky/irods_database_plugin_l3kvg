# [Group Operations & Exception Tracing] Implementation Plan

> **For Gemini:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement missing group management operations and resolve the persistent `basic_string` null-construction exception during `mkuser`.

**Architecture:** 
1. Expand the database plugin operation table to include Group registration and deletion.
2. Update the Catalog Facade to support group persistence in the L3KVG graph.
3. Perform a comprehensive audit of the GenQuery bridge for any remaining unsafe string constructions.

**Tech Stack:** C++, iRODS Database Plugin API, L3KVG Graph Engine.

---

### Task 1: Add Group Operations to Database Plugin

**Files:**
- Modify: `irods_database_plugin_l3kvg/src/db_plugin.cpp`
- Modify: `irods_database_plugin_l3kvg/include/irods/catalog/catalog_facade.hpp`
- Modify: `irods_database_plugin_l3kvg/src/catalog/catalog_facade.cpp`

**Step 1: Implement `db_reg_group_op` and `db_del_group_op` in `db_plugin.cpp`**
- Ensure NULL-pointer guards for group name and zone.
- Wrap in try/catch with logging.

**Step 2: Update `CatalogFacade` and `CatalogImpl`**
- Add `register_group` and `delete_group` methods.
- Store group nodes in the graph with type `rodsgroup`.

**Step 3: Register operations in `l3kvg_database_plugin` constructor**
- Use `DATABASE_OP_REG_GROUP` and `DATABASE_OP_DEL_GROUP`.

### Task 2: Audit GenQuery Bridge for NULL Safety

**Files:**
- Modify: `irods_database_plugin_l3kvg/src/gq1_bridge.cpp`

**Step 1: Audit `synthesize_gq2_ast` and `pack_gq1_results`**
- Verify that every access to `_inp` and `_out` arrays is guarded.
- Specifically check `_inp->condInput.value[i]` and `_inp->selectInp.inx[i]`.

### Task 3: Verification & Core Testing

**Step 1: Rebuild and Update Package**
```bash
cd irods_database_plugin_l3kvg/build_gemini
make -j4 && cpack -G DEB
cp irods-database-plugin-l3kvg-5.0.0-Linux.deb ../../irods_dev/irods_package_output/
```

**Step 2: Reproduce `mkuser` Failure**
```bash
docker exec -u irods ubuntu2404l3kvg-irods-catalog-provider-1 iadmin mkuser testuser rodsuser
```

**Step 3: Run Full Core Test Suite**
```bash
python3 irods_testing_environment/run_plugin_tests.py ... --tests test_ils
```
