# iRODS L3KVG Database Plugin

A high-performance, distributed database plugin for iRODS 5+ based on the L3KVG Actor-Model Property Graph Engine. This plugin replaces traditional relational SQL databases with a shared-nothing distributed graph fabric.

## Architectural Pillars

This implementation is 100% compliant with the L3KVG architectural whitepaper, focusing on the following six core pillars:

1. **Concurrency & Actor-Model:** Eliminates OS-level file locking by utilizing a sharded actor-model message queue. Mutations are isolated to specific hardware threads, ensuring lock-free concurrency.
2. **Distributed Catalog:** Supports transparent horizontal scaling using ZeroMQ, Consistent Hashing, and Hybrid Logical Clocks (HLC). Every iRODS server can act as a shard owner in a shared-nothing topology.
3. **Fluent Querying:** Features a native GenQuery2-to-Graph compiler (`Gq2ToL3kvgCompiler`) that translates iRODS GenQuery2 AST nodes directly into L3KVG fluent traversal pipelines, rendering relational SQL generation obsolete.
4. **Zero-Copy Performance:** Eradicates the "Copy Tax" by utilizing `std::string_view`, Polymorphic Memory Resources (PMR), and Move Semantics. Data flows from legacy iRODS structs into the graph engine without intermediate heap allocations or memory copies.
5. **PIMPL Facade Isolation:** Enforces a strict boundary between the legacy iRODS C-API and the modern C++20 engine. The PIMPL (Pointer to Implementation) pattern prevents dependency bleeding and ensures high-speed compilation.
6. **Modern Data Plane Contract:** Implements the `irods::data_plane::Client` interface, utilizing modern C++ paradigms such as `std::span` for bulk operations, monadic error handling, and RAII Transaction Guards.

## Build Requirements

- C++20/C++23 compatible compiler (Clang 13+ or GCC 11+)
- CMake 3.20+
- ZeroMQ (libzmq)
- iRODS 5+ Development Headers

## Building

```bash
mkdir build
cd build
cmake ..
make -j$(nproc)
```

The built plugin will be located at `libirods_database_plugin_l3kvg.so`.

## Implementation Details

- **`db_plugin.cpp`**: The native iRODS database plugin entry point and 1-to-1 boundary layer.
- **`catalog_facade.cpp`**: PIMPL implementation orchestrating graph operations.
- **`l3kvg_mapper.hpp`**: Compile-time safe, variadic BSON mapper for legacy structs.
- **`gq2_compiler.cpp`**: AST Visitor for GenQuery2-to-Graph translation.
