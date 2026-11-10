# Changelog

All notable changes to ProtoDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - unreleased

ProtoDB 2.0 is a complete rebuild of the 0.1 code base under the design principles of
`docs/design-principles.md`: everything stored is immutable, a transaction is an explicit object, and
every safety argument holds under arbitrary delay and loss. There are no 0.1 data or API to preserve.
The design is `docs/superpowers/specs/2026-11-01-protodb-v2-design.md`; every decision is in
`docs/decisions.md` (D1-D57).

### Added
- **One value model** with a total order across types (`1 == 1.0 == Decimal(1)`, `True != 1`), used by
  collections, indexes and queries identically (D2, D3); frozen `list` and `dict` values (D25).
- **Atoms** with stable identity (`oid`), lazy thread-safe loading and msgpack serialisation (D10, D18, D21).
- **Collections**, all persistent AVL trees and all indexable: `List`, `Dictionary` (key-ordered),
  `HashDictionary`, `RepeatedKeysDictionary`, `Set`, `CountedSet`, and `DBObject` records (D8, D20-D27).
- **Index framework**: `Index` base class with a documented hook contract, `KeySpec` (paths, the element,
  registered extractors), `EqualityIndex`, `RangeIndex` and `VectorIndex` (exact, IVF-Flat, optional HNSW),
  every mutator of every collection keeping its indexes current, and a reusable index contract
  (`tests/contracts/index_contract.py`) for new kinds (D22, D29, D30, D39).
- **Query layer**: expression terms, physical plans, one cost-based planner that never drops a predicate, a
  LINQ front end (`from_collection`, `F`, `Agg`), explicit `PythonFilter` nodes, `explain()`, a query result
  cache keyed by pointers, and `Pager`s with portable tokens over snapshots (D28, D36-D38, D56).
- **Transactions**: snapshot isolation with read-set validation (absent roots included), `Database.run` with
  bounded exponential back-off and jitter, per-key and per-element merge of unread collection roots,
  `MutableObject` cells, group commit, `Database.history()` and read-only `Snapshot`s of any past version,
  `ProtoOutcomeUnknownError` for commits whose outcome cannot be proved (D9, D11, D32-D35, D52).
- **Storage v2**: `MemoryStorage`; `FileStorage` with append-only CRC-checked segments, atomic root
  publication and `fsync` per commit by default (`durability="flush"` and `"none"` are explicit opt-outs);
  `CloudStorage` over S3 or Google Cloud Storage with data strictly before root and a conditional root write;
  `TieredStorage` (D4, D6, D19, D43, D46, D51).
- **Cluster**: several writer processes through a peer coordinator (majority lease, write permissions, commit
  by majority acknowledgement, bounded replication, learners, fencing), with the object store written after
  the commit point (D44-D46, D50, D54, D55). `ObjectSpace.sync()` is a durability barrier; a barrier commit is
  the linearizable read path (it adds a permanent database version per read).
- **Decoded-atom cache** shared by the transactions and threads of a space, by pointer, bounded on loaded
  atoms, with counters (`ObjectSpace.atom_cache_stats()`) that expose a thrashing working set (D53).
- **Runtime services**: `parallel_scan` and `parallel_filter` (real speedup on free-threaded CPython),
  `FSM`; **interop**: `to_arrow`, `to_parquet`, `scan_parquet` (D40-D42).
- **Free-threading**: tested on CPython 3.14t with the GIL disabled and with the GIL.
- **Performance suite** (`python -m benchmarks`): self-verifying benchmarks, with results by workload
  profile in `docs/source/performance.rst` (D49).
- **Documentation**: a rewritten Sphinx site (concepts, design pages, ten executable tutorials, API
  reference) and a test that runs every tutorial code block (`tests/docs/test_tutorials.py`).
- A single exception hierarchy rooted at `ProtoDBError` (D12), and a static check that forbids swallowed
  exceptions and test-aware code in `proto_db/` (D16).

### Changed
- The public API is the set of names in `proto_db.__all__`, resolved lazily: `import proto_db` imports no
  submodule or optional dependency and starts no thread (D47).
- Dictionaries are key-ordered AVL trees (O(log n)), not a binary search over a positional list (D8).
- A conflict raises `ProtoConflictError` and the caller retries; merging applies only to roots the transaction
  did not read and only by replaying recorded operations, never by inferring intent from value types (D9, D33).
- Strings are stored inline; the global `Literal` catalog is gone (D7).
- Python floor 3.11; CI targets 3.11-3.14 and 3.14t. `six` and `wrapt` are no longer dependencies (D14).
- The package is split into subpackages: `core`, `storage`, `collections`, `indexing`, `query`, `cluster`,
  `runtime`, `interop` (D15; the spec's `indexes` became `indexing`).

### Deprecated
- The 0.1 exception names `ProtoBaseException`, `ProtoValidationException`, `ProtoUserException`,
  `ProtoCorruptionException`, `ProtoNotSupportedException`, `ProtoNotAuthorizedException`,
  `ProtoUnexpectedException` and `ProtoLockingException`: aliases of the `Proto*Error` classes, removed in 2.1
  (D12).

### Removed
- The 0.1 implementation (`common`, `db_access`, the 0.1 storages, collections, queries, LINQ, vectors,
  `parallel`, `fsm`, `arrow_bridge`, `hybrid_executor`, `exceptions`) and its test suite `proto_db/tests/`
  (D47, D48; dropped tests: `docs/legacy/dropped-tests.md`). UDP voting is gone from the cloud storages (D5).

### Fixed
Defects of the 0.1 code base found by the 2026-11-01 review, none of which survives the rebuild: integer
values merged as `+1` and special cases for keys named `counter` and `k`; commits that were not durable;
indexes lost on the first write; queries that silently returned empty or partial results; about 280 broad
`except` handlers hiding errors; two writers appending to one segment.

### Known limitations
- No `proto_db.tools.inspect` command-line tool (the spec's section 6 named one); the formats are specified in
  `docs/source/format-spec.rst` and the operations tutorial shows how to read a space without it.
- No online-backup call; see the operations tutorial for what a copy of an open space contains.
- Cluster fairness for eight or more contending writer processes, and for hot-key read-modify-write
  transactions, is not guaranteed (open defect D-LIVE-2).
- A read-only linearizable read (`read_barrier`) is proposed in the D46 amendment but not implemented; the
  barrier commit is the supported path.
- Defect: saving a chain of more than about 150 unsaved versions built in one transaction (the `previous`
  pattern) raises `RecursionError`, because a save recurses once per link. Reported for a product fix.

## Before 2.0

Entries from the 0.1 line, kept as written. They describe code that 2.0 removed.

### Added (0.1 line, unreleased)
- Optional parallel scanning module (proto_db.parallel) with adaptive chunking and a simple work-stealing scheduler.
- Metrics hooks for per-worker and global stats (chunk latencies, steals, queue depths).
- parallel_scan helper for easy opt-in usage without altering existing query code.
- Documentation: user guide (Parallel Scans), API reference, and ADR.
- Unit tests for adaptive chunking, work-stealing behavior, cancellation, and fallback correctness.

### Changed
- Attribute access style: migrated docs/tests/examples to prefer dot notation; clarified DBObject semantics and dynamic getattr in README.

## [0.1.0] - 2023

### Added

#### Google Cloud Storage Support

- Added `GoogleCloudClient` as an implementation of `CloudStorageClient` for Google Cloud Storage
- Ensured that `CloudBlockProvider`, `CloudFileStorage`, and `CloudClusterFileStorage` work with both `S3Client` and
  `GoogleCloudClient`
- Added comprehensive test suites for Google Cloud Storage integration
- Added usage examples for Google Cloud Storage in documentation

#### Multiple Serialization Formats Support

- Added a format indicator after the size header to specify the serialization format
- Added support for JSON UTF-8 (the original format) and MessagePack (a new efficient binary format)
- Maintained backward compatibility with existing data
- Added new methods to support the new formats:
    - `push_atom(atom: dict, format_type: int = FORMAT_JSON_UTF8) -> Future[AtomPointer]`
    - `push_atom_msgpack(atom: dict) -> Future[AtomPointer]`
    - `push_bytes(data: bytes, format_type: int = FORMAT_RAW_BINARY) -> Future[tuple[uuid.UUID, int]]`
    - `push_bytes_msgpack(data: dict) -> Future[tuple[uuid.UUID, int]]`

#### PyPI Package Support

- Created `pyproject.toml` with package metadata, dependencies, and build system information
- Added MIT License file
- Created `MANIFEST.in` to include non-Python files in the package distribution
- Added `test_install.py` script to test the local installation of the package
- Created `PYPI_UPLOAD.md` with documentation on how to build and upload the package to PyPI
- Updated README.md with installation instructions and license information

#### Advanced Storage Options Documentation

- Added comprehensive documentation for `ClusterFileStorage` and `CloudFileStorage`
- Updated `__init__.py` to expose the advanced storage options
- Added detailed use cases for the advanced storage options

### Fixed

#### CloudFileStorage Test Hanging Issue

- Added an `uploader_running` flag to control the background uploader thread
- Modified the `_background_uploader` method to check both the `state` and the `uploader_running` flag
- Updated the `close` method to properly stop the background uploader thread and wait for it to finish
- Added test verification to ensure the uploader thread is properly stopped

### Changed

- Completed the implementation of `S3Client` by adding missing abstract methods:
    - `_init_client`: Initializes the S3 client using boto3 or falls back to a mock implementation
    - `get_object`: Gets an object from S3
    - `put_object`: Puts an object to S3
    - `list_objects`: Lists objects in S3
    - `delete_object`: Deletes an object from S3