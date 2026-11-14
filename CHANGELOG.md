# Changelog

All notable changes to `proto_db` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0rc4] - unreleased

- `proto_db.__version__` reports the package version; in 2.0.0rc3 it still said `2.0.0.dev0`.
- `CloudStorage.publish_root` reports a write refused by its precondition as `ProtoConflictError`, which
  `Database.run` retries, when the object store declares that its refusals are definitive
  (`refusals_are_definitive`: nothing below it re-sends a conditional write). The bundled `S3ObjectStore` and
  `GCSObjectStore` declare it with the clients they build; an S3 client passed in declares it only when it is
  configured for one attempt in total. Before, such a refusal was reported as `ProtoOutcomeUnknownError` when
  another instance had published more than `root_history` roots before the read-back, although nothing of
  ours could have been applied (seen as an intermittent CI failure of two instances sharing a counter). Other
  stores keep the unknown outcome, and the read-back still runs first, so a write that did land is reported as
  applied (D43 amendment, ruling R147a).
- `S3ObjectStore` builds its client with one attempt in total (`retries={"total_max_attempts": 1}`). botocore
  reads the former `max_attempts=1` as one retry, so a conditional `PutObject` could be sent twice.
- Documented a Python 3.14 hazard (concurrency model, "Interrupts and signal handlers on Python 3.14"): an
  exception from a signal handler, such as `KeyboardInterrupt`, can leave a `proto_db` lock held, so a
  process interrupted inside `proto_db` code should end rather than keep using the space.
- Tests: pytest-timeout ends a run that overruns by stopping the process (`timeout_method = "thread"`) rather
  than raising in the main thread, which on 3.14 could leak a lock and left test threads running into the
  next tests (CI-314); `test_pager_stability` stops its threads and bounds its writers.
- Tests: the two compaction checks of `test_owner_locks` held on ext4, where a freed inode number is
  reused; they now keep the original file open and compare with `os.path.samestat`.

## [2.0.0rc3] - 2026-11-13

The first release candidate published on PyPI.
- Python 3.12 is the minimum (D61): on 3.11, comparing deeply nested tuples counts against the recursion
  limit, so a value near `MAX_VALUE_DEPTH` could raise a bare `RecursionError`. `pip` refuses to
  install it on 3.11 (`requires-python >=3.12`).
- Test suite: process-wide object counts replaced by per-test counts, a stress mode for the cluster tests
  (`PROTO_STRESS`, safety asserts only), and fixes to tests that failed in a clean export or under load.
- CI: a test matrix over 3.12, 3.13, 3.14 and 3.14t, and a `sphinx-build -W` documentation job.
- Benchmarks: a 10^7-object inventory and the periodic cluster stress task, with result files under
  `benchmarks/results/`.

## [2.0.0] - unreleased

`proto_db` 2.0 is a complete rebuild of the 0.1 code base under the design principles of
`docs/design-principles.md`: everything stored is immutable, a transaction is an explicit object, and
every safety argument holds under arbitrary delay and loss. There are no 0.1 data or API to preserve.
The design is `docs/superpowers/specs/2026-11-01-protodb-v2-design.md`; every decision is in
`docs/decisions.md` (D1-D61).

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
- Python floor 3.12 (D61; was 3.11 under D14); CI targets 3.12-3.14 and 3.14t. `six` and `wrapt` are no longer
  dependencies (D14).
- The package is split into subpackages: `core`, `storage`, `collections`, `indexing`, `query`, `cluster`,
  `runtime`, `interop` (D15; the spec's `indexes` became `indexing`).

- Storage format 4 (D58): exact tokens are canonical encodings, datetimes in an IANA zone are stored as their
  UTC instant, map keys follow an explicit conflict rule, and the class of a built-in subclass is not persisted
  (a `str` or `int` `Enum` is stored and read back as its value). Formats 2 and 3 are still read. Migration
  cost: a key-group or vector index written by format 3 is rebuilt from its collection at the collection's
  first change, inside that write transaction (an ivf_flat index retrains, an hnsw graph is rebuilt). A
  format-3 map keyed by a zone datetime or by repeated NaNs is read as written but refused when written again
  until the application converts those keys.
- Polyglot freeze (D59, Task 10.P): a record of a type this process has no class for (another language's
  type) loads as an `OpaqueAtom` that keeps its fields readable and is never rewritten, instead of raising
  `ProtoCorruptionError`; root names starting with `__` are reserved for the format (`ProtoValidationError`);
  the attribute-name errors explain the accessor rule. `python -m proto_db.tools.verify <dir>` decodes and
  validates a whole file-storage space and dumps it in the conformance notation. The format specification
  gains "Format stability" (the freeze point, the additive `@sym` and registry), "Atoms of unknown types",
  the accessor rule and "A minimal external writer"; `tests/conformance/vectors.sha256` pins the vectors.
  After the pre-tag review: a reference to an opaque atom may name its type differently from the record's
  `t` (an alias only the owner knows) and keeps the name it was referenced by; writers write the lowest
  sufficient format version (`WRITE_FORMAT_VERSION`, separate from the highest read, `FORMAT_VERSION`); the
  verifier also reports non-canonical encodings and exits 2 only for an unreadable root; a field named like a
  metaclass attribute (`mro`) is readable; a `date` or `timedelta` subclass is stored from its base fields,
  never its `__reduce__`.
- Format-3 data misfiled by built-in subclasses: format 3 wrote a `(str, Enum)` member (on every Python
  version) or a subclass overriding `__str__`, `__float__` or `__bytes__` through that conversion
  (`"Color.RED"`) but filed it by its base value (`"red"`). Collections holding such values cannot find them,
  and a key-ordered Dictionary also loses other keys. `collection.validate()` now detects it
  (`ProtoMisfiledError`, a `ProtoCorruptionError`) and `tx.set_root_object(name, collection.rebuilt())`
  repairs it (entries refiled, indexes rebuilt; one key with two different values is refused, never dropped).
- IVF-Flat training no longer depends on the members' oids (R128, 012db7f): a bulk build of the same vectors
  with the same `seed` gives the same centroids and the same answers, in any order (an index grown by inserts
  still trains on the members present at each retrain threshold, which depends on the insertion order). An `ivf_flat` index built before 012db7f
  keeps its persisted centroids until it retrains; at its next retrain (or rebuild) it gets different
  centroids than that same data gave before, and stable ones from then on. No format change.

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
- Saving a chain of unsaved versions built in one transaction (the `previous` pattern) no longer raises
  `RecursionError` past about 150 links; 10,000 versions save in one commit, and the verifier describes them
  (R128).
- Nested values: `proto_db` no longer writes values it cannot read back (B12, D60). The codec, `canonical()`,
  the map-key, order-key, hash and equality helpers and the verifier walked values by recursion, so a value
  past about 330 nested lists could be written but raised a bare `RecursionError` when read in a fresh
  process (`verify` exited 1), and the limit moved with the Python stack in use. They now go on with an
  explicit stack past 12 levels, and the format has a normative limit, `MAX_VALUE_DEPTH` = 1022 MessagePack
  containers per value (format spec, "Value nesting depth"): a writer refuses a deeper value with
  `ProtoValidationError` naming the field, a reader refuses deeper data with `ProtoCorruptionError`, at the
  same count. No format version change (no conforming writer could produce deeper data); the encoded bytes
  are unchanged. `proto_db` now requires msgpack 1.2.0 or later, the first release whose packer nests the 1024
  containers a record at the limit needs, and refuses to import with an older one. The limit binds collection
  and index nodes too: a Set, CountedSet, HashDictionary or RepeatedKeysDictionary, and an index (whose member
  keys nest an unorderable element about four times deeper: 254 nested lists, 169 nested maps), refuse an
  element they cannot store when it is added, naming the collection or the index and the effective limit,
  never later at commit. The query result cache, `explain()` and `to_arrow` work at any depth too.
  **Readers must accept records 1024 containers deep**: an implementation that stops at 509 levels is not
  conforming from rc2.

Defects of the 0.1 code base found by the 2026-11-01 review, none of which survives the rebuild: integer
values merged as `+1` and special cases for keys named `counter` and `k`; commits that were not durable;
indexes lost on the first write; queries that silently returned empty or partial results; about 280 broad
`except` handlers hiding errors; two writers appending to one segment.

### Known limitations
- No `proto_db.tools.inspect` browser (the spec's section 6 named one); the formats are specified in
  `docs/source/format-spec.rst`, `python -m proto_db.tools.verify` checks and dumps a space, and the operations
  tutorial shows both.
- No online-backup call; see the operations tutorial for what a copy of an open space contains.
- Cluster fairness for eight or more contending writer processes, and for hot-key read-modify-write
  transactions, is not guaranteed (open defect D-LIVE-2).
- A read-only linearizable read (`read_barrier`) is proposed in the D46 amendment but not implemented; the
  barrier commit is the supported path.
- On Python 3.14 an exception from a signal handler (Ctrl-C) can leave a `proto_db` lock held, and the process
  can then hang; end the process after such an interrupt. An interpreter property (CPython #74174); 3.12 and
  3.13 are not affected. See the concurrency model.

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