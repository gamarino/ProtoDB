# Technical Audit: ProtoDB (proto_db)

**Date:** 2025-02-01  
**Scope:** ProtoDB project and `proto_db` package (embedded transactional object-oriented database for Python).  
**Sources:** Repository at `ProtoDB/`, including `proto_db/`, `docs/`, and project documentation.

---

## 1. Executive Summary

ProtoDB is an **embedded, transactional, object-oriented database** for Python 3.11+, distributed as the **proto_db** package. It provides in-process storage with copy-on-write semantics, multiple backends (memory, file WAL, optional cluster/cloud), immutable collections (Dictionary, List, Set, CountedSet), secondary indexes, and an index-aware query planner plus a LINQ-like API and optional vector search.

**Strengths:** Clear layered architecture, rich documentation (Sphinx, concepts, cookbook), extensible storage and query layers, and a broad test suite including concurrency and property-based tests. The design is well thought out for near in-memory performance with background persistence and write-through caching.

**Critical issues:** The package currently **requires `msgpack` at import time** for `StandaloneFileStorage` (and thus for any import of cloud/cluster modules), while `pyproject.toml` does not declare it and the README states that core features have no mandatory third-party dependencies. In addition, two exception classes use a literal `6` instead of their defined error code constants. The test run fails (39 import errors) when `msgpack` is not installed.

**Recommendations:** Declare `msgpack` as a core dependency or make its use optional (lazy import / fallback to JSON); fix exception default codes; add a minimal CI checklist (install deps, run tests) to the docs; and consider type hints and coverage reporting for critical paths.

---

## 2. Project Overview

| Item | Detail |
|------|--------|
| **Name** | ProtoDB (package: `proto_db`) |
| **Version** | 0.1.0 (pyproject.toml); README states 0.1 alpha (2025-09-16) |
| **License** | MIT |
| **Python** | ≥ 3.11 |
| **Status** | Early preview; APIs may change before first stable release |

**Purpose:** Provide ACID transactions, persistence, and querying over Python-native object graphs without a separate server, with optional scaling via cluster and cloud storage.

**Deliverables:** Single package `proto_db` with core (common, db_access, storage, collections, queries, LINQ, FSM), optional extras (parquet, vectors, dev), Sphinx docs under `docs/`, and a large test suite under `proto_db/tests/`.

---

## 3. Architecture Assessment

### 3.1 Layered Design

The architecture is **layered and coherent**:

- **Atoms & identity:** All durable data is built from immutable `Atom`s; identity is stable via `AtomPointer` (transaction_id, offset). This supports hashing, equality, and indexing across sessions.
- **Storage abstraction:** `SharedStorage` (push_atom, get_atom, get_bytes, push_bytes, root management) is implemented by:
  - `MemoryStorage` (in-memory)
  - `StandaloneFileStorage` (file-based WAL, format indicators for JSON/MessagePack/raw)
  - `ClusterFileStorage` (distributed, vote-based locking)
  - `CloudFileStorage` / `CloudClusterFileStorage` (S3/GCS, optional clients)
- **Access layer:** `ObjectSpace` → `Database` → `Transaction`; transactions are snapshot-isolated and copy-on-write. Root updates are coordinated via a storage-provided root context (e.g. file locking or cluster consensus).
- **Data structures:** Dictionary, RepeatedKeysDictionary, List (AVL), Set, CountedSet, HashDictionary, built on Atoms and integrated with the query system.
- **Query system:** Plans (FromPlan, WherePlan, JoinPlan, GroupByPlan, SelectPlan, IndexedSearchPlan, AndMerge, OrMerge, etc.) with `explain()` and optimizer that uses secondary indexes when available.
- **Optional layers:** LINQ-like API (`from_collection`, `F`), parallel scanning (work-stealing), atom caches (bytes + object, 2Q), vector indexes (exact + HNSW), Arrow/Parquet bridge.

Documentation (e.g. `architecture.rst`, `concepts.rst`) accurately describes this and the interaction flow (storage → transaction → operations → commit).

### 3.2 Consistency and Concurrency

- **Immutability and CoW:** Modifications create new atoms and update references; no in-place mutation of persisted data. This simplifies reasoning and caching.
- **Ephemeral vs persistent in Set/CountedSet:** Documented “draft” vs “content” model avoids accidental persistence when hashing temporary atoms.
- **Root locking:** Documented and implemented (file-level lock + atomic replace for standalone; cluster uses majority + local lock). Timeouts configurable via environment variables.
- **Concurrency tests:** Present (e.g. `test_concurrency.py`, `test_singlethread_concurrency.py`), indicating awareness of contention and locking.

### 3.3 Extensibility

- **Custom storage:** Implementing `SharedStorage` (or the documented interface) allows new backends.
- **Custom structures:** Extending `MutableObject` / base collection types and implementing serialization is described in `development.rst`.
- **Custom query plans:** Extending `QueryPlan` is documented.
- Optional features are grouped behind extras (`parquet`, `vectors`, `dev`), which is good practice.

---

## 4. Code Quality

### 4.1 Structure and Organization

- **Package layout:** Clear separation: `common.py`, `db_access.py`, storage modules, collection modules, `queries.py`, `linq.py`, `fsm.py`, `exceptions.py`, etc. Tests are under `proto_db/tests/` with descriptive names (`test_db_access.py`, `test_where_plan.py`, …).
- **Naming:** Consistent use of `proto_db` in imports and public API. Development guide references PEP 8 and Google-style docstrings.

### 4.2 Exception Hierarchy

- **Base:** `ProtoBaseException` with `code`, `exception_type`, `message`.
- **Specialized:** `ProtoUnexpectedException`, `ProtoValidationException`, `ProtoUserException`, `ProtoCorruptionException`, `ProtoNotSupportedException`, `ProtoNotAuthorizedException`, `ProtoLockingException`, each with a dedicated constant (e.g. `LOCKING_ERROR = 70_000`).

**Bug:** In `exceptions.py`, `ProtoNotAuthorizedException` and `ProtoLockingException` use `code if code else 6` instead of their constants (`NOT_AUTHORIZED_ERROR`, `LOCKING_ERROR`). So when `code` is omitted or falsy, the reported code is `6` instead of the intended value. **Recommendation:** Use `code if code is not None else NOT_AUTHORIZED_ERROR` (and similarly for LOCKING_ERROR).

### 4.3 Dependencies and Import Policy

- **Declared (pyproject.toml):** Core: `six`, `wrapt`. Optional: parquet → `pyarrow`; vectors → `numpy`, `hnswlib`; dev → `pytest`, `hypothesis`, `sphinx`, `build`, `twine`.
- **Actual usage:** `standalone_file_storage.py` does a top-level `import msgpack`. That module is used for MessagePack format (FORMAT_MSGPACK) and for reading/writing atoms in that format. Because `cluster_file_storage` and `cloud_file_storage` import `standalone_file_storage`, **any** import of `proto_db` (including `from proto_db.db_access import ObjectSpace`) triggers loading `standalone_file_storage` and thus requires `msgpack` to be installed.
- **Impact:** README and docs state that “core features have no mandatory third-party dependencies,” which is currently false for any usage that goes through file or cloud storage. The test run fails with `ModuleNotFoundError: No module named 'msgpack'` for 39 tests when `msgpack` is not installed.
- **Recommendation:** Either (1) add `msgpack` to core `dependencies` in `pyproject.toml` and document it, or (2) make MessagePack optional: lazy-import `msgpack` only when FORMAT_MSGPACK is used, and keep JSON as the default so that core + StandaloneFileStorage work without msgpack. Option (2) preserves the “no mandatory third-party deps” claim for the default code path.

### 4.4 Test Suite

- **Framework:** `unittest`; docs mention pytest in dev extras.
- **Coverage:** Many modules have dedicated test files (db_access, memory_storage, standalone_file_storage, queries, linq, dictionaries, lists, sets, concurrency, FSM, cloud/cluster with mocks, etc.). Improvement notes (`test_coverage_improvements.md`, `collection_improvements.md`) show prior focus on coverage and edge cases.
- **Execution:** From project root, `python -m unittest discover proto_db/tests` is the documented command. Without `msgpack`, 39 tests fail at import; with msgpack installed, the suite can be re-run to confirm pass/fail and, if available, coverage.
- **Recommendation:** Add a short “Prerequisites” or “Running tests” section stating that `pip install msgpack` (or `pip install -e ".[dev]"` and optional extras) may be required for the full suite, and add a single CI-oriented script or doc step (e.g. install + discover) to avoid confusion.

---

## 5. Documentation

- **Sphinx:** `docs/source/` contains index, introduction, installation, quickstart, examples, concepts, cookbook, architecture, root_locking, storage_cloud, vectors_arrow, performance, testing, development, advanced_usage, parallel_scans, and API (core, storage, data_structures, queries, linq, indexes, vectors, atom_cache, parallel, fsm, exceptions). Build via `cd docs && make html`.
- **Concepts:** `concepts.rst` / `concepts.md` explain atoms, AtomPointer, the hash/persistence interaction, ephemeral vs persistent collections, and transactions in a clear way.
- **README:** Installation, quickstart, indexing/LINQ/vector snippets, docs index, testing, compatibility, and attribute access style. Accurate except for the dependency claim.
- **Development:** Structure, coding guidelines, extending storage/structures/plans, debugging, releasing. The “yourusername” clone URL and some generic release steps could be updated to the real repo and release process.
- **API:** Autodoc is used (e.g. `.. module:: proto_db.…`); references to `proto_db` in examples are consistent. Minor: `introduction` is in the toctree; the doc exists and is up to date (2025-09-19).

Overall, documentation is a **strong point** and sufficient for onboarding and extension.

---

## 6. Security and Robustness

- **Input/output:** Serialization (JSON, MessagePack) and binary formats are used; validation and size limits (e.g. BLOB_MAX_SIZE) are present. No deep security review was performed; a production deployment would warrant a dedicated security review (e.g. malformed WAL, path traversal if paths are user-controlled).
- **Concurrency:** Locking (file, cluster, root context) and documented timeouts reduce risk of indefinite blocking; `ProtoLockingException` allows callers to retry.
- **Corruption:** `ProtoCorruptionException` and validation exceptions provide clear error boundaries; WAL and format indicators support recovery and compatibility policies.
- **Credentials:** Cloud clients (S3, GCS) are expected to use standard credential chains (env, config); no hardcoded secrets were seen in the audited files.

---

## 7. Gaps and Risks

| Gap / Risk | Severity | Notes |
|------------|----------|--------|
| **msgpack required at import** | High | Breaks “no mandatory third-party deps” and causes test failures. Fix: add dependency or make msgpack optional. |
| **Exception default code `6`** | Medium | Wrong code for ProtoNotAuthorizedException and ProtoLockingException when code is not passed. Fix: use the correct constants. |
| **PyPI metadata** | Low | `authors` uses “contact@example.com”; classifiers say “Beta” while README says “0.1 alpha.” Align with project status. |
| **Type hints** | Low | Development guide recommends type hints; coverage is partial. Improving types would help static analysis and IDE support. |
| **Test prerequisites** | Low | New contributors may not know that msgpack (or full extras) are needed for all tests; document clearly. |

---

## 8. Recommendations Summary

1. **Dependencies:** Declare `msgpack` in `pyproject.toml` as a core dependency **or** make MessagePack optional (lazy import + JSON-only default) so that core + file storage work without third-party packages. Update README accordingly.
2. **Exceptions:** In `ProtoNotAuthorizedException` and `ProtoLockingException`, replace `code if code else 6` with the appropriate constant (e.g. `NOT_AUTHORIZED_ERROR`, `LOCKING_ERROR`).
3. **Testing:** Document in README or testing.rst that running the full suite may require `pip install msgpack` (and optionally other extras). Add a one-line “CI” example (e.g. `pip install -e ".[dev]" && python -m unittest discover proto_db/tests`).
4. **Metadata:** Update `authors`/contact in pyproject.toml and align version/status (README vs classifiers).
5. **Ongoing:** Consider enabling coverage reporting and type checking in CI; keep the current level of documentation and architecture documentation as the project evolves.

---

## 9. Conclusion

ProtoDB (proto_db) is a **well-architected** embedded transactional database with strong documentation and a rich feature set (multiple backends, indexes, LINQ, optional vectors/cloud). The main **blockers** for a smooth out-of-the-box experience are the undeclared mandatory use of `msgpack` and the test failures that result, plus the small exception-code bug. Addressing the dependency and exception issues, and clarifying test prerequisites, would bring the project in line with its documented goals and improve contributor and user experience. After these fixes, the codebase is in good shape for continued development toward a stable release.
