# ProtoBase: The In‑Memory Python Database that Remembers

Tagline: Experience the speed of in‑memory computing with the safety of on‑disk persistence. ProtoBase offers fully transactional Python objects that operate at RAM speed.

---

## Core Concept

ProtoBase is a high‑performance, transactional data platform for Python. Its hybrid architecture operates directly on native Python objects at memory speed using copy‑on‑write, while a decoupled persistence engine guarantees durability in the background. Forget heavy ORMs and disk latency on your critical path. With ProtoBase you get the simplicity of a `dict` or `list` with full ACID guarantees.

## Key Characteristics

- Near In‑Memory Performance
  - All reads and writes within a transaction execute in memory over immutable (copy‑on‑write) structures.
  - Persistence runs on background threads, decoupling app latency from disk speed.

- Intelligent Write‑Through Cache
  - Newly written atoms are immediately published to the AtomObjectCache and AtomBytesCache.
  - Eliminates the “read‑your‑own‑writes” penalty and keeps the hottest data in RAM for instant reads.

- Advanced Query Optimizer
  - The engine exploits indexes for `AND`, `OR`, and single‑term predicates, building plans like AndMerge, OrMerge, and IndexedSearchPlan.
  - Instead of table scans, it combines low‑cost reference sets and materializes objects only at the end.

- Full ACID Transactions with Snapshot Isolation
  - Snapshot isolation via immutable structures: consistent reads within each transaction.
  - No dirty or non‑repeatable reads.

- Zero‑Overhead Object Mapping
  - Work directly with your Python objects: no mandatory base classes, no ORM sessions to manage, no separate query DSL to learn.
  - If it’s a Python object, it can be persisted.

## Ideal Use Cases

- High‑Throughput Applications
  - Shopping carts, session management, game leaderboards, telemetry/metrics ingestion.

- Complex Data Modeling
  - Object graphs: nested documents, social graphs, configuration systems.

- Persistent, Transactional Cache
  - A higher‑level replacement for Redis/Memcached when you need multi‑key transactions and automatic durability.

- Rapid Prototyping and Product Development
  - When `pickle`/`shelve` are too limited but Postgres/MySQL add too much administrative overhead.

## Simple Analogy

Think of ProtoBase as a supercharged Python `dict`. It’s as easy to use as a dictionary, but it’s thread‑safe, fully transactional, and asynchronously persists its state to disk so you don’t have to think about it. It’s the perfect bridge between the simplicity of Python data structures and the robustness of a real database.

---

### Why it feels instant

- Copy‑on‑write over an in‑memory object graph: the main thread does not wait for I/O.
- Persistence and serialization delegated to a background executor pool; `commit` synchronizes with the WAL.
- Write‑through cache: read‑after‑write hits the cache, not the disk.

### Why it stays safe

- ACID transactions with snapshot isolation.
- Immutable secondary indexes and an optimizer that avoids premature materialization.
- AtomPointer as a stable identifier for intersections and de‑duplication in `AND`/`OR` plans.

---

## Positioning Statement

ProtoBase is no longer “just” a persistent object database—it is a Transactional Data Platform with Memory‑Class Performance. It combines the speed and ergonomics of an in‑memory solution with the durability, query capabilities, and transactional guarantees of a traditional database—directly in Python.

---

## Competitive Landscape & Positioning

- Traditional RDBMS + ORM (e.g., PostgreSQL + SQLAlchemy/Django ORM)
  - Strengths: mature ecosystems, rich SQL, strong durability and tooling.
  - Limitations: object/relational impedance mismatch, ORM session management, network and disk latency in the critical path.
  - ProtoBase advantage: native Python object model with copy‑on‑write, near in‑memory latency, background persistence, and index‑aware query plans—without ORM ceremony.

- In‑Memory Caches (Redis/Memcached)
  - Strengths: extremely fast key/value operations, broad adoption.
  - Limitations: limited transactional semantics across multiple keys, external service ops burden, optional/complex durability.
  - ProtoBase advantage: built‑in ACID transactions, write‑through persistence, and object‑level queries—all embedded in your Python process.

- Document/NoSQL Stores (MongoDB, DynamoDB‑like systems)
  - Strengths: flexible schemas, good for nested documents and horizontal scale.
  - Limitations: driver round‑trips, eventual consistency modes, vendor‑specific query semantics; complex local development.
  - ProtoBase advantage: local, embedded engine with snapshot isolation, deterministic consistency, and a Pythonic query API over native objects.

- Embedded Databases (SQLite, LMDB)
  - Strengths: simple deployment, proven reliability.
  - Limitations: row/byte‑oriented abstractions, SQL or key/value mindset; requires mapping layers and manual indexing strategies.
  - ProtoBase advantage: higher‑level object model, immutable secondary indexes, and an optimizer that intersects reference sets before materialization.

- In‑Memory/Hybrid Databases (H2, HSQLDB, DuckDB for analytics)
  - Strengths: fast in‑process execution.
  - Limitations: primarily table/column oriented; object mapping and transactional object graphs are out of scope.
  - ProtoBase advantage: designed for Python object graphs with ACID semantics and asynchronous durability, plus optional vector search integration.

- Pure Python Data Structures
  - Strengths: zero overhead for simple scenarios; unparalleled flexibility.
  - Limitations: no durability, no transactions, no consistent snapshots, no secondary indexes.
  - ProtoBase advantage: feels like native structures but adds durability, transactions, indexes, and a query optimizer—without leaving Python.

  ---

  ## Performance-driven positioning (updated 2025-09-13)

  Recent benchmarks underline ProtoBase’s value proposition for indexed queries and primary-key lookups. Two representative runs on in-memory data:

  - Indexed AND + BETWEEN vs linear scan
    - Run A (10k items, 200 queries, window=500, categories=200, statuses=50): indexed_over_linear ≈ 6.72×; indexed_over_python ≈ 1.18×.
    - Run B (50k items, 100 queries, window=500, categories=500, statuses=100): indexed_over_linear ≈ 16.47×; indexed_over_python ≈ 3.44×.
    - Latency improves across the distribution (lower p50/p95) with higher QPS as data size grows.
  - Primary‑key lookups
    - Run A: indexed_pk_over_linear ≈ 41.62×.
    - Run B: indexed_pk_over_linear ≈ 164.09×.
    - A native PK map in collections would further reduce overhead and should extend the lead as data grows.
  - Comparison to pure Python lists
    - At small sizes, a plain Python list comprehension can be competitive due to zero planning overhead; by 50k rows, the indexed path is ~3.4× faster.

  Sources / artifacts:
  - examples/benchmark_results_indexed.json (Run A)
  - examples/benchmark_results_indexed_win200.json (Run B)

  To reproduce:

  ```bash
  # Run A
  python examples/indexed_benchmark.py \
    --items 10000 --queries 200 --window 500 --warmup 20 \
    --categories 200 --statuses 50 \
    --out examples/benchmark_results_indexed.json

  # Run B
  python examples/indexed_benchmark.py \
    --items 50000 --queries 100 --window 500 --warmup 20 \
    --categories 500 --statuses 100 \
    --out examples/benchmark_results_indexed_win200.json
  ```

  Implications for positioning
  - ProtoBase offers memory‑class latency with ACID guarantees and index‑aware query planning that materially outperforms linear scans on realistic sizes.
  - For applications with selective predicates or frequent point lookups, ProtoBase delivers order‑of‑magnitude improvements without abandoning Python’s native object model.