# ProtoBase: Commercial Positioning (2025)

## Executive Summary

ProtoBase is an embedded, transactional, object-oriented database for Python that delivers a unified engine for structured and vector data—featuring a LINQ-like query API, immutable secondary indexes, range-aware optimization, and integrated vector similarity search. New in 2025, ProtoBase introduces an atom-level caching layer (AtomObjectCache + AtomBytesCache) that reduces P95/P99 latencies for hot reads across transactions by avoiding unnecessary page reads and repeated deserializations. In addition, ProtoBase now includes optional adaptive parallel scans with a lightweight work-stealing scheduler, delivering higher throughput and lower tail latency on mixed-cost workloads—designed to be GIL-agnostic today and ready to benefit from Python’s free-threaded (no-GIL) builds without code changes.

Outcome: lower total cost of ownership, faster time-to-market, and a cohesive developer experience for AI-enabled products—particularly Retrieval Augmented Generation (RAG) and semantic search—while preserving transactional consistency. With the atom-level cache, ProtoBase further improves developer ergonomics and cost by cutting I/O and network egress, and by delivering a predictable, low-latency read path for embedded AI workloads.

## Why ProtoBase, Why Now

- AI workloads increasingly require tight coupling of semantic search (vectors) with precise metadata filters and transactional writes.
- Typical stacks combine a vector DB, a document/KV store, a cache, and an indexing service—adding complexity and operational overhead.
- ProtoBase consolidates these needs into a single embedded engine: composable indexes, range operators, index-driven planning, and vector search in one place.

## Value Proposition

1) Embedded, LINQ-like Querying
   - Familiar, Pythonic LINQ-style API with lazy execution and explicit materializers.
   - `explain()` for transparent, explainable plans; pushdown of filters, ranges, and limits.

2) Transactional and Consistent
   - Copy-on-write and transactional rebase keep data and indexes (including vector indexes) consistent across updates.

3) Index-Driven Optimization
   - Immutable secondary indexes; selectivity-ordered intersections for AND predicates; union+dedupe for OR.
   - Range-aware operators with inclusive/exclusive bounds.

4) Vector-Native
   - Vector fields with KNN search (exact and ANN like IVF-Flat and HNSW).
   - Hybrid pipelines: combine similarity search with structured filters in the same query.

5) Operational Simplicity
   - Runs in-process; works with memory, file, distributed, or cloud backends via a unified API.
   - Deterministic, testable, and easy to package (developer-first).

## Target Segments

- Product teams building AI/RAG features in Python that need vector search + business filters with transactional guarantees.
- SaaS and startups optimizing for iteration speed and low TCO.
- Edge/IoT deployments requiring local inference, offline operation, and later synchronization.
- Data science/ML teams that prototype quickly and need a direct path to production without a separate DB stack.

## Competitive Landscape (Executive View)

- SQLite/DuckDB: excellent embedded engines; lack native transactional vector search and Python object semantics; no atom-level cache awareness for Python object graphs.
- Redis/MongoDB: strong services but higher operational footprint for embedded hybrid (vector + metadata) use cases; external cache increases complexity.
- Vector DBs (FAISS, Chroma, pgvector): best-in-class similarity, but typically require separate systems for metadata and transactions.
- ProtoBase unifies: transactions + rich data structures + secondary and vector indexes + range-aware queries, all embedded in Python, now with atom-level caching for predictable latency on hot reads.

## Key Differentiators

1) Declarative, LINQ-like API with lazy, optimized execution (pushdown + early limits).
2) Immutable secondary indexes; reusability and predictable performance.
3) Range operators with configurable inclusivity: [], (), [), (].
4) Practical optimizer based on selectivity and early exit.
5) Integrated vector search (exact + ANN) composable with structured filters.
6) Transactional consistency across data and all indexes via copy-on-write and rebase.
7) Embedded simplicity with unified persistence (memory, file, cluster, cloud).
8) Atom-level caching (new in 2025): object and bytes caches with a 2Q policy and single-flight; improves P95/P99 on hot reads and reduces I/O/egress.
9) Adaptive parallel scans (new in 2025): work-stealing scheduler with per-worker local deques, adaptive chunking (EMA with min/max bounds), and per-worker metrics. Backward compatible via the traditional pool; GIL-agnostic design ready to benefit from Python’s no-GIL builds when available.

## Core Use Cases

- Embedded RAG: store embeddings, documents, and metadata; retrieve via KNN; filter by attributes/ranges; rerank and serve.
- Semantic search and recommendations with complex business rules in SaaS products.
- Deduplication/fuzzy matching where vector similarity and structural constraints must co-exist.
- Edge analytics with local semantic search and low-latency filters.

## Technical Capabilities

### New positioning and potential

- Thesis: the highest cost in embedded-AI applications is not storage itself but tail latency and the operational complexity of multiple services. The atom-level cache reduces both.
- Potential:
  - SaaS B2B and products with RAG/semantic search: 2–3x improvements in P95/P99 for repetitive reads without adding Redis.
  - Edge/IoT and on-device: smaller footprint, less I/O and energy, consistently predictable.
  - Developer-first platforms (SDK/OEM): simple adoption via a Python library with safe defaults.
- Competitive advantage: unique combination of Pythonic LINQ + immutable indexes + vector search + embedded atom-level cache.
- Risks/mitigations: format changes handled with schema_epoch; memory limits and 2Q prevent scan pollution.

- LINQ-like Querying
  - Lazy pipelines; `where`, `select`, `select_many`, `order_by/then_by`, `distinct`, `take/skip`, `group_by` with aggregates.
  - Policies: `on_unsupported("error"|"warn"|"fallback")`; `explain(text/json)`.

- Secondary Indexes
  - Immutable maps of value → frozenset(object_id); incremental updates on add/remove/replace.
  - Selectivity-driven intersections; reuse across queries.

- Range Operators
  - Inclusive/exclusive bounds; detection from chained comparisons in lambdas.
  - Efficient pushdown to range-capable/ordered indexes or discretized buckets.

- Vector Search
  - Vector fields with dimension checks and optional normalization.
  - Exact, IVF-Flat, and (optionally) HNSW; recall and latency benchmarks available.
  - Hybrid: combine top-k vector results with indexed structured filters.

- Caching and Cluster-Aware Reads
  - Read page cache: if a requested object resides in a page already in memory, the storage read is avoided.
  - Atom-level cache (new): two optional in-memory layers ahead of the page cache — AtomObjectCache (deserialized objects) and AtomBytesCache (raw bytes). Keyed by AtomPointer (transaction_id, offset) and, optionally, schema_epoch for the object cache. 2Q policy to avoid scan pollution and single-flight to deduplicate concurrent deserializations. Results: higher hit ratio across transactions and lower latency even when the page is not in cache.
  - Cluster-aware reading: before hitting storage (file or S3), nodes query their peers; if one has the page, an inter-server transfer is performed, typically faster than disk or object I/O.
  - Transactional locality: objects modified in the same transaction are grouped into contiguous pages, increasing the probability of cache hits and reducing read misses.
  - Practical benefits: lower P95/P99 latency on hot reads, I/O and bandwidth savings in clouds (S3/GCS), and better throughput under read contention.

- Transactions and Persistence
  - Copy-on-write and rebase ensure data/index consistency.
  - Pluggable storage backends (local to cloud) under a single API.

### Parallel Scans and No-GIL Readiness

- Optional work-stealing scheduler with per-worker local deques and top-of-queue stealing to balance load under skew.
- Per-worker adaptive chunking (target 0.5–2 ms per chunk, EMA with configurable alpha) with min/max bounds and an in-flight cap per worker to protect tail latency.
- Simple configuration surface (parallel.*):
  - parallel.max_workers (defaults to number of cores or min(cores, 8))
  - parallel.scheduler ("work_stealing" | "thread_pool")
  - parallel.initial_chunk_size, min/max_chunk_size
  - parallel.target_ms_low/high, parallel.chunk_ema_alpha
  - parallel.max_inflight_chunks_per_worker
- Metrics and observability: chunks/records processed, p50/p95/p99 chunk service times, steals attempted/successful, queue depths, etc., via a Python callback.
- Semantics intact and backward compatibility: if you select "thread_pool" with a fixed chunk size, behavior is identical to before.
- Ready for Python without the GIL: a GIL-agnostic design that already improves throughput on current CPython and scales better on free-threaded builds with no code changes.

## Business Benefits

- Reduced TCO: fewer services to provision, operate, and monitor.
- Faster time-to-market: one engine for structured + vector + transactions.
- Lower risk: explainable plans; consistent indexes; deterministic behavior.
- Portability: local dev to edge to cloud without rearchitecture.
- Better SLOs with less infra: the atom cache avoids repeated reads and redundant deserializations, improving P95/P99 in high-QPS environments without adding Redis or CDNs.

## Market Message

“ProtoBase is the embedded, LINQ-style database for Python that unifies semantic search, range filters, and transactional indexes in a single engine. Now with atom-level caching for sub-millisecond hot reads and adaptive parallel scans (work-stealing) for higher throughput and lower tail latency—built to run great on CPython today and ready for Python’s no-GIL future. Build modern AI experiences with the simplicity of a library and the power of a database.”

## Packaging and Pricing (Indicative)

- Open Source (Core)
  - LINQ-like API, secondary indexes, range operators, explain, exact/IVF-Flat vector search.
  - Atom-level cache básico habilitable con defaults seguros y métricas sumarias.

- Commercial (Recommended for business)
  - Priority support (SLAs), advanced security/observability, index snapshot tooling, background rebuild/compaction.
  - Atom cache avanzado: telemetría p50/p95/p99, top-N keys, tuning online de tamaños y epoch bump controlado.
  - Optional advanced ANN (e.g., HNSW) and enterprise deployment aids.
  - Flexible tiers by team, environment, and scale.

## Go-to-Market (Developer-First)

- Content: “Explain your query” + “Cache heatmaps” series (before/after the atom cache; hit ratios; P95/P99), reproducible notebooks, and an included benchmark script.
- Integrations: FastAPI with hybrid search endpoints and cache metrics; ML pipelines with quality validation (recall/latency).
- Community: contribution guide, “good first issue” labels, public roadmap.
- Storytelling: “Fewer services, same SLOs” — comparisons: embedded ProtoBase vs a stack of Redis + vector DB + document store.

## Roadmap (High-Level)

- 1–3 Months
  - LINQ parity for core operators; robust explain; range/index recipes.
  - Vector API polishing (KNN, thresholds, ordering by similarity).
  - Atom cache: basic metrics and docs; FastAPI example with exposed metrics.
  - Tuning guides and comparative benchmarks (ANN params, range ops, cache hit ratio vs memory).

- 3–6 Months
  - Basic joins (inner/left) with filter pushdown; lightweight index stats.
  - Index snapshot/restore and background rebuild.
  - Observability for the optimizer and the cache layer (per-operator and per-layer).

- 6–12 Months
  - Hybrid relevance (BM25 + vector) and reranking recipes.
  - Sharding/partitioning strategies for large vector workloads.
  - Enterprise controls (audit, compliance, encryption at rest/in use).
  - Cache: TinyLFU/W-TinyLFU option and asynchronous preloading of immediate children.

## Conclusion

ProtoBase redefines what an embedded database can do for AI-era applications: a unified, transactional engine for indexed structured data and vector similarity search, with a familiar declarative API and explainable optimization. It delivers the power of a database with the ergonomics of a library—directly inside Python.