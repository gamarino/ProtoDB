# ProtoBase: Commercial Positioning (2025)

## Executive Summary

ProtoBase is an embedded, transactional, object-oriented database for Python that delivers a unified engine for structured and vector data—featuring a LINQ-like query API, immutable secondary indexes, range-aware optimization, and integrated vector similarity search. New in 2025, ProtoBase introduces an atom-level caching layer (AtomObjectCache + AtomBytesCache) that reduces P95/P99 latencies for hot reads across transactions by avoiding unnecessary page reads and repeated deserializations.

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
- ProtoBase unifies: transactions + rich data structures + secondary and vector indexes + range-aware queries, all embedded in Python, ahora con cache a nivel de átomo para latencias predecibles en lecturas calientes.

## Key Differentiators

1) Declarative, LINQ-like API with lazy, optimized execution (pushdown + early limits).
2) Immutable secondary indexes; reusability and predictable performance.
3) Range operators with configurable inclusivity: [], (), [), (].
4) Practical optimizer based on selectivity and early exit.
5) Integrated vector search (exact + ANN) composable with structured filters.
6) Transactional consistency across data and all indexes via copy-on-write and rebase.
7) Embedded simplicity with unified persistence (memory, file, cluster, cloud).
8) Atom-level caching (nuevo 2025): objeto y bytes caches con política 2Q y single-flight; mejora P95/P99 en lecturas calientes y reduce I/O/egress.

## Core Use Cases

- Embedded RAG: store embeddings, documents, and metadata; retrieve via KNN; filter by attributes/ranges; rerank and serve.
- Semantic search and recommendations with complex business rules in SaaS products.
- Deduplication/fuzzy matching where vector similarity and structural constraints must co-exist.
- Edge analytics with local semantic search and low-latency filters.

## Technical Capabilities

### Nuevo posicionamiento y potencial

- Tesis: el mayor costo de las apps AI-embebidas no es el almacenamiento en sí, sino la latencia tail y la complejidad operativa de múltiples servicios. El cache a nivel de átomo reduce ambos.
- Potencial:
  - SaaS B2B y productos con RAG/semantic search: mejoras de 2–3x en P95/P99 de lecturas repetitivas sin agregar Redis.
  - Edge/IoT y on-device: menor huella, menos I/O y energía, consistentemente predecible.
  - Plataformas developer-first (SDK/OEM): adopción simple vía librería Python con defaults seguros.
- Ventaja competitiva: combinación única de LINQ Pythonic + índices inmutables + búsqueda vectorial + cache atómico embebido.
- Riesgos/mitigaciones: cambios de formato resueltos con schema_epoch; límites de memoria y 2Q evitan polución por escaneos.

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

- Caching y Lecturas en Clúster
  - Cache de páginas físicas leídas: si un objeto solicitado reside en una página ya en memoria, se evita la lectura desde storage.
  - Cache a nivel de átomo (nuevo): dos niveles opcionales en memoria antes del page cache — AtomObjectCache (objetos deserializados) y AtomBytesCache (bytes crudos). Clave por AtomPointer (transaction_id, offset) y, opcionalmente, schema_epoch para el object cache. Política 2Q para evitar polución por escaneos y single-flight para deduplicar deserializaciones concurrentes. Resultados: mayor hit ratio entre transacciones y menor latencia aún cuando la página no esté en cache.
  - Lectura consciente de clúster: antes de ir a storage (archivo o S3), los nodos consultan a sus pares; si alguno posee la página, se realiza una transferencia interservidor, típicamente más rápida que I/O de disco u objeto.
  - Localidad transaccional: los objetos modificados en una misma transacción se agrupan en páginas contiguas, aumentando la probabilidad de aciertos de cache y reduciendo misses de lectura.
  - Beneficios prácticos: menor latencia P95/P99 en lecturas calientes, ahorro de I/O y ancho de banda en nubes (S3/GCS), mejor throughput bajo contención de lectura.

- Transactions and Persistence
  - Copy-on-write and rebase ensure data/index consistency.
  - Pluggable storage backends (local to cloud) under a single API.

## Business Benefits

- Reduced TCO: fewer services to provision, operate, and monitor.
- Faster time-to-market: one engine for structured + vector + transactions.
- Lower risk: explainable plans; consistent indexes; deterministic behavior.
- Portability: local dev to edge to cloud without rearchitecture.
- Better SLOs with less infra: el cache de átomos evita lecturas repetidas y deserializaciones redundantes, mejorando P95/P99 en entornos de alto QPS sin agregar Redis o CDNs.

## Market Message

“ProtoBase is the embedded, LINQ-style database for Python that unifies semantic search, range filters, and transactional indexes in a single engine. Now with atom-level caching for sub-millisecond hot reads and predictable latency. Build modern AI experiences with the simplicity of a library and the power of a database.”

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

- Content: “Explain your query” + “Cache heatmaps” series (antes/después del cache de átomos; ratios de hit; P95/P99), notebooks reproducibles y script de benchmark incluido.
- Integrations: FastAPI con endpoints de search híbrido y métricas de cache; pipelines ML con validación de calidad (recall/latencia).
- Community: contribution guide, “good first issue” labels, public roadmap.
- Storytelling: “Menos servicios, mismas SLOs” — comparativas: ProtoBase embebido vs stack Redis + vector DB + doc store.

## Roadmap (High-Level)

- 1–3 Months
  - LINQ parity for core operators; robust explain; range/index recipes.
  - Vector API polishing (KNN, thresholds, ordering by similarity).
  - Atom cache: métricas básicas y docs; ejemplo FastAPI con métricas expuestas.
  - Tuning guides and comparative benchmarks (ANN params, range ops, cache hit ratio vs memoria).

- 3–6 Months
  - Basic joins (inner/left) with filter pushdown; lightweight index stats.
  - Index snapshot/restore and background rebuild.
  - Observability para el optimizador y la capa de cache (per-operator y per-layer).

- 6–12 Months
  - Hybrid relevance (BM25 + vector) and reranking recipes.
  - Sharding/partitioning strategies for large vector workloads.
  - Enterprise controls (audit, compliance, encryption at rest/in use).
  - Cache: opción TinyLFU/W-TinyLFU y precarga asíncrona de hijos inmediatos.

## Conclusion

ProtoBase redefines what an embedded database can do for AI-era applications: a unified, transactional engine for indexed structured data and vector similarity search, with a familiar declarative API and explainable optimization. It delivers the power of a database with the ergonomics of a library—directly inside Python.