Introduction
============

What is ProtoBase?
------------------

ProtoBase is an embedded, transactional, object‑oriented data platform for Python. It operates directly on native Python objects at memory speed using copy‑on‑write, while a decoupled persistence engine ensures durability in the background. You get the ergonomics of Python data structures with ACID guarantees, immutable secondary indexes, and an index‑aware query optimizer.

Key Capabilities
----------------

* Near in‑memory performance: reads and writes within a transaction execute over in‑memory, immutable structures; persistence runs on background threads.
* Intelligent write‑through cache: newly persisted atoms are immediately published to object and bytes caches for instant read‑after‑write.
* Advanced query optimizer: exploits secondary indexes for single‑term, AND, and OR predicates (AndMerge, OrMerge, IndexedSearchPlan, IndexedRangeSearchPlan) and performs efficient range traversal without premature materialization.
* Rich data structures: Dictionary, List, Set, and HashDictionary with durable semantics.
* Multiple storage backends: in‑memory, file‑based with WAL, cluster/cloud variants.
* Vector search (optional): exact and ANN (IVF‑Flat) under the same query framework.

Ideal Use Cases
---------------

* High‑throughput transactional apps: carts, session stores, leaderboards, telemetry.
* Complex object graphs: nested documents, social graphs, configuration systems.
* Persistent, transactional cache: a higher‑level alternative to Redis/Memcached when you need multi‑key ACID semantics.
* Prototyping and product development: when `pickle`/`shelve` are too limited and a full RDBMS/ORM is too heavy.

Architecture Overview
---------------------

ProtoBase is organized around several key abstractions:

* Atom: the base class for all durable objects.
* Storage layer: persistence engines for memory, files (WAL), clusters, and cloud.
* Data structures: durable collections built on Atoms.
* Query system: expression compiler, optimizer, and execution plans.

For a deeper dive, see the :doc:`architecture` section.
