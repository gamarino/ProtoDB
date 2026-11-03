Introduction
============

Updated: 2025-09-19

What is ProtoBase?
------------------

ProtoBase is an embedded, transactional, object‑oriented data platform for Python. It operates directly on native Python objects at memory speed using copy‑on‑write, while a decoupled persistence engine ensures durability in the background. You get the ergonomics of Python data structures with ACID guarantees, immutable secondary indexes, and an index‑aware query optimizer.

Key Capabilities (current)
--------------------------

* Near in‑memory performance: reads and writes within a transaction execute over in‑memory, immutable structures; persistence runs on background threads.
* Write‑through atom caches: newly persisted atoms are immediately published to object and bytes caches for instant read‑after‑write; single‑flight avoids duplicated loads under concurrency.
* Advanced query optimizer: exploits secondary indexes for single‑term, AND, and OR predicates (AndMerge, OrMerge, IndexedSearchPlan, IndexedRangeSearchPlan) and performs efficient range traversal without premature materialization.
* Rich data structures: Dictionary, RepeatedKeysDictionary, List (AVL‑backed), Set, and CountedSet with durable semantics.
* Query composition: JoinPlan (inner/left/right/outer/external variants), FromPlan for aliasing, and a LINQ‑like API including select_many for lateral joins.
* Multiple storage backends: in‑memory, file‑based with WAL; cloud/cluster integrations are optional and gated by dependencies.
* Vector and Arrow integration (optional): helpers live behind optional dependencies and are mocked during docs build.

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
* Storage layer: persistence engines for memory, files (WAL), clusters, and cloud (optional).
* Data structures: durable collections built on Atoms.
* Query system: expression compiler, optimizer, and execution plans, plus LINQ‑like API.

For a deeper dive, see the :doc:`architecture` section.
