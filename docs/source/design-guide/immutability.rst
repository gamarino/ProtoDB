Pointers that never go stale
============================

*Immutability and pointers as the basis for cache validity without invalidation. Decisions D53 and D56;
rulings R75 and R76; principle P1.*

What it gives the ERP
---------------------

Thirty sales people open the pipeline report in the morning, each in a new transaction. In the
:doc:`ERP tutorial </tutorials/erp>` the first report, on a freshly opened space, decoded 2,012 atoms from
disk; the same report in a new transaction decoded **0**, and a related query decoded 6 (the tutorial's
executed output, "Reusing what earlier transactions read"). Nobody wrote a line of cache code, and nothing in
the ERP tells any cache that an opportunity changed. When Bob wins a deal, the next report sees it, and it
cannot see a stale copy of it.

This chapter explains why a cache that is never invalidated can still never be wrong.

The problem
-----------

Transactions in ProtoDB are explicit objects, each reading its own snapshot (P2). Before Task 8.3f, every
transaction decoded every atom it touched, even when another transaction had just decoded the same atom. The
storages already cached record *bytes* by pointer, but each transaction still fetched them, checked them,
unpacked them and built fresh Python objects.

**Measured symptom** (D53, probe ``r75/readbench.py`` with cProfile, before ``c78e4fd``): a point read in a
100,000-entry ``Dictionary`` on ``FileStorage`` loaded about 23 atoms per transaction and took about 1,475 us,
**about 80 % of it in** ``Atom._load``: fetching, CRC checking, unpacking msgpack and running the codec.

Why the obvious solution fails
------------------------------

The obvious fix is the one every application server uses: keep decoded objects in a shared cache, and
invalidate an entry when the object changes. In a database that updates in place, that is the hard part:

* a writer must find every cache that holds the old state, in every thread, process and node, and tell it;
  across machines that costs messages, or it accepts stale reads for a while;
* so ORMs keep their identity caches per session, and the work is paid again by every session (ruling R75);
* a cache of query *results* is harder still, because a write can change a result without touching any row
  the result holds. MySQL removed its query cache in 8.0 for that reason: invalidating it did not scale
  (the maintainer's point in ruling R76).

The other obvious fix, a cache per transaction, is easy and safe, but it pays the full cost again in every
transaction, which is exactly the cost to remove.

The alternatives and their costs
--------------------------------

Two forms were considered for the decoded-atom cache (D53):

* **cache the msgpack-decoded payload only**, and build the objects per transaction. Rejected: the profile
  showed the codec walk and the creation of the objects to be the larger part of a load, so this keeps most
  of the cost. It was not measured as a separate variant;
* **a per-transaction cache.** Rejected: each transaction pays the decode again.

For query results (D56) the alternatives were about *where* a result lives:

* **every query cached automatically.** Rejected: it would materialize results that callers stop reading
  early;
* **results persisted and shared by key across processes.** Rejected: it needs a key-to-pointer table outside
  the root in every storage, keeps every result forever (nothing reclaims data, R77a), and puts a write on the
  read path.

The choice, and the invariant that makes it correct
---------------------------------------------------

**The invariant (P1): a pointer names one content forever.** A storage instance never changes the bytes at a
pointer. Records are appended, never rewritten; a ``TieredStorage`` trims only when it is opened, which
creates a new space. A changed object is a *new* record with a *new* pointer. And a decoded atom is
immutable. So:

* an object cached under a pointer is, by construction, the object any later read of that pointer would
  produce. There is nothing to invalidate;
* an evicted entry stays valid for whoever still holds it;
* any number of transactions and threads may share it.

**Decoded atoms (D53).** Each ``ObjectSpace`` keeps one bounded cache of decoded atoms keyed by storage pointer
(``atom_cache_entries``), shared by all its transactions and threads. ``Transaction.load`` returns the cached
atom for a pointer. The children of a shared atom are loaded through the same cache, so they are shared too.
Conflict control is unchanged: a read set records pointers, and a pointer identifies a version exactly.

Measured (D53, probe ``r75/readbench.py``, 10,000 point reads each in a fresh transaction, 100,000-entry
``Dictionary`` on ``FileStorage``, three runs, ``c78e4fd``): median **821 to 4,721 reads per second (5.8x)**,
1,218 to 212 us per read.

**Query results (D56).** A query's cache key is built from the physical plan without running it: the plan's
structure and parameters, with every collection and index it reads named by **storage pointer**. A commit that
changes a collection gives it a new pointer, hence a new key: the old entry is never *wrong*, only never asked
for again, and it ages out of the LRU. A cached result holds atom rows as ``(pointer, class)``, so it never
keeps decoded objects alive by itself.

Measured (D56, on the Task 8.4b code, D56 recorded at ``e9afb72``; 5 interleaved runs per GIL mode, a range query plus a residual filter returning
about 10^4 rows, N = 10^5): **no cache 77 ms, cache miss 135 ms, cache hit 3.9 ms**; SQLite, which has no
result cache, re-executes in 13.1 ms.

Its cost, stated honestly
-------------------------

* **Memory.** A shared cache holds what it decodes, and a decoded object holds what it references. Bounding
  that turned out to be the hardest problem of the whole design: see :doc:`residency`.
* **A miss costs more than no cache.** A query-cache miss freezes the result rows and estimates their size,
  about 1.7x an uncached execution on 10^4 rows (about 5.7 us per row; D56, open item). The cache is therefore
  opt-in (``Queryable.cached``), and it pays only for a query that repeats on a root that changes less often
  than it is read.
* **Per process.** Results are not shared between processes. A stable *pager* gives what processes really
  need to share (see :doc:`pager-and-rank`).
* **The first, ad-hoc query is not helped at all.**

A mistake: the pointer covers the data, not everything else
-----------------------------------------------------------

The argument "equal pointers, equal results" is only as good as the key. The adversarial review of Task 8.4b
found two keys that were equal while the results were not (``task-8.4b-review.md``, findings C1 and I1, both
reproduced):

* **The decimal context (C1, Critical).** A ``group_by`` that sums ``Decimal`` values depends on the thread's
  decimal context (precision, rounding), which the key omitted. One thread ran a cached sum under
  ``prec=3``; every other thread with the same key then received the rounded totals, silently:
  ``Decimal('3.69')`` instead of ``Decimal('3.70370367')``.
* **Display strings in the key (I1).** An index plan's key used its ``explain()`` details, which render
  values as text, so ``F.tag == b"x"`` and ``F.tag == "b'x'"`` produced the same key, and the second query
  received the first query's rows.

The fix (ruling R99) puts every input of the result into the key: the decimal context whenever the plan
aggregates or computes, and the plans' structured fields instead of display strings. The lesson for the
student: **immutability makes the data side of a cache key free; everything else the result depends on must
still be in the key.** A function registered by name (an extractor) is such an input too; its registration
generation is part of the key, and extractors must be pure functions.

What was tried and rejected
---------------------------

* Invalidation-based caches: never built; the invariant makes them unnecessary.
* A payload-only cache and a per-transaction cache (above).
* Automatic caching of every query, and results persisted and shared by key (above).

Back to the ERP
---------------

The morning's thirty pipeline reports share one set of decoded opportunities, and a repeated report on an
unchanged root is answered from the query cache. When Bob wins a deal, his commit writes new records with new
pointers. The next report starts from the new root, whose pointers lead to the new records; the old cached
atoms and the old cached result are still correct for the old root, which is exactly what a report that
started earlier must see. No code anywhere had to be told that something changed.

The cache layers that make this work, and how each is bounded, are the subject of :doc:`cache-layers`.
