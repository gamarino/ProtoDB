The query planner
=================

One planner, shared by every query front end, turns a predicate, an ordering and a window into an executable
tree of plan nodes. This page describes what it does and why, and states the invariant it must keep. The
decisions are D28, D36, D37, D38 and D56. The tutorial on :doc:`indexing and querying
<tutorials/indexing-querying>` shows plans on running code.

Front ends and plans
--------------------

The LINQ front end (``from_collection(source).where(...).order_by(...).take(...)``) and the direct API
(``plan_query(source, where, order_by, limit, offset)``) build the same plans. A condition is a ``Term``
``(path, operator, value)`` or a combination (``And``, ``Or``, ``Not``). The operators are ``==``, ``!=``,
``<``, ``<=``, ``>``, ``>=``, ``in``, four ``between`` forms, ``?N`` and ``?!N`` (is none, is not none),
``?T`` and ``?!T`` (truthy), ``contains`` and ``near[]``. A Python callable is allowed and is translated to an
explicit ``PythonFilter`` node; ``explain()`` names it, so it is never hidden.

The physical nodes are ``CollectionScan``, an index access node (``IndexLookup``, ``IndexRange``,
``IndexOrderedScan``, or the ``kind`` a custom index chose), ``VectorSearch``, ``Filter``, ``PythonFilter``,
``Intersect``, ``Union`` (by element identity), ``Project``, ``Sort``, ``Limit``, ``GroupBy``, ``Join``,
``Unnest``, ``Distinct``, ``ValuesScan`` and ``Count``. A plan is a pure function of its arguments and the
collection version: nothing is cached between planning calls, and the plan returned is the plan that runs.

What the planner does
---------------------

Normalise
   ``And`` and ``Or`` are flattened and negations are pushed through them by De Morgan's laws, which is exact
   because evaluation is two-valued. A negated term stays a residual ``Not(term)`` filter, except
   ``Not(?N)`` which becomes ``?!N`` and the reverse. ``!=`` is *not* the complement of ``==``: a missing key
   satisfies only the negation. A negated ``near[]`` raises ``ProtoNotSupportedError``.

Choose an access path
   For a conjunction the candidates are the full scan with every conjunct as a residual filter, and, for each
   conjunct that some index can answer, that index's plan with the other conjuncts as residual filters.
   The cheapest candidate wins. Cost is the estimate of the candidate tree: the index estimates of the
   :doc:`index framework <indexing-framework>` composed by the node estimates (a ``Filter`` costs one unit
   per source row it evaluates). Ties prefer a plan without a full scan, then the smaller sorted tuple of
   index names, so a choice is deterministic. No candidate intersects two index plans: under this cost model an
   ``Intersect`` never costs less than a filter over the better of its two inputs, so enumerating them would
   only add untested code.

Disjunctions
   An ``Or`` gets a ``Union`` candidate only when every branch has an index-only plan; otherwise the scan
   filtered by the whole ``Or`` is the only plan, because a ``Union`` that includes a scan is always dearer
   than the scan alone.

Order
   For an ordering on one key, each index that can yield that key in order contributes an ordered scan with the
   whole predicate as a residual, competing with ``Sort`` over the planned predicate. A selective predicate is
   planned by index and sorted; an unselective one streams the ordered index. With several keys the plan uses
   ``Sort``.

Vector search
   ``near(k) AND P`` means the k nearest among the elements satisfying P, not the k nearest with P applied
   afterwards (which returns fewer than k rows). The planner hands the other conjuncts to the vector index
   (``filtered_plan``) and raises ``ProtoNotSupportedError`` if no index can restrict its search. It never
   silently changes the meaning.

The invariant and the cost of a wrong estimate
----------------------------------------------

For every collection, every index configuration and every predicate, the plan returns the same multiset of
elements as a full scan (property-tested). The planner can therefore only be *slow* by choosing badly, never
*wrong*. ``explain()`` prints the plan that will run with the estimate of each node, so a surprising choice is
visible. The cost model is deliberately simple: it counts occurrences produced, key groups visited and tree
levels descended. Known limits: a ``Limit`` is charged the whole source cost, so a top-k query may sort where
streaming an ordered index would stop early; for a tiny collection the scan is cheaper than any index and the
planner says so.

No exception in the query path is caught to return a fallback result. A malformed query raises
``ProtoValidationError`` and an unsupported one ``ProtoNotSupportedError``.

Results, caching and pages
--------------------------

The space's query result cache (``ObjectSpace.query_cache``) keys a result by the *pointers of what the plan
reads* (the collection and each index it uses) and the plan's own description. Because a pointer's content
never changes, an entry is never stale, and a write to an unrelated collection leaves every other entry valid.
A plan that depends on something other than stored content (a Python callable, an unregistered extractor, a
non-deterministic backend) is not cached; the counters of ``query_cache_stats()`` say how many were refused.

A ``Pager`` serves page *k* of one plan on one snapshot. When the plan offers rank access (an index lookup,
range or ordered scan, a ``List`` or ``Dictionary`` scan), page *k* costs O(log n + page size); any other plan is
materialised once and sliced. A pager made through a transaction or snapshot has a **token**: a portable
reference to the snapshot and the query, which any process can resume (``Database.resume_pager``) to get the same
pages. A token whose query now plans differently (another planner or extractor version) is refused
(``ProtoStalePagerError``) rather than paged in another order. Tokens carry no signature: do not hand one to a
party that may not read every version of the database.
