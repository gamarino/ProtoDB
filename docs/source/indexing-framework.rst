The index framework
===================

Every collection can carry indexes and every query can use them. This page explains the model, the hooks a
new kind implements, what the three built-in kinds store, and how to prove a new kind correct. The decisions
are D22, D29, D30, D39 and D56. The :doc:`tutorial on indexing <tutorials/indexing-querying>` uses the
built-in kinds, and the :doc:`tutorial on writing one <tutorials/custom-index>` builds a new kind end to end.

The model
---------

``IndexedCollection`` is the base of every collection:

``indexes``
   an immutable ``IndexSet`` mapping a name to an ``Index``; empty by default.

``add_index(index)``, ``remove_index(name)``, ``get_index(name)``, ``index_names()``
   ``add_index`` builds the index over the elements the collection holds now (``index.empty().build(elements)``,
   so content already held by the object you pass never leaks in) and returns a **new collection version**.

``elements()``
   what the collection indexes and what queries return: a list's items, a dictionary's values, a set's members,
   and a multimap's records.

``as_query_plan()``
   exposes the collection and its indexes to the planner, for every collection.

Every mutator computes the new structure and the new ``IndexSet`` in the same step, through
``IndexSet.on_add / on_remove / on_replace``. There is no code path that returns a collection with a stale
index, and a contract test enforces it for every mutator of every collection. An index is a persistent atom
like any other: it is saved with the collection and shares structure with its previous version.

Keys: ``KeySpec``
-----------------

An index is defined by a persistable description of its key, a ``KeySpec``:

* ``KeySpec.path("address.zip")``: the value at a dotted attribute path (or mapping key). A step through an
  absent attribute, or through a value that is not a record or mapping, yields ``MISSING``. An explicit
  ``None`` stays ``None``: absence and ``None`` are different keys. Private (``_x``) and reserved names are
  never read.
* ``KeySpec.element()``: the element itself.
* ``KeySpec.extractor("name")``: a function registered in this process with ``register_extractor(name, fn,
  version=...)``. Only the name is persisted, never the function. It must be pure: the same element always
  gives the same key, because indexes store the keys it computed and the query result cache serves a result
  for as long as the versions it read are asked for. Its identity across processes is its name and declared
  version, so changing what it returns needs a new name or version, exactly as changing an index definition
  does.

``MISSING`` is recorded under a distinct marker, so ``?N`` / ``?!N`` (is none, is not none) agree between an
index and a scan.

The hooks
---------

An index subclasses ``Index`` (an ``Atom`` with an abstract-method metaclass), gives itself a stable dotted
``__type_name__``, and implements:

``__init__(name, key_spec)``
   Create an **empty** index. Extra definition fields are keyword arguments listed in ``_definition`` so
   that ``empty()`` can rebuild them.

``with_added(element)`` (abstract)
   The index with one more occurrence of ``element``. Elements may repeat (a ``List``), so keep a multiset.

``with_removed(element)`` (abstract)
   The index with one occurrence fewer. Removing something the index does not hold is an error.

``supports(term)`` (abstract)
   Can ``plan`` answer this term **exactly** as a scan would? Start with ``self._on_key(term)``.

``estimate(term)`` (abstract)
   ``Estimate(rows, cost)`` for the planner; ``ProtoNotSupportedError`` if the term is unsupported.

``plan(term)`` (abstract)
   An ``IndexPlan(self, term, producer, kind=...)``. ``producer()`` returns a fresh iterator over the matching
   elements, each occurrence once, capturing immutable state only.

``with_replaced(old, new)``
   Default: remove, then add. Override when the structure allows better.

``build(elements)``
   Bulk build. Default: the fold of ``with_added`` starting from ``empty()``.

``empty()``
   An empty index with the same definition. The default rebuilds it from ``_definition``.

``supports_order(key_spec)`` and ``ordered_plan(descending)``
   Ordered iteration, used by ``order_by``. Default: none.

``supports_filtered(term)`` and ``filtered_plan(term, where)``
   Answer a term over only the elements that satisfy a predicate (``near(k)`` needs it). Default: none.

``explain()``
   ``{"index", "kind", "key", ...}`` for the plan's ``explain()``.

``validate(elements)``
   Check against a full scan; ``ProtoCorruptionError`` on a mismatch. Used by tests and consistency checks.

Indexes are immutable. Every hook returns a **new** index (``self._with(field=value)`` copies the index with
fields replaced) and never modifies the receiver. Public attributes are the persisted state; set them in
``__init__`` with ``object.__setattr__``. A plan captures immutable state only, because plans run more than
once and from several threads.

What exactness means
~~~~~~~~~~~~~~~~~~~~

``supports`` and ``plan`` promise that the plan returns the same multiset of elements that a scan would
return for the term, under the semantics of ``Term.matches`` (D28): ``1 == 1.0``, ``True != 1``, an absent key
matches only ``?N``, an unorderable key (list, dict, ``NaN``) matches only ``!=`` and ``?!N``, ordering
operators match only keys of the operand's type rank. The planner is free to substitute an index for a scan
*because* of this promise. An index that can only prune (a trigram index) keeps a side group for elements it
cannot file, and verifies each candidate with ``term.matches``, so the promise holds by construction.

The built-in kinds
------------------

``EqualityIndex``
   A keyed AVL tree of *key groups*, ordered by ``order_key`` of the key; each group holds its members with
   counts, so duplicates are kept and a plan returns the stored elements, never an equal representative
   (``1``, ``1.0`` and ``Decimal("1.00")`` are one key but three values). Answers ``==``, ``in``, ``!=``,
   ``?N``, ``?!N``. ``!=`` is charged a full pass, so a scan is preferred unless a residual filter makes the
   scan dearer.

``RangeIndex``
   The same structure, also answering ``<``, ``<=``, ``>``, ``>=`` and the four ``between`` variants,
   bounded by the type rank, and yielding elements in key order (``supports_order``). Both key-group indexes
   carry subtree weights, so occurrence *r* is found in O(log n): this is what makes page *k* of a pager cost
   O(log n + page size).

``VectorIndex``
   An id-to-vector store plus a backend: ``exact`` (brute force, in bulk with numpy, re-scoring every
   candidate that could matter so the answer equals brute force exactly), ``ivf_flat`` (persisted centroids
   and inverted lists, ``nprobe`` of ``nlist`` lists searched; ``nprobe == nlist`` is exact) and ``hnsw``
   (optional, a graph rebuilt lazily from the store and cached per process). It answers ``near[]`` with ``k``
   and/or a ``threshold``, for the metrics ``cosine``, ``l2`` and ``ip``, and answers it over only the
   elements that satisfy the other conditions (``filtered_plan``).

Writing a custom index
----------------------

Follow the steps in the class documentation of :class:`proto_db.indexing.base.Index`; the short form is:

1. Subclass ``Index`` and set ``__type_name__`` to a stable dotted name (``"acme.search.TrigramIndex"``). It is
   persisted, and the class must be imported in every process that opens a database holding one. Changing what
   an index stores means a new type name (or a ``__type_aliases__`` entry) and a rebuild.
2. Write ``__init__`` to create an empty index and ``with_added`` / ``with_removed`` to maintain it.
3. Write ``supports`` conservatively, ``estimate`` honestly (rows and a cost that is comparable with the other
   access paths) and ``plan`` with a producer that captures immutable state.
4. Override ``supports_order`` / ``ordered_plan`` if you can yield elements in key order, and
   ``supports_filtered`` / ``filtered_plan`` if your answer depends on the whole collection.
5. Prove it: register an ``IndexContract`` for your kind in ``tests/contracts/index_contract.py``.

The contract is the reusable test suite behind requirement 3 of the design. Given an index factory, a way to
draw random elements and the terms to ask, it checks that: every supported term's plan equals the scan, plans
are re-executable and explainable with a valid estimate; random sequences of add, remove and replace keep
``validate`` passing; the index survives a save and a lazy load; eight concurrent readers get identical
results; unsupported terms (and terms on another path) raise ``ProtoNotSupportedError``; the index attached
to every collection type stays consistent under that collection's mutators; no hook modifies its receiver;
``empty()`` keeps the whole definition; ordered iteration, if declared, matches a sort by ``order_key``;
``with_replaced`` equals remove plus add and ``build`` equals the fold of ``with_added``. A kind that passes
implements every hook correctly. The tutorial runs this contract against a new index, and against a
deliberately broken one to show that the contract fails when it should.

Cost of indexes
---------------

An index costs a new version of its structure per write (path copying, so about the height of its trees),
on top of the collection's own. Index what you query. An index never changes what a query returns: for every
collection, every index configuration and every predicate, ``execute()`` returns the same multiset as a full
scan. That invariant is property-tested (``tests/property/test_index_equals_scan.py``) and is the first
anti-goal of the project: an index whose results differ from a scan is a failure even if every test passes.
