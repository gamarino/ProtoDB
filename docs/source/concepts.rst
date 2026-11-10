Concepts
========

ProtoDB is an embedded, transactional, object-oriented database for Python. It is built on one idea,
stated as the first design principle: **everything stored is immutable**. A modification creates a new
version that shares what did not change with the old one, and becomes visible only when a new root is
published. The rest of the design follows from that, and this page introduces the six concepts you need:
values, atoms, collections, transactions, indexes and queries. The :doc:`design principles
<design-principles>` are the rules every decision answers to; the :doc:`tutorials <tutorials/index>` use
every concept on running code.

Values
------

A value is anything ProtoDB can store: ``None``, ``bool``, ``int``, ``float``, ``Decimal``, ``str``,
``bytes``, ``date``, ``time``, ``datetime`` (naive or aware), ``timedelta``, ``UUID``, ``tuple``, ``list``,
``dict``, atoms, and handles on mutable cells (:class:`~proto_db.values.MutableObject`). Anything else is
refused where you build it (``ProtoValidationError``), not when you commit. A stored ``list`` or ``dict``
becomes a frozen copy that raises when mutated, because other versions share it.

All values are ordered by one **total order across types**, the same one used by collections (key order),
indexes and queries (decision D2):

* the ranks, lowest first, are the absent-key marker ``MISSING``, ``None``, ``bool``, number, ``str``,
  ``bytes``, ``date``, ``time``, ``datetime`` (naive and aware are separate ranks), ``timedelta``, ``UUID``,
  ``tuple``, atoms and, last, ``MutableObject`` handles;
* ``int``, ``float`` and ``Decimal`` share one rank and compare exactly, without converting to ``float``:
  ``1 == 1.0 == Decimal(1)`` and ``10**16 + 1 != 10**16``;
* ``bool`` has its own rank: ``True`` is not the number 1;
* ``NaN`` is accepted as a stored value but refused as a key, because it breaks a total order.

Because there is a single comparator, an index and a full scan can never disagree about which of two values
is smaller or whether they are equal. That is the foundation of the invariant on the :doc:`query planner
<query-planner>` page.

Atoms
-----

An **atom** is a persistent immutable object: something with a stable identity (an ``oid``) that is saved
once and addressed by an ``AtomPointer`` (a segment and an offset). A pointer names one content forever; a
changed object has a different pointer. Collections, index structures, records (``DBObject``) and the
database roots are all atoms.

Atoms are saved children first, exactly once, when a transaction commits, and they are loaded lazily: an atom
read back by pointer is a stub until you first touch an attribute, and the loading is thread-safe. A decoded
atom is shared, by pointer, by every transaction of the space (:doc:`objects-across-transactions`).

Equality of atoms is identity by ``oid``, never ``id()`` or a storage pointer. Two versions of a record with
the same content are different atoms; ``content_equals`` compares content when you want that.

Collections
-----------

The collections are persistent trees whose every update returns a new collection sharing its unchanged
subtrees (path copying):

========================== =================================================================================
``List``                   positional AVL tree; ``get_at``, ``set_at``, ``insert_at``, ``remove_at``,
                           ``append_first`` / ``append_last``, ``extend``, ``slice``
``Dictionary``             AVL tree ordered by the key's order; ``get_at``, ``set_at``, ``remove_at``, ``has``,
                           ``keys``, ``values``, ``items``
``HashDictionary``         AVL tree keyed by a process-stable hash of any canonical key, with collision buckets
``Set``                    members keyed by hash; ``add``, ``remove``, ``has``, ``union``, ``intersection``
``CountedSet``             a multiset with ``count_of`` and ``total_count``
``RepeatedKeysDictionary`` a multimap: several records under one key
========================== =================================================================================

Every collection is an ``IndexedCollection``: it can carry indexes, none by default. There is no
non-indexable collection, so a query can always consult whatever indexes the collection has.

Transactions
------------

An ``ObjectSpace`` owns a storage. A ``Database`` is a named set of roots inside it. A ``Transaction`` is an
explicit object: a snapshot of one database plus pending writes. It is not bound to a thread, several can be
open in one thread, and one can be handed from thread to thread. A commit validates what *that* transaction
read and wrote and nothing else, and it either publishes a new database version or raises.

Every committed version stays readable forever. ``Database.history()`` lists them and ``snapshot_at`` opens
one. The rules of conflict detection, merging and retry are the :doc:`concurrency model <concurrency-model>`.

Indexes
-------

An index is a persistent structure inside a collection that answers "which elements have this key?" without
reading every element. ProtoDB has three kinds: equality, range and vector. New kinds are added by
subclassing ``Index``. Every mutator of every collection updates its indexes in the same step as the
structure, so no code path returns a collection with a stale index (:doc:`indexing-framework`).

Queries
-------

A query is built with the LINQ front end (``from_collection(...).where(...)``) or directly with expression
terms, and turned into a physical plan by one planner shared by every front end. The planner picks the
cheapest access path among the collection's indexes, keeps every condition it did not use as a residual
filter, and never drops a condition. ``explain()`` shows exactly the plan that runs (:doc:`query-planner`).

How the concepts fit
--------------------

.. code-block:: text

   ObjectSpace --- storage (memory | files | cloud | cluster)
      |
      +-- Database "shop" --- versions 0, 1, 2 ... (each an atom pointing at the one before)
             |
             +-- roots: "orders" -> Dictionary --- indexes: by_status, by_amount
             |          "config" -> DBObject
             +-- mutable cells: "customer:acme" -> DBObject

A commit saves the new atoms (the changed leaf-to-root paths of every collection and index it touched),
makes them durable, and then publishes a new root that points at the new database version. Readers that
started before still hold the old root, which still describes a complete database.
