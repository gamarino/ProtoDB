Architecture
============

This page maps the code: which package owns which responsibility, what a commit does as it crosses them,
and how threads are used. The decisions behind each layer are in ``docs/decisions.md`` (cited as D-numbers),
and the principles they answer to in :doc:`design-principles`.

Packages
--------

.. code-block:: text

   proto_db/
     __init__.py        the public API: one lazily resolved export table (D47)
     errors.py          the exception hierarchy rooted at ProtoDBError (D12)
     values.py          the value model: canonical types, total order, equality, hash (D2, D3)
     transactions.py    ObjectSpace, Database, Transaction, Snapshot, the commit protocol (D32-D35, D52)
     core/              atom.py (Atom, AtomPointer, oid, lazy loading), codec.py (atom <-> msgpack),
                        dbobject.py (DBObject, MutableObject)
     storage/           base.py (SharedStorage contract), format.py (records and roots on disk),
                        memory.py, file.py (FileStorage), cloud.py (CloudStorage, S3 and GCS),
                        tiered.py (TieredStorage: a cluster node's local tier plus the object store),
                        cache.py, crc.py, logfile.py, ownerlock.py
     collections/       base.py (IndexedCollection, ConcurrentOptimized), avl.py (the persistent trees),
                        lists.py, dictionaries.py, hash_dictionaries.py, sets.py
     indexing/          base.py (Index), keyspec.py, keyed.py (the key-group machinery),
                        equality.py, range.py, vector.py and vector_backends.py
     query/             terms.py, expressions.py, plans.py, planner.py, linq.py,
                        cache.py (query result cache), pager.py (stable pagination)
     cluster/           coordinator.py (lease, permissions, commit by majority), lease.py, protocol.py,
                        rootlog.py (acceptor log), persister.py, transport.py
     runtime/           parallel.py (parallel scans), fsm.py (finite state machines)
     interop/           arrow.py (Arrow and Parquet export)

Two names differ from the first draft of the design (spec section 3). ``indexing`` replaced ``indexes``,
because the 0.1 module ``indexes.py`` collided with it (ruling R16). ``parallel`` and ``fsm`` live under
``runtime``, and the Arrow bridge under ``interop`` (R33, D48). Dependencies point downward: collections know
nothing of transactions, indexes know nothing of storage, and the query layer sees collections only
through their indexes.

The layers
----------

Values and atoms
   Everything stored is a canonical value or an atom. ``codec.py`` turns an atom's persisted state (its
   public attributes) into a msgpack-ready dictionary with an explicit tag for every canonical type, and
   nested atoms into references. Atom classes register under a stable ``__type_name__``, so third-party
   classes such as custom indexes cannot collide.

Collections and indexes
   Collections are persistent AVL trees (``avl.py``): positional for ``List``, keyed by the value order for
   the others. An update copies the path from the changed node to the root and shares everything else.
   A collection owns an ``IndexSet`` and every mutator updates the structure and the indexes in the same
   step. Indexes use the same trees (a keyed tree of key groups), so an index version is also a path copy.

Query
   A ``Term`` is ``(path, operator, value)``; expressions combine terms. One planner turns an expression, an
   ordering and a window into a tree of physical plan nodes (scans, index lookups, filters, sorts, unions,
   limits), using what each index reports it can answer and at what estimated cost.

Transactions
   ``ObjectSpace`` owns a storage, the space-wide decoded-atom cache (D53) and the query result cache (D56).
   A ``Transaction`` reads the storage root once, records what it reads, and builds new atoms on
   ``tr``. Committing saves them, validates the read set, builds the new database version and publishes.

Storage
   A ``SharedStorage`` appends immutable records (``push_atom``, ``push_blob``), reads them back by pointer,
   makes them durable (``flush``) and publishes a root by compare-and-swap on an epoch (``publish_root``).
   ``MemoryStorage`` keeps everything in process; ``FileStorage`` writes append-only CRC-checked segments and
   replaces a root file atomically; ``CloudStorage`` writes segment objects first and then the root with a
   conditional write; ``TieredStorage`` is the cluster node's storage. The record and root formats are in
   :doc:`format-spec`.

Cluster
   A ``ClusterCoordinator`` lets several processes write one space. The peers agree on commit order among
   themselves (a lease, write permissions, a majority acknowledgement), and the object store is written
   afterwards. See :doc:`coordinator-protocol`.

What a commit crosses
---------------------

.. code-block:: text

   tr.commit()
     1. no writes?  nothing is saved or published; done
     2. save the written values, children first, to the storage (unpublished records are harmless:
        the storage is append-only)
     3. take the root lock (a thread lock plus an OS file lock; a cluster: the write permission)
     4. read the current root; check the read set against it  ->  ProtoConflictError, nothing published
     5. merge the unread mergeable roots onto their current values; build the new DatabaseRoot
        and SpaceRoot atoms
     6. flush the storage (data durable), then publish the root by compare-and-swap on the epoch
     7. mark the written values' op logs consumed; release the lock (nothing here can fail the commit)

Step 6 is the commit point. Data is always durable before the root that refers to it, so a crash at any
point leaves either the old root or the new one, never a root with missing data (:doc:`durability`).

Threads and import
------------------

``import proto_db`` imports no submodule, no optional third-party package (numpy, pyarrow, boto3,
google-cloud-storage, hnswlib) and starts no thread: public names resolve on first access (D47). Threads
exist only where a feature needs them: the parallel scan pool, a finite state machine's worker, and a cluster
node's transport, persister and timers. Free-threaded CPython (3.14t) is a supported target: immutable
structures need no locks, and every piece of shared mutable state (the write buffer, the segment writer, the
caches, the lazy-load publication, the transaction bookkeeping and the space's root reference) is guarded by a
lock whose contract is documented on the class that owns it. ``hnswlib`` re-enables the GIL when imported, so
it is optional and imported only when its backend is used (D13).

Where to read next
------------------

* :doc:`concurrency-model` for isolation, conflicts and merges.
* :doc:`indexing-framework` and :doc:`query-planner` for the two extension points of the data path.
* :doc:`durability` for what each step above guarantees.
