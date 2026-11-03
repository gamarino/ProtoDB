Atom-level Caches
==================

.. module:: proto_db.atom_cache

ProtoBase includes an optional in-memory, atom-addressed cache layer that sits in front of the page/block cache.
Because atoms are immutable and addressed by their AtomPointer (transaction_id, offset), entries never need
logical invalidation. This layer significantly reduces latency for hot objects that are referenced across
transactions or traversals.

Components
----------

AtomPointer Keying
~~~~~~~~~~~~~~~~~~

Cache keys are derived from the atom address:

- Bytes cache key: ``(transaction_uuid, offset)``
- Object cache key: ``(transaction_uuid, offset, schema_epoch)`` (epoch is optional and allows fast
  invalidation on binary format/decoder changes)

AtomBytesCache
~~~~~~~~~~~~~~

.. autoclass:: AtomBytesCache
   :members:
   :special-members: __init__

A capacity-bounded cache that stores the raw payload bytes of an atom (post WAL header). It returns
``memoryview`` slices to avoid copies on the hot path. Capacity is enforced by number of entries and total bytes.

AtomObjectCache
~~~~~~~~~~~~~~~

.. autoclass:: AtomObjectCache
   :members:
   :special-members: __init__

A capacity-bounded cache that stores fully deserialized atom objects (Python dicts for core atoms). An
optional size estimator can be provided; otherwise a shallow ``sys.getsizeof`` is used.

Replacement Policy (2Q)
-----------------------

Both caches implement a simple 2Q policy with two LRU queues: probation and protected.

- Admission: new entries go into probation.
- Promotion: a second reference promotes an entry to the protected queue.
- Eviction: entries are evicted from probation first, then protected, to avoid cache pollution during scans.

Concurrency and Single-flight
-----------------------------

A lightweight single-flight mechanism deduplicates concurrent loads/deserializations for the same key.
Only one worker performs the read/deserialize; followers wait and then hit the cache.

Metrics and Observability
-------------------------

Both caches expose basic statistics and latency buckets via ``AtomCacheBundle``:

.. autoclass:: AtomCacheBundle
   :members:
   :special-members: __init__

- Hits, misses, hit ratios
- Evictions
- Current sizes (entries/bytes)
- Single-flight deduplication count
- Latencies (p50/p95/p99) for object cache lookups, bytes cache lookups, and deserialization

Integration in Storage
----------------------

The caches are integrated in the read path of ``StandaloneFileStorage`` and the constructor accepts
configuration flags and limits:

.. autofunction:: proto_db.standalone_file_storage.StandaloneFileStorage.__init__

Read Path
~~~~~~~~~

1. Try ``AtomObjectCache``; if hit, return immediately.
2. Else try ``AtomBytesCache``; if hit, deserialize, populate ``AtomObjectCache``, return.
3. Else read from WAL/page provider, extract payload, populate ``AtomBytesCache`` and ``AtomObjectCache``.
4. Single-flight ensures only one deserialize per key under concurrency.

Configuration
-------------

Both ``StandaloneFileStorage`` and ``MemoryStorage`` accept the following parameters (with safe defaults):

- ``enable_atom_object_cache`` (bool)
- ``enable_atom_bytes_cache`` (bool)
- ``object_cache_max_entries`` (int)
- ``object_cache_max_bytes`` (int)
- ``bytes_cache_max_entries`` (int)
- ``bytes_cache_max_bytes`` (int)
- ``cache_stripes`` (int) – number of lock stripes
- ``cache_probation_ratio`` (float) – fraction of entries reserved for probation queue
- ``schema_epoch`` (int|None) – optional epoch for object cache keying

Example
-------

::

   from proto_db.file_block_provider import FileBlockProvider
   from proto_db.standalone_file_storage import StandaloneFileStorage

   provider = FileBlockProvider('/tmp/proto_db')
   storage = StandaloneFileStorage(
       provider,
       enable_atom_object_cache=True,
       enable_atom_bytes_cache=True,
       object_cache_max_entries=100_000,
       object_cache_max_bytes=512*1024*1024,
       bytes_cache_max_entries=50_000,
       bytes_cache_max_bytes=256*1024*1024,
       cache_stripes=64,
       cache_probation_ratio=0.5,
       schema_epoch=1,
   )

Notes
-----

- The caches rely on atom immutability; no invalidation is required on writes.
- For schema/decoder changes, bump ``schema_epoch`` to isolate object cache entries.
- ``AtomBytesCache`` stores only the atom payload (without WAL length/format headers).
