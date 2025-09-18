Root locking and root_context_manager
====================================

This page describes how ProtoBase coordinates concurrent root updates across storage backends.
It applies to StandaloneFileStorage (single‑node / filesystem) and ClusterFileStorage (multi‑node).

Overview
--------

ProtoBase uses a root pointer (an AtomPointer pointing into the Write‑Ahead Log) to reference the latest committed state
of the object space. Updating this pointer must be serialized to avoid lost updates. The high‑level strategy is:

- Transactions write data first, then atomically publish a new root pointer.
- A context manager, ``storage.root_context_manager()``, is used to guard the critical section for publishing a new root.
- Standalone storage relies on an OS‑level file lock and atomic file replace. Cluster storage obtains a distributed lock
  by majority vote and also acquires the provider’s local context manager for local mutual exclusion.

Key API
-------

- ``with storage.root_context_manager(): ...`` acquires the appropriate lock(s) for the backend and guarantees that
  updating the root pointer is serialized.
- ``storage.read_current_root()`` returns the current root pointer.
- ``storage.set_current_root(pointer)`` atomically updates the root pointer (must be called while holding the context
  manager in user code that updates the space root directly; normal transactions do this internally).

StandaloneFileStorage
---------------------

- The context manager returned by ``StandaloneFileStorage.root_context_manager()`` delegates to the block provider’s
  context manager (``FileBlockProvider.root_context_manager``), which:
  - Acquires a best‑effort OS‑level exclusive lock on a lockfile (``space_root.lock``) using ``fcntl`` on POSIX or
    ``msvcrt.locking`` on Windows when available.
  - Performs atomic root updates with ``tmp + fsync + os.replace + fsync(dir)`` to ensure durability and crash safety.
- Root reads tolerate transient replace windows and may retry briefly.

ClusterFileStorage
------------------

- ``ClusterNetworkManager.request_vote()`` now counts the local node’s vote and computes majority over ``total_nodes = len(servers) + 1``.
  This enables single‑node clusters to function.
- ``ClusterFileStorage.root_context_manager()``:
  - Obtains a distributed lock by securing a majority of votes (including self).
  - Enters the provider’s local context manager for filesystem‑level exclusion on the active node.
  - On exit, releases the provider context; the distributed lock auto‑expires (time‑based heartbeat/timeout).
- ``read_lock_current_root()`` is available to acquire both locks and read the pointer, with ``unlock_current_root()`` to release the provider lock.

Usage examples
--------------

- Standalone root update

  .. code-block:: python

     from proto_db.file_block_provider import FileBlockProvider
     from proto_db.standalone_file_storage import StandaloneFileStorage

     storage = StandaloneFileStorage(FileBlockProvider('/path/to/db'))
     with storage.root_context_manager():
         ptr = storage.read_current_root()  # optional read
         # compute new_ptr ...
         storage.set_current_root(new_ptr)

- Cluster root update

  .. code-block:: python

     from proto_db.file_block_provider import FileBlockProvider
     from proto_db.cluster_file_storage import ClusterFileStorage

     storage = ClusterFileStorage(FileBlockProvider('/path/to/db'), servers=[('hostA', 8765)])
     with storage.root_context_manager():
         ptr = storage.read_current_root()  # guarded by distributed + provider locks
         # compute new_ptr ...
         storage.set_current_root(new_ptr)

Notes and guarantees
--------------------

- Atomicity: On Standalone, root is written with atomic replace, flush, and directory fsync where supported.
- Mutual exclusion: On Standalone, an OS‑level lock protects readers/writers during root update. On Cluster, majority lock
  plus provider CM yield both distributed and local exclusivity.
- Single‑node clusters: The local server counts toward the majority; a cluster with no remotes (size 1) can acquire the lock.
- CAS layers: Higher‑level commit logic performs compare‑and‑swap checks across the space history to avoid lost updates when
  concurrent transactions race to publish different roots.
- Compatibility: ``FileBlockProvider.get_current_root_object`` returns the raw JSON dict for tests that expect that format;
  storage layers normalize it to ``AtomPointer`` where appropriate.

Troubleshooting
---------------

- If high contention occurs, transactions may raise ``ProtoLockingException`` and be retried by application logic.
- Environment variables ``STORAGE_PUSH_TIMEOUT_SEC`` and ``STORAGE_LOAD_TIMEOUT_SEC`` can bound how long storage operations
  wait under contention before surfacing a retryable error.
