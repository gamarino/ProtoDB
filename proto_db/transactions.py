"""ObjectSpace, Database and Transaction: the transactional layer of ProtoDB 2.0
(spec section 7, decisions D9, D11, D32, D33, D34, D35).

Layout of a space (D32)
    The storage root points to a :class:`SpaceRoot` atom whose ``databases`` field is a
    :class:`~proto_db.collections.dictionaries.Dictionary` ``name -> DatabaseRoot``.  A
    :class:`DatabaseRoot` holds ``objects`` (a Dictionary ``root name -> value``),
    ``mutables`` (a Dictionary ``mutable name -> value``, the mutable table, D35), its
    ``version`` (0 when the database is created, +1 per commit), ``committed_at`` (an
    aware UTC datetime, never decreasing along the chain) and ``previous``, the
    DatabaseRoot it replaced, which :meth:`Database.history` walks.  Every atom is
    immutable; a commit builds new root atoms and publishes a new storage root.
    ``previous`` is loaded on demand, never kept loaded by its root (Task 8.3l, D53):
    history is on disk, not in memory.

Snapshot isolation and the read set (D9)
    A transaction reads the storage root once, when it starts, and every read goes to
    that snapshot.  ``get_root_object(name)`` records ``name`` with what the snapshot
    held -- an exact token (atoms by storage pointer, plain values by type and exact
    payload, so ``1 -> 1.0`` is a change) or ABSENT -- in the read set.
    At commit, under the root lock, a recorded root that changed in the current root
    (including one that appeared or disappeared) raises :class:`ProtoConflictError`
    and nothing is published.  ``root_names()`` does not enter the read set.

Merge of roots written but not read (D9, D33)
    A root that was written without being read is merged when the written value is
    :class:`~proto_db.collections.base.ConcurrentOptimized`, its op log is complete
    (:attr:`~proto_db.collections.base.ConcurrentOptimized.log_is_complete`: the
    value was built from an empty collection in this transaction), and the current
    root holds a value of the same type: the commit stores
    ``written.rebase_onto(current)``, i.e. the transaction's intent replayed onto the
    current value, whether or not that value changed since the snapshot.  The
    original written version is always the one rebased.  A ``ProtoConflictError``
    raised by ``rebase_onto`` is a conflict like any other.  In every other case the
    written value replaces the current one (last writer wins), and so does
    ``remove_root_object`` of an unread root.  ``set_root_object(name, value,
    replace=True)``, and a set following ``remove_root_object(name)`` in the same
    transaction, always replace.  After a successful commit the written values'
    logs are marked incomplete, so reusing such a value in a later transaction
    replaces instead of replaying this transaction's operations again.  Reading back a root written before it
    was read returns the value as written, not the merged one.

Mutable cells (D11, D35)
    The mutable table is a second namespace beside the roots: ``new_mutable``,
    ``get_mutable``, ``set_mutable``, ``remove_mutable`` and ``mutable_names`` mirror
    the root API and share its read set rule (exact tokens, ABSENT included) and its
    conflict check, but per namespace: a root and a mutable of the same name are
    unrelated.  Mutables are never merged: a written value is stored as written
    (blind writes: last writer wins).  ``new_mutable`` requires the name to be absent
    and records that in the read set, so two concurrent creators conflict;
    ``set_mutable`` requires the cell to exist, and a blind set of a cell removed by a
    concurrent commit is a conflict (it never resurrects the cell).  A
    :class:`~proto_db.core.dbobject.MutableObject` handle names a cell and resolves it
    through a transaction.

Commit protocol (D34)
    1. No writes: nothing is saved nor published.
    2. Written values are saved (children first) before taking the root lock; the
       storage is append-only, so unpublished records are harmless.
    3. The root lock is taken with the space's ``lock_timeout``; a timeout raises and
       the commit never proceeds unlocked.
    4. The current root is read and the read set is checked against it.
    5. Unread mergeable roots are rebased onto their current values; new
       DatabaseRoot and SpaceRoot atoms are built and saved.
    6. ``storage.flush()`` makes the data durable according to the storage's
       durability mode, then ``publish_root(ptr, expected_epoch)`` publishes the root
       by compare-and-swap.  If publishing fails, the outcome may be unknown.  A
       ``ProtoOutcomeUnknownError`` is first resolved by the storage's optional
       ``publish_outcome(ptr, epoch)`` hook (True: committed; False: conflict).  For a
       storage whose root lock excludes every writer (``root_lock_is_global``, e.g. a
       file storage failing after the rename) the root is then read again: if it is
       ours the commit succeeded, if another epoch is visible the commit raises
       ``ProtoConflictError``, otherwise the original error is raised (wrapped in
       ``ProtoUnexpectedError`` if it was not a ProtoDB error); if the root cannot even
       be read back, ``ProtoOutcomeUnknownError`` reports an unknown outcome.  For a
       storage with an in-process root lock (``CloudStorage``), and for every commit under
       a cluster lease, an epoch mismatch proves nothing, so an unresolved outcome raises
       ``ProtoOutcomeUnknownError`` and any other error propagates as it is.
       ``Database.run`` never retries ``ProtoOutcomeUnknownError``: the commit may have
       been applied.
    7. Nothing after a successful publish can fail the commit: releasing the root
       lock is best-effort and a failure there is logged, and the written values'
       logs are marked consumed.  On the failure path a lock-release error is
       logged too, so it never masks the original error.

Thread safety
    :class:`ObjectSpace` and :class:`Database` are shared freely between threads; any
    number of transactions may run concurrently.  A :class:`Transaction` is meant for
    one thread at a time; it is not bound to the thread that created it, and an
    internal lock serializes its operations, so a misuse from several threads sees
    each call happen entirely before or after the others (e.g. a write racing a commit
    either is committed or raises because the transaction has ended).

Persistence context (ruling R3)
    A transaction is the ``PersistenceContext`` of the atoms it creates or loads.
    ``save`` and ``load`` stay usable after the transaction has ended, because the
    immutable atoms it produced may outlive it (lazy loads of a value read in an ended
    transaction, or a later transaction storing a version derived from it).
"""
from __future__ import annotations

import copy
import datetime
import functools
import logging
import random
import sys
import threading
import time
import uuid
import weakref
from _weakref import _remove_dead_weakref
from typing import Any, Callable, Optional

from .collections.base import ConcurrentOptimized, _Immutable
from .collections.dictionaries import Dictionary, RepeatedKeysDictionary
from .collections.hash_dictionaries import HashDictionary
from .collections.lists import List
from .collections.sets import CountedSet, Set
from .core.atom import Atom, AtomPointer, PersistenceContext
from .core.dbobject import DBObject, MutableObject
from .errors import (ProtoClosedError, ProtoConflictError, ProtoCorruptionError, ProtoDBError, ProtoOutcomeUnknownError,
                     ProtoReferenceError, ProtoTimeoutError, ProtoUnexpectedError, ProtoUserError,
                     ProtoValidationError)
from .storage.base import check_lock_timeout
from .storage.cache import BoundedCache
from .query.cache import QUERY_CACHE_BYTES, QueryCache
from .query.pager import check_resumed, decode_ref, encode_ref, pager_arguments, portable_pager
from .storage.format import FORMAT_VERSION, RootRecord
from .cluster.coordinator import REFOUND_TERM_STEP
from .cluster.lease import Permission
from .cluster.rootlog import compare_roots
from .values import canonical, exact_token

__all__ = ["ObjectSpace", "Database", "Transaction", "HistoryEntry", "SpaceRoot", "DatabaseRoot"]

logger = logging.getLogger(__name__)


class _Sentinel:
    """Named marker object."""

    def __init__(self, name: str):
        self._name = name

    def __repr__(self):
        return self._name


_ABSENT = _Sentinel("ABSENT")
_REMOVED = _Sentinel("REMOVED")

_ACTIVE, _COMMITTING, _COMMITTED, _ABORTED = "active", "committing", "committed", "aborted"


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _check_name(name, what: str = "root") -> str:
    if not isinstance(name, str) or not name:
        raise ProtoValidationError(f"A {what} name must be a non-empty str, not {name!r}")
    return name


# -- root atoms -------------------------------------------------------------------------------

class SpaceRoot(_Immutable, Atom):
    """The atom the storage root points to: ``databases`` maps database names to :class:`DatabaseRoot`.

    A root committed in cluster mode (D46) also carries ``cluster_term`` (the term of the
    lease that committed it), ``cluster_epoch`` (its cluster epoch) and ``cluster_previous``
    (the space root it was built on), so every root is self-describing and the persister can
    walk the chain of commits; these read as ``None`` on other roots.
    """

    __type_name__ = "proto_db.transactions.SpaceRoot"
    _CLUSTER_FIELDS = ("cluster_term", "cluster_epoch", "cluster_previous")
    _history_links = ("cluster_previous",)

    def __init__(self, *, databases: Dictionary, context: PersistenceContext | None = None,
                 cluster: Optional["_ClusterStamp"] = None, previous: Optional["SpaceRoot"] = None):
        """Create a space root over the ``databases`` dictionary (with the cluster fields of ``cluster``)."""
        super().__init__(context=context)
        object.__setattr__(self, "databases", databases)
        if cluster is not None:
            object.__setattr__(self, "cluster_term", cluster.term)
            object.__setattr__(self, "cluster_epoch", cluster.epoch)
            self._set_link("cluster_previous", previous)

    def _absent_attribute(self, name: str):
        if name in self._CLUSTER_FIELDS:
            return None
        return super()._absent_attribute(name)


class _ClusterStamp:
    """What a commit under the cluster lease adds to the new space root, and its stage timings."""

    __slots__ = ("term", "epoch", "timings")

    def __init__(self, term: int, epoch: int):
        self.term = term
        self.epoch = epoch
        self.timings = {"validation": 0.0, "replay": 0.0, "path_copy": 0.0, "replication": 0.0}


class DatabaseRoot(_Immutable, Atom):
    """One version of a database: ``objects`` (root name -> value), ``mutables`` (mutable
    name -> value, D35), ``version``, ``committed_at``, the ``previous`` version
    (``None`` for version 0) and ``origin``: 16 random bytes drawn when the database is
    created and carried by every later version, the database's identity across processes
    (D56, R99 I2).  A database created before ``origin`` existed has none (``None``).

    ``previous`` (and ``SpaceRoot.cluster_previous``) is a history link (Task 8.3l, D53): held
    as a pointer and loaded on each read through the root's context, so a root never keeps
    its predecessors loaded."""

    __type_name__ = "proto_db.transactions.DatabaseRoot"
    _history_links = ("previous",)

    def __init__(self, *, objects: Dictionary, mutables: Dictionary, previous: Optional["DatabaseRoot"],
                 version: int, committed_at: datetime.datetime, origin: bytes | None = None,
                 context: PersistenceContext | None = None):
        """Create a database version."""
        super().__init__(context=context)
        osa = object.__setattr__
        osa(self, "objects", objects)
        osa(self, "mutables", mutables)
        self._set_link("previous", previous)
        osa(self, "version", version)
        osa(self, "committed_at", committed_at)
        if origin is not None:
            osa(self, "origin", origin)

    def _absent_attribute(self, name: str):
        if name == "origin":
            return None
        if name == "mutables":
            # Written before the mutable table existed (D35); no migration is provided.
            raise ProtoCorruptionError(
                f"DatabaseRoot at {self.atom_pointer} has no mutable table: the database was written "
                f"by a ProtoDB build older than decision D35 and cannot be opened")
        return super()._absent_attribute(name)


class HistoryEntry:
    """One committed version of a database, as listed by :meth:`Database.history`.

    ``version`` and ``committed_at`` are plain attributes; :attr:`root_names`,
    :attr:`mutable_names` and :meth:`get_mutable` load that version's tables on access.
    """

    __slots__ = ("version", "committed_at", "_root", "_database")

    def __init__(self, root: DatabaseRoot, database: str | None = None):
        """Describe the database version ``root`` of the database named ``database``."""
        self.version = root.version
        self.committed_at = root.committed_at
        self._root = root
        self._database = database

    @property
    def root_names(self) -> tuple:
        """Sorted names of the roots present in this version."""
        return tuple(sorted(self._root.objects.keys()))

    @property
    def mutable_names(self) -> tuple:
        """Sorted names of the mutable cells present in this version."""
        return tuple(sorted(self._root.mutables.keys()))

    def get_mutable(self, name: str):
        """Value of the mutable cell ``name`` in this version (``None`` if absent)."""
        return self._root.mutables.get_at(_check_name(name, "mutable"))

    @property
    def root(self) -> "DatabaseRoot":
        """The ``DatabaseRoot`` of this version (protected use: :meth:`Database.snapshot_at`)."""
        return self._root

    def __repr__(self):
        return f"HistoryEntry(version={self.version}, committed_at={self.committed_at.isoformat()})"


class Snapshot:
    """A read-only view of one committed version of a database (Task 8.4b, D56).

    Made by :meth:`Database.snapshot_at` from a reference: :attr:`Transaction.snapshot_ref`,
    :attr:`Snapshot.ref`, a :class:`HistoryEntry` or a pager token.  Every version stays
    readable forever (P1, R77a), so a snapshot needs no pinning; a version whose local copy
    was released is read from the object store.  It is not a transaction: it records no
    reads, cannot commit, and every write method raises ``ProtoUserError``.  The values it
    returns are the version's immutable values, shareable with any transaction (P2).
    Thread-safe.
    """

    __slots__ = ("_database", "_root")

    def __init__(self, database: "Database", root: "DatabaseRoot"):
        """Bind the view (use :meth:`Database.snapshot_at`)."""
        self._database = database
        self._root = root

    @property
    def database(self) -> "Database":
        """The database this version belongs to."""
        return self._database

    @property
    def version(self) -> int:
        """The version number (0 at creation, +1 per commit)."""
        return self._root.version

    @property
    def committed_at(self) -> datetime.datetime:
        """When this version was committed (aware UTC)."""
        return self._root.committed_at

    @property
    def ref(self) -> bytes:
        """A reference to this version, valid in any process of the space (bytes)."""
        return encode_ref("snapshot", self._database.name, self._root.atom_pointer, self._root.origin)

    @property
    def root_names(self) -> tuple:
        """Sorted names of the roots present in this version."""
        return tuple(sorted(self._root.objects.keys()))

    def get_root_object(self, name: str):
        """Value of root ``name`` in this version (``None`` if absent)."""
        return self._root.objects.get_at(_check_name(name))

    @property
    def mutable_names(self) -> tuple:
        """Sorted names of the mutable cells present in this version."""
        return tuple(sorted(self._root.mutables.keys()))

    def get_mutable(self, name: str):
        """Value of the mutable cell ``name`` in this version (``None`` if absent)."""
        return self._root.mutables.get_at(_check_name(name, "mutable"))

    def pager(self, root_name: str, *, where=None, order_by=None, page_size: int):
        """A portable :class:`~proto_db.query.pager.Pager` over the collection at ``root_name`` in
        this version, filtered by ``where`` and ordered by ``order_by`` (``plan_query`` forms;
        paths only); its token resumes it in any process (:meth:`Database.resume_pager`)."""
        collection = self.get_root_object(_check_name(root_name))
        if collection is None:
            raise ProtoValidationError(f"Root {root_name!r} does not exist in version {self.version}")
        return portable_pager(collection, database=self._database.name, origin=self._root.origin,
                              root_ptr=self._root.atom_pointer,
                              root_name=root_name, where=where, order_by=order_by, page_size=page_size,
                              cache=self._database.space.query_cache)

    def _read_only(self, *args, **kwargs):
        raise ProtoUserError(f"A snapshot of {self._database.name!r} (version {self.version}) is read-only; "
                             f"write in a transaction (Database.new_transaction)")

    set_root_object = remove_root_object = new_mutable = set_mutable = remove_mutable = commit = _read_only

    def __repr__(self):
        return f"Snapshot({self._database.name!r}, version={self.version})"


# -- persistence helpers ----------------------------------------------------------------------

def _save_atom(context, atom: Atom) -> AtomPointer:
    """Save ``atom`` through ``context`` (adopting it if it has no context); refuse foreign storages."""
    if atom.context is None:
        atom._bind(context)
    owner = atom.context
    if owner is not context and getattr(owner, "storage", None) is not context.storage:
        raise ProtoUserError(f"{atom!r} belongs to another storage; atoms cannot move between spaces")
    return atom._save()


class _SpaceContext:
    """PersistenceContext of space-level work (database creation, history, listing)."""

    def __init__(self, storage):
        self.storage = storage

    def save(self, atom: Atom) -> AtomPointer:
        """Persist ``atom`` (children first) and return its pointer."""
        return _save_atom(self, atom)

    def load(self, ptr: AtomPointer, type_name: str) -> Atom:
        """Return a lazy stub of the atom stored at ``ptr``."""
        return Atom._stub(self, ptr, type_name)


def _one(_value) -> int:
    return 1


#: Default number of atoms an :class:`ObjectSpace` keeps decoded for every transaction (R75).
ATOM_CACHE_ENTRIES = 50_000

#: Inline ``str``/``bytes`` an atom may hold and still be cached decoded on save (review 8.3i I1),
#: unless its class sets ``_cache_large_on_save`` (tree nodes, which builds traverse).
SAVED_INLINE_MAX = 4096


class _AtomCache:
    """Decoded atoms shared by every transaction of one space, by pointer (ruling R75, D53).

    Safe without invalidation: a storage instance never changes the content at a pointer
    (records are appended, never rewritten), and a decoded atom is immutable.  An atom
    served here is bound to a space-level context whose ``load`` goes through this cache, so
    the children of a shared atom are shared too.  Thread-safe (:class:`BoundedCache`; loading a
    stub is thread-safe, :meth:`Atom._load`).

    It also receives every atom saved through it or through a transaction that shares it
    (:meth:`_atom_saved`, Task 8.3i), decoded already: the next build on a root this space committed
    finds that root's new atoms without reading or decoding them.

    Residency (Task 8.3m, rulings R102-R104): the bound is on LOADED atoms.  ``_cache`` holds the
    loaded atoms this cache produced, by pointer, at most ``entries`` of them; an atom it evicts
    is unloaded (:meth:`Atom._unload`: back to the stub state, read again on demand), so a loaded
    atom never keeps evicted state reachable.  ``_ident`` maps every pointer to a weak reference
    of the one object served for it while anything holds that object (stub or loaded), so a
    parent's child and a fresh load are the same object.  Only objects produced here (stubs from
    :meth:`load`, decoded twins) carry the ``_cached`` mark and are ever admitted or unloaded.
    """

    def __init__(self, storage, entries: int):
        self.storage = storage
        self._cache = BoundedCache(entries, sizeof=_one)
        self._lock = threading.Lock()
        self._ident: dict = {}
        self._forget = functools.partial(_forget_dead, self._ident)
        self._closed = False
        self.hits = 0
        self.misses = 0
        self.unloads = 0

    def save(self, atom: Atom) -> AtomPointer:
        """Persist ``atom`` (children first) and return its pointer."""
        return _save_atom(self, atom)

    def load(self, ptr: AtomPointer, type_name: str) -> Atom:
        """The shared atom stored at ``ptr``: the object already served for it while anything holds
        it, else a new lazy stub."""
        with self._lock:
            ref = self._ident.get(ptr)
            atom = ref() if ref is not None else None
            hit = atom is not None and type(atom).__type_name__ == type_name
            if hit:
                self.hits += 1
            else:
                self.misses += 1
        if hit:
            self._cache.touch(ptr)
            return atom
        stub = Atom._stub(self, ptr, type_name)
        if atom is not None:
            # Another type for a known pointer (corrupt reference): a one-off stub, never marked,
            # so its load can never be admitted and displace the shared atom (review 8.3m M2).
            return stub
        object.__setattr__(stub, "_cached", True)
        with self._lock:
            ref = self._ident.get(ptr)
            other = ref() if ref is not None else None
            if other is not None and type(other).__type_name__ == type_name:
                return other                # another thread served this pointer meanwhile
            if not self._closed:
                self._ident[ptr] = weakref.KeyedRef(stub, self._forget, ptr)
        return stub

    def _admit(self, atom: Atom) -> None:
        """Make the loaded ``atom`` resident; unload what that evicts (no lock held meanwhile)."""
        if self._closed or not atom._cache_produced():
            return
        unloaded = 0
        for victim in self._cache.put_evicting(atom.atom_pointer, atom):
            if victim._unload():
                unloaded += 1
        if unloaded:
            with self._lock:
                self.unloads += unloaded

    def _atom_loaded(self, atom: Atom) -> None:
        """The :meth:`Atom._load` hook: ``atom`` was just loaded (published by this call)."""
        self._admit(atom)

    def _atom_saved(self, atom: Atom, ptr: AtomPointer, state: dict) -> None:
        """``atom`` was just pushed at ``ptr`` with ``state`` (the :meth:`Atom._save` hook,
        Task 8.3i): cache what a load of ``ptr`` would produce, decoded already
        (:meth:`Atom._decoded_twin`), so the next build on the root that references it neither
        reads nor decodes it.  The live ``atom`` is never cached (its runtime state, an op log
        for example, is not what a load produces)."""
        cls = type(atom)
        if not cls._cache_on_save or self._closed:
            return
        inline = [0]
        twin = atom._decoded_twin(self, ptr, state, inline)
        if inline[0] > SAVED_INLINE_MAX and not cls._cache_large_on_save:
            # Review 8.3i I1: an entry is one atom of any size (the bound counts atoms, not bytes),
            # so an atom holding large values inline is left to the read path: a parent cached
            # later refers to it as an unloaded stub, and its bytes are read only if something
            # reads it (a write-only ingest retains none of them).
            return
        object.__setattr__(twin, "_cached", True)
        with self._lock:
            if self._closed:
                return
            self._ident[ptr] = weakref.KeyedRef(twin, self._forget, ptr)
        self._admit(twin)

    def _unload_all(self) -> int:
        """Unload every resident atom (memory pressure, diagnostics); return how many."""
        unloaded = sum(1 for atom in self._cache.drain() if atom._unload())
        with self._lock:
            self.unloads += unloaded
        return unloaded

    def close(self) -> None:
        """Forget every atom (the space is closing), without unloading: an atom the application
        still holds keeps the state it has, and nothing else stays reachable through this cache
        (``atom.context`` is this cache).  Reading what is not loaded then raises
        :class:`~proto_db.errors.ProtoClosedError` (the storage is closed)."""
        with self._lock:
            self._closed = True
            self._ident.clear()
        self._cache.clear()

    def stats(self) -> dict:
        """``hits`` and ``misses`` of :meth:`load`, ``entries`` (loaded atoms resident) and
        ``capacity``, ``unloads`` and ``identity_map`` (objects served and still alive)."""
        stats = self._cache.stats()
        with self._lock:
            return {"hits": self.hits, "misses": self.misses, "entries": stats["entries"],
                    "capacity": stats["capacity"], "unloads": self.unloads, "identity_map": len(self._ident)}


def _forget_dead(ident: dict, ref) -> None:
    """Weak-reference callback: drop ``ident[ref.key]`` only if it is still this dead reference
    (atomic, as ``WeakValueDictionary`` does)."""
    _remove_dead_weakref(ident, ref.key)


def _load_space(context, record) -> Optional[SpaceRoot]:
    if record is None:
        return None
    return context.load(record.pointer, SpaceRoot.__type_name__)


# -- read-set tokens --------------------------------------------------------------------------

def _atom_token(atom: Atom) -> tuple:
    return ("@atom", atom.atom_pointer)


def _token(value):
    """What the read set remembers of a root value (D34): ABSENT or an exact token.

    Plain values are compared exactly (:func:`proto_db.values.exact_token`: same
    type and same payload, so ``1`` and ``1.0``, ``Decimal("1")`` and
    ``Decimal("1.00")`` or one instant in two time zones differ); atoms, nested
    ones included, are identified by their storage pointer.
    """
    if value is _ABSENT:
        return _ABSENT
    if isinstance(value, Atom):
        # Directly, not through exact_token: its is_atom() probe reads ``oid``, which
        # loads a stub (D21) -- under the root lock, for the commit-time check.
        return _atom_token(value)
    return exact_token(value, _atom_token)


def _same_token(a, b) -> bool:
    if a is _ABSENT or b is _ABSENT:
        return a is b
    return a == b


# -- the publish protocol ---------------------------------------------------------------------

def _ask_outcome(storage, ptr: AtomPointer, epoch: int) -> Optional[bool]:
    """The storage's ``publish_outcome`` verdict on ``ptr`` at ``epoch`` (``None``: unknown or no hook)."""
    hook = getattr(storage, "publish_outcome", None)
    if hook is None:
        return None
    try:
        verdict = hook(ptr, epoch)
    except (ProtoDBError, OSError) as e:
        logger.warning("publish_outcome failed (%s); the outcome of the commit stays unknown", e)
        return None
    return verdict if verdict is True or verdict is False else None


def _publish(storage, ptr: AtomPointer, expected_epoch: int, *, writers_excluded: Optional[bool] = None) -> None:
    """Publish ``ptr`` by CAS; resolve an unknown outcome (step 6 of D34).

    ``writers_excluded`` overrides the storage's ``root_lock_is_global``: the cluster lease
    path passes False, because a lease excludes other writers only while it is valid, so an
    epoch mismatch read back after a failure never proves that nothing was applied.

    The verdict comes from the storage's ``publish_outcome`` hook when it has one.
    Inferring a conflict from an epoch mismatch is valid only when the root lock excludes
    every other writer (``root_lock_is_global``): with an in-process root lock another
    process may have published on top of our root, so an unresolved outcome is reported
    as ``ProtoOutcomeUnknownError`` (which ``Database.run`` never retries).
    """
    try:
        storage.publish_root(ptr, expected_epoch)
        return
    except (ProtoDBError, OSError) as e:
        error = e
    unknown = isinstance(error, ProtoOutcomeUnknownError)
    if unknown:
        verdict = _ask_outcome(storage, ptr, expected_epoch + 1)
        if verdict is True:
            logger.warning("publish_root reported %r but the root was applied; the commit succeeded", error)
            return
        if verdict is False:
            raise ProtoConflictError("Another root was published concurrently; nothing was committed") from error
    if writers_excluded is None:
        writers_excluded = getattr(storage, "root_lock_is_global", True)
    if not writers_excluded:
        # Only an outcome-unknown error leaves doubt; any other error means nothing was applied.
        if isinstance(error, ProtoDBError):
            raise error
        raise ProtoUnexpectedError(f"Publishing the root failed: {error}") from error
    try:
        root = storage.read_root()
    except (ProtoDBError, OSError) as e:
        raise ProtoOutcomeUnknownError(
            f"Publishing the root failed ({error}) and reading it back failed ({e}); "
            f"the outcome of the commit is unknown") from error
    if root is not None and root.pointer == ptr:
        logger.warning("publish_root reported %r but the new root is visible; the commit succeeded", error)
        return
    if (root.epoch if root is not None else 0) != expected_epoch:
        raise ProtoConflictError("Another root was published concurrently; nothing was committed") from error
    if isinstance(error, ProtoDBError):
        raise error
    raise ProtoUnexpectedError(f"Publishing the root failed: {error}") from error


def _commit_root(storage, lock_timeout, build: Callable, coordinator=None) -> bool:
    """Run steps 3-7 of the commit protocol (D34); return True if a root was published.

    ``build(current_record, stamp)`` runs under the root lock; it checks conflicts and returns
    the pointer of the saved new :class:`SpaceRoot`, or ``None`` to publish nothing.
    With a cluster ``coordinator`` the cluster lease replaces the root lock and majority
    replication replaces the publish (:func:`_commit_under_lease`).

    Without one, the commit joins the storage's *group* (group commit, ruling R71): one thread
    at a time takes every queued commit, holds the root lock once, builds each commit on the
    root the previous one produced, and makes the last root durable with ONE flush and ONE
    publish (:func:`_standalone_round`).  A commit alone in the queue is exactly the
    single-commit protocol.
    """
    if coordinator is not None:
        return _commit_under_lease(coordinator, lock_timeout, build)
    group = _group_of(storage)
    entry = _Entry(build, storage, group)
    with group.cond:
        group.queue.append(entry)
        group.cond.notify_all()
        while not entry.done and group.running:
            group.cond.wait()
        runner = not entry.done
        if runner:
            group.running = True
            batch, group.queue = group.queue, []
    if runner:
        try:
            _standalone_round(storage, lock_timeout, batch)
        finally:
            with group.cond:
                group.running = False
                group.cond.notify_all()
    if entry.error is not None:
        if entry.error_shared:
            error = copy.copy(entry.error)
            error.__cause__ = entry.error
            raise error
        raise entry.error
    return entry.result


def _standalone_round(storage, lock_timeout, batch: list) -> None:
    """Decide ``batch`` under one root lock with one flush and one publish (group commit, R71).

    Each commit is built on the root the previous one produced (its validation reads that
    root, so conflicts within the group are detected as between sequential commits).  A
    commit whose build raises gets that error alone, in its own thread; the others go on.  The
    flush and the publish decide every built commit together: on failure each gets the same
    error (``ProtoOutcomeUnknownError`` only when the publish outcome is unknown, as alone).
    Nothing of the round is visible before the publish.
    """
    try:
        lock = storage.root_lock(lock_timeout)
        lock.__enter__()
    except BaseException as e:
        for entry in batch:
            entry.finish(error=e, shared=len(batch) > 1)
        if not isinstance(e, ProtoDBError):
            raise
        return
    built = []
    try:
        current = storage.read_root()
        expected = current.epoch if current is not None else 0
        ptr = None
        for entry in batch:
            try:
                built_ptr = entry.build(current, None)
            except Exception as e:      # noqa: BLE001 -- delivered to that commit's thread
                # One commit's error, even an unexpected one (a bug, a type error in user data),
                # is that commit's alone: it is raised in its own thread and the others go on
                # (review M2).  Nothing of it is published.
                entry.finish(error=e)
                continue
            if built_ptr is None:
                entry.finish(result=False)
                continue
            ptr = built_ptr
            current = RootRecord(FORMAT_VERSION, ptr.segment, ptr.offset, expected + 1)
            built.append(entry)
        if built:
            storage.flush()
            _publish(storage, ptr, expected)
    except BaseException as e:
        # Nothing of ours is published: release, but never let a release error mask the original one.
        try:
            lock.__exit__(*sys.exc_info())
        except (ProtoDBError, OSError) as release_error:
            logger.error("Releasing the root lock after a failed commit failed: %s", release_error)
        undecided = [entry for entry in batch if not entry.done]
        for entry in undecided:     # one error object raised in several threads: each gets a copy
            entry.finish(error=e, shared=len(undecided) > 1)
        if not isinstance(e, ProtoDBError):
            raise
        return
    # Published (or nothing to publish): nothing below may fail the commits.
    try:
        lock.__exit__(None, None, None)
    except (ProtoDBError, OSError) as e:
        logger.error("Releasing the root lock after a commit failed: %s", e)
    for entry in built:
        entry.finish(result=True)


def _confirm_build(current, stamp):
    """The build step of a confirmation: nothing to publish (see :func:`_commit_under_lease`)."""
    return None


def _commit_under_lease(coordinator, lock_timeout, build: Callable) -> bool:
    """Steps 3-7 of the commit protocol in cluster mode (D44, D45, D46, D50).

    The caller has saved its records to the local tier.  A node whose backlog reached its
    limit refuses the commit first (``ProtoBacklogFullError``).  The commit joins this node's
    *group* (D50): one round is in flight per node; the thread that finds none running takes
    every queued commit, obtains a write permission (:meth:`~proto_db.cluster.coordinator.
    ClusterCoordinator.permit`) and, under it, builds each commit in turn on the root the
    previous one produced -- the base being the newest committed root the owner piggybacked
    (a recovery round when it is not certain) -- then replicates the last root with the
    records of all of them; :meth:`~proto_db.cluster.coordinator.ClusterCoordinator.replicate`
    returns once a majority acknowledged it -- the commit point.  No storage root is written
    here: the persister writes it later.  A commit whose validation fails gets
    ``ProtoConflictError`` alone (after the round's replication, so its retry starts from the
    root the round committed), and the round keeps the permission for the retries of such
    commits (D-LIVE-2; :data:`_PERMIT_RETRIES`).  ``ProtoConflictError`` when the permission
    ended before anything was sent (retryable), ``ProtoOutcomeUnknownError`` when no majority
    acknowledged in time (never retried).
    """
    tier = coordinator.tier
    if build is not _confirm_build:
        tier.check_commit_allowed()
    group = _group_of(coordinator)
    entry = _Entry(build, tier, group)
    with group.cond:
        group.queue.append(entry)
        group.cond.notify_all()
        while not entry.done and group.running:
            group.cond.wait()
        runner = not entry.done
        if runner:
            group.running = True
    if runner:
        try:
            stale = 0
            while not entry.done:
                with group.cond:
                    batch, group.queue = group.queue, []
                if not batch:
                    break               # pragma: no cover -- our own entry is queued or done
                if _run_round(coordinator, lock_timeout, batch, group):
                    stale = 0
                else:
                    stale += 1
                    if stale >= _TERM_RETRIES:
                        with group.cond:
                            pending, group.queue = group.queue, []
                        error = ProtoConflictError(
                            f"No permission with a base at or above the committed root after {_TERM_RETRIES} "
                            f"attempts")
                        for other in pending:
                            other.finish(error=error)
        finally:
            with group.cond:
                group.running = False
                group.cond.notify_all()
    if entry.error is not None:
        if entry.error_shared:
            error = copy.copy(entry.error)
            error.__cause__ = entry.error
            raise error
        raise entry.error
    return entry.result


#: Extra rounds a permission runs for the retries of commits that conflicted in it (D50, D-LIVE-2).
_PERMIT_RETRIES = 3
#: Seconds a round waits for the retries of commits that will not retry at once (a caller's own
#: loop) before it ends the permission.
_RETRY_WAIT = 0.005
#: Longest a round keeps its permission for the immediate retries of :meth:`Database.run`
#: (never beyond the permission's validity; D50 Amendment "held retries").
_RETRY_HOLD = 0.1

#: Per thread: whether the commit being made comes from :meth:`Database.run` with retries left
#: (it will retry a conflict at once when the conflict says the permission is held for it).
_RUN = threading.local()


class _Entry:
    """One commit in a group (D50 for a cluster node, R71 for a standalone storage): its build
    step and the tier (or storage) it wrote to, then its result (``True``/``False``) or error."""

    __slots__ = ("build", "tier", "done", "result", "error", "error_shared", "group", "retrying", "thread")

    def __init__(self, build: Callable, tier, group: "_Group"):
        self.build = build
        self.tier = tier
        self.retrying = getattr(_RUN, "retry_left", False)   # Database.run will retry a conflict
        self.thread = threading.get_ident()
        self.done = False
        self.result = None
        self.error = None
        self.error_shared = False
        self.group = group

    def finish(self, *, result=None, error=None, shared: bool = False) -> None:
        """Decide this commit and wake its thread."""
        group = self.group
        with group.cond:
            self.result, self.error, self.error_shared, self.done = result, error, shared, True
            group.cond.notify_all()


class _Group:
    """The commits of one coordinator (one node, D50) or one standalone storage (R71) waiting
    for a round."""

    def __init__(self):
        self.cond = threading.Condition()
        self.queue = []
        self.running = False
        # A permission kept for the immediate retry of the runner's own conflicting commit
        # (D50 Amendment "held retries"): ``(permission, tier, rounds run, keeper thread,
        # coordinator, deadline, expiry timer)``; kept for at most _RETRY_HOLD (review I2).
        self.kept = None


_GROUPS = weakref.WeakKeyDictionary()
_GROUPS_LOCK = threading.Lock()


def _group_of(owner) -> _Group:
    """The commit group of ``owner`` (a coordinator or a standalone storage)."""
    with _GROUPS_LOCK:
        group = _GROUPS.get(owner)
        if group is None:
            group = _GROUPS[owner] = _Group()
        return group


def _requeue(group: _Group, entries: list) -> None:
    """Put undecided commits back at the head of the queue (nothing of theirs was sent)."""
    with group.cond:
        group.queue[:0] = [e for e in entries if not e.done]
        group.cond.notify_all()


def _take_kept(group: _Group, coordinator, tier):
    """``(permission, rounds run)`` kept by a previous runner of ``group`` for the retry of its
    own commit, or ``None``.  A kept permission that is no longer usable is ended here."""
    with group.cond:
        kept, group.kept = group.kept, None
    if kept is None:
        return None
    kept[6].cancel()
    permission, kept_tier, rounds, deadline = kept[0], kept[1], kept[2], kept[5]
    if kept_tier is tier and time.monotonic() < deadline and permission.is_valid() \
            and permission.outcome != "unknown" and not getattr(permission, "gate_closed", False):
        return permission, rounds
    _end_permission(kept[4], permission)
    return None


def _keep(group: _Group, permission, tier, rounds: int, coordinator) -> None:
    """Keep ``permission`` in ``group`` for the immediate retry of this thread's commit, for at
    most :data:`_RETRY_HOLD` (review I2).  A timer ends it if no commit took it by then, so a
    retry whose ``fn`` works longer never stalls the other members of the cluster behind the
    owner's single outstanding permission.  Liveness only: ending a permission is always
    allowed, and the timer never decides an acceptance or an acknowledgement."""
    def expire():
        with group.cond:
            if group.kept is not kept:
                return              # taken (or dropped) meanwhile
            group.kept = None
        _end_permission(coordinator, permission)

    timer = threading.Timer(_RETRY_HOLD, expire)
    timer.daemon = True
    kept = (permission, tier, rounds, threading.get_ident(), coordinator,
            time.monotonic() + _RETRY_HOLD, timer)
    with group.cond:
        group.kept = kept
        group.cond.notify_all()
    timer.start()


def _end_permission(coordinator, permission) -> None:
    try:
        coordinator.finish(permission)
    except (ProtoDBError, OSError) as e:
        logger.error("Ending the cluster write permission failed: %s", e)


def _drop_kept() -> None:
    """End the permission this thread's last commit kept for a retry that is not coming
    (:meth:`Database.run` returned or raised instead of retrying)."""
    group = getattr(_RUN, "kept_group", None)
    if group is None:
        return
    _RUN.kept_group = None
    with group.cond:
        kept = group.kept
        if kept is None or kept[3] != threading.get_ident():
            return
        group.kept = None
    kept[6].cancel()
    _end_permission(kept[4], kept[0])


def _run_round(coordinator, lock_timeout, batch: list, group: _Group) -> bool:
    """Run one permission's rounds for ``batch`` (D50); return False when the permission's base
    was stale (its commits were put back in the queue for the next permission).

    When the runner's own commit conflicts in a round that keeps the permission, the runner
    does not end it: it leaves it in ``group.kept`` and returns, so its thread can retry at
    once from the root the permission committed (:meth:`Database.run`), and the next runner
    -- that retry, or any commit queued meanwhile -- continues under the same permission.
    The runner cannot wait for its own retry: it is the thread that has to run it (measured,
    D50 Amendment "held retries")."""
    attached = coordinator.tier
    live = []
    for entry in batch:
        if entry.tier is not attached:
            entry.finish(error=ProtoValidationError(
                "The space this commit belongs to was closed: its tier is no longer attached to the "
                "coordinator; nothing was replicated"))
        else:
            live.append(entry)
    if not live:
        return True
    tier = attached
    me = threading.get_ident()
    kept = _take_kept(group, coordinator, tier)
    if kept is not None:
        permission, rounds = kept
    else:
        rounds = 0
        try:
            permission = coordinator.permit(lock_timeout)
        except BaseException as e:
            for entry in live:
                entry.finish(error=e, shared=True)
            raise_now = not isinstance(e, ProtoDBError)
            if raise_now:
                raise
            return True
    fresh = True
    keep = False
    try:
        with tier.critical_section():
            # Commits that queued while this runner waited for the permission join its first
            # round, as the retries of a held permission join its later rounds
            # (:func:`_await_retries`).  Waiting for a permission takes the other members'
            # rounds; without this, they would wait for one more permission.
            live += _drain_queue(group, attached)
            while live:
                rounds += 1
                result = _permit_round(coordinator, tier, permission, live, group,
                                       hold=rounds <= _PERMIT_RETRIES)
                if result is None:
                    fresh = False
                    break
                if getattr(permission, "proposed", False):
                    # D55 rule H1: a permission that proposed its root runs no further round
                    # (its successor may already build on that root); commits queued since
                    # wait for this node's next permission.
                    break
                if not result or rounds > _PERMIT_RETRIES or not permission.is_valid() \
                        or permission.outcome == "unknown" or getattr(permission, "gate_closed", False):
                    # A closed gate (D55) ends the permission too: its base is a proposal that
                    # will not be known committed under it, and a retry would rebuild on it.
                    break
                if any(e.thread == me and getattr(e.error, "permit_held", False) for e in result):
                    keep = True         # this thread retries its own commit under the permission
                    break
                live = _await_retries(group, result, attached, permission)
    finally:
        if keep:
            _keep(group, permission, tier, rounds, coordinator)
            _RUN.kept_group = group
        else:
            _end_permission(coordinator, permission)
    return fresh


def _await_retries(group: _Group, conflicted: list, tier, permission) -> list:
    """The retries of the round's conflicting commits, and whatever else queued meanwhile.

    A commit of :meth:`Database.run` retries at once from the root the round committed (its
    conflict said the permission is held for it): the round waits until every such thread
    has queued its retry, for at most :data:`_RETRY_HOLD` and never beyond the permission's
    validity.  Without the hold, a retry slept the backoff of ``run`` (7-10 ms measured with a
    10 ms backoff ceiling) and arrived after the permission had ended; the next permission carried a newer
    root again, and a member behind a delayed link committed nothing for many permissions
    (D50 Amendment "held retries", D-LIVE-2).  Other conflicting commits get
    :data:`_RETRY_WAIT`."""
    held = {e.thread for e in conflicted if e.retrying} - {threading.get_ident()}
    hold = min(_RETRY_HOLD, max(0.0, permission.remaining())) if held else 0.0
    deadline = time.monotonic() + max(hold, _RETRY_WAIT)
    with group.cond:
        while True:
            queued = {e.thread for e in group.queue}
            if held <= queued if held else len(group.queue) >= len(conflicted):
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            group.cond.wait(timeout=remaining)
    return _drain_queue(group, tier)


def _drain_queue(group: _Group, tier) -> list:
    """Take every commit queued in ``group``; return those written to ``tier`` (the others are
    failed: their space was closed)."""
    with group.cond:
        batch, group.queue = group.queue, []
    live = []
    for entry in batch:
        if entry.tier is not tier:
            entry.finish(error=ProtoValidationError(
                "The space this commit belongs to was closed: its tier is no longer attached to the "
                "coordinator; nothing was replicated"))
        else:
            live.append(entry)
    return live


def _permit_round(coordinator, tier, permission, entries: list, group: _Group, *,
                  hold: bool = False) -> Optional[list]:
    """Build ``entries`` on the permission's base and replicate the result (one round, D50).

    ``hold``: the permission is kept for another round, so a conflict of a commit that
    :meth:`Database.run` will retry says so (``ProtoConflictError.permit_held``) and the
    retry starts at once.  Returns the commits that conflicted (decided only after the replication), or ``None`` when
    the base is stale -- the commits go back to the queue and the permission ends with an
    outcome its owner cannot take for granted (``"unknown"``: it abandons the term, D50).
    """
    sent = False
    built = []
    conflicted = []
    try:
        base = _lease_base(coordinator, tier, permission)
        if base is None:
            # A proposal base superseded by a committed root of this term (D55, H2) reports
            # "none": nothing was sent.  Any other stale base: an outcome the owner cannot take
            # for granted.
            if permission.outcome != "committed" and getattr(permission, "after", None) is None:
                permission.outcome = "unknown"
            _requeue(group, entries)
            return None
        current, label, replicated = base
        if replicated:
            coordinator.await_records(current, label, permission)   # its records: never read under the lease
        permission.bind_root(current, label)
        if current is not None and _base_needs_commit(coordinator, permission, current):
            # The base was accepted by some peer but is not known committed (its holder's
            # replication failed or its release was lost).  Commit it first with an unchanged
            # root on top, so that transactions starting from the committed root see it.  The
            # base travels with its records.  Never for a proposal base (D55, H2): it is not
            # committed yet by construction, and only the gate may let a root follow it.
            sent = True
            current = _commit_base(coordinator, tier, permission, label, current)
            permission.outcome, permission.committed = "committed", (permission.term, current)
            permission.bind_root(current, permission.term)
            sent = False
        gate = None         # D55, review 8.3k I1: the gate's verdict on a proposal base, once
        for entry in entries:
            stamp = _ClusterStamp(permission.term, (current.epoch if current is not None else 0) + 1)
            try:
                try:
                    ptr = entry.build(current, stamp)
                except (ProtoCorruptionError, ProtoUnexpectedError):
                    # A base read failed.  On a proposal base (``after``) that is not corruption:
                    # its records may be nowhere yet, or never -- the predecessor died or stalled
                    # before any copy left it.  Wait for the gate: once it opens the base is
                    # committed with ``f + 1`` copies and the read is retried once (a failure
                    # then is a real one); if it closes, nothing was sent, so the commit gets a
                    # truthful, retryable conflict (Lemma N).
                    if getattr(permission, "after", None) is None or permission.sent is not None:
                        raise
                    if gate is None:
                        gate = coordinator.await_proposal_base(permission)
                    if not gate:
                        raise ProtoConflictError("The proposal this permission was issued on never committed "
                                                 "(its holder failed or stalled); nothing was sent")
                    ptr = entry.build(current, stamp)
            except ProtoConflictError as e:
                entry.error = e
                conflicted.append(entry)
                continue
            except ProtoDBError as e:
                entry.finish(error=e)
                continue
            if ptr is None:
                entry.finish(result=False)
                continue
            current = RootRecord(FORMAT_VERSION, ptr.segment, ptr.offset, stamp.epoch)
            built.append((entry, stamp))
        if not built and getattr(permission, "after", None) is not None and permission.sent is None:
            # A pipelined round that sends nothing (every commit conflicted, or none had
            # anything to publish): wait for the gate first (D55, Task 8.3k ruling).  The
            # conflicted commits' retries then start from the proposal base, committed by
            # then (a transaction reads committed roots only), as the held retries of D-LIVE-2
            # need; and the "none" this permission reports most likely reaches the owner after
            # its predecessor's done, the single-entry case of rule O5.  Liveness only.
            coordinator.await_proposal_base(permission)
        if built:
            sent = True
            # D55 rule H1: only a round in which no commit conflicted proposes its root.
            replication = coordinator.replicate(permission, current, tier=tier, propose=not conflicted)
            sent = False
            permission.outcome, permission.committed = "committed", (permission.term, current)
            permission.bind_root(current, permission.term)
            if getattr(permission, "proposed", False):
                # D55 rule H6: its successor's gate waits for this done.
                coordinator.finish(permission)
            for entry, stamp in built:
                stamp.timings["replication"] = replication
                coordinator.record_critical(stamp.timings)
                entry.finish(result=True)
        for entry in conflicted:
            if hold and entry.retrying and permission.is_valid() and permission.outcome != "unknown" \
                    and not getattr(permission, "gate_closed", False):
                entry.error.permit_held = True
            entry.finish(error=entry.error)
        return conflicted
    except BaseException as e:
        # Nothing sent (a conflict or validation error raised before the replication left this
        # node; ``replicate`` raises a conflict exactly when nothing a voter may accept left it,
        # D55 rule H7): the commits may be retried.  Otherwise the outcome is unknown, for every
        # commit of the round, and the owner abandons the term.
        nothing_sent = not sent or isinstance(e, (ProtoConflictError, ProtoValidationError, ProtoUserError))

        if not nothing_sent:
            permission.outcome = "unknown"
        for entry in conflicted:
            if not entry.done:
                entry.finish(error=entry.error)     # not part of what was sent
        for entry in entries:
            if not entry.done:
                entry.finish(error=e, shared=True)
        if not isinstance(e, ProtoDBError):
            raise
        return []


def _base_needs_commit(coordinator, permission, current) -> bool:
    """True when the round must first commit its base by re-proposing it (:func:`_commit_base`):
    the base is not known committed here.  Never for a proposal base (D55, H2): it is not
    committed yet by construction, and only the gate may let a root follow it.  Planted test P5
    breaks this seam."""
    return getattr(permission, "after", None) is None and coordinator.known_root() != (True, current)


#: Leases :func:`_commit_under_lease` acquires before giving up on finding a high enough term.
_TERM_RETRIES = 5


def _lease_base(coordinator, tier, lease) -> Optional[tuple]:
    """``(base record, its label, replicated)`` for a commit under ``lease``, or ``None`` when
    the lease's term is not above the base's label (the term floor has been raised: acquire
    again).  ``replicated`` is False for the persisted root (no replication carries it).

    The base is the lease's root when it is certain, else the result of a recovery round.
    When no node of the answering majority knows any root (a cluster started on empty local
    tiers, or one that lost a majority of its disks), the cluster refounds from the
    persisted root with a term above any root a lost node accepted (D46).

    Hard guard: the base never orders below the committed root this node knows.  Every
    committed root was accepted by a majority, which every recovery and every granting
    majority of witnesses intersects, so an older base means acceptor logs were lost beyond
    the failure model (a majority of them); committing over it would silently drop an
    acknowledged commit.  When the committed root it knows is the persisted root (or an older
    one), the cluster refounds from the persisted root, whose records are in the object store,
    as for any majority loss (D46).  Otherwise it raises ``ProtoCorruptionError``, naming the
    operator's remedy (``force_rejoin()`` on this node once the loss is established).  The
    guard concerns roots committed before this lease was granted (labelled below its term); a
    committed root labelled with this term or a later one means another holder committed
    after this lease was granted -- the holder was paused past its lease -- so the lease is
    superseded and the caller acquires again.
    """
    replicated = True
    if lease.root_certain or getattr(lease, "after", None) is not None:
        # A proposal base (D55, H2) is the permit's root as given: no recovery round.
        current, label = lease.root, lease.root_term
    else:
        known, current, label = coordinator.recover_root()
        if not known:
            persisted = tier.read_persisted()
            label, current = persisted if persisted is not None else (0, None)
            replicated = False
            coordinator.observe_root(current, from_storage=True, term=label, accept=False, tier=tier)
            if lease.term < label + REFOUND_TERM_STEP:
                coordinator.refound(label)
                return None
    known, committed, committed_label = coordinator.committed_root()[:3]
    permission = isinstance(lease, Permission)
    if known and (committed_label > lease.term or (committed_label == lease.term and (
            not permission or compare_roots(committed_label, committed, label, current) > 0))):
        # Superseded: by a later owner's commit, or (under a permission, D50) by a commit of
        # this term after the base the owner gave: acquire again.
        return None
    if known and compare_roots(label, current, committed_label, committed) < 0:
        try:
            persisted = tier.read_persisted()
        except (ProtoDBError, OSError) as e:
            raise _guard_error(label, current, committed_label, committed) from e
        if persisted is None or compare_roots(persisted[0], persisted[1], committed_label, committed) < 0:
            raise _guard_error(label, current, committed_label, committed)
        # The witnesses lost the persisted root (a majority of logs lost): refound from it.
        label, current = persisted
        replicated = False
        coordinator.observe_root(current, from_storage=True, term=label, accept=False, tier=tier)
        if lease.term < label + REFOUND_TERM_STEP:
            coordinator.refound(label)
            return None
    if label > lease.term or (label == lease.term and not permission):
        return None                 # acquire learnt the label: the next term is above it
    return current, label, replicated


def _guard_error(label, current, committed_label, committed) -> ProtoCorruptionError:
    """The error of the commit guard of :func:`_lease_base`, naming the operator's remedy."""
    return ProtoCorruptionError(
        f"Protocol violation: the base root (label {label}, epoch {current.epoch if current else 0}) "
        f"orders below the committed root this node knows (label {committed_label}, epoch "
        f"{committed.epoch if committed else 0}); refusing to commit over it.  This follows the loss "
        f"of a majority of acceptor logs, which lost that commit.  Once an operator has established "
        f"that loss, call force_rejoin() on this node: it forgets the commit it cannot recover and "
        f"continues from the highest root the cluster accepted (D46)")


def _commit_base(coordinator, tier, lease, label: int, base: RootRecord) -> RootRecord:
    """Replicate a root equal to ``base`` (labelled ``label``; same databases), labelled with the
    lease term, on top of it, together with the base's records the object store lacks; return
    the new root's record (see :func:`_commit_under_lease`).

    Re-proposing the base is what makes it committed; without its records, a base whose
    records lived only on its holder's disk would be lost with that disk (ruling R50).
    ``ProtoConflictError`` (nothing replicated) when they are on no reachable peer.
    """
    where, records, ranges = coordinator.commit_records(label, base)
    if where is None:
        raise ProtoConflictError(f"The base root (epoch {base.epoch}) cannot be re-committed now: {records}")
    context = _SpaceContext(tier.storage)      # the commit's tier, captured when it began
    space = _load_space(context, base)
    stamp = _ClusterStamp(lease.term, base.epoch + 1)
    ptr = context.save(_new_space_root(space.databases, context, stamp, space))
    record = RootRecord(FORMAT_VERSION, ptr.segment, ptr.offset, stamp.epoch)
    coordinator.replicate(lease, record, base=(label, base, records, ranges) if where == "records" else None,
                          tier=tier)
    return record


def _new_space_root(databases: Dictionary, context, stamp: Optional[_ClusterStamp],
                    previous: Optional[SpaceRoot]) -> SpaceRoot:
    return SpaceRoot(databases=databases, context=context, cluster=stamp, previous=previous)


def _cluster_tier(storage, node_id: str):
    """The cluster tier serving ``storage`` for node ``node_id`` (D46): the storage itself when
    it is a tiered storage, a :class:`~proto_db.cluster.persister.PlainTier` for a storage
    shared by every node; an object store must go through a tiered storage."""
    if getattr(storage, "cluster_tier", False):
        return storage
    if not getattr(storage, "root_lock_is_global", True):
        raise ProtoValidationError(
            "In cluster mode an object store is used through TieredStorage (local WAL tier plus "
            "asynchronous replication), so that it stays off the commit path (D46)")
    from .cluster.persister import PlainTier
    return PlainTier(storage, node_id)


# -- ObjectSpace ------------------------------------------------------------------------------

class ObjectSpace:
    """A set of named databases over one :class:`~proto_db.storage.base.SharedStorage`.

    The space owns the storage: :meth:`close` closes it.

    Args:
        storage: the shared storage.
        lock_timeout: seconds commits wait for the root lock (``None``: the storage's
            default), or for the cluster write lease (``None``: the coordinator's
            ``acquire_timeout``); expiry raises ``ProtoTimeoutError``.
        coordinator: a started :class:`~proto_db.cluster.coordinator.ClusterCoordinator`
            (D44, D45, D46) when several processes write the space.  Commits then flush
            their records to the local tier, acquire the cluster write lease (instead of
            the root lock), validate and merge against the newest root of the granting
            majority and commit by majority-acknowledged replication; the storage root is
            persisted afterwards by the node's persister (:meth:`sync` waits for it).
            Transactions take their snapshot from the newest committed root the coordinator
            knows.  The storage must be a :class:`~proto_db.storage.tiered.TieredStorage`
            or a storage shared by every node (``MemoryStorage``, ``FileStorage`` on a
            shared filesystem).  A commit is durable when it returns -- in the local WAL or
            acceptor log of a majority, ``fsync``-ed per the durability mode -- except over
            ``MemoryStorage``, whose cluster keeps its acceptor logs in memory and is not
            durable.  A node grants and acknowledges only while a space is open on it.  Every writer of the space must use a coordinator of the
            same cluster.  The caller owns the coordinator (:meth:`close` detaches it but
            does not stop it).  ``None`` (default): no cluster, unchanged behaviour.
        atom_cache_entries: how many decoded atoms the space shares between its
            transactions and threads, by storage pointer (ruling R75): a pointer's content
            never changes, so the cache needs no invalidation and is never stale.  ``0``
            disables it (each transaction decodes what it reads).
        query_cache_bytes: capacity, in estimated bytes, of the space's query result cache
            (:attr:`query_cache`, D56), shared by its transactions and threads and keyed by
            the pointers of what each query reads, so it is never stale.  ``0`` stores
            nothing (results are still computed).
    """

    def __init__(self, storage, *, lock_timeout: Optional[float] = None, coordinator=None,
                 atom_cache_entries: int = ATOM_CACHE_ENTRIES, query_cache_bytes: int = QUERY_CACHE_BYTES):
        if lock_timeout is not None:
            check_lock_timeout(lock_timeout)
        if type(atom_cache_entries) is not int or atom_cache_entries < 0:
            raise ProtoValidationError("atom_cache_entries must be a non-negative int")
        self._storage = storage
        self._atoms = _AtomCache(storage, atom_cache_entries) if atom_cache_entries else None
        self._query_cache = QueryCache(query_cache_bytes, self._space_load)
        self._lock_timeout = lock_timeout
        self._coordinator = coordinator
        self._tier = None
        self._closed = False
        self._state_lock = threading.Lock()
        if coordinator is not None:
            self._tier = _cluster_tier(storage, coordinator.node_id)
            try:
                coordinator.attach(self._tier)
            except BaseException:
                if self._tier is not storage:
                    self._tier.close()
                raise

    @property
    def storage(self):
        """The underlying storage."""
        return self._storage

    def _space_load(self, ptr: AtomPointer, type_name: str) -> Atom:
        """Load an atom outside any transaction: through the shared atom cache, else as a stub."""
        atoms = self._atoms
        if atoms is not None:
            return atoms.load(ptr, type_name)
        return Atom._stub(_SpaceContext(self._storage), ptr, type_name)

    @property
    def query_cache(self) -> QueryCache:
        """The space's query result cache (D56): ``query_cache.result(query)`` returns the
        immutable :class:`~proto_db.query.cache.QueryResult` of a plan, Queryable or collection."""
        return self._query_cache

    def query_cache_stats(self) -> dict:
        """Counters of the query result cache (see :meth:`QueryCache.stats`)."""
        return self._query_cache.stats()

    def atom_cache_stats(self) -> Optional[dict]:
        """Counters of the shared decoded-atom cache (``hits``, ``misses``, ``entries``,
        ``capacity``), or ``None`` when it is disabled."""
        return None if self._atoms is None else self._atoms.stats()

    @property
    def lock_timeout(self) -> Optional[float]:
        """Seconds a commit waits for the root lock (``None``: the storage default)."""
        return self._lock_timeout

    @property
    def coordinator(self):
        """The cluster coordinator commits go through, or ``None``."""
        return self._coordinator

    def _snapshot_record(self):
        """The root a new transaction starts from: in cluster mode the newest committed root
        the coordinator knows (recovered from the peers, else the persisted root, when it
        knows none; epochs never go backwards), else the storage's."""
        return self._read_storage_root()

    def _read_storage_root(self):
        """The current root: the storage's, or in cluster mode the cluster's committed root
        (the persisted one may lag)."""
        coordinator = self._coordinator
        if coordinator is None:
            return self._storage.read_root()
        if coordinator.needs_confirmation:
            # One attempt per attach: under contention the node learns newer roots anyway, and
            # its logged committed root keeps its reads from going backwards across a process
            # crash.  Across a power loss a committed entry not yet synced by a later acceptor
            # entry can be lost (committed knowledge is not acceptor state, rootlog module
            # docstring, Task 8.3e); if this confirmation fails too, a read can then start from
            # an older committed root -- a committed state still, never a wrong one.
            coordinator.confirmed()
            try:
                _commit_under_lease(coordinator, self._lock_timeout, _confirm_build)
            except (ProtoTimeoutError, ProtoConflictError, ProtoOutcomeUnknownError) as e:
                logger.warning("Confirming the committed root under the cluster lease failed: %s", e)
        return coordinator.current_root()[1]

    @property
    def committed_epoch(self) -> int:
        """Cluster mode: the epoch of the newest committed root this node knows (else the storage epoch)."""
        if self._coordinator is None:
            record = self._storage.read_root()
            return record.epoch if record is not None else 0
        return self._coordinator.committed_epoch

    @property
    def persisted_epoch(self) -> int:
        """Cluster mode: the highest epoch this node knows to be persisted in the storage (else
        the storage epoch: every commit is persisted when it returns)."""
        if self._coordinator is None:
            return self.committed_epoch
        persister = self._coordinator.persister
        return persister.persisted_epoch if persister is not None else 0

    def sync(self, timeout: Optional[float] = None) -> None:
        """Wait until every commit this node knows is persisted in the storage (cluster mode:
        the persisted epoch reaches the committed epoch); ``ProtoTimeoutError`` after
        ``timeout`` seconds.  Without a cluster commits are persisted when they return."""
        self._check_open()
        if self._coordinator is None:
            return
        persister = self._coordinator.persister
        if persister is None:
            raise ProtoValidationError("The coordinator has no persister for this space")
        known, record, label = self._coordinator.committed_root()[:3]
        if known:
            persister.wait_persisted(label, record, timeout)

    def _check_open(self) -> None:
        if self._closed:
            raise ProtoClosedError("ObjectSpace is closed")

    def _current_space(self, context) -> Optional[SpaceRoot]:
        self._check_open()
        return _load_space(context, self._read_storage_root())

    def new_database(self, name: str) -> "Database":
        """Create the database ``name`` (``ProtoValidationError`` if it exists) and return it."""
        _check_name(name, "database")
        self._check_open()
        context = _SpaceContext(self._storage)

        def build(current, stamp):
            space = _load_space(context, current)
            databases = space.databases if space is not None else Dictionary(context=context)
            if databases.has(name):
                raise ProtoValidationError(f"Database {name!r} already exists")
            root = DatabaseRoot(objects=Dictionary(context=context), mutables=Dictionary(context=context),
                                previous=None, version=0, committed_at=_utc_now(), origin=uuid.uuid4().bytes,
                                context=context)
            return context.save(_new_space_root(databases.set_at(name, root), context, stamp, space))

        _commit_root(self._storage, self._lock_timeout, build, self._coordinator)
        return Database(self, name)

    def open_database(self, name: str) -> "Database":
        """Return the existing database ``name`` (``ProtoValidationError`` if absent)."""
        _check_name(name, "database")
        space = self._current_space(_SpaceContext(self._storage))
        if (space is None or not space.databases.has(name)) and self._coordinator is not None:
            # This node's committed root may lag a commit another node just made (its release
            # is on the way): ask a majority before reporting the database missing.
            try:
                self._coordinator.recover_root()
            except ProtoConflictError as e:
                logger.info("Recovery before reporting database %r missing failed: %s", name, e)
            space = self._current_space(_SpaceContext(self._storage))
        if space is None or not space.databases.has(name):
            raise ProtoValidationError(f"Database {name!r} does not exist")
        return Database(self, name)

    def list_databases(self) -> list:
        """Sorted names of the databases of the space."""
        space = self._current_space(_SpaceContext(self._storage))
        return [] if space is None else sorted(space.databases.keys())

    def close(self) -> None:
        """Close the space and its storage (detaching the coordinator first).  Idempotent."""
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
        if self._coordinator is not None:
            self._coordinator.detach(self._tier)
            if self._tier is not self._storage:
                self._tier.close()          # the node's acceptor log of a shared storage
        if self._atoms is not None:
            self._atoms.close()
        self._storage.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


# -- Database ---------------------------------------------------------------------------------

class Database:
    """Handle on a named database of an :class:`ObjectSpace`; cheap, stateless and thread-safe."""

    def __init__(self, space: ObjectSpace, name: str):
        """Bind the handle; use :meth:`ObjectSpace.new_database` / :meth:`ObjectSpace.open_database`."""
        self._space = space
        self._name = name
        self._origin_value = _ABSENT

    @property
    def space(self) -> ObjectSpace:
        """The space holding this database."""
        return self._space

    @property
    def name(self) -> str:
        """The database name."""
        return self._name

    def _root_in(self, context, record) -> DatabaseRoot:
        space = _load_space(context, record)
        root = None if space is None else space.databases.get_at(self._name)
        if root is None:
            raise ProtoValidationError(f"Database {self._name!r} does not exist")
        return root

    def new_transaction(self) -> "Transaction":
        """Start a transaction on the current snapshot."""
        self._space._check_open()
        return Transaction(self)

    def history(self) -> list:
        """The committed versions of this database, newest first, as :class:`HistoryEntry` objects."""
        self._space._check_open()
        context = _SpaceContext(self._space.storage)
        root = self._root_in(context, self._space._read_storage_root())
        entries = []
        while root is not None:
            entries.append(HistoryEntry(root, self._name))
            root = root.previous
        return entries

    def sync(self, timeout: Optional[float] = None) -> None:
        """Wait until the commits this node knows are persisted (see :meth:`ObjectSpace.sync`)."""
        self._space.sync(timeout)

    def _origin(self):
        """This database's identity (``DatabaseRoot.origin``, D56): read once per handle (R101 N2).
        It never changes: ``new_database`` draws it, every commit copies it, and no API removes or
        renames a database."""
        origin = self._origin_value
        if origin is _ABSENT:
            context = _SpaceContext(self._space.storage)
            origin = self._origin_value = self._root_in(context, self._space._read_storage_root()).origin
        return origin

    def _check_owned(self, name, root) -> None:
        """``ProtoReferenceError`` unless ``root`` is a version of this database (R99 I2): the name
        recorded with the reference and the identity the version itself carries must both be
        this database's.  A misuse check, not a security boundary (references carry no MAC)."""
        if name is not None and name != self._name:
            raise ProtoReferenceError(f"The reference names the database {name!r}, not {self._name!r}")
        if root.origin != self._origin():
            raise ProtoReferenceError(f"The reference names a version of another database, not of "
                                      f"{self._name!r} (another incarnation of the name, or another space)")

    def snapshot_at(self, ref) -> Snapshot:
        """A read-only :class:`Snapshot` of the version ``ref`` names (Task 8.4b, D56).

        ``ref`` is a reference (bytes) from :attr:`Transaction.snapshot_ref` or
        :attr:`Snapshot.ref`, a pager token (:meth:`Pager.token`), a :class:`Snapshot` or a
        :class:`HistoryEntry`.  Any process of the space may open it; the version is read
        through the space's atom cache, or from the object store when no local copy is
        left.  ``ProtoReferenceError`` (a client error) for a malformed reference, one that
        names no database version, or one of another database: the version's own identity
        (``DatabaseRoot.origin``) is checked, not only the name a reference records.  This
        is a misuse check, not a security boundary: a reference carries no MAC, so never hand
        one to a party that must not read every version of the database.
        """
        self._space._check_open()
        if isinstance(ref, Snapshot):
            name, root = ref.database.name, ref._root
        elif isinstance(ref, HistoryEntry):
            name, root = ref._database, ref.root
        else:
            return self._snapshot_of(decode_ref(ref, self._space._space_load))
        self._check_owned(name, root)
        return Snapshot(self, root)

    def _snapshot_of(self, fields: dict) -> Snapshot:
        """The snapshot a decoded reference names, after the identity checks of :meth:`snapshot_at`."""
        name = fields["db"]
        if name != self._name:
            raise ProtoReferenceError(f"The reference names the database {name!r}, not {self._name!r}")
        if fields["origin"] != self._origin():
            raise ProtoReferenceError(f"The reference names another database than {self._name!r} "
                                      f"(another incarnation of the name, or another space)")
        root = self._space._space_load(fields["root"], DatabaseRoot.__type_name__)
        try:
            root._load()
            root.origin, root.version            # a pointer that does not hold a DatabaseRoot fails here
        except (ProtoCorruptionError, ProtoValidationError, AttributeError, TypeError) as e:
            raise ProtoReferenceError(f"The reference names no version of {self._name!r} "
                                      f"({type(e).__name__})") from None
        if type(root) is not DatabaseRoot:
            raise ProtoReferenceError(f"The reference names no version of {self._name!r}")
        self._check_owned(name, root)
        return Snapshot(self, root)

    def resume_pager(self, token: bytes):
        """The pager a token describes (:meth:`Pager.token`), on the same snapshot: in any process,
        any later transaction, the same pages (Task 8.4b, D56).  ``ProtoReferenceError`` for a
        malformed token or one of another database; ``ProtoStalePagerError`` when the query
        no longer plans as it did (another query semantics version, planner or extractor
        version), since its pages could then come in another order."""
        self._space._check_open()
        fields = decode_ref(token, self._space._space_load)
        arguments = pager_arguments(fields)
        snapshot = self._snapshot_of(fields)
        root_name = arguments.pop("root_name")
        try:
            pager = snapshot.pager(root_name, **arguments)
        except ProtoReferenceError:
            raise
        except (ProtoValidationError, ProtoUserError) as e:
            raise ProtoReferenceError(f"The pager token does not describe a query of this version: {e}") from None
        return check_resumed(pager, fields)

    def run(self, fn: Callable[["Transaction"], Any], retries: int = 10,
            backoff: tuple = (0.001, 0.2)) -> Any:
        """Run ``fn(tr)`` in a fresh transaction, commit it and return ``fn``'s result.

        On ``ProtoConflictError`` (from the commit, from a merge, or raised by ``fn``)
        the attempt is aborted and retried with a fresh transaction, up to ``retries``
        times, sleeping a bounded exponential backoff with jitter between attempts:
        ``uniform(d/2, d)`` with ``d = min(backoff[1], backoff[0] * 2**attempt)``.  After
        the last retry the conflict is re-raised.  In cluster mode a conflict decided under a
        write permission that the commit round keeps for this retry is retried at once,
        without the backoff (D50 Amendment "held retries"), so the retry starts from the
        root that permission committed.  Any other exception aborts the
        transaction and propagates unchanged; notably ``ProtoOutcomeUnknownError`` is never
        retried, because the commit may have been applied.  If ``fn`` ends the transaction itself
        (commit or abort), ``run`` returns its result without committing.
        """
        if isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
            raise ProtoValidationError("retries must be a non-negative int")
        try:
            low, high = backoff
        except (TypeError, ValueError) as e:
            raise ProtoValidationError("backoff must be a (min_seconds, max_seconds) pair") from e
        if not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in (low, high)) \
                or low < 0 or high < low:
            raise ProtoValidationError("backoff must satisfy 0 <= min_seconds <= max_seconds")
        # A run nested in an outer run's ``fn`` keeps its own retry state and restores the
        # outer one when it ends (review M1).
        outer_retry = getattr(_RUN, "retry_left", False)
        outer_kept = getattr(_RUN, "kept_group", None)
        _RUN.kept_group = None
        try:
            for attempt in range(retries + 1):
                tr = self.new_transaction()
                _RUN.retry_left = attempt < retries
                held = False
                try:
                    result = fn(tr)
                    if tr.is_active:
                        tr.commit()
                    return result
                except ProtoConflictError as e:
                    if attempt == retries:
                        raise
                    held = getattr(e, "permit_held", False)
                    if held:
                        continue    # the cluster round keeps its permission for this retry (D50)
                finally:
                    _RUN.retry_left = outer_retry
                    if tr.is_active:
                        tr.abort()
                    if not held:
                        _drop_kept()    # a permission kept for a retry that is not coming
                delay = min(high, low * (2 ** attempt))
                time.sleep(delay * (0.5 + random.random() / 2))
            raise ProtoUnexpectedError("unreachable: the retry loop always returns or raises")  # pragma: no cover
        finally:
            _RUN.kept_group = outer_kept


# -- Transaction ------------------------------------------------------------------------------

class Transaction:
    """A snapshot-isolated unit of work on a :class:`Database` (see the module docstring).

    Implements :class:`~proto_db.core.atom.PersistenceContext`.  Once committed or
    aborted, every public method raises ``ProtoUserError``.  As a context manager it
    commits on success and aborts on an exception (which propagates).
    """

    def __init__(self, database: Database):
        """Start a transaction on ``database``'s current snapshot (use :meth:`Database.new_transaction`)."""
        self._database = database
        self.storage = database.space.storage
        atoms = database.space._atoms
        # Shared decoded atoms only when they read the storage this transaction writes to.
        self._atoms = atoms if atoms is not None and atoms.storage is self.storage else None
        self._lock = threading.RLock()
        self._state = _ACTIVE
        self._reads: dict = {}
        self._writes: dict = {}
        self._replaced: set = set()
        self._mreads: dict = {}         # mutable name -> token (or ABSENT) seen in the snapshot
        self._mwrites: dict = {}        # mutable name -> value or REMOVED
        self._mexpected: set = set()    # mutables set blindly: must still exist at commit
        record = database.space._snapshot_record()
        self._epoch = record.epoch if record is not None else 0
        self._snapshot_pointer = record.pointer if record is not None else None
        self._snapshot_space = _load_space(self, record)
        self._db_root = database._root_in(self, record)
        self._objects = self._db_root.objects
        self._mutables = self._db_root.mutables

    # -- PersistenceContext -----------------------------------------------------------------

    def save(self, atom: Atom) -> AtomPointer:
        """Persist ``atom`` (children first) and return its pointer; adopts atoms with no context."""
        return _save_atom(self, atom)

    def load(self, ptr: AtomPointer, type_name: str) -> Atom:
        """Return the atom stored at ``ptr``: the space's shared decoded atom (R75), or a lazy
        stub of this transaction when the space's atom cache is disabled."""
        atoms = self._atoms
        if atoms is not None:
            return atoms.load(ptr, type_name)
        return Atom._stub(self, ptr, type_name)

    def _atom_saved(self, atom: Atom, ptr: AtomPointer, state: dict) -> None:
        """The :meth:`Atom._save` hook: the space's atom cache receives what this transaction
        pushes (Task 8.3i), when it shares decoded atoms with this transaction."""
        atoms = self._atoms
        if atoms is not None:
            atoms._atom_saved(atom, ptr, state)

    # -- state ------------------------------------------------------------------------------

    @property
    def database(self) -> Database:
        """The database this transaction works on."""
        return self._database

    @property
    def is_active(self) -> bool:
        """True until the transaction is committed or aborted."""
        return self._state == _ACTIVE

    def _check_active(self) -> None:
        if self._state != _ACTIVE:
            raise ProtoUserError(f"The transaction is {self._state}; start a new one")

    # -- roots ------------------------------------------------------------------------------

    def get_root_object(self, name: str):
        """Return the value of root ``name`` (``None`` if absent); records the read (see module docs)."""
        _check_name(name)
        with self._lock:
            self._check_active()
            if name in self._writes:
                value = self._writes[name]
                return None if value is _REMOVED else value
            value = self._objects.get_at(name, _ABSENT)
            if name not in self._reads:
                self._reads[name] = _token(value)
            return None if value is _ABSENT else value

    @property
    def snapshot_ref(self) -> bytes:
        """A reference to the version this transaction reads (:meth:`Database.snapshot_at`)."""
        return encode_ref("snapshot", self._database.name, self._db_root.atom_pointer, self._db_root.origin)

    def pager(self, root_name: str, *, where=None, order_by=None, page_size: int):
        """A portable pager over root ``root_name`` of this transaction's snapshot (see
        :meth:`Snapshot.pager`).  Refused (``ProtoUserError``) when this transaction has written
        that root: uncommitted data does not exist for anyone else (P2).  The read is recorded."""
        _check_name(root_name)
        with self._lock:
            self._check_active()
            if root_name in self._writes:
                raise ProtoUserError(f"Root {root_name!r} was written in this transaction; a pager pages a "
                                     f"committed snapshot (commit first, or page from another transaction)")
        self.get_root_object(root_name)
        return Snapshot(self._database, self._db_root).pager(root_name, where=where, order_by=order_by,
                                                            page_size=page_size)

    def set_root_object(self, name: str, value, *, replace: bool = False) -> None:
        """Set root ``name`` to ``value`` (any canonical value or atom) at commit.

        A fresh collection written to an existing root you did not read is merged
        into it, not substituted: its operations (intent) are replayed onto the
        value current at commit (D33).  Use ``replace=True`` to substitute: the value
        is then stored as written, last writer wins, no merge.  A set that follows a
        :meth:`remove_root_object` of the same name in this transaction is a
        replacement too.  Once a name is marked for replacement it stays so for the
        rest of the transaction.  A root read with :meth:`get_root_object` before
        being written is never merged: a concurrent change to it is a conflict.
        """
        _check_name(name)
        if not isinstance(replace, bool):
            raise ProtoValidationError("replace must be a bool")
        value = canonical(value)
        with self._lock:
            self._check_active()
            if replace or self._writes.get(name) is _REMOVED:
                self._replaced.add(name)
            self._writes[name] = value

    def remove_root_object(self, name: str) -> None:
        """Remove root ``name`` at commit (a no-op if it is absent then); a later set of it replaces."""
        _check_name(name)
        with self._lock:
            self._check_active()
            self._writes[name] = _REMOVED
            self._replaced.add(name)

    def root_names(self) -> list:
        """Sorted names of the roots as this transaction sees them.

        Listing is not recorded in the read set.  To create a root only if it is
        absent, test it with ``get_root_object(name) is None``: that read is
        recorded (ABSENT included), so a concurrent creator makes the commit
        conflict; deciding from ``root_names()`` gives no such protection.
        """
        with self._lock:
            self._check_active()
            names = set(self._objects.keys())
            for name, value in self._writes.items():
                if value is _REMOVED:
                    names.discard(name)
                else:
                    names.add(name)
            return sorted(names)

    # -- mutable cells (D11, D35) ------------------------------------------------------------

    def _mutable_view(self, name: str, record: bool):
        """The cell ``name`` as this transaction sees it (value or ABSENT); the caller holds the lock.

        With ``record``, a snapshot read enters the read set (first read only).
        """
        if name in self._mwrites:
            value = self._mwrites[name]
            return _ABSENT if value is _REMOVED else value
        value = self._mutables.get_at(name, _ABSENT)
        if record and name not in self._mreads:
            self._mreads[name] = _token(value)
        return value

    def new_mutable(self, name: str, value) -> MutableObject:
        """Create the mutable cell ``name`` holding ``value`` at commit; return its handle.

        Raises ``ProtoValidationError`` if the cell exists in this transaction's view.
        The absence is recorded in the read set, so a concurrent creator of the same
        name makes one of the two commits raise ``ProtoConflictError``.
        """
        _check_name(name, "mutable")
        value = canonical(value)
        with self._lock:
            self._check_active()
            if self._mutable_view(name, record=True) is not _ABSENT:
                raise ProtoValidationError(f"Mutable {name!r} already exists")
            self._mwrites[name] = value
        return MutableObject(name)

    def get_mutable(self, name: str):
        """Return the value of the mutable cell ``name`` (``None`` if absent); records the read.

        Like :meth:`get_root_object`: the snapshot value (an exact token, or ABSENT)
        enters the read set, so a concurrent change, creation or removal of the cell
        makes the commit raise ``ProtoConflictError``.
        """
        _check_name(name, "mutable")
        with self._lock:
            self._check_active()
            value = self._mutable_view(name, record=True)
            return None if value is _ABSENT else value

    def set_mutable(self, name: str, value) -> None:
        """Set the mutable cell ``name`` to ``value`` at commit; the cell must exist.

        Raises ``ProtoValidationError`` if the cell is absent in this transaction's view
        (use :meth:`new_mutable`).  The value is stored as written, never merged.  A set
        without a prior read is blind: a concurrent set is not a conflict (last writer
        wins), but a concurrent removal of the cell is.
        """
        _check_name(name, "mutable")
        value = canonical(value)
        with self._lock:
            self._check_active()
            if self._mutable_view(name, record=False) is _ABSENT:
                raise ProtoValidationError(f"Mutable {name!r} does not exist; create it with new_mutable")
            if name not in self._mwrites:
                self._mexpected.add(name)
            self._mwrites[name] = value

    def remove_mutable(self, name: str) -> None:
        """Remove the mutable cell ``name`` at commit; the cell must exist in this transaction's view.

        Raises ``ProtoValidationError`` if it is absent.  A cell removed meanwhile by a
        concurrent commit makes a blind removal a no-op, not a conflict.
        """
        _check_name(name, "mutable")
        with self._lock:
            self._check_active()
            if self._mutable_view(name, record=False) is _ABSENT:
                raise ProtoValidationError(f"Mutable {name!r} does not exist")
            self._mwrites[name] = _REMOVED

    def mutable_names(self) -> list:
        """Sorted names of the mutable cells as this transaction sees them (not recorded in the read set)."""
        with self._lock:
            self._check_active()
            names = set(self._mutables.keys())
            for name, value in self._mwrites.items():
                if value is _REMOVED:
                    names.discard(name)
                else:
                    names.add(name)
            return sorted(names)

    # -- factories --------------------------------------------------------------------------

    def _new(self, cls):
        self._check_active()
        return cls(context=self)

    def new_list(self) -> List:
        """A new empty :class:`List` bound to this transaction."""
        return self._new(List)

    def new_dictionary(self) -> Dictionary:
        """A new empty :class:`Dictionary` bound to this transaction."""
        return self._new(Dictionary)

    def new_hash_dictionary(self) -> HashDictionary:
        """A new empty :class:`HashDictionary` bound to this transaction."""
        return self._new(HashDictionary)

    def new_set(self) -> Set:
        """A new empty :class:`Set` bound to this transaction."""
        return self._new(Set)

    def new_counted_set(self) -> CountedSet:
        """A new empty :class:`CountedSet` bound to this transaction."""
        return self._new(CountedSet)

    def new_repeated_keys_dictionary(self) -> RepeatedKeysDictionary:
        """A new empty :class:`RepeatedKeysDictionary` bound to this transaction."""
        return self._new(RepeatedKeysDictionary)

    def new_object(self, **attributes) -> DBObject:
        """A new :class:`DBObject` with ``attributes``, bound to this transaction."""
        self._check_active()
        return DBObject(context=self, **attributes)

    # -- end --------------------------------------------------------------------------------

    def abort(self) -> None:
        """Discard the transaction's writes; publishes nothing."""
        with self._lock:
            self._check_active()
            self._state = _ABORTED

    def commit(self) -> None:
        """Commit the writes (commit protocol in the module docstring).

        Raises ``ProtoConflictError`` when a root in the read set changed or a merge
        conflicts; nothing is published then.  The transaction ends either way: after a
        failed commit it is aborted.
        """
        with self._lock:
            self._check_active()
            self._state = _COMMITTING
            committed = False
            try:
                self._commit()
                committed = True
            finally:
                self._state = _COMMITTED if committed else _ABORTED

    def _commit(self) -> None:
        if not self._writes and not self._mwrites:
            return
        written = list(self._writes.values()) + list(self._mwrites.values())
        for value in written:
            if isinstance(value, Atom):
                self.save(value)
        space = self._database.space
        # In cluster mode the round's replication makes every record of the round durable with
        # one local sync (D50); no flush per transaction.
        if _commit_root(self.storage, space.lock_timeout, self._build_root, space.coordinator):
            # Post-publish, cannot fail: a committed value's log was consumed by this
            # commit, so a later transaction reusing it must not replay it again (D33).
            for value in written:
                if isinstance(value, ConcurrentOptimized):
                    object.__setattr__(value, "_log_complete", False)

    def _build_root(self, current, stamp: Optional[_ClusterStamp] = None) -> AtomPointer:
        """Under the root lock (or the cluster lease): check the read set, merge, save and return
        the new SpaceRoot pointer.  With a cluster ``stamp`` the stage timings are recorded in it."""
        clock = time.perf_counter
        started = clock()
        replay = 0.0
        # The snapshot is current only if it is the very same root: in cluster mode, after a
        # refound, an epoch no longer identifies a root (D46).
        if (current.pointer if current is not None else None) == self._snapshot_pointer:
            space = self._snapshot_space
            db_root = self._db_root
        else:
            space = _load_space(self, current)
            db_root = space.databases.get_at(self._database.name) if space is not None else None
            if db_root is None:
                raise ProtoConflictError(f"Database {self._database.name!r} no longer exists")
            for name, token in self._reads.items():
                if not _same_token(token, _token(db_root.objects.get_at(name, _ABSENT))):
                    raise ProtoConflictError(
                        f"Root {name!r} of database {self._database.name!r} was changed by a concurrent commit")
            if self._mreads or self._mexpected:
                self._check_mutables(db_root.mutables)
        validated = clock()
        current_objects = db_root.objects
        objects = current_objects
        for name, value in self._writes.items():
            if value is _REMOVED:
                if objects.has(name):
                    objects = objects.remove_at(name)
                continue
            if name not in self._reads and name not in self._replaced:
                merge_started = clock()
                value = self._merged(value, current_objects.get_at(name, _ABSENT))
                replay += clock() - merge_started
            objects = objects.set_at(name, value)
        mutables = db_root.mutables
        for name, value in self._mwrites.items():
            if value is not _REMOVED:
                mutables = mutables.set_at(name, value)
            elif mutables.has(name):
                mutables = mutables.remove_at(name)
        new_root = DatabaseRoot(objects=objects, mutables=mutables, previous=db_root,
                                version=db_root.version + 1,
                                committed_at=max(_utc_now(), db_root.committed_at), origin=db_root.origin,
                                context=self)
        new_space = _new_space_root(space.databases.set_at(self._database.name, new_root), self, stamp, space)
        ptr = self.save(new_space)
        if stamp is not None:
            stamp.timings["validation"] = validated - started
            stamp.timings["replay"] = replay
            stamp.timings["path_copy"] = clock() - validated - replay
        return ptr

    def _check_mutables(self, current: Dictionary) -> None:
        """Under the root lock: raise ``ProtoConflictError`` if a read cell changed or a blindly set one vanished."""
        for name, token in self._mreads.items():
            if not _same_token(token, _token(current.get_at(name, _ABSENT))):
                raise ProtoConflictError(
                    f"Mutable {name!r} of database {self._database.name!r} was changed by a concurrent commit")
        for name in self._mexpected:
            if not current.has(name):
                raise ProtoConflictError(
                    f"Mutable {name!r} of database {self._database.name!r} was removed by a concurrent commit")

    @staticmethod
    def _merged(mine, current):
        """The value to store for a root written but not read (merge rule in the module docstring)."""
        if (current is _ABSENT or not isinstance(mine, ConcurrentOptimized) or not mine.log_is_complete
                or type(current) is not type(mine)):
            return mine
        return mine.rebase_onto(current)

    # -- context manager --------------------------------------------------------------------

    def __enter__(self):
        self._check_active()
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            if self.is_active:
                self.commit()
        elif self.is_active:
            self.abort()
        return False
