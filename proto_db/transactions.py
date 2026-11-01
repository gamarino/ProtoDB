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
       by compare-and-swap.  If publishing fails, the outcome may be unknown (e.g. a
       file storage failing after the rename): the root is read again; if it is ours
       the commit succeeded, if another epoch is visible the commit raises
       ``ProtoConflictError``, otherwise the original error is raised (wrapped in
       ``ProtoUnexpectedError`` if it was not a ProtoDB error).  If the root cannot
       even be read back, ``ProtoUnexpectedError`` reports an unknown outcome.
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

import datetime
import logging
import random
import sys
import threading
import time
from typing import Any, Callable, Optional

from .collections.base import ConcurrentOptimized, _Immutable
from .collections.dictionaries import Dictionary, RepeatedKeysDictionary
from .collections.hash_dictionaries import HashDictionary
from .collections.lists import List
from .collections.sets import CountedSet, Set
from .core.atom import Atom, AtomPointer, PersistenceContext
from .core.dbobject import DBObject, MutableObject
from .errors import (ProtoConflictError, ProtoCorruptionError, ProtoDBError, ProtoUnexpectedError, ProtoUserError,
                     ProtoValidationError)
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
    """The atom the storage root points to: ``databases`` maps database names to :class:`DatabaseRoot`."""

    __type_name__ = "proto_db.transactions.SpaceRoot"

    def __init__(self, *, databases: Dictionary, context: PersistenceContext | None = None):
        """Create a space root over the ``databases`` dictionary."""
        super().__init__(context=context)
        object.__setattr__(self, "databases", databases)


class DatabaseRoot(_Immutable, Atom):
    """One version of a database: ``objects`` (root name -> value), ``mutables`` (mutable
    name -> value, D35), ``version``, ``committed_at`` and the ``previous`` version
    (``None`` for version 0)."""

    __type_name__ = "proto_db.transactions.DatabaseRoot"

    def __init__(self, *, objects: Dictionary, mutables: Dictionary, previous: Optional["DatabaseRoot"],
                 version: int, committed_at: datetime.datetime, context: PersistenceContext | None = None):
        """Create a database version."""
        super().__init__(context=context)
        osa = object.__setattr__
        osa(self, "objects", objects)
        osa(self, "mutables", mutables)
        osa(self, "previous", previous)
        osa(self, "version", version)
        osa(self, "committed_at", committed_at)

    def _absent_attribute(self, name: str):
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

    __slots__ = ("version", "committed_at", "_root")

    def __init__(self, root: DatabaseRoot):
        """Describe the database version ``root``."""
        self.version = root.version
        self.committed_at = root.committed_at
        self._root = root

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

    def __repr__(self):
        return f"HistoryEntry(version={self.version}, committed_at={self.committed_at.isoformat()})"


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

def _publish(storage, ptr: AtomPointer, expected_epoch: int) -> None:
    """Publish ``ptr`` by CAS; resolve an unknown outcome by reading the root back (step 6 of D34)."""
    try:
        storage.publish_root(ptr, expected_epoch)
        return
    except (ProtoDBError, OSError) as e:
        error = e
    try:
        root = storage.read_root()
    except (ProtoDBError, OSError) as e:
        raise ProtoUnexpectedError(
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


def _commit_root(storage, lock_timeout, build: Callable) -> bool:
    """Run steps 3-7 of the commit protocol (D34); return True if a root was published.

    ``build(current_record)`` runs under the root lock; it checks conflicts and returns
    the pointer of the saved new :class:`SpaceRoot`, or ``None`` to publish nothing.
    """
    lock = storage.root_lock(lock_timeout)
    lock.__enter__()
    try:
        current = storage.read_root()
        ptr = build(current)
        if ptr is not None:
            storage.flush()
            _publish(storage, ptr, current.epoch if current is not None else 0)
    except BaseException:
        # Nothing of ours is published: release, but never let a release error mask the original one.
        try:
            lock.__exit__(*sys.exc_info())
        except (ProtoDBError, OSError) as e:
            logger.error("Releasing the root lock after a failed commit failed: %s", e)
        raise
    # Published (or nothing to publish): nothing below may fail the commit.
    try:
        lock.__exit__(None, None, None)
    except (ProtoDBError, OSError) as e:
        logger.error("Releasing the root lock after a commit failed: %s", e)
    return ptr is not None


# -- ObjectSpace ------------------------------------------------------------------------------

class ObjectSpace:
    """A set of named databases over one :class:`~proto_db.storage.base.SharedStorage`.

    The space owns the storage: :meth:`close` closes it.

    Args:
        storage: the shared storage.
        lock_timeout: seconds commits wait for the root lock (``None``: the storage's
            default); expiry raises ``ProtoTimeoutError``.
    """

    def __init__(self, storage, *, lock_timeout: Optional[float] = None):
        if lock_timeout is not None and (isinstance(lock_timeout, bool)
                                         or not isinstance(lock_timeout, (int, float)) or lock_timeout < 0):
            raise ProtoValidationError("lock_timeout must be None or a non-negative number of seconds")
        self._storage = storage
        self._lock_timeout = lock_timeout
        self._closed = False
        self._state_lock = threading.Lock()

    @property
    def storage(self):
        """The underlying storage."""
        return self._storage

    @property
    def lock_timeout(self) -> Optional[float]:
        """Seconds a commit waits for the root lock (``None``: the storage default)."""
        return self._lock_timeout

    def _check_open(self) -> None:
        if self._closed:
            raise ProtoValidationError("ObjectSpace is closed")

    def _current_space(self, context) -> Optional[SpaceRoot]:
        self._check_open()
        return _load_space(context, self._storage.read_root())

    def new_database(self, name: str) -> "Database":
        """Create the database ``name`` (``ProtoValidationError`` if it exists) and return it."""
        _check_name(name, "database")
        self._check_open()
        context = _SpaceContext(self._storage)

        def build(current):
            space = _load_space(context, current)
            databases = space.databases if space is not None else Dictionary(context=context)
            if databases.has(name):
                raise ProtoValidationError(f"Database {name!r} already exists")
            root = DatabaseRoot(objects=Dictionary(context=context), mutables=Dictionary(context=context),
                                previous=None, version=0, committed_at=_utc_now(), context=context)
            return context.save(SpaceRoot(databases=databases.set_at(name, root), context=context))

        _commit_root(self._storage, self._lock_timeout, build)
        return Database(self, name)

    def open_database(self, name: str) -> "Database":
        """Return the existing database ``name`` (``ProtoValidationError`` if absent)."""
        _check_name(name, "database")
        space = self._current_space(_SpaceContext(self._storage))
        if space is None or not space.databases.has(name):
            raise ProtoValidationError(f"Database {name!r} does not exist")
        return Database(self, name)

    def list_databases(self) -> list:
        """Sorted names of the databases of the space."""
        space = self._current_space(_SpaceContext(self._storage))
        return [] if space is None else sorted(space.databases.keys())

    def close(self) -> None:
        """Close the space and its storage.  Idempotent."""
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
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
        root = self._root_in(context, self._space.storage.read_root())
        entries = []
        while root is not None:
            entries.append(HistoryEntry(root))
            root = root.previous
        return entries

    def run(self, fn: Callable[["Transaction"], Any], retries: int = 10,
            backoff: tuple = (0.001, 0.2)) -> Any:
        """Run ``fn(tr)`` in a fresh transaction, commit it and return ``fn``'s result.

        On ``ProtoConflictError`` (from the commit, from a merge, or raised by ``fn``)
        the attempt is aborted and retried with a fresh transaction, up to ``retries``
        times, sleeping a bounded exponential backoff with jitter between attempts:
        ``uniform(d/2, d)`` with ``d = min(backoff[1], backoff[0] * 2**attempt)``.  After
        the last retry the conflict is re-raised.  Any other exception aborts the
        transaction and propagates unchanged.  If ``fn`` ends the transaction itself
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
        for attempt in range(retries + 1):
            tr = self.new_transaction()
            try:
                result = fn(tr)
                if tr.is_active:
                    tr.commit()
                return result
            except ProtoConflictError:
                if attempt == retries:
                    raise
            finally:
                if tr.is_active:
                    tr.abort()
            delay = min(high, low * (2 ** attempt))
            time.sleep(delay * (0.5 + random.random() / 2))
        raise ProtoUnexpectedError("unreachable: the retry loop always returns or raises")  # pragma: no cover


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
        self._lock = threading.RLock()
        self._state = _ACTIVE
        self._reads: dict = {}
        self._writes: dict = {}
        self._replaced: set = set()
        self._mreads: dict = {}         # mutable name -> token (or ABSENT) seen in the snapshot
        self._mwrites: dict = {}        # mutable name -> value or REMOVED
        self._mexpected: set = set()    # mutables set blindly: must still exist at commit
        record = self.storage.read_root()
        self._epoch = record.epoch if record is not None else 0
        self._snapshot_space = _load_space(self, record)
        self._db_root = database._root_in(self, record)
        self._objects = self._db_root.objects
        self._mutables = self._db_root.mutables

    # -- PersistenceContext -----------------------------------------------------------------

    def save(self, atom: Atom) -> AtomPointer:
        """Persist ``atom`` (children first) and return its pointer; adopts atoms with no context."""
        return _save_atom(self, atom)

    def load(self, ptr: AtomPointer, type_name: str) -> Atom:
        """Return a lazy stub of the atom stored at ``ptr``."""
        return Atom._stub(self, ptr, type_name)

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
        if _commit_root(self.storage, space.lock_timeout, self._build_root):
            # Post-publish, cannot fail: a committed value's log was consumed by this
            # commit, so a later transaction reusing it must not replay it again (D33).
            for value in written:
                if isinstance(value, ConcurrentOptimized):
                    object.__setattr__(value, "_log_complete", False)

    def _build_root(self, current) -> AtomPointer:
        """Under the root lock: check the read set, merge, save and return the new SpaceRoot pointer."""
        epoch = current.epoch if current is not None else 0
        if epoch == self._epoch:
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
        current_objects = db_root.objects
        objects = current_objects
        for name, value in self._writes.items():
            if value is _REMOVED:
                if objects.has(name):
                    objects = objects.remove_at(name)
                continue
            if name not in self._reads and name not in self._replaced:
                value = self._merged(value, current_objects.get_at(name, _ABSENT))
            objects = objects.set_at(name, value)
        mutables = db_root.mutables
        for name, value in self._mwrites.items():
            if value is not _REMOVED:
                mutables = mutables.set_at(name, value)
            elif mutables.has(name):
                mutables = mutables.remove_at(name)
        new_root = DatabaseRoot(objects=objects, mutables=mutables, previous=db_root,
                                version=db_root.version + 1,
                                committed_at=max(_utc_now(), db_root.committed_at), context=self)
        new_space = SpaceRoot(databases=space.databases.set_at(self._database.name, new_root), context=self)
        return self.save(new_space)

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
