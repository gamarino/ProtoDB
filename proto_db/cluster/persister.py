"""Asynchronous persistence of committed roots, and the cluster tier interface (Task 8.3d, D44, D46).

In cluster mode a commit is acknowledged by majority replication; the storage root is
written afterwards by one :class:`Persister` thread per node, never in a critical section.

Each pass of a node's persister:

1. ships the node's own sealed segments to the object store, in creation order (tiered
   storage only; ``ship_interval`` coalesces commits into segments);
2. considers the newest committed root the node knows.  It persists it when this node
   committed it, or when the oldest commit it knows and not yet persisted has waited longer
   than ``persist_lag`` (a crashed or partitioned committer): any node then persists the
   newest root it knows.  Only the newest root is written (coalescing), and only once every
   record it references is in the object store: the persister walks the chain of roots
   (each cluster root links the previous one) back to the persisted epoch and requires,
   for each commit, the byte ranges of the records shipped with it (when this node
   accepted it) or else the sealed object of the segment holding its root (by the strict
   per-node upload order, that covers every record of the commit).  When persistence has
   lagged beyond ``persist_lag``, missing ranges of other nodes' segments are uploaded
   from this node's copies (part objects; a writer lost before shipping).  The write is a
   compare-and-swap that never moves the persisted epoch backwards (a lost race re-reads
   and gives up if behind);
3. refreshes its view of the persisted epoch from the store while it is behind and not
   the one expected to write (so :meth:`Persister.wait_persisted` returns);
4. releases local segments the persisted root covers and checks the backlog (alert hook).

Errors (the object store unreachable, for instance) are logged and retried with a
growing back-off; commits are never affected.

The cluster tier
    What the coordinator and the persister need from the storage of a space: a duck-typed
    interface.  :class:`~proto_db.storage.tiered.TieredStorage` implements it natively
    (local WAL tier plus object store); :class:`PlainTier` adapts a storage shared by
    every node (``MemoryStorage`` in one process, ``FileStorage`` on a shared filesystem),
    where records need no shipping and the persisted root is the storage's own root.
    Methods: ``storage``, ``outgoing()``, ``flush_local()``, ``shipped(records, root)``,
    ``apply(records, root)``, ``prefetch(records)``, ``log_root(kind, label, record, sync=)``,
    ``load_roots()``, ``local_record(ptr)``, ``set_fetcher(fn)``, ``read_persisted()``,
    ``persist(label, record)``, ``ship(force)``, ``refresh_remote()``, ``commit_ranges(root)``,
    ``ensure_remote(needs, upload_copies=)``,
    ``release_local(epoch)``, ``check_backlog()``, ``check_commit_allowed()``,
    ``critical_section()``.
"""
import collections
import contextlib
import logging
import threading
import time
from typing import Optional

from ..core.atom import Atom
from ..errors import ProtoConflictError, ProtoDBError, ProtoOutcomeUnknownError, ProtoTimeoutError, \
    ProtoUnexpectedError
from ..storage.format import FORMAT_VERSION, RootRecord

__all__ = ["Persister", "PlainTier"]

logger = logging.getLogger(__name__)

_SPACE_ROOT = "proto_db.transactions.SpaceRoot"
_MAX_BACKOFF = 1.0
#: Minimum seconds between two reads of the persisted root by a node that is not expected to write it.
_REFRESH_PERIOD = 0.25


class _ReadContext:
    """Minimal PersistenceContext to load root atoms outside any transaction."""

    def __init__(self, storage):
        self.storage = storage

    def save(self, atom):
        """Never used: the persister only reads."""
        raise ProtoUnexpectedError("The persister never saves atoms")

    def load(self, ptr, type_name: str):
        """Return a lazy stub of the atom stored at ``ptr``."""
        return Atom._stub(self, ptr, type_name)


def cluster_fields(storage, record: RootRecord) -> tuple:
    """``(label, cluster epoch, previous SpaceRoot or None)`` of the space root at ``record``.

    A root written outside cluster mode has no cluster fields: its label is ``0``, its
    epoch the storage epoch and it has no predecessor link.
    """
    space = Atom._stub(_ReadContext(storage), record.pointer, _SPACE_ROOT)
    epoch = space.cluster_epoch
    if epoch is None:
        return 0, record.epoch, None
    return space.cluster_term or 0, epoch, space.cluster_previous


class PlainTier:
    """The cluster tier over a storage shared by every node (see the module docstring).

    The persisted root is the storage's root; its cluster epoch and label come from the
    space root's cluster fields.  Records live in the shared storage, so nothing is shipped
    or applied, and no root log is kept (a node that restarts recovers from its peers).
    """

    def __init__(self, storage):
        self.storage = storage

    def outgoing(self) -> list:
        """Nothing to ship: the peers read the shared storage."""
        return []

    def flush_local(self) -> None:
        """Make this node's records durable in the shared storage."""
        self.storage.flush()

    def shipped(self, records: list, root=None) -> None:
        """Nothing to track."""

    def apply(self, records: list, root=None) -> None:
        """Nothing to store."""

    def prefetch(self, records: list) -> None:
        """Nothing to prefetch."""

    def log_root(self, kind: str, label: int, record, *, sync: bool) -> None:
        """No root log for a shared storage."""

    def load_roots(self) -> tuple:
        """No root log: ``(None, None, 0)``."""
        return None, None, 0

    def local_record(self, ptr) -> Optional[bytes]:
        """Peers read the shared storage themselves."""
        return None

    def set_fetcher(self, fetcher) -> None:
        """Peer page requests are not needed."""

    def read_persisted(self) -> Optional[tuple]:
        """``(label, RootRecord with the cluster epoch)`` of the storage root, or ``None``."""
        record = self.storage.read_root()
        if record is None:
            return None
        label, epoch, _ = cluster_fields(self.storage, record)
        return label, RootRecord(FORMAT_VERSION, record.segment, record.offset, epoch)

    def persist(self, label: int, record: RootRecord) -> bool:
        """Publish ``record`` as the storage root by compare-and-swap on the storage epoch,
        unless the storage already holds that root or a newer one in the ``(label, epoch)``
        order (returns False)."""
        for _ in range(100):
            current = self.storage.read_root()
            if current is not None and cluster_fields(self.storage, current)[:2] >= (label, record.epoch):
                return False
            try:
                self.storage.publish_root(record.pointer, current.epoch if current is not None else 0)
                return True
            except ProtoConflictError:
                continue            # another node persisted meanwhile: read again
            except ProtoOutcomeUnknownError:
                continue            # reading the root again decides
        raise ProtoUnexpectedError(f"Persisting root epoch {record.epoch} lost 100 races")

    def ship(self, force: bool = False) -> bool:
        """Nothing to ship."""
        return False

    def refresh_remote(self) -> None:
        """Nothing to list."""

    def commit_ranges(self, root) -> Optional[list]:
        """Not tracked: every record is in the shared storage."""
        return None

    def ensure_remote(self, needs, *, upload_copies: bool = False) -> bool:
        """Every record is in the shared storage."""
        return True

    def release_local(self, persisted_epoch: int) -> int:
        """Nothing local to release."""
        return 0

    def check_backlog(self) -> Optional[dict]:
        """No backlog."""
        return None

    def check_commit_allowed(self) -> None:
        """Always allowed."""

    def critical_section(self):
        """No instrumentation."""
        return contextlib.nullcontext()


class Persister:
    """One node's persister thread (see the module docstring); started by the coordinator."""

    def __init__(self, coordinator, tier, *, interval: float, lag: float):
        self._coordinator = coordinator
        self._tier = tier
        self._interval = interval
        self._lag = lag
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._stopped = False
        self._urgent = False
        self._waiters = 0
        self._persisted = 0            # highest persisted epoch known
        self._verified = 0             # highest epoch whose chain is known to be remote
        self._pending = collections.deque()   # (committed epoch, monotonic time first seen), unpersisted
        self._last_refresh = 0.0
        self._last_error = None
        self._stats = {"cycles": 0, "roots_written": 0, "roots_behind": 0, "errors": 0, "refreshes": 0}
        self._thread = threading.Thread(target=self._run, name=f"proto-persister-{coordinator.node_id}",
                                        daemon=True)

    def start(self) -> None:
        """Start the thread."""
        self._thread.start()

    def stop(self) -> None:
        """Stop and join the thread.  Idempotent."""
        with self._cond:
            self._stopped = True
            self._cond.notify_all()
        if self._thread.is_alive() and self._thread is not threading.current_thread():
            self._thread.join()

    @property
    def persisted_epoch(self) -> int:
        """The highest epoch this node knows to be persisted."""
        with self._lock:
            return self._persisted

    def stats(self) -> dict:
        """Counters (passes, roots written, roots found already persisted, errors, refreshes)
        and the last error message, if any."""
        with self._lock:
            out = dict(self._stats)
            out["persisted_epoch"] = self._persisted
            out["last_error"] = None if self._last_error is None else str(self._last_error)
            return out

    def wait_persisted(self, epoch: int, timeout: Optional[float]) -> None:
        """Wait until the persisted epoch reaches ``epoch``; ``ProtoTimeoutError`` after ``timeout``."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            self._waiters += 1
            self._urgent = True
            self._cond.notify_all()
            try:
                while self._persisted < epoch:
                    if self._stopped:
                        raise ProtoTimeoutError("The persister stopped before the root was persisted")
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        raise ProtoTimeoutError(
                            f"The persisted epoch is {self._persisted}, waiting for {epoch}"
                            + (f" (last error: {self._last_error})" if self._last_error else ""))
                    self._cond.wait(timeout=remaining if remaining is None else min(remaining, 0.05))
            finally:
                self._waiters -= 1

    def _set_persisted(self, epoch: int) -> None:
        with self._cond:
            if epoch > self._persisted:
                self._persisted = epoch
                self._cond.notify_all()

    def _run(self) -> None:
        delay = self._interval
        first = True
        while True:
            with self._cond:
                if not first:
                    self._cond.wait_for(lambda: self._stopped or self._urgent, timeout=delay)
                if self._stopped:
                    return
                self._urgent = False
            try:
                if first:
                    self._refresh(force=True)
                    self._tier.refresh_remote()
                    first = False
                self._cycle()
                delay = self._interval
                with self._lock:
                    self._last_error = None
            except (ProtoDBError, OSError) as e:
                first = False
                with self._lock:
                    self._stats["errors"] += 1
                    self._last_error = e
                delay = min(_MAX_BACKOFF, max(self._interval, delay * 2))
                logger.info("Persister of node %s: %s (retrying in %.2f s)", self._coordinator.node_id, e, delay)

    def _refresh(self, force: bool = False) -> None:
        """Read the persisted root from the store (at most once per interval unless forced)."""
        now = time.monotonic()
        if not force and now - self._last_refresh < max(self._interval, min(_REFRESH_PERIOD, self._lag / 4)):
            return
        self._last_refresh = now
        persisted = self._tier.read_persisted()
        with self._lock:
            self._stats["refreshes"] += 1
        if persisted is not None:
            label, record = persisted
            self._coordinator.observe_root(record, term=label, accept=False)
            self._set_persisted(record.epoch)

    def _persistable(self, record: RootRecord, upload_copies: bool) -> bool:
        """True when every record the root at ``record`` can reach is in the object store
        (uploading parts from local copies with ``upload_copies``); see the module docstring."""
        if not getattr(self._tier, "cluster_tier", False):
            return True
        floor = max(self._persisted, self._verified)
        storage = self._tier.storage
        needs = []
        current = record
        while True:
            ranges = self._tier.commit_ranges(current)
            if ranges is None:
                needs.append((current.segment, None))
            else:
                needs.extend((segment, end) for segment, _, end in ranges)
            _, _, previous = cluster_fields(storage, current)
            if previous is None:
                break
            epoch = previous.cluster_epoch
            if epoch is None or epoch <= floor:
                break
            ptr = previous.atom_pointer
            current = RootRecord(FORMAT_VERSION, ptr.segment, ptr.offset, epoch)
        if not self._tier.ensure_remote(needs, upload_copies=upload_copies):
            return False
        self._verified = max(self._verified, record.epoch)
        return True

    def _cycle(self) -> None:
        with self._lock:
            self._stats["cycles"] += 1
            waiting = self._waiters > 0
        try:
            self._advance(waiting)
        finally:
            self._tier.check_backlog()      # also while the store is unreachable

    def _advance(self, waiting: bool) -> None:
        coordinator = self._coordinator
        known, record, label, by, _, _ = coordinator.committed_root()
        behind = known and record is not None and record.epoch > self._persisted
        self._tier.ship(force=waiting and behind)
        now = time.monotonic()
        pending = self._pending
        if behind and (not pending or record.epoch > pending[-1][0]):
            pending.append((record.epoch, now))
        while pending and pending[0][0] <= self._persisted:
            pending.popleft()
        if behind:
            # Lag: how long the oldest commit known and not yet persisted has waited.
            mine = by == coordinator.node_id
            lagging = bool(pending) and now - pending[0][1] >= self._lag
            if mine or lagging:
                if self._persistable(record, upload_copies=lagging):
                    if self._tier.persist(label, record):
                        with self._lock:
                            self._stats["roots_written"] += 1
                        self._set_persisted(record.epoch)
                    else:
                        with self._lock:
                            self._stats["roots_behind"] += 1
                        self._refresh(force=True)
            else:
                self._refresh(force=waiting)
        self._tier.release_local(self._persisted)
