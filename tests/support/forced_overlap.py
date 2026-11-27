"""Commits forced to overlap: concurrency coverage that never depends on the scheduler.

A concurrency test that must exercise the paths a commit takes when another commit was published
after its snapshot (reapplication on a newer root, R34; delta validation under the space root,
R65) cannot rely on the threads meeting by chance: on a GIL build the scheduler may run a whole
transaction between two switches, and on a fast machine the pre-pass outside the lock absorbs
most races, so a run can end with no such commit at all (the 2.0.0rc5 CI failures on Python
3.14: no reapplication, no delta validation).

:func:`overlap_commits` makes the overlap a logical rule instead.  Each commit of a member thread
(a callable wrapped by :meth:`CommitOverlap.member`) waits, before its pre-pass, until either

* the published root is no longer the transaction's snapshot -- another commit landed after the
  snapshot, so the pre-pass (or, with a stale R0, the build under the space root) meets a
  concurrent change; or
* every other live member waits there too -- then they are all released together: the first to
  take the space root publishes, and every other one meets that change.

No member ever waits on time, only on these conditions, and every wait ends: a member that does
not wait is running, and it either reaches the wait, publishes (the root moves) or finishes (it
leaves the live set).  Waiting members hold neither the space root nor any storage lock.  Commits
of threads that are not members (seeding, checks) never wait.  Standalone storage only (the
published root is the storage's).
"""
import threading

from proto_db.transactions import Transaction

_local = threading.local()


class CommitOverlap:
    """The rule of :func:`overlap_commits` for ``members`` threads, and what it did: ``met``
    commits found the root moved without waiting, ``moved`` waited until it moved, ``released``
    were released together with every other live member."""

    def __init__(self, members: int):
        self._cond = threading.Condition()
        self._live = members
        self._waiting = set()
        self._generation = 0
        self.met = 0
        self.moved = 0
        self.released = 0

    def member(self, fn):
        """``fn`` run as a member thread: its commits follow the rule; it leaves the live set
        when it returns or raises.  Exactly ``members`` callables must be wrapped and run."""
        def run(*args, **kwargs):
            _local.overlap = self
            try:
                return fn(*args, **kwargs)
            finally:
                _local.overlap = None
                with self._cond:
                    self._live -= 1
                    self._release_if_all_wait()
                    self._cond.notify_all()
        return run

    @staticmethod
    def _moved(tr: Transaction) -> bool:
        record = tr._database.space._storage.read_root()
        return record is not None and record.pointer != tr._snapshot_pointer

    def _release_if_all_wait(self) -> None:
        if self._waiting and len(self._waiting) >= self._live:
            self.released += len(self._waiting)
            self._waiting.clear()
            self._generation += 1
            self._cond.notify_all()

    def _before_prepass(self, tr: Transaction) -> None:
        with self._cond:
            if self._moved(tr):
                self.met += 1
                return
            token = object()
            self._waiting.add(token)
            generation = self._generation
            self._release_if_all_wait()
            while generation == self._generation and not self._moved(tr):
                self._cond.wait()
            if generation == self._generation:
                self._waiting.discard(token)
                self.moved += 1

    def _published(self) -> None:
        with self._cond:
            self._cond.notify_all()

    def __repr__(self):
        return f"CommitOverlap(met={self.met}, moved={self.moved}, released={self.released})"


def overlap_commits(monkeypatch, members: int) -> CommitOverlap:
    """Patch ``Transaction._prepass`` (the wait of the rule) and ``Transaction._commit`` (every
    finished commit wakes the waiters to look at the published root again); undone by
    ``monkeypatch``.  Wrap each of the ``members`` thread bodies with :meth:`CommitOverlap.member`."""
    overlap = CommitOverlap(members)
    prepass, commit = Transaction._prepass, Transaction._commit

    def waiting_prepass(self):
        current = getattr(_local, "overlap", None)
        if current is overlap and self._snapshot_pointer is not None:
            overlap._before_prepass(self)
        return prepass(self)

    def notifying_commit(self):
        try:
            return commit(self)
        finally:
            if getattr(_local, "overlap", None) is overlap:
                overlap._published()

    monkeypatch.setattr(Transaction, "_prepass", waiting_prepass)
    monkeypatch.setattr(Transaction, "_commit", notifying_commit)
    return overlap
