"""A pre-pass forced onto an older root (spec R65; Task 12 review, minor a).

:func:`stale_prepass` makes every commit's pre-pass read the transaction's own snapshot as the
last published root R0, whatever was published since.  The pre-pass then proves nothing new, and
every concurrent change since the snapshot is decided under the space root by delta validation
over the diff R0 -> R1: with ``transactions._VERIFY_DELTA`` on, each of those decisions is also
compared with a full validation of the same root.  Standalone storage only (a cluster pre-pass
reads the coordinator's root).
"""
import threading
from dataclasses import dataclass

from proto_db.transactions import ObjectSpace, Transaction


@dataclass(frozen=True)
class _SnapshotRecord:
    """The published-root record of a transaction's snapshot: what the pre-pass reads of R0."""

    pointer: object
    epoch: int


class StalePrepass:
    """Counts the pre-passes forced onto their snapshot (``forced``) and the delta validations
    run under the space root (``delta_checks``)."""

    def __init__(self):
        self.forced = 0
        self.delta_checks = 0
        self._lock = threading.Lock()

    def _count(self, name: str) -> None:
        with self._lock:
            setattr(self, name, getattr(self, name) + 1)


def stale_prepass(monkeypatch) -> StalePrepass:
    """Patch ``Transaction._prepass`` and ``ObjectSpace._published_record`` so that a pre-pass reads
    its transaction's snapshot record as R0 (the first read only: a catch-up round of R69 reads the
    published root again), and count the delta validations; undone by ``monkeypatch``."""
    stats = StalePrepass()
    local = threading.local()
    prepass, published, delta = Transaction._prepass, ObjectSpace._published_record, Transaction._delta_check

    def stale(self):
        local.transaction = self
        try:
            return prepass(self)
        finally:
            local.transaction = None

    def record(self):
        tr = getattr(local, "transaction", None)
        if tr is not None and tr._snapshot_pointer is not None and self._coordinator is None:
            local.transaction = None
            stats._count("forced")
            return _SnapshotRecord(tr._snapshot_pointer, tr._epoch)
        return published(self)

    def counted(self, candidate, db_root):
        stats._count("delta_checks")
        return delta(self, candidate, db_root)

    monkeypatch.setattr(Transaction, "_prepass", stale)
    monkeypatch.setattr(ObjectSpace, "_published_record", record)
    monkeypatch.setattr(Transaction, "_delta_check", counted)
    return stats
