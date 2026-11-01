"""Behavioural contract every ``SharedStorage`` implementation must satisfy.

Subclass :class:`StorageContract` in a ``Test*`` class and implement ``make_storage``
returning a fresh, empty, open storage.  The contract only uses the public
``SharedStorage`` API, so it applies unchanged to every implementation.
"""
import threading
import uuid

import pytest

from proto_db.core.atom import AtomPointer
from proto_db.errors import (ProtoConflictError, ProtoCorruptionError, ProtoLockingError,
                             ProtoTimeoutError, ProtoValidationError)

THREADS, PER_THREAD = 16, 1000


def atom_payload(n):
    """A payload shaped like the output of core/atom."""
    return {"t": "Probe", "oid": n, "s": {"n": n, "label": f"item-{n}", "raw": b"\x00\x01"}}


def run_threads(count, target):
    """Run ``target(index)`` in ``count`` threads; re-raise the first failure in the caller."""
    errors = []

    def wrap(i):
        try:
            target(i)
        except BaseException as e:  # collected and re-raised below
            errors.append(e)
            raise

    threads = [threading.Thread(target=wrap, args=(i,)) for i in range(count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]


class StorageContract:
    """Mixin of contract tests; the concrete class provides ``make_storage()``."""

    def make_storage(self):  # pragma: no cover - overridden
        """Return a fresh empty open storage."""
        raise NotImplementedError

    @pytest.fixture
    def storage(self):
        s = self.make_storage()
        yield s
        s.close()

    def test_fresh_storage_has_no_root(self, storage):
        assert storage.read_root() is None

    def test_atom_round_trip(self, storage):
        payload = atom_payload(1)
        ptr = storage.push_atom(payload)
        assert isinstance(ptr, AtomPointer)
        assert storage.get_atom(ptr) == payload

    def test_pointers_are_distinct(self, storage):
        ptrs = {storage.push_atom(atom_payload(i)) for i in range(50)}
        assert len(ptrs) == 50

    def test_stored_atoms_are_not_aliased(self, storage):
        payload = atom_payload(1)
        ptr = storage.push_atom(payload)
        payload["s"]["n"] = 999
        first = storage.get_atom(ptr)
        assert first["s"]["n"] == 1
        first["s"]["n"] = 555
        assert storage.get_atom(ptr)["s"]["n"] == 1

    def test_blob_round_trip(self, storage):
        data = bytes(range(256)) * 100
        ptr = storage.push_blob(data)
        assert storage.get_blob(ptr) == data
        assert storage.get_blob(storage.push_blob(b"")) == b""

    def test_kind_mismatch_is_corruption(self, storage):
        a = storage.push_atom(atom_payload(1))
        b = storage.push_blob(b"blob")
        with pytest.raises(ProtoCorruptionError):
            storage.get_blob(a)
        with pytest.raises(ProtoCorruptionError):
            storage.get_atom(b)

    def test_unknown_pointer_is_corruption(self, storage):
        storage.push_atom(atom_payload(1))
        with pytest.raises(ProtoCorruptionError):
            storage.get_atom(AtomPointer(uuid.uuid4(), 0))

    def test_invalid_arguments(self, storage):
        with pytest.raises(ProtoValidationError):
            storage.push_atom("not a dict")
        with pytest.raises(ProtoValidationError):
            storage.push_blob("not bytes")

    def test_flush_keeps_data_readable(self, storage):
        ptr = storage.push_atom(atom_payload(3))
        storage.flush()
        assert storage.get_atom(ptr) == atom_payload(3)

    def test_publish_root_advances_epoch(self, storage):
        p1 = storage.push_atom(atom_payload(1))
        r1 = storage.publish_root(p1, 0)
        assert (r1.epoch, r1.pointer) == (1, p1)
        assert storage.read_root() == r1
        p2 = storage.push_atom(atom_payload(2))
        r2 = storage.publish_root(p2, 1)
        assert (r2.epoch, r2.pointer) == (2, p2)
        assert storage.read_root() == r2

    def test_publish_root_conflict_changes_nothing(self, storage):
        p1 = storage.push_atom(atom_payload(1))
        with pytest.raises(ProtoConflictError):
            storage.publish_root(p1, 3)
        assert storage.read_root() is None
        r1 = storage.publish_root(p1, 0)
        p2 = storage.push_atom(atom_payload(2))
        with pytest.raises(ProtoConflictError):
            storage.publish_root(p2, 0)
        assert storage.read_root() == r1

    def test_publish_root_unconditional(self, storage):
        p1 = storage.push_atom(atom_payload(1))
        assert storage.publish_root(p1, None).epoch == 1
        p2 = storage.push_atom(atom_payload(2))
        assert storage.publish_root(p2, None).epoch == 2

    def test_publish_rejects_negative_epoch(self, storage):
        p1 = storage.push_atom(atom_payload(1))
        with pytest.raises(ProtoValidationError):
            storage.publish_root(p1, -1)

    def test_racing_publishers_have_exactly_one_winner(self, storage):
        ptrs = [storage.push_atom(atom_payload(i)) for i in range(8)]
        wins, conflicts = [], []

        def race(i):
            try:
                wins.append(storage.publish_root(ptrs[i], 0))
            except ProtoConflictError:
                conflicts.append(i)

        run_threads(8, race)
        assert len(wins) == 1 and len(conflicts) == 7
        assert storage.read_root() == wins[0]

    def test_no_lost_writes_from_concurrent_pushers(self, storage):
        results = [None] * THREADS

        def push(t):
            results[t] = [(storage.push_atom(atom_payload(t * PER_THREAD + i)), t * PER_THREAD + i)
                          for i in range(PER_THREAD)]

        run_threads(THREADS, push)
        every = [pair for chunk in results for pair in chunk]
        assert len({ptr for ptr, _ in every}) == THREADS * PER_THREAD
        for ptr, n in every:
            assert storage.get_atom(ptr) == atom_payload(n)

    def test_root_lock_is_exclusive(self, storage):
        inside = 0
        peak = 0
        counter_guard = threading.Lock()

        def worker(_):
            nonlocal inside, peak
            for _ in range(200):
                with storage.root_lock():
                    with counter_guard:
                        inside += 1
                        peak = max(peak, inside)
                    with counter_guard:
                        inside -= 1

        run_threads(8, worker)
        assert peak == 1

    def test_root_lock_timeout(self, storage):
        held, release = threading.Event(), threading.Event()

        def holder():
            with storage.root_lock():
                held.set()
                release.wait(10)

        t = threading.Thread(target=holder)
        t.start()
        try:
            assert held.wait(10)
            with pytest.raises(ProtoTimeoutError):
                with storage.root_lock(timeout=0.05):
                    pytest.fail("lock acquired while held elsewhere")
        finally:
            release.set()
            t.join()
        with storage.root_lock(timeout=1):
            pass

    def test_root_lock_released_after_exception(self, storage):
        with pytest.raises(RuntimeError):
            with storage.root_lock():
                raise RuntimeError("boom")
        with storage.root_lock(timeout=1):
            pass

    def test_root_lock_is_not_reentrant(self, storage):
        with storage.root_lock():
            with pytest.raises(ProtoLockingError):
                with storage.root_lock():
                    pytest.fail("re-entered")
        with storage.root_lock(timeout=1):
            pass

    def test_close_is_idempotent_and_final(self):
        s = self.make_storage()
        ptr = s.push_atom(atom_payload(1))
        s.close()
        s.close()
        calls = [lambda: s.push_atom(atom_payload(2)), lambda: s.get_atom(ptr),
                 lambda: s.push_blob(b"x"), lambda: s.get_blob(ptr), s.flush, s.read_root,
                 lambda: s.publish_root(ptr, 0), s.root_lock]
        for call in calls:
            with pytest.raises(ProtoValidationError):
                call()
