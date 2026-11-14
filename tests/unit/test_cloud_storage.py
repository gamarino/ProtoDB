"""CloudStorage over an object store with conditional writes (Task 8.3, D43).

Catches: a root written before the segments it references, a precondition failure
reported as anything but ProtoConflictError, an unknown publish outcome ("applied, then
timed out") that cannot be resolved or is resolved wrongly (a lost or a doubled commit),
segment uploads that are not idempotent under retries, a failed upload that loses
buffered data or lets a root through, corrupt or missing objects not reported as
ProtoCorruptionError, a read cache that misses, and SDK adapters that map native errors
wrongly or fail with anything but ProtoNotSupportedError when the SDK is missing.
"""
import subprocess
import sys
import types

import msgpack
import pytest

from proto_db.core.atom import AtomPointer
from proto_db.errors import (ProtoConflictError, ProtoCorruptionError, ProtoDBError,
                             ProtoNotAuthorizedError, ProtoNotSupportedError, ProtoOutcomeUnknownError,
                             ProtoUnexpectedError, ProtoValidationError)
from proto_db.storage import cloud as cloud_mod
from proto_db.storage.base import SharedStorage
from proto_db.storage.cloud import (CloudStorage, GCSObjectStore, ObjectNotFoundError,
                                    ObjectStoreError, OutcomeUnknownError,
                                    PreconditionFailedError, S3ObjectStore,
                                    TransientObjectStoreError)
from proto_db.storage.format import FORMAT_VERSION, RootRecord, decode_root, encode_root
from proto_db.transactions import ObjectSpace
from tests.contracts.storage_contract import StorageContract, atom_payload
from tests.support.fake_object_store import FakeObjectStore

FAST = dict(backoff=(0.0, 0.0))


def make(store=None, **kw):
    """A CloudStorage over ``store`` (a fresh FakeObjectStore by default) without retry sleeps."""
    kw = {**FAST, **kw}
    return CloudStorage(store if store is not None else FakeObjectStore(), **kw)


def root_writes(store, storage):
    """Indices in ``store.log`` of the writes of the root object."""
    return [i for i, (_, key) in enumerate(store.log) if key == storage.root_key]


# --- the shared contract -------------------------------------------------------------------

class TestCloudStorageContract(StorageContract):
    def make_storage(self):
        return make()


class TestCloudStorageContractSmallSegments(StorageContract):
    """Tiny segments (many objects per flush) and no read cache."""

    def make_storage(self):
        return make(segment_max_bytes=512, cache_bytes=0)


class TestCloudStorageContractFlakyStore(StorageContract):
    """Every operation may fail transiently or time out before or after being applied."""

    def make_storage(self):
        return make(FakeObjectStore(random_faults=0.05, seed=7), retries=30)


def test_is_a_shared_storage():
    assert isinstance(make(), SharedStorage)


# --- data strictly before the root ----------------------------------------------------------

def test_root_is_written_only_after_every_segment_it_follows():
    store = FakeObjectStore()
    s = make(store, segment_max_bytes=600)
    ptrs = [s.push_atom(atom_payload(i)) for i in range(40)]
    segments = {p.segment for p in ptrs}
    assert len(segments) > 3
    assert store.log == []  # nothing is uploaded before a flush or a publish
    s.publish_root(ptrs[-1], 0)
    (root_at,) = root_writes(store, s)
    seg_at = {key: i for i, (_, key) in enumerate(store.log) if key != s.root_key}
    assert {s.segment_key(u) for u in segments} <= set(seg_at)
    assert max(seg_at.values()) < root_at
    assert all(op == "put_if_absent" for op, key in store.log if key != s.root_key)


def test_failed_upload_never_lets_the_root_through_and_keeps_data():
    store = FakeObjectStore()
    s = make(store, retries=2)
    ptr = s.push_atom(atom_payload(1))
    store.inject("put_if_absent", "transient", match=lambda k: k != s.root_key, times=3)
    with pytest.raises(ProtoUnexpectedError):
        s.publish_root(ptr, 0)
    assert s.root_key not in store.keys()
    assert s.read_root() is None
    assert s.get_atom(ptr) == atom_payload(1)  # still served from the local buffer
    record = s.publish_root(ptr, 0)  # the next attempt uploads the kept segment
    assert record.epoch == 1 and s.segment_key(ptr.segment) in store.keys()
    assert make(store).get_atom(ptr) == atom_payload(1)


def test_flush_uploads_immutable_segments():
    store = FakeObjectStore()
    s = make(store)
    a = s.push_atom(atom_payload(1))
    s.flush()
    b = s.push_atom(atom_payload(2))
    s.flush()
    s.flush()  # nothing pending: no new object
    assert a.segment != b.segment  # a flushed segment is never appended to
    assert sorted(store.keys()) == sorted([s.segment_key(a.segment), s.segment_key(b.segment)])
    assert all(op == "put_if_absent" for op, _ in store.log)


def test_publish_rejects_pointer_to_unknown_segment():
    s = make()
    with pytest.raises(ProtoCorruptionError):
        s.publish_root(AtomPointer(cloud_mod.uuid.uuid4(), 30), 0)
    assert s.read_root() is None


# --- compare-and-swap -----------------------------------------------------------------------

def test_two_instances_share_data_and_conflict_by_cas():
    store = FakeObjectStore()
    a, b = make(store), make(store)
    pa = a.push_atom(atom_payload(1))
    pb = b.push_atom(atom_payload(2))
    ra = a.publish_root(pa, 0)
    with pytest.raises(ProtoConflictError):
        b.publish_root(pb, 0)
    assert b.read_root() == ra
    assert b.get_atom(ra.pointer) == atom_payload(1)
    rb = b.publish_root(pb, 1)
    assert a.read_root() == rb and a.get_atom(pb) == atom_payload(2)


def test_injected_precondition_failure_is_a_conflict():
    store = FakeObjectStore()
    s = make(store)
    r1 = s.publish_root(s.push_atom(atom_payload(1)), 0)
    store.inject("put_if_match", "precondition")
    with pytest.raises(ProtoConflictError):
        s.publish_root(s.push_atom(atom_payload(2)), 1)
    assert s.read_root() == r1


def test_unconditional_publish_retries_through_contention():
    store = FakeObjectStore()
    s = make(store)
    s.publish_root(s.push_atom(atom_payload(1)), 0)
    store.inject("put_if_match", "precondition", times=3)
    p = s.push_atom(atom_payload(2))
    assert s.publish_root(p, None) == RootRecord(FORMAT_VERSION, p.segment, p.offset, 2)


# --- unknown outcomes -----------------------------------------------------------------------

def test_root_applied_then_timed_out_is_resolved_as_success():
    store = FakeObjectStore()
    s = make(store)
    r1 = s.publish_root(s.push_atom(atom_payload(1)), 0)
    p2 = s.push_atom(atom_payload(2))
    store.inject("put_if_match", "unknown_after")
    r2 = s.publish_root(p2, r1.epoch)
    assert (r2.pointer, r2.epoch) == (p2, 2)
    assert s.read_root() == r2
    assert len(root_writes(store, s)) == 2  # applied exactly once, never re-sent


def test_first_root_applied_then_timed_out_is_resolved_as_success():
    store = FakeObjectStore()
    s = make(store)
    p = s.push_atom(atom_payload(1))
    store.inject("put_if_absent", "unknown_after", match=lambda k: k == s.root_key)
    assert s.publish_root(p, 0).epoch == 1
    assert len(root_writes(store, s)) == 1


@pytest.mark.parametrize("fault", ["unknown_before", "transient"])
def test_root_write_not_applied_is_retried(fault):
    store = FakeObjectStore()
    s = make(store)
    r1 = s.publish_root(s.push_atom(atom_payload(1)), 0)
    store.inject("put_if_match", fault)
    p2 = s.push_atom(atom_payload(2))
    assert s.publish_root(p2, r1.epoch).pointer == p2
    assert store.calls["put_if_match"] == 2 and len(root_writes(store, s)) == 2


def _supersede_on_read_back(store, mine, other, pointers):
    """Make the first read of the root after our put_if_match commit ``pointers`` from ``other``."""
    original_get = store.get
    state = {}

    def get(key):
        if key == mine.root_key and store.calls["put_if_match"] == 1 and not state:
            state["done"] = True
            epoch = 2
            for p in pointers:
                other.publish_root(p, epoch)
                epoch += 1
        return original_get(key)

    store.get = get


def test_applied_then_superseded_is_still_recognised_as_ours():
    """Our write lands, the reply is lost, another instance commits on top before we look."""
    store = FakeObjectStore()
    mine, other = make(store), make(store)
    r1 = mine.publish_root(mine.push_atom(atom_payload(1)), 0)
    p2 = mine.push_atom(atom_payload(2))
    p3 = other.push_atom(atom_payload(3))
    other.flush()
    store.inject("put_if_match", "unknown_after")
    _supersede_on_read_back(store, mine, other, [p3])
    r2 = mine.publish_root(p2, r1.epoch)
    assert (r2.pointer, r2.epoch) == (p2, 2)
    current = mine.read_root()
    assert (current.pointer, current.epoch) == (p3, 3)


def test_superseded_beyond_history_is_reported_unknown():
    store = FakeObjectStore()
    mine, other = make(store, root_history=2), make(store, root_history=2)
    r1 = mine.publish_root(mine.push_atom(atom_payload(1)), 0)
    p2 = mine.push_atom(atom_payload(2))
    others = [other.push_atom(atom_payload(10 + i)) for i in range(3)]
    other.flush()
    store.inject("put_if_match", "unknown_after")
    _supersede_on_read_back(store, mine, other, others)
    with pytest.raises(ProtoOutcomeUnknownError, match="unknown"):
        mine.publish_root(p2, r1.epoch)
    assert mine.publish_outcome(p2, 2) is None  # the history no longer reaches epoch 2


def test_not_applied_and_superseded_is_a_conflict():
    store = FakeObjectStore()
    mine, other = make(store), make(store)
    r1 = mine.publish_root(mine.push_atom(atom_payload(1)), 0)
    p2 = mine.push_atom(atom_payload(2))
    p3 = other.push_atom(atom_payload(3))
    other.flush()
    store.inject("put_if_match", "unknown_before")
    _supersede_on_read_back(store, mine, other, [p3])
    with pytest.raises(ProtoConflictError):
        mine.publish_root(p2, r1.epoch)


def _refused_beyond_history(definitive: bool):
    """Our first conditional root write is refused by a real precondition (the other instance
    publishes epochs 2 to 5 just before it), over a store whose ``refusals_are_definitive`` is
    ``definitive``; returns ``(store, mine, theirs, publish)``."""
    store = FakeObjectStore()
    store.refusals_are_definitive = definitive
    mine, other = make(store, root_history=2), make(store, root_history=2)
    r1 = mine.publish_root(mine.push_atom(atom_payload(1)), 0)
    p2 = mine.push_atom(atom_payload(2))
    theirs = [other.push_atom(atom_payload(10 + i)) for i in range(4)]
    other.flush()
    raced = []

    def before_write(op, key):
        if op == "put_if_match" and key == mine.root_key and not raced:
            raced.append(True)
            for epoch, p in enumerate(theirs, start=r1.epoch):
                other.publish_root(p, epoch)

    store.before_write = before_write
    return store, mine, theirs, lambda: mine.publish_root(p2, r1.epoch)


def test_a_refusal_from_a_store_without_definitive_refusals_beyond_the_history_is_unknown():
    """Ruling R147a: a store that does not declare ``refusals_are_definitive`` may have re-sent
    the write below the adapter (a hidden SDK retry), so a refusal proves nothing about our own
    first send; beyond the history the outcome stays unknown, exactly as at 263e0d4."""
    store, mine, theirs, publish = _refused_beyond_history(False)
    with pytest.raises(ProtoOutcomeUnknownError):
        publish()
    assert store.failed_preconditions[mine.root_key] == 1


def test_a_refused_root_write_with_no_earlier_uncertain_attempt_is_a_conflict():
    """A real failed precondition (another instance published between our read and our write),
    on the first attempt, over a store that declares ``refusals_are_definitive`` (ruling R147a):
    nothing of ours was applied, so the outcome is a conflict, which ``Database.run`` retries
    (D43 and its R147a amendment).

    It stays a conflict when, before the read-back, the other instance has published more roots
    than ``root_history`` keeps: the read-back cannot find our epoch, but no attempt of ours is
    in doubt.  Two instances over one store reached exactly this under CPU starvation on a CI
    runner (``root_history`` 64; ``test_plain_int_counter_across_instances[reliable]``)."""
    store, mine, theirs, publish = _refused_beyond_history(True)
    with pytest.raises(ProtoConflictError):
        publish()
    assert store.failed_preconditions[mine.root_key] == 1     # a real refusal, nothing injected
    assert mine.read_root().pointer == theirs[-1]              # ours was never applied


def test_a_refusal_after_a_hidden_retry_that_applied_our_write_is_still_resolved_as_applied():
    """A client passed in with the SDK's own retries left on (against the S3ObjectStore advice) can
    apply our write, then re-send it and get a failed precondition.  The read-back still finds
    our epoch, so ``publish_root`` reports success, never a conflict: a conflict would make
    ``Database.run`` apply the transaction a second time."""
    store = FakeObjectStore()
    store.refusals_are_definitive = False      # it emulates a hidden SDK retry (see the fake)
    s = make(store)
    r1 = s.publish_root(s.push_atom(atom_payload(1)), 0)
    p2 = s.push_atom(atom_payload(2))
    real = store.put_if_match

    def hidden_retry(key, data, etag):
        if key == s.root_key:
            store.put_if_match = real
            real(key, data, etag)                 # the first send lands ...
            return real(key, data, etag)          # ... and the SDK's re-send is refused
        return real(key, data, etag)

    store.put_if_match = hidden_retry
    r2 = s.publish_root(p2, r1.epoch)
    assert r2.epoch == r1.epoch + 1 and s.read_root().pointer == p2
    assert store.failed_preconditions[s.root_key] == 1


def test_a_hidden_retry_that_landed_then_beyond_the_history_is_never_a_conflict():
    """Fix round 1 of Task CI-FLAKES (review C1, the reviewer's probe): a client with hidden
    retries left on applies our write, more than ``root_history`` roots follow, and the SDK's
    re-send is refused.  The read-back cannot find our epoch any more; a conflict here would make
    ``Database.run`` apply the transaction a second time.  Such a store does not declare
    ``refusals_are_definitive`` (R147a), so the outcome must be unknown, never a conflict."""
    store = FakeObjectStore()
    store.refusals_are_definitive = False
    mine, other = make(store, root_history=2), make(store, root_history=2)
    r1 = mine.publish_root(mine.push_atom(atom_payload(1)), 0)
    p2 = mine.push_atom(atom_payload(2))
    theirs = [other.push_atom(atom_payload(10 + i)) for i in range(4)]
    other.flush()
    real = store.put_if_match

    def hidden_retry(key, data, etag):
        if key == mine.root_key:
            store.put_if_match = real
            real(key, data, etag)                       # the first send lands: epoch 2 is ours
            for epoch, p in enumerate(theirs, start=r1.epoch + 1):
                other.publish_root(p, epoch)            # epochs 3 to 6, beyond root_history
            return real(key, data, etag)                # the SDK's re-send is refused
        return real(key, data, etag)

    store.put_if_match = hidden_retry
    with pytest.raises(ProtoOutcomeUnknownError):
        mine.publish_root(p2, r1.epoch)


def test_the_stores_declare_whether_their_refusals_are_definitive(monkeypatch):
    """R147a: the fake store and both adapters with the clients they build declare definitive
    refusals; CloudStorage reads the flag with ``getattr(store, ..., False)``."""
    fake_botocore(monkeypatch)
    fake_gcs(monkeypatch)
    assert FakeObjectStore().refusals_are_definitive is True
    assert S3ObjectStore("b").refusals_are_definitive is True
    assert GCSObjectStore("b").refusals_are_definitive is True
    assert getattr(object(), "refusals_are_definitive", False) is False


class _Retries:
    """A stub boto3 client exposing only ``meta.config.retries``."""

    def __init__(self, retries, with_meta=True):
        if with_meta:
            self.meta = types.SimpleNamespace(config=types.SimpleNamespace(retries=retries))


@pytest.mark.parametrize("client, definitive", [
    (_Retries({"total_max_attempts": 1, "mode": "standard"}), True),
    (_Retries({"total_max_attempts": 2, "mode": "standard"}), False),   # botocore's view of max_attempts=1
    (_Retries({"mode": "legacy"}), False),                              # no retries configured: 5 attempts
    (_Retries(None), False),
    (_Retries(None, with_meta=False), False),
], ids=["one-attempt", "one-retry", "legacy-default", "retries-unset", "no-meta"])
def test_an_s3_client_passed_in_has_definitive_refusals_only_with_one_attempt(monkeypatch, client, definitive):
    """R147a: a client passed in declares definitive refusals only when its configuration shows
    exactly one attempt in total; any other client is accepted and gets the conservative unknown
    outcome beyond the history."""
    fake_botocore(monkeypatch)
    assert S3ObjectStore("b", client=client).refusals_are_definitive is definitive


def test_unknown_outcome_is_resolvable_by_read_root():
    """The read-back itself fails: publish_root reports an unknown outcome, read_root resolves it."""
    store = FakeObjectStore()
    s = make(store, retries=0)
    r1 = s.publish_root(s.push_atom(atom_payload(1)), 0)
    p2 = s.push_atom(atom_payload(2))
    store.inject("put_if_match", "unknown_after")

    def fail_read_back(op, key):
        if op == "put_if_match" and key == s.root_key:
            store.inject("get", "transient", match=lambda k: k == s.root_key)

    store.before_write = fail_read_back
    with pytest.raises(ProtoOutcomeUnknownError, match="unknown"):
        s.publish_root(p2, r1.epoch)
    store.before_write = None
    root = s.read_root()
    assert (root.pointer, root.epoch) == (p2, 2)
    assert s.publish_outcome(p2, 2) is True


def test_publish_outcome_verdicts():
    store = FakeObjectStore()
    a, b = make(store, root_history=2), make(store, root_history=2)
    p1 = a.push_atom(atom_payload(1))
    assert a.publish_outcome(p1, 1) is None  # nothing published yet: an in-flight write could land
    a.publish_root(p1, 0)
    assert a.publish_outcome(p1, 1) is True
    p2 = b.push_atom(atom_payload(2))
    assert b.publish_outcome(p2, 1) is False  # epoch 1 belongs to another root
    assert b.publish_outcome(p2, 2) is None   # epoch 2 not reached yet
    b.publish_root(p2, 1)
    later = [b.push_atom(atom_payload(10 + i)) for i in range(2)]
    for epoch, p in enumerate(later, start=2):
        b.publish_root(p, epoch)
    assert a.publish_outcome(p2, 2) is True    # superseded, still in the history
    assert a.publish_outcome(p1, 1) is None    # beyond root_history=2
    with pytest.raises(ProtoValidationError):
        a.publish_outcome("ptr", 1)


def test_cloud_storage_declares_an_in_process_root_lock():
    assert CloudStorage.root_lock_is_global is False


@pytest.mark.parametrize("history", [2, 64], ids=["beyond-history", "within-history"])
def test_database_run_never_double_applies_after_unknown_outcome(history):
    """Regression (review round 1): unknown outcome + 3 commits from another instance before read-back.

    The commit is applied exactly once: either run returns (resolved from the history) or
    the caller gets ProtoOutcomeUnknownError; Database.run never retries it.
    """
    store = FakeObjectStore()
    a = ObjectSpace(make(store, root_history=history))
    b = ObjectSpace(make(store, root_history=history))
    dba = a.new_database("db")
    dba.run(lambda tr: tr.set_root_object("n", 0))
    dbb = b.open_database("db")

    def inc(tr):
        tr.set_root_object("n", tr.get_root_object("n") + 1)

    original_get = store.get
    state = {"pm": store.calls["put_if_match"]}

    def get(key):
        if key == a.storage.root_key and "done" not in state and store.calls["put_if_match"] > state["pm"]:
            state["done"] = True
            for _ in range(3):
                dbb.run(inc)
        return original_get(key)

    store.get = get
    store.inject("put_if_match", "unknown_after", match=lambda k: k == a.storage.root_key)
    calls = []

    def inc_a(tr):
        calls.append(1)
        inc(tr)

    if history == 2:
        with pytest.raises(ProtoOutcomeUnknownError):
            dba.run(inc_a)
    else:
        dba.run(inc_a)
    assert len(calls) == 1
    store.get = original_get
    reader = ObjectSpace(make(store)).open_database("db")
    with reader.new_transaction() as tr:
        assert tr.get_root_object("n") == 4  # 0 + ours (once) + 3 from the other instance


def test_exhausted_retries_after_a_possible_landing_are_outcome_unknown():
    store = FakeObjectStore()
    s = make(store, retries=1)
    r1 = s.publish_root(s.push_atom(atom_payload(1)), 0)
    store.inject("put_if_match", "unknown_before", times=2)
    with pytest.raises(ProtoOutcomeUnknownError):
        s.publish_root(s.push_atom(atom_payload(2)), r1.epoch)
    store.inject("put_if_match", "transient", times=2)
    with pytest.raises(ProtoUnexpectedError) as info:
        s.publish_root(s.push_atom(atom_payload(3)), r1.epoch)
    assert not isinstance(info.value, ProtoOutcomeUnknownError)  # certainly not applied


def test_lost_races_back_off(monkeypatch):
    store = FakeObjectStore()
    s = make(store)
    s.publish_root(s.push_atom(atom_payload(1)), 0)
    sleeps = []
    monkeypatch.setattr(s, "_sleep", lambda attempt: sleeps.append(attempt))
    store.inject("put_if_match", "precondition", times=3)
    s.publish_root(s.push_atom(atom_payload(2)), None)
    assert len(sleeps) == 3


def test_publish_does_not_download_the_segment_it_just_uploaded():
    store = FakeObjectStore()
    s = make(store, cache_bytes=0)
    p = s.push_atom(atom_payload(1))
    original_get = store.get
    fetched = []

    def get(key):
        fetched.append(key)
        return original_get(key)

    store.get = get
    s.publish_root(p, 0)
    s.flush()
    p2 = s.push_atom(atom_payload(2))
    s.flush()  # p2's segment uploaded before the publish
    s.publish_root(p2, 1)
    assert all(key == s.root_key for key in fetched), fetched
    with pytest.raises(ProtoCorruptionError):
        s.publish_root(AtomPointer(p2.segment, 10 ** 6), 2)  # offset beyond the uploaded segment


def test_transaction_commit_survives_applied_then_timed_out_publish():
    """End to end: transactions.py resolves the unknown outcome through read_root (no double commit)."""
    store = FakeObjectStore()
    storage = make(store, retries=0)
    space = ObjectSpace(storage)
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("n", 0))
    writes_before = len(root_writes(store, storage))
    store.inject("put_if_match", "unknown_after", match=lambda k: k == storage.root_key)

    def fail_read_back(op, key):
        if op == "put_if_match" and key == storage.root_key:
            store.inject("get", "transient", match=lambda k: k == storage.root_key)
            store.before_write = None

    store.before_write = fail_read_back
    calls = []

    def increment(tr):
        calls.append(1)
        tr.set_root_object("n", tr.get_root_object("n") + 1)

    db.run(increment)
    assert len(calls) == 1  # committed once, not retried
    assert len(root_writes(store, storage)) == writes_before + 1
    reader = ObjectSpace(make(store)).open_database("db")
    with reader.new_transaction() as tr:
        assert tr.get_root_object("n") == 1
    space.close()


def test_segment_applied_then_timed_out_is_idempotent():
    store = FakeObjectStore()
    s = make(store)
    p = s.push_atom(atom_payload(1))
    store.inject("put_if_absent", "unknown_after", match=lambda k: k != s.root_key)
    s.flush()
    assert store.calls["put_if_absent"] == 2  # the retry met the object and verified it
    assert make(store).get_atom(p) == atom_payload(1)


def test_segment_key_taken_by_foreign_content_is_an_error():
    store = FakeObjectStore()
    s = make(store)
    p = s.push_atom(atom_payload(1))
    store.overwrite(s.segment_key(p.segment), b"someone else's bytes")
    with pytest.raises(ProtoUnexpectedError, match="different content"):
        s.flush()
    with pytest.raises(ProtoUnexpectedError):
        s.publish_root(p, 0)
    assert s.read_root() is None


# --- reads, corruption, opening -------------------------------------------------------------

def test_reads_of_remote_segments_are_cached():
    store = FakeObjectStore()
    writer = make(store)
    ptrs = [writer.push_atom(atom_payload(i)) for i in range(10)]
    writer.flush()
    reader = make(store)
    before = store.calls["get"]
    for p in ptrs:
        reader.get_atom(p)
    assert store.calls["get"] == before + 1
    uncached = make(store, cache_bytes=0)
    before = store.calls["get"]
    for p in ptrs:
        uncached.get_atom(p)
    assert store.calls["get"] == before + len(ptrs)


def test_transient_read_failures_are_retried():
    store = FakeObjectStore()
    w = make(store)
    p = w.push_atom(atom_payload(1))
    w.flush()
    store.inject("get", "transient", times=2)
    assert make(store, retries=2).get_atom(p) == atom_payload(1)
    store.inject("get", "transient", times=3)
    with pytest.raises(ProtoUnexpectedError):
        make(store, retries=2, verify_root=False).get_atom(p)


def test_missing_segment_is_corruption():
    store = FakeObjectStore()
    w = make(store)
    p = w.push_atom(atom_payload(1))
    w.flush()
    store.delete(w.segment_key(p.segment))
    with pytest.raises(ProtoCorruptionError):
        make(store).get_atom(p)


def test_damaged_segment_is_corruption():
    store = FakeObjectStore()
    w = make(store)
    p = w.push_atom(atom_payload(1))
    w.flush()
    key = w.segment_key(p.segment)
    data = bytearray(store.raw(key))
    data[-1] ^= 0xFF
    store.overwrite(key, bytes(data))
    with pytest.raises(ProtoCorruptionError):
        make(store).get_atom(p)
    store.overwrite(key, b"not a segment")
    with pytest.raises(ProtoCorruptionError):
        make(store).get_atom(p)


def test_segment_with_foreign_header_is_corruption():
    store = FakeObjectStore()
    w = make(store)
    a = w.push_atom(atom_payload(1))
    w.flush()
    b = w.push_atom(atom_payload(2))
    w.flush()
    store.overwrite(w.segment_key(b.segment), store.raw(w.segment_key(a.segment)))
    with pytest.raises(ProtoCorruptionError, match="foreign"):
        make(store).get_atom(b)


def test_open_validates_the_root():
    store = FakeObjectStore()
    w = make(store)
    p = w.push_atom(atom_payload(1))
    w.publish_root(p, 0)
    assert make(store).read_root().pointer == p
    store.delete(w.segment_key(p.segment))
    with pytest.raises(ProtoCorruptionError):
        make(store)
    store.overwrite(w.root_key, b"\x00\x01garbage")
    with pytest.raises(ProtoCorruptionError):
        make(store, verify_root=False).read_root()


def test_root_object_is_a_format_v2_root_record():
    store = FakeObjectStore()
    s = make(store)
    record = s.publish_root(s.push_atom(atom_payload(1)), 0)
    assert decode_root(store.raw(s.root_key)) == record


def test_prefix_isolates_spaces_in_one_bucket():
    store = FakeObjectStore()
    a, b = make(store, prefix="a/"), make(store, prefix="b/")
    a.publish_root(a.push_atom(atom_payload(1)), 0)
    assert b.read_root() is None
    assert all(k.startswith("a/") for k in store.keys())


def test_invalid_configuration():
    with pytest.raises(ProtoValidationError):
        CloudStorage(object())
    for kw in (dict(segment_max_bytes=10), dict(retries=-1), dict(cache_bytes=-1),
               dict(lock_timeout=-1), dict(backoff=(1, 0)), dict(root_history=0),
               dict(write_buffer_bytes=0), dict(prefix=3)):
        with pytest.raises(ProtoValidationError):
            make(**kw)


def test_publish_validates_arguments():
    s = make()
    p = s.push_atom(atom_payload(1))
    with pytest.raises(ProtoValidationError):
        s.publish_root("ptr", 0)
    with pytest.raises(ProtoValidationError):
        s.publish_root(p, 1.5)


def test_write_buffer_spills_sealed_segments():
    store = FakeObjectStore()
    s = make(store, segment_max_bytes=600, write_buffer_bytes=1500)
    ptrs = [s.push_atom(atom_payload(i)) for i in range(60)]
    assert store.keys()  # sealed segments went out before any flush
    for i, p in enumerate(ptrs):
        assert s.get_atom(p) == atom_payload(i)


# --- SDK adapters: missing SDKs ---------------------------------------------------------------

def test_module_import_does_not_import_sdks():
    code = ("import sys; import proto_db.storage.cloud; "
            "print([m for m in ('boto3', 'botocore', 'google.cloud.storage') if m in sys.modules])")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "[]"


def test_missing_boto3_raises_not_supported(monkeypatch):
    for name in ("boto3", "botocore", "botocore.exceptions", "botocore.config"):
        monkeypatch.setitem(sys.modules, name, None)
    with pytest.raises(ProtoNotSupportedError, match="pip install boto3") as info:
        S3ObjectStore("bucket")
    assert isinstance(info.value.__cause__, ImportError)


def test_missing_gcs_raises_not_supported(monkeypatch):
    for name in ("google", "google.cloud", "google.cloud.storage", "google.api_core",
                 "google.api_core.exceptions"):
        monkeypatch.setitem(sys.modules, name, None)
    with pytest.raises(ProtoNotSupportedError, match="pip install google-cloud-storage") as info:
        GCSObjectStore("bucket")
    assert isinstance(info.value.__cause__, ImportError)


# --- SDK adapters: S3 error mapping against a fake boto3/botocore -------------------------------

def _install(monkeypatch, name, **attrs):
    module = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(module, k, v)
    monkeypatch.setitem(sys.modules, name, module)
    parent, _, child = name.rpartition(".")
    if parent and parent in sys.modules and sys.modules[parent] is not None:
        setattr(sys.modules[parent], child, module)
    return module


def fake_botocore(monkeypatch):
    class BotoCoreError(Exception):
        pass

    class ConnectionError(BotoCoreError):  # noqa: A001 - mirrors botocore's name
        pass

    class EndpointConnectionError(ConnectionError):
        pass

    class HTTPClientError(BotoCoreError):
        pass

    class ReadTimeoutError(HTTPClientError):
        pass

    class NoCredentialsError(BotoCoreError):
        pass

    class PartialCredentialsError(BotoCoreError):
        pass

    class CredentialRetrievalError(BotoCoreError):
        pass

    class IncompleteReadError(BotoCoreError):
        pass

    class ResponseStreamingError(HTTPClientError):
        pass

    class ClientError(Exception):
        def __init__(self, response, operation):
            super().__init__(f"{operation}: {response}")
            self.response = response

    class Config:
        def __init__(self, **kw):
            self.kw = kw

    created = []

    def client(service, **kw):
        created.append((service, kw))
        return FakeS3Client(ClientError)

    _install(monkeypatch, "botocore")
    exc = _install(monkeypatch, "botocore.exceptions", BotoCoreError=BotoCoreError,
                   ConnectionError=ConnectionError, EndpointConnectionError=EndpointConnectionError,
                   HTTPClientError=HTTPClientError, ReadTimeoutError=ReadTimeoutError,
                   NoCredentialsError=NoCredentialsError, ClientError=ClientError,
                   PartialCredentialsError=PartialCredentialsError,
                   CredentialRetrievalError=CredentialRetrievalError,
                   IncompleteReadError=IncompleteReadError, ResponseStreamingError=ResponseStreamingError)
    _install(monkeypatch, "botocore.config", Config=Config)
    _install(monkeypatch, "boto3", client=client)
    return exc, created


def client_error(exc, code, status):
    return exc.ClientError({"Error": {"Code": code, "Message": code},
                            "ResponseMetadata": {"HTTPStatusCode": status}}, "PutObject")


class FakeS3Client:
    """Just enough of a boto3 S3 client over a dict, with an injectable next error."""

    def __init__(self, client_error_cls):
        self.objects = {}
        self.calls = []
        self.fail = None
        self.body_error = None
        self._n = 0
        self._ClientError = client_error_cls

    def _maybe_fail(self):
        if self.fail is not None:
            error, self.fail = self.fail, None
            raise error

    def _error(self, code, status):
        return self._ClientError({"Error": {"Code": code},
                                  "ResponseMetadata": {"HTTPStatusCode": status}}, "op")

    def put_object(self, **kw):
        self.calls.append(("put_object", kw))
        self._maybe_fail()
        key = kw["Key"]
        current = self.objects.get(key)
        if kw.get("IfNoneMatch") == "*" and current is not None:
            raise self._error("PreconditionFailed", 412)
        if "IfMatch" in kw:
            if current is None:
                raise self._error("NoSuchKey", 404)
            if current[1] != kw["IfMatch"]:
                raise self._error("PreconditionFailed", 412)
        self._n += 1
        etag = f'"e{self._n}"'
        self.objects[key] = (kw["Body"], etag)
        return {"ETag": etag}

    def get_object(self, **kw):
        self.calls.append(("get_object", kw))
        self._maybe_fail()
        current = self.objects.get(kw["Key"])
        if current is None:
            raise self._error("NoSuchKey", 404)
        def read():
            if self.body_error is not None:
                error, self.body_error = self.body_error, None
                raise error
            return current[0]

        body = types.SimpleNamespace(read=read)
        return {"Body": body, "ETag": current[1]}

    def delete_object(self, **kw):
        self.calls.append(("delete_object", kw))
        self._maybe_fail()
        self.objects.pop(kw["Key"], None)
        return {}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        client = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                client._maybe_fail()
                keys = sorted(k for k in client.objects if k.startswith(Prefix))
                yield {"Contents": [{"Key": k} for k in keys[:1]]}
                yield {"Contents": [{"Key": k} for k in keys[1:]]}
                yield {}

        return Paginator()


def test_s3_adapter_round_trip_and_conditions(monkeypatch):
    exc, created = fake_botocore(monkeypatch)
    s3 = S3ObjectStore("bkt", region_name="eu")
    (service, kw), = created
    assert service == "s3" and kw["region_name"] == "eu"
    # SDK retries off: CloudStorage retries.  botocore reads ``max_attempts`` as RETRIES (it sends
    # max_attempts + 1 requests, measured: 2 PutObject requests for max_attempts=1), so the one
    # attempt in total is ``total_max_attempts`` (fix round 1 of Task CI-FLAKES, R147a).
    assert kw["config"].kw["retries"] == {"total_max_attempts": 1, "mode": "standard"}
    e1 = s3.put_if_absent("k", b"one")
    assert s3.get("k") == (b"one", e1)
    with pytest.raises(PreconditionFailedError):
        s3.put_if_absent("k", b"two")
    e2 = s3.put_if_match("k", b"two", e1)
    with pytest.raises(PreconditionFailedError):
        s3.put_if_match("k", b"three", e1)
    with pytest.raises(PreconditionFailedError):
        s3.put_if_match("absent", b"x", e2)  # S3 answers 404 to If-Match on a missing key
    with pytest.raises(ObjectNotFoundError):
        s3.get("absent")
    s3.put_if_absent("k2", b"x")
    assert s3.list("k") == ["k", "k2"]
    s3.delete("k2")
    s3.delete("k2")
    assert s3.list("") == ["k"]
    client = s3.client
    assert ("put_object", {"Bucket": "bkt", "Key": "k", "Body": b"one", "IfNoneMatch": "*"}) in client.calls


@pytest.mark.parametrize("make_error, expected", [
    (lambda exc: client_error(exc, "PreconditionFailed", 412), PreconditionFailedError),
    (lambda exc: client_error(exc, "ConditionalRequestConflict", 409), PreconditionFailedError),
    (lambda exc: client_error(exc, "SlowDown", 503), TransientObjectStoreError),
    (lambda exc: client_error(exc, "InternalError", 500), OutcomeUnknownError),
    (lambda exc: client_error(exc, "AccessDenied", 403), ProtoNotAuthorizedError),
    (lambda exc: client_error(exc, "InvalidRequest", 400), ObjectStoreError),
    (lambda exc: exc.EndpointConnectionError("no route"), TransientObjectStoreError),
    (lambda exc: exc.ReadTimeoutError("read timeout"), OutcomeUnknownError),
    (lambda exc: exc.NoCredentialsError("no credentials"), ProtoNotAuthorizedError),
    (lambda exc: exc.PartialCredentialsError("partial"), ProtoNotAuthorizedError),
    (lambda exc: exc.CredentialRetrievalError("retrieval"), ProtoNotAuthorizedError),
])
def test_s3_adapter_error_mapping(monkeypatch, make_error, expected):
    exc, _ = fake_botocore(monkeypatch)
    s3 = S3ObjectStore("bkt")
    error = make_error(exc)
    s3.client.fail = error
    with pytest.raises(expected) as info:
        s3.put_if_absent("k", b"x")
    assert info.value.__cause__ is error
    assert isinstance(info.value, ProtoDBError)


@pytest.mark.parametrize("name", ["IncompleteReadError", "ResponseStreamingError"])
def test_s3_get_body_read_failure_is_transient(monkeypatch, name):
    exc, _ = fake_botocore(monkeypatch)
    s3 = S3ObjectStore("bkt")
    s3.put_if_absent("k", b"x")
    error = getattr(exc, name)("body cut")
    s3.client.body_error = error
    with pytest.raises(TransientObjectStoreError) as info:
        s3.get("k")
    assert info.value.__cause__ is error
    assert s3.get("k")[0] == b"x"


def test_s3_streaming_error_on_a_write_stays_unknown(monkeypatch):
    exc, _ = fake_botocore(monkeypatch)
    s3 = S3ObjectStore("bkt")
    s3.client.fail = exc.ResponseStreamingError("cut")
    with pytest.raises(OutcomeUnknownError):
        s3.put_if_absent("k", b"x")


def test_s3_adapter_accepts_a_client(monkeypatch):
    exc, created = fake_botocore(monkeypatch)
    client = FakeS3Client(exc.ClientError)
    s3 = S3ObjectStore("bkt", client=client)
    assert s3.client is client and created == []


# --- SDK adapters: GCS error mapping against a fake google-cloud-storage ------------------------

def fake_gcs(monkeypatch):
    class GoogleAPICallError(Exception):
        code = None

    class PreconditionFailed(GoogleAPICallError):
        code = 412

    class NotFound(GoogleAPICallError):
        code = 404

    class Conflict(GoogleAPICallError):
        code = 409

    class Forbidden(GoogleAPICallError):
        code = 403

    class Unauthorized(GoogleAPICallError):
        code = 401

    class TooManyRequests(GoogleAPICallError):
        code = 429

    class ServiceUnavailable(GoogleAPICallError):
        code = 503

    class InternalServerError(GoogleAPICallError):
        code = 500

    class RetryError(Exception):
        pass

    class TransportError(Exception):
        pass

    class RequestException(OSError):
        pass

    class ReqConnectionError(RequestException):
        pass

    class Timeout(RequestException):
        pass

    class ConnectTimeout(ReqConnectionError, Timeout):
        pass

    api = dict(GoogleAPICallError=GoogleAPICallError, PreconditionFailed=PreconditionFailed,
               NotFound=NotFound, Conflict=Conflict, Forbidden=Forbidden, Unauthorized=Unauthorized,
               TooManyRequests=TooManyRequests, ServiceUnavailable=ServiceUnavailable,
               InternalServerError=InternalServerError, RetryError=RetryError)
    created = []

    def Client(**kw):
        created.append(kw)
        return FakeGCSClient(api)

    _install(monkeypatch, "google")
    _install(monkeypatch, "google.cloud")
    _install(monkeypatch, "google.cloud.storage", Client=Client)
    _install(monkeypatch, "google.api_core")
    exc = _install(monkeypatch, "google.api_core.exceptions", **api)
    _install(monkeypatch, "google.auth")
    _install(monkeypatch, "google.auth.exceptions", TransportError=TransportError)
    _install(monkeypatch, "requests")
    _install(monkeypatch, "requests.exceptions", RequestException=RequestException,
             ConnectionError=ReqConnectionError, Timeout=Timeout, ConnectTimeout=ConnectTimeout)
    errors = types.SimpleNamespace(TransportError=TransportError, ConnectTimeout=ConnectTimeout,
                                   Timeout=Timeout, ConnectionError=ReqConnectionError, **api)
    return errors, created


class FakeGCSClient:
    """Just enough of google.cloud.storage: Client.bucket, Bucket.blob/get_blob, Blob I/O."""

    def __init__(self, api):
        self.api = api
        self.objects = {}
        self.generation = 0
        self.fail = None
        self.uploads = []
        self.change_before_download = False

    def _maybe_fail(self):
        if self.fail is not None:
            error, self.fail = self.fail, None
            raise error

    def bucket(self, name):
        return FakeBucket(self, name)

    def list_blobs(self, bucket, prefix=None):
        self._maybe_fail()
        return [types.SimpleNamespace(name=k) for k in sorted(self.objects) if k.startswith(prefix)]


class FakeBucket:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def blob(self, key):
        return FakeBlob(self.client, key)

    def get_blob(self, key):
        self.client._maybe_fail()
        current = self.client.objects.get(key)
        if current is None:
            return None
        blob = FakeBlob(self.client, key)
        blob.generation = current[1]
        return blob


class FakeBlob:
    def __init__(self, client, key):
        self.client = client
        self.name = key
        self.generation = None

    def upload_from_string(self, data, if_generation_match=None, retry="default"):
        c = self.client
        c.uploads.append((self.name, if_generation_match, retry))
        c._maybe_fail()
        current = c.objects.get(self.name)
        if if_generation_match == 0 and current is not None:
            raise c.api["PreconditionFailed"]("exists")
        if if_generation_match not in (None, 0):
            if current is None:
                raise c.api["NotFound"]("absent")
            if current[1] != if_generation_match:
                raise c.api["PreconditionFailed"]("generation")
        c.generation += 1
        c.objects[self.name] = (bytes(data), c.generation)
        self.generation = c.generation

    def download_as_bytes(self, if_generation_match=None):
        c = self.client
        if c.change_before_download:
            c.change_before_download = False
            data, _ = c.objects[self.name]
            c.generation += 1
            c.objects[self.name] = (data + b"!", c.generation)
        current = c.objects.get(self.name)
        if current is None:
            raise c.api["NotFound"]("absent")
        if if_generation_match is not None and current[1] != if_generation_match:
            raise c.api["PreconditionFailed"]("changed")
        return current[0]

    def delete(self):
        c = self.client
        c._maybe_fail()
        if c.objects.pop(self.name, None) is None:
            raise c.api["NotFound"]("absent")


def test_gcs_adapter_round_trip_and_conditions(monkeypatch):
    errors, created = fake_gcs(monkeypatch)
    gcs = GCSObjectStore("bkt", project="p")
    assert created == [{"project": "p"}]
    g1 = gcs.put_if_absent("k", b"one")
    assert gcs.get("k") == (b"one", g1)
    with pytest.raises(PreconditionFailedError):
        gcs.put_if_absent("k", b"two")
    g2 = gcs.put_if_match("k", b"two", g1)
    with pytest.raises(PreconditionFailedError):
        gcs.put_if_match("k", b"three", g1)
    with pytest.raises(PreconditionFailedError):
        gcs.put_if_match("absent", b"x", g2)
    with pytest.raises(ObjectNotFoundError):
        gcs.get("absent")
    gcs.put_if_absent("k2", b"x")
    assert gcs.list("k") == ["k", "k2"]
    gcs.delete("k2")
    gcs.delete("k2")
    assert gcs.list("") == ["k"]
    # Conditional uploads disable the SDK's own retry: CloudStorage decides about retries.
    assert all(retry is None for _, _, retry in gcs.client.uploads)
    assert gcs.client.uploads[0][1] == 0


def test_gcs_get_rereads_when_the_object_changes_mid_read(monkeypatch):
    fake_gcs(monkeypatch)
    gcs = GCSObjectStore("bkt")
    gcs.put_if_absent("k", b"one")
    gcs.client.change_before_download = True
    data, generation = gcs.get("k")
    assert data == b"one!" and generation == str(gcs.client.objects["k"][1])


@pytest.mark.parametrize("name, expected", [
    ("PreconditionFailed", PreconditionFailedError),
    ("Conflict", PreconditionFailedError),
    ("TooManyRequests", TransientObjectStoreError),
    ("ServiceUnavailable", TransientObjectStoreError),
    ("InternalServerError", OutcomeUnknownError),
    ("RetryError", OutcomeUnknownError),
    ("Forbidden", ProtoNotAuthorizedError),
    ("Unauthorized", ProtoNotAuthorizedError),
    ("GoogleAPICallError", ObjectStoreError),
    ("ConnectTimeout", TransientObjectStoreError),
    ("Timeout", OutcomeUnknownError),
    ("ConnectionError", OutcomeUnknownError),
    ("TransportError", OutcomeUnknownError),
])
def test_gcs_adapter_error_mapping(monkeypatch, name, expected):
    errors, _ = fake_gcs(monkeypatch)
    gcs = GCSObjectStore("bkt")
    error = getattr(errors, name)("boom")
    gcs.client.fail = error
    with pytest.raises(expected) as info:
        gcs.put_if_absent("k", b"x")
    assert info.value.__cause__ is error


def test_adapters_satisfy_the_protocol(monkeypatch):
    fake_botocore(monkeypatch)
    fake_gcs(monkeypatch)
    for store in (S3ObjectStore("b"), GCSObjectStore("b"), FakeObjectStore()):
        CloudStorage(store, **FAST).close()
