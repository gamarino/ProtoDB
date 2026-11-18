"""``Transaction.snapshot_mutable`` and the semantic field-version slot (Task 6, fix round 2).

Catches: a snapshot read that returns the transaction's own writes, that enters the read set (a concurrent
change of the cell would then conflict), or that answers on a finished transaction; a field-version slot
whose compare-and-set replaces a value it was not given.
"""
from __future__ import annotations

import pytest

from proto_db.errors import ProtoUserError
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace

pytestmark = [pytest.mark.timeout(60)]


@pytest.fixture
def db():
    space = ObjectSpace(MemoryStorage())
    try:
        database = space.new_database("db")
        database.run(lambda tr: tr.new_mutable("x", 1))
        yield database
    finally:
        space.close()


def test_snapshot_mutable_never_shows_the_transactions_own_writes(db):
    tr = db.new_transaction()
    tr.set_mutable("x", 2)
    tr.new_mutable("y", 3)
    assert tr.snapshot_mutable("x") == 1
    assert tr.snapshot_mutable("y") is None                 # created by this transaction: not in the snapshot
    assert tr.get_mutable("x") == 2
    tr.abort()


def test_snapshot_mutable_is_not_recorded_in_the_read_set(db):
    db.run(lambda tr: tr.new_mutable("other", 0))
    tr = db.new_transaction()
    assert tr.snapshot_mutable("x") == 1
    db.run(lambda t: t.set_mutable("x", 5))                 # a concurrent change of the cell read
    tr.set_mutable("other", 1)
    tr.commit()                                             # no conflict: the read was never recorded
    reader = db.new_transaction()
    assert reader.get_mutable("x") == 5 and reader.get_mutable("other") == 1
    reader.abort()


def test_snapshot_mutable_on_a_finished_transaction_raises(db):
    tr = db.new_transaction()
    tr.abort()
    with pytest.raises(ProtoUserError):
        tr.snapshot_mutable("x")
    with pytest.raises(ProtoUserError):
        tr.semantic_field_version("x")


def test_semantic_field_version_compare_and_set(db):
    tr = db.new_transaction()
    first, second = object(), object()
    assert tr.semantic_field_version("x") is None
    assert tr.set_semantic_field_version("x", None, first) is first
    assert tr.set_semantic_field_version("x", None, second) is first     # not the expected one: kept
    assert tr.set_semantic_field_version("x", first, second) is second
    assert tr.semantic_field_version("x") is second
    tr.abort()
