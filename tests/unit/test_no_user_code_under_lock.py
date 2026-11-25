"""No user code under the space root (spec P-C, R24, R49; section 12 item 3).

A key extractor (:class:`tests.support.recording_index.RootRecordingExtractor`) records, at every
call, whether its thread holds the space root (``Transaction._holding_root_lock()``).  Each test
runs an explicit interleaving -- open A, open B, B writes another unit of the same indexed root
and commits, A commits -- so A's intentions are reapplied over a changed current root: the
extractor must have run (the preparation computed A's keys) and never with the root held.  The
reapplied index must hold exactly what a fresh build over the final elements holds.

Catches: index maintenance running extractors under the root lock during a reapplication (the
retired rebase behaviour), keys computed against the wrong elements (the index then disagrees
with its elements), and an element the preparation did not cover being filed silently.
"""
import pytest

from proto_db.errors import ProtoConflictError
from proto_db.indexing.equality import EqualityIndex
from proto_db.preconditions import Unit, UnitKind
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace, Transaction
from tests.support.recording_index import RootRecordingExtractor


@pytest.fixture
def db():
    space = ObjectSpace(MemoryStorage())
    yield space.new_database("db")
    space.close()


@pytest.fixture
def extractor():
    with RootRecordingExtractor("p_c_tens", key=lambda element: element // 10) as rec:
        yield rec


def _assert_index_rebuilt_equal(coll, name="e"):
    """The maintained index holds exactly what a build over the elements holds."""
    elements = list(coll.elements())
    coll.get_index(name).validate(elements)


# Each case: how to seed the root, A's write, B's write (another unit), and the expected content.
CASES = {
    "Dictionary": (lambda tr: tr.new_dictionary().set_at("a", 11).set_at("b", 21),
                   lambda c: c.set_at("a", 12).set_at("n", 35),
                   lambda c: c.set_at("b", 22),
                   lambda c: dict(c.items()) == {"a": 12, "b": 22, "n": 35}),
    "HashDictionary": (lambda tr: tr.new_hash_dictionary().set_at("a", 11).set_at("b", 21),
                       lambda c: c.remove_at("a").set_at("n", 35),
                       lambda c: c.set_at("b", 22),
                       lambda c: dict(c.items()) == {"b": 22, "n": 35}),
    "Set": (lambda tr: tr.new_set().add(11).add(21),
            lambda c: c.add(35).remove(11),
            lambda c: c.add(47),
            lambda c: sorted(c.elements()) == [21, 35, 47]),
    "CountedSet": (lambda tr: tr.new_counted_set().add(11).add(11).add(21),
                   lambda c: c.remove(11).add(35),
                   lambda c: c.add(21),
                   lambda c: sorted(c.items()) == [(11, 1), (21, 2), (35, 1)]),
    "RepeatedKeysDictionary": (lambda tr: tr.new_repeated_keys_dictionary().set_at("a", 11).set_at("b", 21),
                               lambda c: c.remove_key("a").set_at("n", 35),
                               lambda c: c.set_at("b", 22),
                               lambda c: sorted(c.elements()) == [21, 22, 35]),
    "List": (lambda tr: tr.new_list().extend([11, 21]),
             lambda c: c.set_at(0, 12).append_last(35),
             lambda c: c.append_last(47),
             lambda c: list(c.elements()) == [12, 21, 47, 35]),
}


@pytest.mark.parametrize("kind", sorted(CASES))
def test_index_keys_precomputed_no_extractor_under_lock(db, extractor, kind):
    seed, write_a, write_b, expected = CASES[kind]
    db.run(lambda tr: tr.set_root_object("r", seed(tr).add_index(EqualityIndex("e", extractor.key_spec))))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("r", write_a(a.get_root_object("r")))
    b.set_root_object("r", write_b(b.get_root_object("r")))
    b.commit()
    extractor.reset()
    a.commit()                                       # reapplied over B's root (R34)
    assert extractor.calls, "the preparation computed A's keys with the extractor"
    assert extractor.held_calls() == 0, "an extractor ran with the space root held (P-C)"
    with db.new_transaction() as tr:
        coll = tr.get_root_object("r")
        assert expected(coll)
        extractor.reset()
        _assert_index_rebuilt_equal(coll)
    assert not Transaction._holding_root_lock()


def test_reapplication_without_a_concurrent_change_needs_no_extractor_under_lock(db, extractor):
    """The snapshot is current: the version is published as is (no reapplication at all)."""
    db.run(lambda tr: tr.set_root_object("r", tr.new_dictionary().add_index(EqualityIndex("e", extractor.key_spec))))
    extractor.reset()
    with db.new_transaction() as tr:
        tr.set_root_object("r", tr.get_root_object("r").set_at("a", 11))
    assert extractor.calls and extractor.held_calls() == 0


def test_element_without_precomputed_key_conflicts(db, extractor):
    """A concurrent commit replaced a Set member by an equal member of another exact value (10 by
    10.0): A's removal would take the stored 10.0 out of the index, whose key A never computed.
    On an indexed set the ELEMENT token holds the exact stored member, so the commit conflicts
    on the declared unit (R36) before any reapplication, and the retry succeeds."""
    db.run(lambda tr: tr.set_root_object("s", tr.new_set().add(10).add(20).add_index(
        EqualityIndex("e", extractor.key_spec))))
    a, b = db.new_transaction(), db.new_transaction()
    a.set_root_object("s", a.get_root_object("s").remove(10))
    b.set_root_object("s", b.get_root_object("s").remove(10).add(10.0))
    b.commit()
    extractor.reset()
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == Unit(UnitKind.ELEMENT, "s", key=10)
    assert extractor.held_calls() == 0
    db.run(lambda tr: tr.set_root_object("s", tr.get_root_object("s").remove(10)))
    with db.new_transaction() as tr:
        s = tr.get_root_object("s")
        assert sorted(s.elements()) == [20]
        _assert_index_rebuilt_equal(s)
