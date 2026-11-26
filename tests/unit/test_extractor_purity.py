"""Extractor purity (spec R27, N4): an extractor may not read through a transaction.

Catches: an extractor that reads a root, resolves a mutable handle or loads atoms silently
working at index maintenance time (and so depending on state the commit cannot validate), the
purity context leaking after an extractor raised, and a pure extractor being disturbed.
"""
import pytest

from proto_db.errors import ProtoUserError
from proto_db.indexing import EqualityIndex, KeySpec
from proto_db.indexing.keyspec import register_extractor, unregister_extractor
from proto_db.indexing.purity import check_not_extracting, extracting
from proto_db.query.terms import Term
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from proto_db.values import MutableObject


@pytest.fixture
def db():
    space = ObjectSpace(MemoryStorage())
    yield space.new_database("db")
    space.close()


@pytest.fixture
def extractors():
    names = []

    def reg(name, fn):
        register_extractor(name, fn)
        names.append(name)

    yield reg
    for n in names:
        unregister_extractor(n)


def test_extractor_reading_transaction_fails_at_mutation(db, extractors):
    tx_box = {}
    extractors("p8_reads_root", lambda e: tx_box["tx"].get_root_object("other"))
    with db.new_transaction() as tx:
        tx_box["tx"] = tx
        items = tx.new_list().add_index(EqualityIndex("by", KeySpec.extractor("p8_reads_root")))
        with pytest.raises(ProtoUserError, match="get_root_object"):
            items.append_last({"k": 1})
        check_not_extracting("after")  # the context was left


def test_extractor_resolving_mutable_fails(db, extractors):
    db.run(lambda tr: tr.new_mutable("c", 1))
    box = {}
    extractors("p8_mut", lambda e: MutableObject("c").get(box["tx"]))
    with db.new_transaction() as tx:
        box["tx"] = tx
        with pytest.raises(ProtoUserError, match="MutableObject.get"):
            KeySpec.extractor("p8_mut").extract({"k": 1})
    for name in ("root_names", "mutable_names"):
        extractors("p8_" + name, lambda e, n=name: getattr(box["tx"], n)())
        with db.new_transaction() as tx:
            box["tx"] = tx
            with pytest.raises(ProtoUserError, match=name):
                KeySpec.extractor("p8_" + name).extract(1)


def test_extractor_reading_transaction_fails_in_query(db, extractors):
    box = {}
    extractors("p8_q", lambda e: box["tx"].get_mutable("c"))
    with db.new_transaction() as tx:
        box["tx"] = tx
        with pytest.raises(ProtoUserError, match="get_mutable"):
            Term("@p8_q", "==", 1).matches({"k": 1})


def test_pure_extractor_unaffected(db, extractors):
    extractors("p8_pure", lambda e: e["k"] * 2)
    assert Term("@p8_pure", "==", 4).matches({"k": 2})
    with db.new_transaction() as tx:
        items = tx.new_list().add_index(EqualityIndex("by", KeySpec.extractor("p8_pure"))).append_last({"k": 1})
        tx.set_root_object("items", items)
        assert tx.get_root_object("items") is not None  # reads work outside extractors
        assert tx.get_mutable("nothing") is None


def test_extracting_nests_and_unwinds():
    with extracting():
        with extracting():
            pass
        with pytest.raises(ProtoUserError):
            check_not_extracting("x")
    check_not_extracting("x")


def test_extractor_calling_load_or_snapshot_mutable_fails(db, extractors):
    db.run(lambda tr: tr.new_mutable("c", 1))
    box = {}
    extractors("p8_load", lambda e: box["tx"].load(e.atom_pointer, type(e).__type_name__))
    extractors("p8_snap", lambda e: box["tx"].snapshot_mutable("c"))
    from proto_db.core.dbobject import DBObject
    db.run(lambda tr: tr.set_root_object("o", DBObject(v=1)))
    with db.new_transaction() as tx:
        box["tx"] = tx
        obj = tx.get_root_object("o")
        with pytest.raises(ProtoUserError, match="load"):
            KeySpec.extractor("p8_load").extract(obj)
        with pytest.raises(ProtoUserError, match="snapshot_mutable"):
            KeySpec.extractor("p8_snap").extract(obj)


def test_pure_extractor_walks_cold_nested_stub(extractors):
    """A pure extractor may resolve the atoms its own value references, even cold stubs of a
    fresh space (no atom cache entry): the engine's decoding does not hit the guarded load."""
    from proto_db.core.dbobject import DBObject
    storage = MemoryStorage()
    space = ObjectSpace(storage)
    space.new_database("db").run(
        lambda tr: tr.set_root_object("o", DBObject(child=DBObject(v=7), n=1)))
    extractors("p8_child", lambda e: e.child.v)
    for kw in ({}, {"atom_cache_entries": 0}):
        space2 = ObjectSpace(storage, **kw)     # not closed: it would close the shared storage
        with space2.open_database("db").new_transaction() as tx:
            obj = tx.get_root_object("o")
            assert KeySpec.extractor("p8_child").extract(obj) == 7
    space.close()
