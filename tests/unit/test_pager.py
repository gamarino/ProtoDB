"""Snapshots by reference and the stable pager (Task 8.4b, D56; rulings R77, R77a).

A pager serves page k by rank when its plan can seek, else from one materialized result;
every page comes from one snapshot, so pages never repeat or skip rows whatever commits
meanwhile; a token resumes the same pager in another transaction or process; a snapshot
reference opens a read-only view of a past version.
"""
import os
import subprocess
import sys
import textwrap

import pytest

from proto_db.errors import ProtoCorruptionError, ProtoUserError, ProtoValidationError
from proto_db.indexing import RangeIndex
from proto_db.query import pager as pager_module
from proto_db.query.linq import F, from_collection
from proto_db.query.pager import Pager, decode_ref, encode_ref
from proto_db.query.terms import Term
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace, Snapshot

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _db(storage=None, **kw):
    space = ObjectSpace(storage or MemoryStorage(), **kw)
    return space, space.new_database("db")


def _seed(db, n=500):
    def build(tx):
        items = tx.new_list().add_index(RangeIndex("k", "k"))
        tx.set_root_object("items", items.extend([{"k": (i * 7919) % n, "v": i} for i in range(n)]))
    db.run(build)


def _all_pages(pager):
    rows = []
    k = 0
    while True:
        page = pager.page(k)
        if not page:
            return rows
        assert len(page) <= pager.page_size
        rows.extend(page)
        k += 1


def test_ranked_pages_equal_the_ordered_result():
    space, db = _db()
    _seed(db)
    with db.new_transaction() as tx:
        p = tx.pager("items", where=Term("k", ">=", 100), order_by="k", page_size=37)
        assert p.by_rank
        expected = list(from_collection(tx.get_root_object("items")).where(F.k >= 100).order_by(F.k))
        assert p.rows == len(expected) == 400 and p.pages == 11
        assert _all_pages(p) == expected
        assert p.page(3) == tuple(expected[111:148])
        assert p.page(99) == ()


def test_unranked_pages_come_from_one_cached_result():
    space, db = _db()
    _seed(db)
    with db.new_transaction() as tx:
        p = tx.pager("items", where=Term("v", "<", 50), order_by=("k", True), page_size=8)
        assert not p.by_rank
        expected = list(from_collection(tx.get_root_object("items")).where(F.v < 50).order_by(F.k, True))
        assert _all_pages(p) == expected
        assert space.query_cache_stats()["entries"] == 1


def test_pages_are_stable_while_writers_insert_before_and_after_the_page():
    space, db = _db()
    _seed(db, 300)
    with db.new_transaction() as tx:
        p = tx.pager("items", order_by="k", page_size=20)
        expected = list(from_collection(tx.get_root_object("items")).order_by(F.k))
    seen = []
    for k in range(p.pages):
        seen.extend(p.page(k))
        # Keys below and above the current page, and removals, between every two pages.
        db.run(lambda t, k=k: t.set_root_object("items", t.get_root_object("items")
                                                .append_last({"k": -1 - k, "v": -1})
                                                .append_last({"k": 10_000 + k, "v": -2})
                                                .remove_at(0)))
    assert seen == expected
    with db.new_transaction() as tx:
        assert tx.pager("items", order_by="k", page_size=20).rows == 300 + p.pages


def test_a_token_resumes_the_same_pages_in_a_later_transaction():
    space, db = _db()
    _seed(db)
    with db.new_transaction() as tx:
        p = tx.pager("items", where=Term("k", "between[]", (10, 300)), order_by=[("k", True)], page_size=25)
        token = p.token()
        first = p.page(4)
    db.run(lambda t: t.set_root_object("items", t.get_root_object("items").append_last({"k": 200, "v": -5})))
    again = db.resume_pager(token)
    assert again.page(4) == first and again.rows == p.rows
    assert again.token() == token


def test_resuming_refuses_another_semantics_version(monkeypatch):
    space, db = _db()
    _seed(db, 20)
    with db.new_transaction() as tx:
        token = tx.pager("items", order_by="k", page_size=5).token()
    monkeypatch.setattr(pager_module, "QUERY_SEMANTICS_VERSION", 99)
    with pytest.raises(ProtoValidationError, match="semantics version"):
        db.resume_pager(token)


def test_a_pager_of_a_written_root_is_refused_and_plan_pagers_have_no_token():
    space, db = _db()
    _seed(db, 20)
    with db.new_transaction() as tx:
        tx.set_root_object("items", tx.get_root_object("items").append_last({"k": 1, "v": 1}))
        with pytest.raises(ProtoUserError, match="written in this transaction"):
            tx.pager("items", page_size=5)
        tx.abort()
    with db.new_transaction() as tx:
        p = Pager(from_collection(tx.get_root_object("items")).where(lambda e: e["v"] % 2), 3)
        assert len(_all_pages(p)) == 10
        with pytest.raises(ProtoUserError, match="no token"):
            p.token()
        with pytest.raises(ProtoValidationError):
            tx.pager("items", where=lambda e: True, page_size=2)
        with pytest.raises(ProtoValidationError):
            p.page(-1)
        with pytest.raises(ProtoValidationError):
            Pager(p.plan, 0)


def test_snapshot_at_reads_a_past_version_and_refuses_writes():
    space, db = _db()
    _seed(db, 50)
    with db.new_transaction() as tx:
        ref = tx.snapshot_ref
        version = tx._db_root.version
    for i in range(3):
        db.run(lambda t, i=i: t.set_root_object("items", t.get_root_object("items").append_last({"k": 99, "v": i})))
    snap = db.snapshot_at(ref)
    assert isinstance(snap, Snapshot) and snap.version == version
    assert snap.get_root_object("items").count == 50
    assert snap.root_names == ("items",)
    assert db.snapshot_at(snap.ref).version == version
    assert db.snapshot_at(snap).version == version
    entry = [h for h in db.history() if h.version == version][0]
    assert db.snapshot_at(entry).get_root_object("items").count == 50
    for write in (lambda: snap.set_root_object("x", 1), lambda: snap.remove_root_object("items"),
                  lambda: snap.set_mutable("m", 1), lambda: snap.new_mutable("m", 1), snap.commit):
        with pytest.raises(ProtoUserError, match="read-only"):
            write()
    p = snap.pager("items", order_by="k", page_size=10)
    assert p.rows == 50 and db.resume_pager(p.token()).page(2) == p.page(2)


def test_snapshot_references_are_validated():
    space, db = _db()
    _seed(db, 5)
    other = space.new_database("other")
    with db.new_transaction() as tx:
        ref = tx.snapshot_ref
        items_ptr = tx.get_root_object("items").atom_pointer
    with pytest.raises(ProtoValidationError, match="names the database"):
        other.snapshot_at(ref)
    with pytest.raises(ProtoValidationError):
        db.snapshot_at(b"garbage")
    with pytest.raises(ProtoValidationError):
        db.snapshot_at("not bytes")
    with pytest.raises(ProtoCorruptionError):
        db.snapshot_at(encode_ref("snapshot", "db", items_ptr))
    with pytest.raises(ProtoValidationError, match="Not a pager token"):
        db.resume_pager(ref)
    assert decode_ref(ref)["kind"] == "snapshot"


def test_a_token_resumes_in_another_process(tmp_path):
    path = str(tmp_path / "store")
    space = ObjectSpace(FileStorage(path))
    db = space.new_database("db")
    _seed(db, 400)
    with db.new_transaction() as tx:
        p = tx.pager("items", where=Term("k", ">", 50), order_by="k", page_size=30)
        token = p.token()
        expected = [p.page(k) for k in (0, 5, 11)]
    db.run(lambda t: t.set_root_object("items", t.get_root_object("items").append_last({"k": 60, "v": -1})))
    space.close()
    script = textwrap.dedent(f"""
        import sys
        sys.path.insert(0, {ROOT!r})
        from proto_db.indexing import RangeIndex
        from proto_db.storage.file import FileStorage
        from proto_db.transactions import ObjectSpace
        space = ObjectSpace(FileStorage({path!r}))
        p = space.open_database("db").resume_pager(bytes.fromhex(sys.argv[1]))
        print(repr([[e["v"] for e in p.page(k)] for k in (0, 5, 11)]), p.rows, p.by_rank)
        space.close()
    """)
    out = subprocess.run([sys.executable, "-c", script, token.hex()], capture_output=True, text=True,
                         timeout=120, check=True).stdout.split("\n")[0]
    assert out == f"{[[e['v'] for e in page] for page in expected]!r} 349 True"


def test_queryable_conveniences_cached_and_pager():
    from proto_db.query.cache import QueryCache, QueryResult
    from proto_db.query.pager import Pager
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")
    db.run(lambda tx: tx.set_root_object("t", tx.new_list().add_index(RangeIndex("k", "k"))
                                         .extend([{"k": i % 9, "i": i} for i in range(60)])))
    with db.new_transaction() as tx:
        q = from_collection(tx.get_root_object("t")).where(F.k >= 3).order_by(F.k)
        result = q.cached(space.query_cache)
        assert isinstance(result, QueryResult) and result.key is not None
        assert q.cached(space.query_cache) is result                     # a hit: the same value
        assert [r["i"] for r in result] == [r["i"] for r in q.to_list()]
        pager = q.pager(7)
        assert isinstance(pager, Pager) and pager.by_rank
        assert [r["i"] for k in range(pager.pages) for r in pager.page(k)] == [r["i"] for r in result]
        assert not from_collection(tx.get_root_object("t")).where(F.i < 5).pager(2, cache=space.query_cache).by_rank
        import pytest
        with pytest.raises(Exception):
            q.cached(None)
