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
    from proto_db.errors import ProtoReferenceError
    with db.new_transaction() as tx:
        origin = tx._db_root.origin
    with pytest.raises(ProtoReferenceError):          # a pointer to a collection, not a version (M6)
        db.snapshot_at(encode_ref("snapshot", "db", items_ptr, origin))
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


# -- fix round 1 (review of Task 8.4b; ruling R99) -------------------------------------------

def _two_databases():
    space = ObjectSpace(MemoryStorage())
    a, b = space.new_database("a"), space.new_database("b")
    for db, tag in ((a, "secret-a"), (b, "b")):
        db.run(lambda tx, tag=tag: tx.set_root_object("people", tx.new_list().add_index(RangeIndex("age", "age"))
                                                      .extend([{"name": f"{tag}-{i}", "age": i} for i in range(5)])))
    return space, a, b


def test_a_token_or_entry_of_another_database_is_refused():
    """I2 (review repro r3_token.py): the database a token names is checked against the identity
    the snapshot itself carries, not only the token's own field; a HistoryEntry or Snapshot of
    another database is refused too.  A misuse check, not a security boundary (R99)."""
    import msgpack
    from proto_db.errors import ProtoReferenceError
    space, a, b = _two_databases()
    with a.new_transaction() as tx:
        token = tx.pager("people", order_by="age", page_size=10).token()
        ref = tx.snapshot_ref
    body = msgpack.unpackb(token, raw=False)
    body["db"] = "b"
    forged = msgpack.packb(body, use_bin_type=True)
    for call in (lambda: b.resume_pager(forged), lambda: b.snapshot_at(forged), lambda: b.snapshot_at(ref),
                 lambda: b.snapshot_at(a.history()[0]), lambda: b.snapshot_at(a.snapshot_at(ref))):
        with pytest.raises(ProtoReferenceError):
            call()
    assert [r["name"] for r in a.resume_pager(token).page(0)][:1] == ["secret-a-0"]
    # The database's own references still open.
    assert a.snapshot_at(a.history()[0]).get_root_object("people").count == 5


def test_a_recreated_database_refuses_the_old_incarnations_tokens():
    """I2: the identity is the database's incarnation, created with it, not its name."""
    from proto_db.errors import ProtoReferenceError
    space1 = ObjectSpace(MemoryStorage())
    db1 = space1.new_database("db")
    db1.run(lambda tx: tx.set_root_object("p", tx.new_list().extend([1, 2])))
    with db1.new_transaction() as tx:
        ref = tx.snapshot_ref
    space2 = ObjectSpace(MemoryStorage())
    db2 = space2.new_database("db")
    db2.run(lambda tx: tx.set_root_object("p", tx.new_list().extend([1, 2])))
    with pytest.raises(ProtoReferenceError):
        db2.snapshot_at(ref)                  # same name, another database: refused, not "corruption"


def test_malformed_and_foreign_tokens_raise_a_client_error():
    """M6 (review repro r7_malformed.py): never TypeError/ValueError, never ProtoCorruptionError."""
    import uuid as uuid_module
    import msgpack
    from proto_db.errors import ProtoReferenceError
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")
    db.run(lambda tx: tx.set_root_object("p", tx.new_list().add_index(RangeIndex("k", "k"))
                                         .extend([{"k": i} for i in range(5)])))
    with db.new_transaction() as tx:
        token = tx.pager("p", order_by="k", page_size=2).token()
    original = msgpack.unpackb(token, raw=False)

    def forge(**changes):
        body = dict(original)
        body.update(changes)
        return msgpack.packb(body, use_bin_type=True)
    cases = [forge(order=5), forge(order=[[1, 2, 3]]), forge(page=True), forge(name=7), forge(where=[1]),
             forge(root=[uuid_module.uuid4().bytes, 0]), forge(root=[original["root"][0], 10 ** 12]),
             forge(qv=None), forge(plan=b"other"), b"\x00garbage", 17]
    for bad in cases:
        with pytest.raises(ProtoReferenceError):
            db.resume_pager(bad)
    assert issubclass(ProtoReferenceError, ProtoValidationError)


def test_a_token_whose_plan_changed_is_refused_as_stale(monkeypatch):
    """I4 (review repro r8_ties.py): a resumed pager re-plans; if the plan differs (a planner
    change, another library version) it would page another tie order, so it is refused."""
    from proto_db.errors import ProtoStalePagerError
    from proto_db.query import planner
    from proto_db.query.plans import CollectionScanPlan, Filter, Sort
    from proto_db.query.terms import Term
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")
    db.run(lambda tx: tx.set_root_object("p", tx.new_list().add_index(RangeIndex("k", "k")).extend(
        [{"k": (7 * i) % 10, "o": i % 2, "i": i} for i in range(10)])))
    term = Term("k", ">=", 3)
    with db.new_transaction() as tx:
        token = tx.pager("p", where=term, order_by=[("o", False)], page_size=3).token()
    assert db.resume_pager(token).rows == 7                     # same plan: resumes

    def other_plan(collection, where=None, order_by=None, **kw):
        return Sort(Filter(CollectionScanPlan(collection), where), order_by)
    monkeypatch.setattr(planner, "plan_query", other_plan)
    with pytest.raises(ProtoStalePagerError):
        db.resume_pager(token)


def test_a_token_names_extractors_by_name_and_declared_version():
    """M4, R101: across processes an extractor's identity is its name plus the version declared at
    registration (no code fingerprint).  Another version under the name is refused as stale;
    the same name and version resume, whatever function object is registered."""
    from proto_db.errors import ProtoStalePagerError
    from proto_db.indexing.keyspec import (KeySpec, extractor_version, register_extractor,
                                           unregister_extractor)
    from proto_db.query.terms import Term

    def double(e):
        return 2 * e["k"]
    register_extractor("t84b_tok", double, version="1")
    try:
        assert extractor_version("t84b_tok") == "1"
        space = ObjectSpace(MemoryStorage())
        db = space.new_database("db")
        db.run(lambda tx: tx.set_root_object("p", tx.new_list().add_index(
            RangeIndex("x", KeySpec.extractor("t84b_tok"))).extend([{"k": i} for i in range(6)])))
        with db.new_transaction() as tx:
            token = tx.pager("p", where=Term("@t84b_tok", ">=", 4), order_by="@t84b_tok", page_size=2).token()
        unregister_extractor("t84b_tok")
        register_extractor("t84b_tok", lambda e: 2 * e["k"], version="1")       # same identity
        assert [r["k"] for r in db.resume_pager(token).page(0)] == [2, 3]
        unregister_extractor("t84b_tok")
        register_extractor("t84b_tok", double, version="2")
        with pytest.raises(ProtoStalePagerError, match="t84b_tok"):
            db.resume_pager(token)
        unregister_extractor("t84b_tok")
        register_extractor("t84b_tok", double)                                   # no version
        with pytest.raises(ProtoStalePagerError):
            db.resume_pager(token)
        with pytest.raises(ProtoValidationError):
            register_extractor("t84b_bad", double, version=3)
    finally:
        unregister_extractor("t84b_tok")


_XPROC_EXTRACTORS = textwrap.dedent("""
    import sys
    from proto_db.indexing.keyspec import register_extractor
    def genexpr_key(e):
        return sum(1 for c in e["s"] if c in "aeiou")
    def setconst_key(e):
        return e["s"] in {"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}
    register_extractor("genexpr_key", genexpr_key)
    register_extractor("setconst_key", setconst_key, version="2026-11")
""")


def test_extractor_tokens_resume_in_processes_with_other_hash_seeds(tmp_path):
    """N1 (re-review repro t3_xproc.py): a token over extractors holding a generator expression and
    a set literal, written in one process, resumes in processes with other hash seeds."""
    path = str(tmp_path / "store")
    write = _XPROC_EXTRACTORS + textwrap.dedent(f"""
        from proto_db.storage.file import FileStorage
        from proto_db.transactions import ObjectSpace
        space = ObjectSpace(FileStorage({path!r}))
        db = space.new_database("db")
        words = ["alpha", "beta", "gamma", "omega", "zeta", "iota", "kappa"]
        db.run(lambda tx: tx.set_root_object("items", tx.new_list().extend(
            [{{"s": words[i % 7] + str(i), "i": i}} for i in range(40)])))
        with db.new_transaction() as tx:
            for n in ("genexpr_key", "setconst_key"):
                p = tx.pager("items", order_by="@" + n, page_size=7)
                print(n, p.token().hex(), ",".join(str(e["i"]) for e in p.page(1)))
        space.close()
    """)
    resume = _XPROC_EXTRACTORS + textwrap.dedent(f"""
        from proto_db.storage.file import FileStorage
        from proto_db.transactions import ObjectSpace
        space = ObjectSpace(FileStorage({path!r}))
        db = space.open_database("db")
        for line in sys.stdin:
            n, tok, page = line.split()
            got = ",".join(str(e["i"]) for e in db.resume_pager(bytes.fromhex(tok)).page(1))
            print(n, got == page)
        space.close()
    """)

    def run(code, seed, stdin=None):
        env = dict(os.environ, PYTHONHASHSEED=str(seed), PYTHONPATH=ROOT)
        out = subprocess.run([sys.executable, "-c", code], input=stdin, capture_output=True, text=True,
                             env=env, cwd=ROOT, timeout=120)
        assert out.returncode == 0, out.stderr
        return out.stdout
    tokens = run(write, 1)
    for seed in (2, 3):
        assert run(resume, seed, tokens).split() == ["genexpr_key", "True", "setconst_key", "True"]


def test_a_forged_atom_operand_in_a_token_is_a_client_error():
    """N5 (re-review repro t8_errors.py): a reference anywhere in a token, including an atom
    operand of its predicate, is probed while decoding: never ProtoCorruptionError."""
    import uuid as uuid_module
    import msgpack
    from proto_db.errors import ProtoReferenceError
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")
    db.run(lambda tx: tx.set_root_object("p", tx.new_list().add_index(RangeIndex("k", "k")).extend(
        [{"k": i, "o": None} for i in range(30)])))
    with db.new_transaction() as tx:
        token = tx.pager("p", where=["o", "==", None], order_by="k", page_size=5).token()
        seg = tx._db_root.atom_pointer.segment.bytes
    for wire in ({"@ref": [seg, 10 ** 9, 0]}, {"@ref": [uuid_module.uuid4().bytes, 1, 0]},
                 {"@ref": [seg, 1, 999999]}):
        body = msgpack.unpackb(token, raw=False)
        body["where"] = ["o", "==", wire]
        with pytest.raises(ProtoReferenceError):
            db.resume_pager(msgpack.packb(body, use_bin_type=True)).page(0)


def test_a_pager_whose_plan_has_no_structured_digest_has_no_token():
    """N6: a token is built only from the structured digest; a plan without one (here an index
    class that did not opt in to caching) pages normally but refuses token() clearly."""
    from proto_db.indexing.range import RangeIndex as Base

    class Private(Base):
        __type_name__ = "tests.t84b.PrivateRangeIndex"
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")
    db.run(lambda tx: tx.set_root_object("p", tx.new_list().add_index(Private("k", "k")).extend(
        [{"k": i} for i in range(10)])))
    with db.new_transaction() as tx:
        pager = tx.pager("p", order_by="k", page_size=3)
        assert [r["k"] for r in pager.page(1)] == [3, 4, 5]
        with pytest.raises(ProtoUserError, match="no structured digest"):
            pager.token()


def test_resume_reads_the_database_identity_once_per_handle(monkeypatch):
    """N2: a database's identity never changes, so a handle reads it once, not on every resume."""
    space, db = _db()
    _seed(db, 50)
    with db.new_transaction() as tx:
        token = tx.pager("items", order_by="k", page_size=5).token()
    db.resume_pager(token)
    reads = [0]
    original = type(space)._read_storage_root

    def counting(self, *a, **kw):
        reads[0] += 1
        return original(self, *a, **kw)
    monkeypatch.setattr(type(space), "_read_storage_root", counting)
    for _ in range(5):
        db.resume_pager(token).page(1)
    assert reads[0] == 0, reads
