"""Index creation catch-up (spec R69; R26, R64, R65, P-C).

A transaction whose only operations on a collection root are ``add_index``/``remove_index``, and
that recorded no other read of it, does not fail because the collection changed: the pre-pass
diffs the base against the last published root R0, applies the changed elements to the index
under construction (extractors run there, outside the lock), takes R0's collection as the
expected value of the whole-collection precondition, and repeats with a newer published root at
most ``INDEX_CATCH_UP_ROUNDS`` times.  Under the space root that precondition compares against
the last caught-up value: any change since is a conflict (no catch-up under the lock).

Interleavings are forced with hooks (the pre-pass's read of the published root, the start of the
lock step), never with clocks.  Every test runs with ``transactions._VERIFY_DELTA`` on.

Catches: an index definition change that conflicts with an unrelated concurrent write (the R26
behaviour R69 replaces), a caught-up index that misses or keeps an element changed concurrently,
a catch-up that hides a read or a write of the same collection (then the whole-collection
conflict stands), a catch-up that runs extractors under the root, and a catch-up loop without a
bound.
"""
import pytest

from proto_db import transactions
from proto_db.errors import ProtoConflictError, ProtoValidationError
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.keyspec import KeySpec
from proto_db.indexing.vector import VectorIndex
from proto_db.preconditions import INDEX_CATCH_UP_ROUNDS, Unit, UnitKind
from proto_db.query.planner import plan_query
from proto_db.query.terms import EQ, Term
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.support.recording_index import RootRecordingExtractor

pytestmark = [pytest.mark.timeout(120)]


@pytest.fixture(autouse=True)
def verify_delta(monkeypatch):
    monkeypatch.setattr(transactions, "_VERIFY_DELTA", True)


@pytest.fixture
def space():
    s = ObjectSpace(MemoryStorage())
    yield s
    s.close()


@pytest.fixture
def db(space):
    return space.new_database("db")


def whole(root="d"):
    return Unit(UnitKind.WHOLE, root)


def element_index(name="e"):
    return EqualityIndex(name, KeySpec.element())


def seed(db, index=None, **items):
    def build(tr):
        d = tr.new_dictionary()
        for k, v in items.items():
            d = d.set_at(k, v)
        if index is not None:
            d = d.add_index(index)
        tr.set_root_object("d", d)
    db.run(build)


def committed(db, change, name="d"):
    """Commit ``change(collection)`` on root ``name`` in a transaction of its own."""
    db.run(lambda tr: tr.set_root_object(name, change(tr.get_root_object(name))))


def read(db, fn, name="d"):
    with db.new_transaction() as tr:
        return fn(tr.get_root_object(name))


def lookup(d, value, index="e"):
    return sorted(plan_query(d, Term(None, EQ, value)).execute())


def assert_index_matches(d, name="e"):
    """The index holds exactly what a fresh build over the elements holds, and finds each one."""
    elements = list(d.elements())
    d.get_index(name).validate(elements)
    for element in elements:
        assert element in lookup(d, element), element


def published_root_hook(space, action):
    """Run ``action()`` before each read of the published root that a commit's pre-pass makes,
    while ``armed`` is true (``action``'s own commit reads it unhooked)."""
    original = space._published_record
    state = {"armed": True, "calls": 0}

    def hooked():
        if state["armed"]:
            state["armed"] = False
            try:
                state["calls"] += 1
                action()
            finally:
                state["armed"] = True
        return original()
    space._published_record = hooked
    return state


def before_lock_step(monkeypatch, action):
    """Run ``action()`` once, after the next commit's pre-pass and before its lock step."""
    original = transactions._commit_root
    pending = [action]

    def hooked(*args, **kwargs):
        if pending:
            pending.pop()()
        return original(*args, **kwargs)
    monkeypatch.setattr(transactions, "_commit_root", hooked)


# -- the brief's tests ------------------------------------------------------------------------------

def test_add_index_survives_concurrent_writes(db):
    seed(db, a=1, b=2, c=3)
    a = db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").add_index(element_index()))
    committed(db, lambda d: d.set_at("x", 10))          # an addition
    committed(db, lambda d: d.set_at("a", 11))          # a replacement
    committed(db, lambda d: d.remove_at("b"))           # a removal
    a.commit()
    assert read(db, lambda d: dict(d.items())) == {"a": 11, "c": 3, "x": 10}
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        assert d.index_names() == ["e"]
        assert_index_matches(d)
        assert lookup(d, 1) == [] and lookup(d, 2) == []        # the replaced and removed values


def test_add_index_with_prior_read_does_not_catch_up(db):
    """A transaction that also read the collection depends on its content: the add_index keeps the
    whole-collection precondition against the base (R26), and a concurrent write conflicts."""
    seed(db, a=1, b=2)
    a = db.new_transaction()
    d = a.get_root_object("d")
    assert d.get_at("a") == 1
    a.set_root_object("d", d.add_index(element_index()))
    committed(db, lambda d: d.set_at("x", 10))
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == whole()
    assert read(db, lambda d: d.index_names()) == []


def test_catch_up_round_limit_then_conflict(db, space):
    """Each read of the published root by the pre-pass finds a newer commit: after
    INDEX_CATCH_UP_ROUNDS rounds the commit conflicts on the whole collection."""
    seed(db, a=1)
    a = db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").add_index(element_index()))
    writes = iter(range(100))
    hook = published_root_hook(space, lambda: committed(db, lambda d: d.set_at(f"w{next(writes)}", 5)))
    rounds = []
    original = transactions.Transaction._catch_up_round
    transactions.Transaction._catch_up_round = lambda self, *args: rounds.append(1) or original(self, *args)
    try:
        with pytest.raises(ProtoConflictError) as e:
            a.commit()
    finally:
        transactions.Transaction._catch_up_round = original
    assert e.value.unit == whole()
    assert len(rounds) == INDEX_CATCH_UP_ROUNDS == 3
    assert hook["calls"] == INDEX_CATCH_UP_ROUNDS + 1
    hook["armed"] = False
    assert read(db, lambda d: d.index_names()) == []


# -- R69 beyond the brief's names -------------------------------------------------------------------

def test_catch_up_round_limit_not_reached_commits(db, space):
    """Writes during fewer rounds than the limit: the commit catches up with each and publishes."""
    seed(db, a=1)
    a = db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").add_index(element_index()))
    left = [f"w{i}" for i in range(INDEX_CATCH_UP_ROUNDS)]
    hook = published_root_hook(space, lambda: left and committed(db, lambda d: d.set_at(left.pop(), 5)))
    a.commit()
    hook["armed"] = False
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        assert sorted(d.keys()) == ["a", "w0", "w1", "w2"]
        assert_index_matches(d)


def test_add_index_with_prior_write_does_not_catch_up(db):
    """An add_index on a collection the transaction also wrote is not a catch-up (R26 stands)."""
    seed(db, a=1, b=2)
    a = db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").set_at("n", 7).add_index(element_index()))
    committed(db, lambda d: d.set_at("x", 10))
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == whole()


def test_read_after_add_index_does_not_catch_up(db):
    """A read of the collection after the root was set to the new version still counts."""
    seed(db, a=1, b=2)
    a = db.new_transaction()
    d = a.get_root_object("d")
    a.set_root_object("d", d.add_index(element_index()))
    assert sorted(d.elements()) == [1, 2]
    committed(db, lambda d: d.set_at("x", 10))
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == whole()


def test_remove_index_survives_concurrent_writes(db):
    seed(db, index=element_index(), a=1, b=2)
    a = db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").remove_index("e"))
    committed(db, lambda d: d.set_at("x", 10).remove_at("a"))
    a.commit()
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        assert dict(d.items()) == {"b": 2, "x": 10} and d.index_names() == []


def test_catch_up_keeps_other_indexes_of_the_newer_state(db):
    """The caught-up version is R0's collection: its other indexes are R0's, maintained by the
    concurrent writes; a remove-then-add of one name rebuilds that index from the newer state."""
    seed(db, index=EqualityIndex("old", KeySpec.element()), a=1, b=2)
    a = db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").add_index(element_index()).remove_index("old")
                      .add_index(EqualityIndex("old", KeySpec.element())))
    committed(db, lambda d: d.set_at("x", 10).remove_at("a"))
    a.commit()
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        assert d.index_names() == ["e", "old"]
        assert_index_matches(d, "e")
        assert_index_matches(d, "old")


def test_catch_up_conflicts_on_a_concurrent_index_definition_change(db):
    """Catch-up covers content changes only: a concurrent change of the definitions conflicts."""
    seed(db, a=1)
    a = db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").add_index(element_index()))
    committed(db, lambda d: d.add_index(EqualityIndex("other", KeySpec.element())))
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == Unit(UnitKind.INDEX_DEFS, "d")


def test_catch_up_root_changed_under_the_lock_conflicts(db, monkeypatch):
    """Ruling 1: the catch-up runs in the pre-pass only.  A change of the collection between the
    pre-pass and the lock step fails the whole-collection precondition by delta validation."""
    seed(db, a=1)
    a = db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").add_index(element_index()))
    committed(db, lambda d: d.set_at("x", 10))                   # caught up in the pre-pass
    before_lock_step(monkeypatch, lambda: committed(db, lambda d: d.set_at("y", 11)))
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == whole()
    assert read(db, lambda d: d.index_names()) == []


def test_catch_up_other_root_changed_under_the_lock_commits(db, monkeypatch):
    """A change of another root between the pre-pass and the lock step leaves the caught-up
    collection's precondition holding."""
    seed(db, a=1)
    db.run(lambda tr: tr.set_root_object("other", tr.new_dictionary()))
    a = db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").add_index(element_index()))
    committed(db, lambda d: d.set_at("x", 10))
    before_lock_step(monkeypatch, lambda: committed(db, lambda o: o.set_at("k", 1), name="other"))
    a.commit()
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        assert dict(d.items()) == {"a": 1, "x": 10}
        assert_index_matches(d)
        assert dict(tr.get_root_object("other").items()) == {"k": 1}


def test_finished_transaction_keeps_no_candidate(db):
    """Task 12 minor (b): a finished commit, successful or not, drops its pre-pass candidate."""
    seed(db, a=1)
    ok = db.new_transaction()
    ok.set_root_object("d", ok.get_root_object("d").set_at("b", 2))
    ok.commit()
    assert ok._candidate is None
    bad = db.new_transaction()
    assert bad.get_root_object("d").get_at("a") == 1
    bad.set_root_object("d", bad.get_root_object("d").set_at("c", 3))
    committed(db, lambda d: d.set_at("a", 5))
    with pytest.raises(ProtoConflictError):
        bad.commit()
    assert bad._candidate is None


# -- every collection kind ------------------------------------------------------------------------

# Each case: the seeded collection, and three concurrent writes (an addition, a change, a removal).
KINDS = {
    "Dictionary": (lambda tr: tr.new_dictionary().set_at("a", 11).set_at("b", 21).set_at("c", 31),
                   (lambda c: c.set_at("n", 45), lambda c: c.set_at("a", 12), lambda c: c.remove_at("b"))),
    "HashDictionary": (lambda tr: tr.new_hash_dictionary().set_at("a", 11).set_at("b", 21).set_at("c", 31),
                       (lambda c: c.set_at("n", 45), lambda c: c.set_at("a", 12), lambda c: c.remove_at("b"))),
    "Set": (lambda tr: tr.new_set().add(11).add(21).add(31),
            (lambda c: c.add(45), lambda c: c.remove(21).add(22), lambda c: c.remove(11))),
    "CountedSet": (lambda tr: tr.new_counted_set().add(11).add(11).add(21),
                   (lambda c: c.add(45), lambda c: c.add(21), lambda c: c.remove(11))),
    "RepeatedKeysDictionary": (lambda tr: tr.new_repeated_keys_dictionary().set_at("a", 11).set_at("a", 12)
                               .set_at("b", 21),
                               (lambda c: c.set_at("n", 45), lambda c: c.set_at("b", 22),
                                lambda c: c.remove_record("a", 11))),
    "List": (lambda tr: tr.new_list().extend([11, 21, 31]),
             (lambda c: c.append_last(45), lambda c: c.set_at(1, 22), lambda c: c.append_last(11))),
    "List shrinking": (lambda tr: tr.new_list().extend([11, 21, 31]),
                       (lambda c: c.append_last(45), lambda c: c.remove_at(0), lambda c: c.set_at(0, 23))),
}


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_catch_up_every_collection_kind(db, kind):
    build, writes = KINDS[kind]
    with RootRecordingExtractor("catch_up_tens", key=lambda element: element // 10) as extractor:
        db.run(lambda tr: tr.set_root_object("c", build(tr)))
        a = db.new_transaction()
        a.set_root_object("c", a.get_root_object("c").add_index(EqualityIndex("e", extractor.key_spec)))
        for write in writes:
            committed(db, write, name="c")
        extractor.reset()
        a.commit()
        assert extractor.calls and extractor.held_calls() == 0     # caught up outside the lock (P-C)
        with db.new_transaction() as tr:
            c = tr.get_root_object("c")
            assert c.index_names() == ["e"]
            c.get_index("e").validate(list(c.elements()))


def test_catch_up_applies_the_diff_not_a_rebuild(db):
    """A catch-up costs the changed elements, not the collection: one replaced value of 100 runs
    the extractor twice (the old value out, the new one in)."""
    with RootRecordingExtractor("catch_up_cost", key=lambda element: element // 10) as extractor:
        db.run(lambda tr: tr.set_root_object("c", _hundred(tr)))
        a = db.new_transaction()
        a.set_root_object("c", a.get_root_object("c").add_index(EqualityIndex("e", extractor.key_spec)))
        committed(db, lambda c: c.set_at("k5", 1000), name="c")
        extractor.reset()
        a.commit()
        assert len(extractor.calls) == 2 and extractor.held_calls() == 0
        with db.new_transaction() as tr:
            c = tr.get_root_object("c")
            c.get_index("e").validate(list(c.elements()))
            assert c.get_at("k5") == 1000


def _hundred(tr):
    d = tr.new_dictionary()
    for i in range(100):
        d = d.set_at(f"k{i}", i)
    return d


def unique_index(name="u"):
    return EqualityIndex(name, KeySpec.element(), unique=True)


def test_unique_index_catch_up_refuses_a_concurrent_duplicate(db):
    """R29: a commit that stored a duplicate key since the base makes the unique index's catch-up
    a conflict on the whole collection; ``Database.run`` retries, and the retry's ``add_index``
    meets the duplicate itself (``ProtoValidationError``, not retried)."""
    seed(db, a=1, b=2)
    a = db.new_transaction()
    a.set_root_object("d", a.get_root_object("d").add_index(unique_index()))
    committed(db, lambda d: d.set_at("c", 1))
    with pytest.raises(ProtoConflictError) as e:
        a.commit()
    assert e.value.unit == whole()
    committed(db, lambda d: d.remove_at("c"))

    attempts = []

    def fn(tr):
        attempts.append(1)
        tr.set_root_object("d", tr.get_root_object("d").add_index(unique_index()))
        if len(attempts) == 1:
            committed(db, lambda d: d.set_at("e", 2))        # a duplicate of b, after the base
    with pytest.raises(ProtoValidationError, match="unique"):
        db.run(fn, backoff=(0, 0))
    assert len(attempts) == 2
    assert read(db, lambda d: d.index_names()) == []


def test_unique_index_retry_succeeds_once_the_duplicate_is_gone(db):
    """The duplicate a concurrent commit stored is removed by a later commit before the retry:
    the retry builds the unique index."""
    seed(db, a=1, b=2)
    attempts, before_next = [], []
    new_transaction = db.new_transaction

    def started():
        while before_next:
            before_next.pop()()
        return new_transaction()
    db.new_transaction = started        # the retry's transaction starts after the removal

    def fn(tr):
        attempts.append(1)
        tr.set_root_object("d", tr.get_root_object("d").add_index(unique_index()))
        if len(attempts) == 1:
            committed(db, lambda d: d.set_at("c", 1))        # the duplicate: attempt 1 conflicts
            before_next.append(lambda: committed(db, lambda d: d.remove_at("c")))
    db.run(fn, backoff=(0, 0))
    assert len(attempts) == 2
    with db.new_transaction() as tr:
        d = tr.get_root_object("d")
        assert dict(d.items()) == {"a": 1, "b": 2} and d.index_names() == ["u"]
        assert_index_matches(d, "u")


# Two units swap their elements: removals must all run before the additions, or the unique index
# meets the moved element twice in between (fix round 1, Critical).
SWAPS = {
    "Dictionary": (lambda tr: tr.new_dictionary().set_at("k1", "b").set_at("k2", "a"),
                   lambda c: c.set_at("k1", "a").set_at("k2", "b"),
                   lambda c: dict(c.items()) == {"k1": "a", "k2": "b"}),
    "List": (lambda tr: tr.new_list().append_last("b").append_last("a"),
             lambda c: c.set_at(0, "a").set_at(1, "b"),
             lambda c: list(c.elements()) == ["a", "b"]),
}


@pytest.mark.parametrize("kind", sorted(SWAPS))
def test_unique_index_catch_up_over_a_swap(db, kind):
    build, swap, expected = SWAPS[kind]
    db.run(lambda tr: tr.set_root_object("c", build(tr)))
    a = db.new_transaction()
    a.set_root_object("c", a.get_root_object("c").add_index(unique_index()))
    committed(db, swap, name="c")
    a.commit()
    with db.new_transaction() as tr:
        c = tr.get_root_object("c")
        assert expected(c) and c.index_names() == ["u"]
        c.get_index("u").validate(list(c.elements()))


def _vector(i):
    return {"vec": [float((i * 7) % 11) + 0.5, float((i * 3) % 5) + 1.0], "n": i}


VECTOR_INDEXES = {
    "exact": lambda: VectorIndex("v", "vec", 2, backend="exact"),
    "hnsw": lambda: VectorIndex("v", "vec", 2, backend="hnsw"),
    "ivf_flat": lambda: VectorIndex("v", "vec", 2, backend="ivf_flat", nlist=4, nprobe=4),
}
VECTOR_KINDS = {
    "Dictionary": (lambda tr: _dictionary_of(tr, [(f"k{i}", _vector(i)) for i in range(12)]),
                   (lambda c: c.set_at("n", _vector(40)), lambda c: c.set_at("k1", _vector(41)),
                    lambda c: c.remove_at("k2"))),
    "List": (lambda tr: tr.new_list().extend(_vector(i) for i in range(12)),
             (lambda c: c.append_last(_vector(40)), lambda c: c.set_at(1, _vector(41)),
              lambda c: c.remove_at(2))),
}


def _dictionary_of(tr, items):
    d = tr.new_dictionary()
    for k, v in items:
        d = d.set_at(k, v)
    return d


@pytest.mark.parametrize("backend", sorted(VECTOR_INDEXES))
@pytest.mark.parametrize("kind", sorted(VECTOR_KINDS))
def test_vector_index_catch_up(db, kind, backend):
    build, writes = VECTOR_KINDS[kind]
    db.run(lambda tr: tr.set_root_object("c", build(tr)))
    a = db.new_transaction()
    a.set_root_object("c", a.get_root_object("c").add_index(VECTOR_INDEXES[backend]()))
    for write in writes:
        committed(db, write, name="c")
    a.commit()
    with db.new_transaction() as tr:
        c = tr.get_root_object("c")
        assert c.index_names() == ["v"] and len(c) == 12
        elements = list(c.elements())
        index = c.get_index("v")
        index.validate(elements)
        fresh = VECTOR_INDEXES[backend]().build(elements)
        assert getattr(index, "total_count", None) == getattr(fresh, "total_count", None)
