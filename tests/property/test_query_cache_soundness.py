"""Key soundness of the query result cache (Task 8.4b, D56; the maintainer's riskiest rule).

Two equal keys must always produce equal results.  Hypothesis draws random data and
random queries (predicates of terms, And, Or and Not over indexed and unindexed paths;
orders, windows, projections, distinct, groups), then a random sequence of commits, some
changing the collection the queries read and some changing only another root.  After
every commit, each query is answered from the cache and executed afresh, both on the new
snapshot and on the oldest snapshot still open; the results must be identical value by
value, with exact types (``1``, ``1.0``, ``Decimal("1")`` and ``True`` stay apart).  A key
that did not change must have been a hit, and a commit that changed the collection must
have changed every key that reads it.
"""
from decimal import Decimal

from hypothesis import HealthCheck, given, settings, strategies as st

from proto_db.indexing import EqualityIndex, RangeIndex
from proto_db.query.cache import cache_key
from proto_db.query.expressions import And, Not, Or
from proto_db.query.linq import Agg, F, from_collection
from proto_db.query.terms import Term
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from proto_db.values import exact_token

K_VALUES = [0, 1, 1.0, True, 2, Decimal("2"), Decimal("2.0"), None, "a", "b"]
S_VALUES = ["x", "y", "z"]


@st.composite
def records(draw):
    rec = {"v": draw(st.integers(-50, 50))}
    k = draw(st.sampled_from(K_VALUES + ["<missing>"]))
    if k != "<missing>":
        rec["k"] = k
    rec["s"] = draw(st.sampled_from(S_VALUES))
    return rec


@st.composite
def terms(draw):
    path = draw(st.sampled_from(["k", "s", "v"]))
    pool = {"k": K_VALUES, "s": S_VALUES, "v": [-10, 0, 3, 7.5]}[path]
    op = draw(st.sampled_from(["==", "!=", "<", "<=", ">", ">=", "in", "between[]", "?N", "?!N"]))
    candidates = [v for v in pool if v is not None] or [0]
    if op in ("?N", "?!N"):
        return Term(path, op)
    if op == "in":
        return Term(path, op, tuple(draw(st.lists(st.sampled_from(pool), min_size=1, max_size=3))))
    if op == "between[]":
        a = draw(st.sampled_from(candidates))
        same = [v for v in candidates if type(v) is type(a)]
        b = draw(st.sampled_from(same))
        lo, hi = sorted([a, b])
        return Term(path, op, (lo, hi))
    if op == "==" or op == "!=":
        return Term(path, op, draw(st.sampled_from(pool)))
    return Term(path, op, draw(st.sampled_from(candidates)))


predicates = st.recursive(terms(), lambda inner: st.one_of(
    st.lists(inner, min_size=2, max_size=3).map(lambda xs: And(*xs)),
    st.lists(inner, min_size=2, max_size=3).map(lambda xs: Or(*xs)),
    inner.map(Not)), max_leaves=5)


def _recipe(where, order, window, tail):
    """A function from a collection to ``(kind, Queryable)``."""
    def build(coll):
        q = from_collection(coll)
        if where is not None:
            q = q.where(where)
        if order is not None:
            q = q.order_by(F[order[0]], order[1])
            if order[0] == "s":          # a tie-breaker on one key only: single-key orders are drawn too
                q = q.then_by(F.v)
        if window is not None:
            q = q.skip(window[0]).take(window[1])
        if tail == "select":
            q = q.select(["k", "v"])
        elif tail == "distinct":
            q = q.select("s").distinct()
        elif tail == "group":
            q = q.group_by(F.s, {"n": Agg.count(), "total": Agg.sum("v")})
        elif tail == "count":
            return ("count", q)
        return ("rows", q)
    return build


ORDERS = st.none() | st.tuples(st.sampled_from(["k", "v", "s"]), st.booleans())
WINDOWS = st.none() | st.tuples(st.integers(0, 6), st.integers(0, 8))
TAILS = st.sampled_from(["none", "select", "distinct", "group", "count"])


@st.composite
def queries(draw):
    """A random query recipe (each component drawn independently)."""
    return _recipe(draw(st.none() | predicates), draw(ORDERS), draw(WINDOWS), draw(TAILS))


@st.composite
def query_families(draw):
    """Many recipes built from small pools of components, so that queries differing in exactly
    one component (a window offset, a Not, an order direction) are common: the cases a key
    that forgot a component would make collide."""
    wheres = draw(st.lists(st.none() | predicates, min_size=1, max_size=3))
    wheres += [Not(w) for w in wheres if w is not None]
    orders = draw(st.lists(ORDERS, min_size=1, max_size=2))
    orders += [(o[0], not o[1]) for o in orders if o is not None]
    windows = draw(st.lists(WINDOWS, min_size=1, max_size=3))
    windows += [(w[0] + 1, w[1]) for w in windows if w is not None]
    pick = st.sampled_from
    return [_recipe(draw(pick(wheres)), draw(pick(orders)), draw(pick(windows)), draw(TAILS))
            for _ in range(draw(st.integers(8, 30)))]


steps = st.lists(st.one_of(
    st.tuples(st.just("other"), st.integers()),
    st.tuples(st.just("append"), records()),
    st.tuples(st.just("remove"), st.integers(0, 1000)),
    st.tuples(st.just("set"), st.integers(0, 1000), records())), min_size=1, max_size=6)


def _plain(value):
    """Frozen containers as plain ones: a cached result is frozen by design (D25), a fresh
    projection is not; scalars keep their exact types."""
    if isinstance(value, dict):
        return {_plain(k): _plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if type(value) is tuple:
        return tuple(_plain(v) for v in value)
    return value


def _exact(rows):
    return [exact_token(_plain(r)) for r in rows]


def _answer(space, recipe, coll):
    kind, q = recipe(coll)
    if kind == "count":
        return q.count(), q.count(), None
    key, _ = cache_key(q)
    cached = space.query_cache.result(q)
    return _exact(cached), _exact(list(q)), key


@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(initial=st.lists(records(), max_size=25), recipes=st.lists(queries(), min_size=1, max_size=4),
       script=steps)
def test_cached_results_equal_fresh_execution_across_commits(initial, recipes, script):
    space = ObjectSpace(MemoryStorage(), atom_cache_entries=500)
    db = space.new_database("db")

    def seed(tx):
        coll = tx.new_list().add_index(RangeIndex("k", "k")).add_index(EqualityIndex("s", "s"))
        tx.set_root_object("c", coll.extend(initial))
        tx.set_root_object("other", 0)
    db.run(seed)

    oldest = db.new_transaction()           # an old snapshot kept open across every commit
    old_coll = oldest.get_root_object("c")
    expected_old = [_answer(space, r, old_coll) for r in recipes]
    previous = {i: (key, old_coll.atom_pointer) for i, (_, _, key) in enumerate(expected_old)}

    for step in script:
        def apply(tx, step=step):
            coll = tx.get_root_object("c")
            if step[0] == "other":
                tx.set_root_object("other", step[1])
            elif step[0] == "append":
                tx.set_root_object("c", coll.append_last(step[1]))
            elif step[0] == "remove" and coll.count:
                tx.set_root_object("c", coll.remove_at(step[1] % coll.count))
            elif step[0] == "set" and coll.count:
                tx.set_root_object("c", coll.set_at(step[1] % coll.count, step[2]))
        db.run(apply)
        with db.new_transaction() as tx:
            coll = tx.get_root_object("c")
            for i, recipe in enumerate(recipes):
                hits_before = space.query_cache_stats()["hits"]
                cached, fresh, key = _answer(space, recipe, coll)
                assert cached == fresh, (step, recipe(coll)[1].explain())
                old_key, old_ptr = previous[i]
                if key is not None:
                    if coll.atom_pointer == old_ptr:
                        assert key == old_key
                        assert space.query_cache_stats()["hits"] > hits_before
                    else:
                        assert key != old_key
                previous[i] = (key, coll.atom_pointer)
        # The old snapshot still answers its own version, from the cache or not.
        again = [_answer(space, r, old_coll) for r in recipes]
        assert [a[:2] for a in again] == [e[:2] for e in expected_old]
        assert all(a[0] == a[1] for a in again)
    oldest.abort()


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(initial=st.lists(records(), min_size=3, max_size=25), recipes=query_families())
def test_equal_keys_mean_equal_results(initial, recipes):
    """Many close queries on one version: queries sharing a key must give identical results
    (executed afresh, never from the cache), and the cache must serve each one its own."""
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")

    def seed(tx):
        coll = tx.new_list().add_index(RangeIndex("k", "k")).add_index(EqualityIndex("s", "s"))
        tx.set_root_object("c", coll.extend(initial))
    db.run(seed)
    with db.new_transaction() as tx:
        coll = tx.get_root_object("c")
        by_key = {}
        for recipe in recipes:
            kind, q = recipe(coll)
            if kind == "count":
                continue
            key, _ = cache_key(q)
            fresh = _exact(list(q))
            if key is not None:
                assert by_key.setdefault(key, fresh) == fresh, q.explain()
            assert _exact(space.query_cache.result(q)) == fresh, q.explain()


# -- plan families over the whole plan vocabulary (fix round 1, ruling R99 M1) ---------------
#
# Plans are built directly from the plan nodes, from small pools of components, so that two
# plans differing in exactly one component are common: a single-key order in either
# direction, an ordered index scan ascending and descending, projections whose output names
# differ, aggregations differing in their operator (under different decimal contexts),
# inner and left joins, unnest with and without the parent, union and intersection, vector
# searches with and without a residual predicate (including operands whose display strings
# coincide, b"x" and "b'x'"), and terms differing in their path.  Plans sharing a key must
# give identical results, each executed afresh in its own decimal context.

import decimal  # noqa: E402

from proto_db.indexing import VectorIndex  # noqa: E402
from proto_db.query.plans import (CollectionScanPlan, Filter, GroupBy, Intersect, Join, Limit,  # noqa: E402
                                  Project, Sort, Union, Unnest, VectorSearch)
from proto_db.query.terms import NearQuery  # noqa: E402

CONTEXTS = [None, {"prec": 3}, {"prec": 5, "rounding": decimal.ROUND_DOWN}]
TAGS = [b"x", "b'x'", "y"]


@st.composite
def rich_records(draw):
    return {"k": draw(st.integers(0, 4)), "s": draw(st.sampled_from(["x", "y"])), "v": draw(st.integers(-5, 5)),
            "tags": draw(st.lists(st.integers(0, 2), max_size=2)), "tag": draw(st.sampled_from(TAGS)),
            "vec": (float(draw(st.integers(-3, 3))), float(draw(st.integers(-3, 3)))),
            "price": draw(st.sampled_from([Decimal("1.234567"), Decimal("2.5"), Decimal("0.333333")]))}


def _sources(coll):
    k = coll.get_index("k")
    return [CollectionScanPlan(coll), k.ordered_plan(False), k.ordered_plan(True),
            k.plan(Term("k", ">=", 2)), coll.get_index("s").plan(Term("s", "==", "x"))]


def _build(coll, recipe):
    """A plan from a recipe: a tuple of small pool indexes, interpreted node by node."""
    sources = _sources(coll)
    vec = coll.get_index("vec")
    src = sources[recipe[0] % len(sources)]
    other = sources[recipe[1] % len(sources)]
    node, a, b = recipe[2], recipe[3], recipe[4]
    if node == 0:
        return Sort(src, [("v", bool(a % 2))])
    if node == 1:
        return Project(src, [{"a": "k", "b": "v"}, {"b": "k", "a": "v"}, {"a": "v"}][a % 3])
    if node == 2:
        return GroupBy(src, "s", {"x": (["count", "sum", "min", "max", "avg"][a % 5], "price" if b % 2 else "v")})
    if node == 3:
        return Join(src, other, "k", "k", ["inner", "left"][a % 2])
    if node == 4:
        return Unnest(src, "tags", with_parent=bool(a % 2))
    if node == 5:
        return (Union if a % 2 else Intersect)(src, other)
    if node == 6:
        near = Term("vec", "near[]", NearQuery((float(a % 3), 1.0), k=1 + a % 4))
        where = [None, Term("tag", "==", b"x"), Term("tag", "==", "b'x'"), Term("s", "==", "x")][b % 4]
        return VectorSearch(vec, near, where)
    if node == 7:
        near = Term("vec", "near[]", NearQuery((1.0, float(a % 3)), k=3))
        return vec.filtered_plan(near, Term("tag", "==", TAGS[b % 3]))
    if node == 8:
        return Limit(src, a % 4, b % 3)
    return Filter(src, Term(["k", "v"][a % 2], ">=", b % 3))


def _in_context(context, fn):
    if context is None:
        return fn()
    with decimal.localcontext(**context):
        return fn()


plan_recipes = st.tuples(st.integers(0, 4), st.integers(0, 4), st.integers(0, 9), st.integers(0, 5),
                         st.integers(0, 5), st.sampled_from(range(len(CONTEXTS))))


@settings(max_examples=120, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(initial=st.lists(rich_records(), min_size=4, max_size=18),
       bases=st.lists(plan_recipes, min_size=2, max_size=8))
def test_plan_families_with_equal_keys_give_equal_results(initial, bases):
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")

    def seed(tx):
        coll = tx.new_list().add_index(RangeIndex("k", "k")).add_index(EqualityIndex("s", "s")) \
            .add_index(VectorIndex("vec", "vec", dim=2, metric="l2"))
        tx.set_root_object("c", coll.extend(initial))
    db.run(seed)
    with db.new_transaction() as tx:
        coll = tx.get_root_object("c")
        by_key = {}
        # Every base recipe with its neighbours differing in exactly one parameter (a join kind,
        # union or intersection, a vector predicate, an aggregation, a decimal context...).
        recipes = [base[:i] + (base[i] + d,) + base[i + 1:] for base in bases
                   for i in (3, 4) for d in (0, 1, 2, 3)]
        recipes += [base[:5] + (c,) for base in bases for c in range(len(CONTEXTS))]
        for recipe in recipes:
            plan = _build(coll, recipe)
            context = CONTEXTS[recipe[5]]
            key, reason = _in_context(context, lambda: cache_key(plan))
            assert key is not None, reason
            fresh = _in_context(context, lambda: _exact(list(plan.execute())))
            assert by_key.setdefault(key, (fresh, recipe))[0] == fresh, (recipe, by_key[key][1], plan.explain())
            assert _in_context(context, lambda: _exact(space.query_cache.result(plan))) == fresh, recipe
