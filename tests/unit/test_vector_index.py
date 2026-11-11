"""Unit tests for VectorIndex and its backends (Task 7.1, spec section 8.3, D39, ruling R27).

Catches: an exact backend whose answers differ from brute force (numpy screening
dropping a near tie, a tie broken non-deterministically, an occurrence of a
duplicate element cut off, an equal-but-different value returned in place of
the stored one); an IVF-Flat index whose full probe is not exact or whose
centroids are lost on a round trip through FileStorage; elements with an
absent or malformed vector returned by a search or lost from ``validate``;
``supports`` accepting a query of the wrong dimension or metric; a planner that
post-filters ``near(k) AND P`` (fewer than k results, R27) or that pushes a
threshold-only query's residual into the vector plan; a commit through a
Transaction losing the index; concurrent readers disturbed by the derived-structure
cache; a missing hnswlib surfacing as anything but ProtoNotSupportedError; and
the hnsw backend importing hnswlib (which re-enables the GIL on 3.14t) at
module import time.  HNSW search quality, its cache under concurrency and the
contract for the hnsw backend run in a child process (``tests/unit/hnsw_checks.py``)
so that importing hnswlib never re-enables the GIL in this test process.
"""
import math
import os
import random
import subprocess
import sys
import threading
from collections import Counter
from decimal import Decimal

import pytest

from proto_db.collections import avl
from proto_db.collections.lists import List
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoValidationError
from proto_db.indexing import KeySpec, element_identity
from proto_db.indexing.equality import EqualityIndex
from proto_db.indexing.vector import VectorIndex, near_oracle
from proto_db.indexing.vector_backends import exact_score
from proto_db.query.expressions import And, Or
from proto_db.query.linq import F, from_collection
from proto_db.query.planner import plan_query
from proto_db.query.plans import Filter, VectorSearch
from proto_db.query.terms import EQ, GE, NEAR, NearQuery, Term
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.support.persistence import DirectContext

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def ids(elements):
    return Counter((element_identity(e), type(e).__name__, repr(e) if not isinstance(e, DBObject) else 0)
                   for e in elements)


def random_vector(rng, dim, ints=False):
    if ints:
        return tuple(float(rng.randrange(-2, 3)) for _ in range(dim))
    return tuple(rng.uniform(-1.0, 1.0) for _ in range(dim))


def records(rng, n, dim, ints=False):
    return [DBObject(i=i, g=i % 3, vec=random_vector(rng, dim, ints)) for i in range(n)]


def near(vector, k=None, threshold=None, metric=None, path="vec"):
    return Term(path, NEAR, NearQuery(vector, k=k, threshold=threshold, metric=metric))


def thresholds(metric):
    return {"cosine": (-0.5, 0.0, 0.3, 0.9, 1.0), "l2": (0.0, 0.5, 1.5, 3.0), "ip": (-1.0, 0.0, 0.5, 2.0)}[metric]


def assert_same_as_oracle(index, elements, term, predicate=None):
    if predicate is None:
        got = list(index.plan(term).execute())
    else:
        got = list(index.filtered_plan(term, predicate).execute())
    expected = near_oracle(elements, index.key_spec, term.value, index.metric, predicate)
    assert ids(got) == ids(expected), f"{index.explain()} {term}"
    return got


# -- definition and supports ------------------------------------------------------------------------------

def test_definition_type_names_and_validation():
    index = VectorIndex("v", "vec", 4)
    assert index.key_spec == KeySpec.path("vec") and index.dim == 4
    assert index.metric == "cosine" and index.backend == "exact"
    assert VectorIndex.__type_name__ == "proto_db.indexing.VectorIndex"
    ivf = VectorIndex("v", "vec", 4, metric="l2", backend="ivf_flat", nlist=8, nprobe=2)
    assert dict(ivf.params)["nlist"] == 8 and dict(ivf.params)["nprobe"] == 2
    for bad in [dict(dim=0), dict(dim=True), dict(dim=2.0), dict(metric="dot"), dict(backend="annoy"),
                dict(backend="exact", nlist=3), dict(backend="ivf_flat", nlist=0),
                dict(backend="ivf_flat", nlist=4, nprobe=5), dict(backend="hnsw", M=1),
                dict(backend="hnsw", ef_search=0)]:
        kwargs = dict(dim=4)
        kwargs.update(bad)
        with pytest.raises(ProtoValidationError):
            VectorIndex("v", "vec", **kwargs)


def test_supports_only_near_on_its_key_with_matching_dimension_and_metric():
    index = VectorIndex("v", "vec", 3, metric="l2")
    assert index.supports(near((1, 2, 3), k=2))
    assert index.supports(near((1, 2, 3), k=2, metric="l2"))
    assert not index.supports(near((1, 2, 3), k=2, metric="cosine"))
    assert not index.supports(near((1, 2), k=2))
    assert not index.supports(near((1, 2, 3), k=2, path="other"))
    assert not index.supports(Term("vec", EQ, 1))
    for hook in (index.plan, index.estimate):
        with pytest.raises(ProtoNotSupportedError):
            hook(near((1, 2), k=1))
    assert index.supports_filtered(near((1, 2, 3), k=2))
    with pytest.raises(ProtoNotSupportedError):
        index.filtered_plan(near((1, 2), k=1), Term("g", EQ, 1))


# -- exact backend == brute force --------------------------------------------------------------------------

@pytest.mark.parametrize("metric", ["cosine", "l2", "ip"])
@pytest.mark.parametrize("use_numpy", [True, False])
def test_exact_backend_equals_brute_force(metric, use_numpy):
    rng = random.Random(7)
    elements = records(rng, 300, 8) + records(rng, 60, 8, ints=True)
    elements += [elements[3], elements[3], elements[200]]  # duplicates
    index = VectorIndex("v", "vec", 8, metric=metric, numpy=use_numpy).build(elements)
    for _ in range(30):
        q = rng.choice([random_vector(rng, 8), elements[rng.randrange(len(elements))].vec,
                        random_vector(rng, 8, ints=True)])
        k = rng.choice([1, 2, 5, 17, 400])
        thr = rng.choice(thresholds(metric))
        for term in (near(q, k=k), near(q, threshold=thr), near(q, k=k, threshold=thr)):
            assert_same_as_oracle(index, elements, term)


@pytest.mark.parametrize("metric", ["cosine", "l2", "ip"])
@pytest.mark.parametrize("scale", [1e-161, 1e-155, 1e-140, 1e140, 1e152])
def test_exact_backend_equals_brute_force_at_extreme_magnitudes(metric, scale):
    # Tiny cosine norms make numpy's products subnormal (inaccurate); huge ones approach overflow.
    rng = random.Random(int(math.log10(scale)) + 400)
    def vec():  # positive small multiples: many near-equal cosines, so tiny numpy errors reorder them
        return tuple(rng.choice([1, 2, 3, 5, 7]) * scale for _ in range(4))
    elements = [DBObject(i=i, vec=vec()) for i in range(200)]
    index = VectorIndex("v", "vec", 4, metric=metric).build(elements)
    assert index.explain()["vectors"] == 200
    for _ in range(25):
        q = rng.choice([vec(), elements[rng.randrange(200)].vec])
        for term in (near(q, k=rng.choice([1, 3, 10])), near(q, k=5, threshold=0.5 if metric == "cosine" else
                                                               (-1.0 if metric == "ip" else 10 * scale))):
            assert_same_as_oracle(index, elements, term)


def test_results_are_nearest_first_and_ties_follow_the_member_key():
    a, b, c = DBObject(vec=(1.0, 0.0)), DBObject(vec=(1.0, 0.0)), DBObject(vec=(0.0, 1.0))
    index = VectorIndex("v", "vec", 2, metric="l2").build([c, b, a])
    first = list(index.plan(near((1.0, 0.0), k=1)).execute())
    # a and b tie: the one with the smaller member key (oid order for atoms) wins, every time.
    expected = min((a, b), key=lambda e: e.oid.int)
    assert first == [expected]
    assert list(index.plan(near((1.0, 0.0), k=3)).execute())[2] is c


def test_duplicates_are_counted_and_equal_values_come_back_exactly():
    spec = KeySpec.element()
    one, one_f, one_d = (1.0, 0.0), (1.0, 0.0), (Decimal("1"), 0)
    elements = [one, one, (0.0, 1.0), one_d]
    index = VectorIndex("v", spec, 2, metric="l2").build(elements)
    got = list(index.plan(Term(None, NEAR, NearQuery((1.0, 0.0), k=1))).execute())
    # k counts distinct stored elements; an element held twice comes back twice (never cut, D37).
    assert len(got) in (1, 2)
    assert ids(got) == ids(near_oracle(elements, spec, NearQuery((1.0, 0.0), k=1), "l2"))
    got = list(index.plan(Term(None, NEAR, NearQuery((1.0, 0.0), k=2))).execute())
    assert Counter(map(repr, got)) == Counter({repr(one): 2, repr(one_d): 1})
    assert one_f == one  # the same tuple value: one member


def test_invalid_vectors_are_kept_aside_and_never_returned():
    good = [DBObject(vec=(1.0, 0.0)), DBObject(vec=[0.0, 1.0])]
    bad = [DBObject(), DBObject(vec=None), DBObject(vec=(1.0,)), DBObject(vec=(1.0, float("nan"))),
           DBObject(vec=(True, 0.0)), DBObject(vec="ab"), DBObject(vec=(1e200, 1e200)),
           DBObject(vec=(float("inf"), 0.0))]
    index = VectorIndex("v", "vec", 2, metric="l2").build(good + bad)
    info = index.explain()
    assert info["entries"] == 10 and info["vectors"] == 2 and info["invalid"] == 8
    got = list(index.plan(near((0.0, 0.0), k=20)).execute())
    assert ids(got) == ids(good)
    index.validate(good + bad)
    with pytest.raises(ProtoCorruptionError):
        index.validate(good)
    smaller = index.with_removed(bad[2])
    smaller.validate(good + bad[:2] + bad[3:])


def test_zero_vectors_have_cosine_similarity_zero():
    assert exact_score("cosine", (0.0, 0.0), (1.0, 2.0)) == 0.0
    assert exact_score("cosine", (1.0, 0.0), (2.0, 0.0)) == 1.0
    assert exact_score("l2", (0.0, 0.0), (3.0, 4.0)) == -5.0
    assert exact_score("ip", (1.0, 2.0), (3.0, 4.0)) == 11.0


def test_with_removed_of_an_absent_element_raises():
    index = VectorIndex("v", "vec", 2).build([DBObject(vec=(1.0, 0.0))])
    with pytest.raises(ProtoValidationError):
        index.with_removed(DBObject(vec=(1.0, 0.0)))
    with pytest.raises(ProtoValidationError):
        index.with_removed(DBObject(vec=None))


# -- ivf_flat --------------------------------------------------------------------------------------------

@pytest.mark.parametrize("metric", ["cosine", "l2", "ip"])
def test_ivf_flat_with_full_probe_equals_exact(metric):
    rng = random.Random(11)
    elements = records(rng, 400, 6)
    ivf = VectorIndex("v", "vec", 6, metric=metric, backend="ivf_flat", nlist=8, nprobe=8).build(elements)
    exact = VectorIndex("v", "vec", 6, metric=metric).build(elements)
    assert len(ivf.centroids) == 8
    for _ in range(25):
        q = random_vector(rng, 6)
        for term in (near(q, k=rng.choice([1, 10, 50])), near(q, threshold=rng.choice(thresholds(metric))),
                     near(q, k=7, threshold=rng.choice(thresholds(metric)))):
            assert ids(ivf.plan(term).execute()) == ids(exact.plan(term).execute())
            assert_same_as_oracle(ivf, elements, term)


def test_ivf_flat_trains_on_growth_and_stays_consistent():
    rng = random.Random(3)
    index = VectorIndex("v", "vec", 4, metric="l2", backend="ivf_flat", nlist=4, nprobe=4)
    elements = []
    for i in range(80):
        e = DBObject(i=i, vec=random_vector(rng, 4))
        elements.append(e)
        index = index.with_added(e)
        if i == 2:
            assert index.centroids == ()  # untrained below nlist vectors: one exhaustive list
    assert len(index.centroids) == 4 and index.trained_size >= 4
    index.validate(elements)
    for e in elements[:40]:
        index = index.with_removed(e)
    index.validate(elements[40:])
    assert_same_as_oracle(index, elements[40:], near(random_vector(rng, 4), k=9))


def test_ivf_flat_partial_probe_has_good_recall_and_widens_for_k():
    rng = random.Random(5)
    elements = records(rng, 2000, 16)
    ivf = VectorIndex("v", "vec", 16, metric="l2", backend="ivf_flat", nlist=16, nprobe=4).build(elements)
    exact = VectorIndex("v", "vec", 16, metric="l2").build(elements)
    hits = 0
    for _ in range(40):
        q = random_vector(rng, 16)
        a = list(ivf.plan(near(q, k=10)).execute())
        assert len(a) == 10
        b = list(exact.plan(near(q, k=10)).execute())
        hits += len({e.i for e in a} & {e.i for e in b})
    assert hits / 400 >= 0.6
    # A k beyond what nprobe lists hold widens the probe until k elements are found.
    assert len(list(ivf.plan(near(random_vector(rng, 16), k=1500)).execute())) == 1500


def test_ivf_flat_round_trip_through_file_storage_keeps_centroids(tmp_path):
    rng = random.Random(9)
    elements = records(rng, 120, 5)
    index = VectorIndex("v", "vec", 5, metric="cosine", backend="ivf_flat", nlist=6, nprobe=2).build(elements)
    storage = FileStorage(str(tmp_path / "s"))
    ptr = DirectContext(storage).save(index)
    storage.flush()
    storage.close()
    storage = FileStorage(str(tmp_path / "s"))
    try:
        loaded = DirectContext(storage).load(ptr, VectorIndex.__type_name__)
        assert loaded.centroids == index.centroids and len(loaded.centroids) == 6
        assert loaded.params == index.params and loaded.explain() == index.explain()
        q = random_vector(rng, 5)
        term = near(q, k=8)
        assert [e.i for e in loaded.plan(term).execute()] == [e.i for e in index.plan(term).execute()]
        loaded.validate(elements)
        extra = DBObject(i=-1, vec=q)
        grown = loaded.with_added(extra)
        assert [e.i for e in grown.plan(near(q, k=1)).execute()] == [-1]
        grown.validate(elements + [extra])
    finally:
        storage.close()


# -- validate --------------------------------------------------------------------------------------------

def test_validate_detects_structural_corruption():
    rng = random.Random(2)
    elements = records(rng, 30, 3)
    for backend, params in (("exact", {}), ("ivf_flat", {"nlist": 3, "nprobe": 1})):
        index = VectorIndex("v", "vec", 3, metric="l2", backend=backend, **params).build(elements)
        index.validate(elements)
        key, (element, count, vector) = next(avl.key_iter(index.store))
        tampered = [
            index._with(store=avl.key_put(index.store, key, (element, count, (9.0, 9.0, 9.0)))[0]),
            index._with(store=avl.key_put(index.store, key, (element, count + 1, vector))[0]),
            index._with(store=avl.key_put(index.store, key, (element, 0, vector))[0]),
            index._with(store=avl.key_remove(index.store, key)[0]),
            index._with(store=avl.key_put(index.store, (key[0] + 1, key[1]), (element, count, vector))[0]),
            index._with(total_count=index.total_count + 1),
        ]
        if backend == "ivf_flat":
            tampered.append(index._with(centroids=index.centroids[:-1]))
        for bad in tampered:
            with pytest.raises(ProtoCorruptionError):
                bad.validate(elements)


# -- residual predicates (ruling R27) ----------------------------------------------------------------------

@pytest.mark.parametrize("backend, params", [("exact", {}), ("ivf_flat", {"nlist": 8, "nprobe": 1})])
def test_near_k_and_predicate_returns_k_among_the_matching(backend, params):
    rng = random.Random(4)
    elements = records(rng, 300, 6)
    coll = List.from_iterable(elements).add_index(
        VectorIndex("vec_idx", "vec", 6, metric="l2", backend=backend, **params))
    for _ in range(10):
        q = random_vector(rng, 6)
        where = And(near(q, k=5), Term("g", EQ, 1))
        plan = plan_query(coll, where)
        assert isinstance(plan, VectorSearch)
        info = plan.explain()
        assert info["where"] == Term("g", EQ, 1).describe()
        got = list(plan.execute())
        assert len(got) == 5 and all(e.g == 1 for e in got)
        if backend == "exact":
            assert ids(got) == ids(near_oracle(elements, KeySpec.path("vec"), NearQuery(q, k=5), "l2",
                                               Term("g", EQ, 1)))
    # Fewer matches than k: every match comes back.
    where = And(near(random_vector(rng, 6), k=50), Term("i", "<", 7))
    assert sorted(e.i for e in plan_query(coll, where).execute()) == list(range(7))


def test_near_k_with_threshold_and_predicate_and_threshold_only_post_filter():
    rng = random.Random(8)
    elements = records(rng, 200, 4)
    coll = List.from_iterable(elements).add_index(VectorIndex("vec_idx", "vec", 4, metric="l2"))
    q = random_vector(rng, 4)
    pred = Term("g", EQ, 2)
    plan = plan_query(coll, And(near(q, k=6, threshold=1.0), pred))
    assert isinstance(plan, VectorSearch)
    assert ids(plan.execute()) == ids(near_oracle(elements, KeySpec.path("vec"),
                                                  NearQuery(q, k=6, threshold=1.0), "l2", pred))
    plan = plan_query(coll, And(near(q, threshold=1.0), pred))
    assert isinstance(plan, Filter) and isinstance(plan.source, VectorSearch)
    assert ids(plan.execute()) == ids(near_oracle(elements, KeySpec.path("vec"),
                                                  NearQuery(q, threshold=1.0), "l2", pred))


def test_several_near_terms_and_disjunctions_keep_set_semantics():
    rng = random.Random(6)
    elements = records(rng, 100, 3)
    coll = List.from_iterable(elements).add_index(VectorIndex("vec_idx", "vec", 3, metric="l2"))
    q1, q2 = random_vector(rng, 3), random_vector(rng, 3)
    plan = plan_query(coll, And(near(q1, k=30), near(q2, k=30), Term("g", EQ, 0)))
    assert plan.explain()["plan"] == "Filter" and plan.explain()["children"][0]["plan"] == "Intersect"
    a = {e.i for e in near_oracle(elements, KeySpec.path("vec"), NearQuery(q1, k=30), "l2")}
    b = {e.i for e in near_oracle(elements, KeySpec.path("vec"), NearQuery(q2, k=30), "l2")}
    assert {e.i for e in plan.execute()} == {i for i in a & b if i % 3 == 0}
    plan = plan_query(coll, Or(And(near(q1, k=3), Term("g", EQ, 1)), Term("i", EQ, 99)))
    assert plan.explain()["plan"] == "Union"
    got = {e.i for e in plan.execute()}
    assert 99 in got and len([i for i in got if i % 3 == 1 and i != 99]) == 3


def test_oracle_with_predicate_callable_and_expression_agree():
    rng = random.Random(1)
    elements = records(rng, 50, 3)
    q = NearQuery(random_vector(rng, 3), k=4)
    by_expr = near_oracle(elements, KeySpec.path("vec"), q, "ip", Term("g", EQ, 0))
    by_fn = near_oracle(elements, KeySpec.path("vec"), q, "ip", lambda e: e.g == 0)
    assert ids(by_expr) == ids(by_fn) and len(by_expr) == 4


# -- transactions, LINQ, reopen ------------------------------------------------------------------------------

def test_commit_through_a_transaction_and_query_after_reopen(tmp_path):
    rng = random.Random(12)
    path = str(tmp_path / "space")
    vectors = [random_vector(rng, 4) for _ in range(60)]
    space = ObjectSpace(FileStorage(path))
    db = space.new_database("vecs")
    with db.new_transaction() as tr:
        coll = tr.new_list().add_index(VectorIndex("by_vec", "vec", 4, metric="l2")) \
            .add_index(VectorIndex("by_vec_ivf", "vec", 4, metric="cosine", backend="ivf_flat", nlist=4, nprobe=4))
        for i, v in enumerate(vectors[:40]):
            coll = coll.append_last(tr.new_object(i=i, g=i % 2, vec=v))
        tr.set_root_object("docs", coll)
    with db.new_transaction() as tr:
        coll = tr.get_root_object("docs")
        for i, v in enumerate(vectors[40:], start=40):
            coll = coll.append_last(tr.new_object(i=i, g=i % 2, vec=v))
        tr.set_root_object("docs", coll)
    space.close()

    space = ObjectSpace(FileStorage(path))
    try:
        tr = space.open_database("vecs").new_transaction()
        coll = tr.get_root_object("docs")
        elements = list(coll.elements())
        assert len(elements) == 60
        coll.get_index("by_vec").validate(elements)
        coll.get_index("by_vec_ivf").validate(elements)
        q = vectors[17]
        got = [e.i for e in plan_query(coll, near(q, k=3, metric="l2")).execute()]
        assert got[0] == 17 and len(got) == 3
        assert got == [e.i for e in near_oracle(elements, KeySpec.path("vec"), NearQuery(q, k=3), "l2")]
        linq = from_collection(coll).where(F.vec.near(list(q), k=4, metric="l2") & (F.g == 0)).to_list()
        assert len(linq) == 4 and all(e.g == 0 for e in linq)
        cos = [e.i for e in from_collection(coll).where(F.vec.near(list(q), k=2, metric="cosine")).to_list()]
        assert cos == [e.i for e in near_oracle(elements, KeySpec.path("vec"), NearQuery(q, k=2), "cosine")]
        tr.abort()
    finally:
        space.close()


def test_linq_near_k_and_lambda_is_k_nearest_among_the_lambda_matches():
    rng = random.Random(13)
    elements = records(rng, 200, 4)
    coll = List.from_iterable(elements).add_index(VectorIndex("by_vec", "vec", 4, metric="l2"))
    for _ in range(5):
        q = random_vector(rng, 4)
        query = from_collection(coll).where(F.vec.near(list(q), k=4) & (lambda e: e.g == 0))
        got = query.to_list()
        assert len(got) == 4 and all(e.g == 0 for e in got)
        expected = near_oracle(elements, KeySpec.path("vec"), NearQuery(q, k=4), "l2", lambda e: e.g == 0)
        assert ids(got) == ids(expected)
        info = query.explain()
        assert info["plan"] == "VectorSearch" and "python" in str(info["where"])
    # pure and impure parts together: both restrict the search
    q = random_vector(rng, 4)
    got = from_collection(coll).where(F.vec.near(list(q), k=3) & (F.i < 100) & (lambda e: e.g == 1)).to_list()
    assert ids(got) == ids(near_oracle(elements, KeySpec.path("vec"), NearQuery(q, k=3), "l2",
                                       lambda e: e.i < 100 and e.g == 1))
    # a threshold-only near keeps the lambda as a PythonFilter on top (exactly equivalent)
    query = from_collection(coll).where(F.vec.near(list(q), threshold=1.0) & (lambda e: e.g == 1))
    assert query.explain()["plan"] == "PythonFilter"


def test_python_predicate_expression_node():
    from proto_db.query.expressions import PythonPredicate, evaluate, iter_terms
    from proto_db.query.planner import normalize
    from proto_db.query.expressions import Not
    fn = lambda e: e.g == 1  # noqa: E731
    p = PythonPredicate(fn, "g is one")
    assert p == PythonPredicate(fn, "g is one") and hash(p) == hash(PythonPredicate(fn, "g is one"))
    assert p != PythonPredicate(lambda e: e.g == 1, "g is one")
    assert p.describe() == {"python": "g is one"} and list(iter_terms(p)) == []
    assert evaluate(p, DBObject(g=1)) and not evaluate(Not(p), DBObject(g=1))
    assert normalize(Not(And(p, Term("x", EQ, 1)))) == Or(Not(p), Not(Term("x", EQ, 1)))
    with pytest.raises(ProtoNotSupportedError):
        p.to_list()
    for bad in [(3, "x"), (fn, ""), (Term("x", EQ, 1), "x")]:
        with pytest.raises(ProtoValidationError):
            PythonPredicate(*bad)
    coll = List.from_iterable([DBObject(g=i % 3) for i in range(9)])
    assert len(list(plan_query(coll, And(Term("g", GE, 1), p)).execute())) == 3


# -- concurrency ---------------------------------------------------------------------------------------------

@pytest.mark.parametrize("backend, params", [("exact", {}), ("exact", {"numpy": False}),
                                             ("ivf_flat", {"nlist": 8, "nprobe": 3})])
def test_eight_threads_query_one_version(backend, params):
    rng = random.Random(10)
    elements = records(rng, 500, 8)
    index = VectorIndex("v", "vec", 8, metric="l2", backend=backend, **params).build(elements)
    context = DirectContext(MemoryStorage())
    stub = context.load(context.save(index), VectorIndex.__type_name__)
    terms = [near(random_vector(rng, 8), k=rng.choice([1, 5, 20])) for _ in range(12)]
    # The expected answers come from a separate, equal version, so the threads below race to build the
    # derived structures (snapshot) of `index` and `stub` themselves.
    twin = VectorIndex("v", "vec", 8, metric="l2", backend=backend, **params).build(elements)
    expected = {t: [element_identity(e) for e in twin.plan(t).execute()] for t in terms}
    for version in (index, stub):
        barrier = threading.Barrier(8)
        outputs, errors = [None] * 8, []

        def read(slot, version=version, barrier=barrier, outputs=outputs, errors=errors):
            barrier.wait()
            try:
                outputs[slot] = {t: [element_identity(e) for e in version.plan(t).execute()] for t in terms}
            except BaseException as e:  # reported below
                errors.append(e)
                raise

        threads = [threading.Thread(target=read, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, errors
        assert all(out == expected for out in outputs)


# -- hnsw ----------------------------------------------------------------------------------------------------

def test_missing_hnswlib_raises_not_supported_at_first_use(monkeypatch):
    if "hnswlib" in sys.modules:
        pytest.fail("hnswlib was imported into the test process (it re-enables the GIL)")
    monkeypatch.setitem(sys.modules, "hnswlib", None)  # makes `import hnswlib` raise ImportError
    rng = random.Random(0)
    elements = records(rng, 20, 4)
    index = VectorIndex("v", "vec", 4, backend="hnsw").build(elements)  # no import needed to maintain it
    index.validate(elements)
    plan = index.plan(near(random_vector(rng, 4), k=3))
    with pytest.raises(ProtoNotSupportedError, match="hnswlib") as info:
        list(plan.execute())
    assert isinstance(info.value.__cause__, ImportError)


def run_child(*args, timeout=600):
    env = dict(os.environ, PYTHONPATH=REPO)
    proc = subprocess.run([sys.executable, "-m", "tests.unit.hnsw_checks", *args], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=timeout)
    assert proc.returncode == 0, f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    return proc.stdout


def test_importing_the_vector_modules_does_not_import_hnswlib():
    out = run_child("no-import")
    assert "ok" in out


def hnswlib_available():
    env = dict(os.environ, PYTHONPATH=REPO)
    proc = subprocess.run([sys.executable, "-c", "import importlib.util, sys; "
                           "sys.exit(0 if importlib.util.find_spec('hnswlib') else 1)"], env=env)
    return proc.returncode == 0


needs_hnswlib = pytest.mark.skipif(not hnswlib_available(), reason="hnswlib is not installed")


@needs_hnswlib
def test_hnsw_recall_at_10_on_2000_random_vectors():
    out = run_child("recall")
    recall = float(out.split("recall=")[1].split()[0])
    assert recall >= 0.95, out


@needs_hnswlib
def test_hnsw_index_contract_in_a_child_process():
    assert "ok" in run_child("contract")


@needs_hnswlib
def test_hnsw_cache_under_eight_concurrent_readers_and_predicates():
    assert "ok" in run_child("concurrency")


# -- the vector contract has teeth ---------------------------------------------------------------------------

class OnceVectorIndex(VectorIndex):
    """Defect: returns each member once, ignoring how many times it is stored."""

    __type_name__ = "tests.unit.test_vector_index.OnceVectorIndex"

    def plan(self, term):
        inner = super().plan(term)
        from proto_db.query.plans import IndexPlan

        def produce():
            seen = set()
            for e in inner.execute():
                key = (element_identity(e), repr(e) if not isinstance(e, DBObject) else 0)
                if key not in seen:
                    seen.add(key)
                    yield e

        return IndexPlan(self, term, produce, kind="Broken")


class MetricBlindVectorIndex(VectorIndex):
    """Defect: accepts a query naming another metric (and answers it with its own)."""

    __type_name__ = "tests.unit.test_vector_index.MetricBlindVectorIndex"

    def supports(self, term):
        return self._on_key(term) and term.op == NEAR and len(term.value.vector) == self.dim


class LastTieVectorIndex(VectorIndex):
    """Defect: breaks score ties by the largest member key instead of the smallest."""

    __type_name__ = "tests.unit.test_vector_index.LastTieVectorIndex"

    def plan(self, term):
        from proto_db.indexing.keyed import member_key
        from proto_db.indexing.vector_backends import exact_score, iter_entries, score_floor
        from proto_db.query.plans import IndexPlan
        from proto_db.values import order_key
        q, k, metric = term.value.vector, term.value.k, self.metric
        floor = score_floor(metric, term.value.threshold)
        entries = [e for e in iter_entries(self.store)
                   if floor is None or exact_score(metric, q, e[3]) >= floor]
        entries.sort(key=lambda e: order_key(member_key(e[1])), reverse=True)
        entries.sort(key=lambda e: -exact_score(metric, q, e[3]))  # stable: ties keep the reversed order

        def produce():
            for _, element, count, _ in (entries if k is None else entries[:k]):
                yield from [element] * count

        return IndexPlan(self, term, produce, kind="Broken")


@pytest.mark.parametrize("cls", [OnceVectorIndex, MetricBlindVectorIndex, LastTieVectorIndex])
def test_vector_contract_rejects_broken_variants(cls):
    import dataclasses
    from tests.contracts.index_contract import SEEDS, VECTOR_DIM, vector_contract
    caught = 0
    for metric in ("cosine", "l2"):
        contract = dataclasses.replace(
            vector_contract(f"broken-{cls.__name__}", metric, "exact", {}),
            index_factory=lambda n, ks, metric=metric: cls(n, ks, VECTOR_DIM, metric=metric))
        for seed in SEEDS:
            try:
                contract.check_plan_matches_scan(seed)
            except AssertionError:
                caught += 1
    assert caught, f"the vector contract did not catch {cls.__name__}"


def test_closing_a_space_releases_its_derived_vector_structures():
    """The process-wide cache of flat snapshots and IVF lists (D39) holds the stored elements of
    the versions it built them from; closing the space drops those entries, so nothing of a closed
    space stays reachable through process-global state (full-suite 3e07e0b, item 1)."""
    import gc
    import weakref

    from proto_db.indexing.vector_backends import DERIVED
    rng = random.Random(5)
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("vecs")
    with db.new_transaction() as tr:
        coll = tr.new_list().add_index(VectorIndex("by_vec", "vec", 4, metric="l2")) \
            .add_index(VectorIndex("by_ivf", "vec", 4, metric="cosine", backend="ivf_flat", nlist=4, nprobe=4))
        for i in range(30):
            coll = coll.append_last(tr.new_object(i=i, vec=random_vector(rng, 4)))
        tr.set_root_object("docs", coll)
    with db.new_transaction() as tr:
        coll = tr.get_root_object("docs")
        q = coll.get_at(3).vec
        assert [e.i for e in plan_query(coll, near(q, k=1, metric="l2")).execute()] == [3]
        assert len(list(from_collection(coll).where(F.vec.near(list(q), k=2, metric="cosine")))) == 2
        element = weakref.ref(coll.get_at(3))
        tr.abort()
    del coll, tr
    space.close()
    gc.collect()
    assert element() is None, "an element of a closed space is still reachable"
    assert not any(owner is space.storage for owner in DERIVED.owners())
