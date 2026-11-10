"""Child-process checks of the hnsw vector backend (run by tests/unit/test_vector_index.py).

Importing hnswlib re-enables the GIL for the whole process on CPython 3.14t,
which would silently turn every later free-threading test of the suite into a
GIL test.  The parent test process therefore never imports hnswlib: each check
below runs in a fresh interpreter (``python -m tests.unit.hnsw_checks <check>``),
prints what it measured and exits non-zero on failure.  The module name has no
``test_`` prefix, so pytest does not collect it.

Checks:

* ``no-import`` -- importing the vector modules (and building/validating an
  hnsw index) does not import hnswlib, and the GIL stays as it was.
* ``recall`` -- recall@10 of the hnsw backend against the exact backend on
  2000 random 32-dimensional vectors (prints ``recall=<value>`` and latencies).
* ``contract`` -- the IndexContract checks for the hnsw backend (results
  compared with the brute-force oracle).
* ``concurrency`` -- 8 threads querying one version and a loaded stub (one
  cached graph), with and without residual predicates, agree with the oracle.
"""
import random
import sys
import threading
import time


def check_no_import():
    gil_before = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True
    from proto_db.core.dbobject import DBObject
    from proto_db.indexing import vector, vector_backends  # noqa: F401
    from proto_db.indexing.vector import VectorIndex
    elements = [DBObject(vec=(float(i), 1.0)) for i in range(10)]
    index = VectorIndex("v", "vec", 2, backend="hnsw").build(elements)
    index.validate(elements)
    index.explain()
    assert "hnswlib" not in sys.modules, "hnswlib was imported without a search"
    gil_after = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True
    assert gil_after == gil_before, "the GIL state changed"
    print(f"ok gil={gil_after}")


def _vectors(rng, n, dim):
    return [tuple(rng.gauss(0.0, 1.0) for _ in range(dim)) for _ in range(n)]


def check_recall():
    from proto_db.core.dbobject import DBObject
    from proto_db.indexing.vector import VectorIndex
    from proto_db.query.terms import NEAR, NearQuery, Term
    rng = random.Random(2026)
    dim, n, queries, k = 32, 2000, 100, 10
    elements = [DBObject(i=i, vec=v) for i, v in enumerate(_vectors(rng, n, dim))]
    report = []
    for metric in ("l2", "cosine"):
        exact = VectorIndex("e", "vec", dim, metric=metric).build(elements)
        hnsw = VectorIndex("h", "vec", dim, metric=metric, backend="hnsw").build(elements)
        ivf = VectorIndex("i", "vec", dim, metric=metric, backend="ivf_flat", nlist=32, nprobe=8).build(elements)
        qs = [Term("vec", NEAR, NearQuery(v, k=k)) for v in _vectors(rng, queries, dim)]
        t0 = time.perf_counter()
        list(hnsw.plan(qs[0]).execute())  # graph build
        build = time.perf_counter() - t0
        timings, recalls = {}, {}
        truth = None
        for name, index in (("exact", exact), ("hnsw", hnsw), ("ivf_flat", ivf)):
            t0 = time.perf_counter()
            answers = [[e.i for e in index.plan(q).execute()] for q in qs]
            timings[name] = (time.perf_counter() - t0) / queries * 1000
            if truth is None:
                truth = answers
            recalls[name] = sum(len(set(a) & set(t)) for a, t in zip(answers, truth)) / (k * queries)
            assert all(len(a) == k for a in answers)
        report.append((metric, recalls, timings, build))
        if metric == "l2":
            print(f"recall={recalls['hnsw']:.4f}")
    for metric, recalls, timings, build in report:
        print(f"metric={metric} graph_build_s={build:.3f} " + " ".join(
            f"{name}: recall@10={recalls[name]:.4f} ms/query={timings[name]:.3f}" for name in recalls))
    worst = min(r["hnsw"] for _, r, _, _ in report)
    assert worst >= 0.95, f"hnsw recall@10 {worst} < 0.95"


def check_contract():
    from proto_db.indexing.vector import VectorIndex
    from tests.contracts.index_contract import IndexContract, SEEDS
    from tests.contracts.index_contract import vector_contract
    for metric in ("cosine", "l2", "ip"):
        contract = vector_contract(f"VectorIndex-hnsw-{metric}", metric, "hnsw", {})
        assert isinstance(contract, IndexContract)
        for check in contract.checks():
            for seed in SEEDS:
                if check == "check_collections" and seed > 2:
                    continue  # every registered collection is exercised; three seeds keep the child fast
                getattr(contract, check)(seed)
    assert VectorIndex is not None
    print("ok")


def check_concurrency():
    from proto_db.core.dbobject import DBObject
    from proto_db.indexing import KeySpec, element_identity
    from proto_db.indexing.vector import VectorIndex, near_oracle
    from proto_db.query.terms import EQ, NEAR, NearQuery, Term
    from proto_db.storage.memory import MemoryStorage
    from tests.support.persistence import DirectContext
    rng = random.Random(5)
    elements = [DBObject(i=i, g=i % 4, vec=v) for i, v in enumerate(_vectors(rng, 600, 8))]
    index = VectorIndex("h", "vec", 8, metric="l2", backend="hnsw", ef_search=128).build(elements)
    context = DirectContext(MemoryStorage())
    stub = context.load(context.save(index), VectorIndex.__type_name__)
    pred = Term("g", EQ, 1)
    queries = [NearQuery(v, k=rng.choice([1, 5, 10])) for v in _vectors(rng, 10, 8)]
    jobs = [(Term("vec", NEAR, q), None) for q in queries] + [(Term("vec", NEAR, q), pred) for q in queries]
    for version in (index, stub):
        barrier = threading.Barrier(8)
        outputs, errors = [None] * 8, []

        def read(slot, version=version, barrier=barrier, outputs=outputs, errors=errors):
            barrier.wait()
            try:
                out = []
                for term, where in jobs:
                    plan = version.plan(term) if where is None else version.filtered_plan(term, where)
                    out.append([element_identity(e) for e in plan.execute()])
                outputs[slot] = out
            except BaseException as e:  # reported below
                errors.append(e)
                raise

        threads = [threading.Thread(target=read, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, errors
        assert all(out == outputs[0] for out in outputs), "concurrent readers disagree"
        hits = total = 0
        for (term, where), got in zip(jobs, outputs[0]):
            truth = [element_identity(e) for e in
                     near_oracle(elements, KeySpec.path("vec"), term.value, "l2", where)]
            assert len(got) == len(truth)
            if where is not None:
                assert all(e.g == 1 for e in version.filtered_plan(term, where).execute())
            hits += len(set(got) & set(truth))
            total += len(truth)
        assert hits / total >= 0.95, f"recall under concurrency {hits / total}"
    print("ok")


CHECKS = {"no-import": check_no_import, "recall": check_recall, "contract": check_contract,
          "concurrency": check_concurrency}

if __name__ == "__main__":
    CHECKS[sys.argv[1]]()
