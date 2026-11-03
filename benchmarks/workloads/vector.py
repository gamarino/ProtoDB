"""Vector kNN latency, build time and recall by backend, checked against brute force.

Data: ``n`` vectors drawn around ``clusters`` Gaussian centres in ``dim`` dimensions (a
seeded generator), queries are fresh points around the same centres.  The reference
answer is a numpy brute-force top-k (cosine).  ``exact`` must reproduce it exactly
(recall 1.0); approximate backends report recall@k and fail the run only below a sanity
floor (``min_recall``) that would indicate a broken index, not a slow one.

hnswlib re-enables the GIL for the whole process when imported on a free-threaded
interpreter, so the ``hnsw`` backend is a separate, isolated workload
(``vector_knn_hnsw``) that runs in its own child interpreter.
"""
import random

from benchmarks.harness import BenchResult, benchmark, measure


def _dataset(n: int, dim: int, clusters: int, queries: int, seed: int = 51):
    rng = random.Random(seed)
    centres = [[rng.gauss(0, 1) for _ in range(dim)] for _ in range(clusters)]

    def point():
        c = centres[rng.randrange(clusters)]
        return tuple(x + rng.gauss(0, 0.35) for x in c)

    return [point() for _ in range(n)], [point() for _ in range(queries)]


def _brute_force(vectors, queries, k):
    import numpy as np
    m = np.asarray(vectors, dtype=np.float64)
    m /= np.linalg.norm(m, axis=1, keepdims=True)
    out = []
    for q in queries:
        qv = np.asarray(q, dtype=np.float64)
        scores = m @ (qv / np.linalg.norm(qv))
        top = np.argsort(-scores, kind="stable")[:k]
        out.append([int(i) for i in top])
    return out


def _vector_workload(ctx, p, backends):
    import numpy as np  # noqa: F401  (fails clearly when numpy is missing)

    from proto_db import F, MemoryStorage, ObjectSpace, VectorIndex, from_collection
    vectors, queries = _dataset(p["n"], p["dim"], p["clusters"], p["queries"])
    k = p["k"]
    truth = _brute_force(vectors, queries, k)
    result = BenchResult({"vectors": len(vectors), "dim": p["dim"], "queries": len(queries), "k": k,
                          "metric": "cosine", "backends": {b: prm for b, prm in backends}}, None, None)
    samples, got = measure(lambda: _brute_force(vectors, queries, k), ctx.repeat, ctx.warmup)
    result.check("numpy brute force is deterministic", got == truth, "")
    result.add_timing("numpy brute force (reference)", samples, per=len(queries))
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("bench")
    with db.new_transaction() as tr:
        docs = tr.new_list()
        for i, v in enumerate(vectors):
            docs = docs.append_last(tr.new_object(i=i, vec=v))
        tr.set_root_object("docs", docs)
    tr = db.new_transaction()
    docs = tr.get_root_object("docs")
    recalls = {}
    for backend, params in backends:
        def build(backend=backend, params=params):
            return docs.add_index(VectorIndex("by_vec", "vec", p["dim"], metric="cosine", backend=backend, **params))
        samples, coll = measure(build, 1, 0)
        result.add_timing(f"{backend}: build index over {len(vectors)} vectors", samples, per=len(vectors))

        def search(coll=coll):
            return [[d.i for d in from_collection(coll).where(F.vec.near(q, k=k)).to_list()] for q in queries]

        samples, _ = measure(lambda coll=coll: from_collection(coll).where(F.vec.near(queries[0], k=k)).to_list(),
                             1, 0)
        result.add_timing(f"{backend}: first query after build (derived structures)", samples)
        plan = from_collection(coll).where(F.vec.near(queries[0], k=k)).explain()
        result.check(f"{backend}: planner uses the vector index",
                     plan["plan"] == "VectorSearch" and plan["index"]["backend"] == backend, str(plan.get("plan")))
        samples, got = measure(search, ctx.repeat, ctx.warmup)
        result.add_timing(f"{backend}: kNN query (k={k})", samples, per=len(queries))
        hits = sum(len(set(g) & set(t)) for g, t in zip(got, truth))
        recall = hits / (k * len(queries))
        recalls[backend] = round(recall, 4)
        if backend == "exact":
            result.check("exact: identical to brute force", got == truth, f"recall {recall:.4f}")
        else:
            result.check(f"{backend}: recall@{k} >= {p['min_recall']}", recall >= p["min_recall"],
                         f"recall {recall:.4f}")
        result.check(f"{backend}: k results per query", all(len(g) == k for g in got), "")
    result.metrics[f"recall@{k} vs brute force"] = recalls
    result.checksum = result.expected = {b: len(queries) for b, _ in backends}
    tr.abort()
    space.close()
    return result


_QUICK = {"n": 400, "dim": 8, "clusters": 8, "queries": 10, "k": 5, "min_recall": 0.5}
_FULL = {"n": 10_000, "dim": 32, "clusters": 50, "queries": 100, "k": 10, "min_recall": 0.8}


@benchmark("vector_knn", quick=_QUICK, full=_FULL)
def vector_knn(ctx, p):
    """kNN latency, build time and recall: exact and ivf_flat backends vs numpy brute force."""
    nlist = max(4, int(p["n"] ** 0.5))
    return _vector_workload(ctx, p, [("exact", {}), ("ivf_flat", {"nlist": nlist, "nprobe": max(2, nlist // 10)})])


@benchmark("vector_knn_hnsw", quick=_QUICK, full=_FULL, isolate=True)
def vector_knn_hnsw(ctx, p):
    """kNN latency, build time and recall of the hnsw backend (isolated: hnswlib enables the GIL)."""
    try:
        import hnswlib  # noqa: F401
    except ImportError:
        return BenchResult.pending("hnswlib is not installed")
    result = _vector_workload(ctx, p, [("hnsw", {})])
    result.notes.append("hnswlib re-enables the GIL when imported on a free-threaded build; this workload "
                        "ran in its own interpreter")
    return result
