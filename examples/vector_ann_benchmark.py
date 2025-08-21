#!/usr/bin/env python3
"""
Vector ANN Benchmark for ProtoBase

This script benchmarks similarity search performance for ProtoBase's vector indexes.
It measures build time and query latency for:
- ExactVectorIndex (baseline, exact brute-force)
- HNSWVectorIndex (if hnswlib is installed)
- NumPy brute-force baseline (if numpy is installed)
- Optional: scikit-learn NearestNeighbors (if installed)

It emits a JSON report with timings and configuration.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from typing import List, Tuple

# Ensure parent directory is on path to import proto_db when running from examples
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto_db.vectors import Vector
from proto_db.vector_index import ExactVectorIndex

# Optional backends
try:
    import numpy as np  # type: ignore
except Exception:  # pragma: no cover
    np = None  # type: ignore

try:
    from proto_db.vector_index import HNSWVectorIndex
    _HNSW_AVAILABLE = True
except Exception:  # pragma: no cover
    _HNSW_AVAILABLE = False

try:
    from sklearn.neighbors import NearestNeighbors  # type: ignore
    _SKLEARN_AVAILABLE = True
except Exception:  # pragma: no cover
    _SKLEARN_AVAILABLE = False


def make_dataset(n: int, dim: int, seed: int = 42) -> Tuple[List[Vector], List[str]]:
    random.seed(seed)
    if np is not None:
        rng = np.random.default_rng(seed)
        data = rng.standard_normal((n, dim), dtype=np.float32)
        vecs = [Vector.from_list(row.tolist(), normalize=True) for row in data]
    else:
        # pure Python fallback
        vecs = []
        for _ in range(n):
            row = [random.random() for _ in range(dim)]
            vecs.append(Vector.from_list(row, normalize=True))
    ids = [f"id_{i}" for i in range(n)]
    return vecs, ids


def bench_build_exact(vecs: List[Vector], ids: List[str], metric: str) -> Tuple[ExactVectorIndex, float]:
    idx = ExactVectorIndex(metric=metric)
    t0 = time.time()
    idx.build(vecs, ids, metric=metric)
    return idx, time.time() - t0


def bench_build_hnsw(vecs: List[Vector], ids: List[str], metric: str, M: int, efC: int, efS: int):
    if not _HNSW_AVAILABLE:
        return None, None
    idx = HNSWVectorIndex(metric=metric, M=M, efConstruction=efC, efSearch=efS)
    t0 = time.time()
    idx.build(vecs, ids, metric=metric, params={'M': M, 'efConstruction': efC, 'efSearch': efS})
    return idx, time.time() - t0


def _latency_stats(latencies: List[float]) -> tuple[float, float, float]:
    if not latencies:
        return (None, None, None)  # type: ignore
    import math
    ms = [t * 1000.0 for t in latencies]
    ms_sorted = sorted(ms)
    p50 = ms_sorted[int(0.50 * (len(ms_sorted) - 1))]
    p95 = ms_sorted[int(0.95 * (len(ms_sorted) - 1))]
    avg = sum(ms) / len(ms)
    # std of ms
    mean = avg
    var = sum((x - mean) ** 2 for x in ms) / len(ms)
    std = math.sqrt(var)
    return avg, p50, p95, std  # type: ignore


def bench_knn_queries(idx, queries: List[Vector], k: int, metric: str, label: str, warmup: int = 5) -> dict:
    # Warm-up
    for q in queries[:warmup]:
        try:
            _ = idx.search(q, k=k, metric=metric)
        except Exception:
            pass
    # Batch if supported
    times = []
    t0 = time.time()
    if hasattr(idx, 'search_batch'):
        _ = idx.search_batch(queries, k=k, metric=metric)
        times.append(time.time() - t0)
    else:
        for q in queries:
            t1 = time.time()
            _ = idx.search(q, k=k, metric=metric)
            times.append(time.time() - t1)
    total = sum(times)
    avg, p50, p95, std = _latency_stats(times if len(times) > 1 else [total/len(queries) if queries else 0.0])
    qps = (len(queries) / total) if total > 0 else None
    return {"label": label, "avg_ms": avg, "p50_ms": p50, "p95_ms": p95, "std_ms": std, "qps": qps}


def bench_numpy_bruteforce(vecs: List[Vector], queries: List[Vector], k: int) -> dict:
    if np is None:
        return {"label": "numpy_bruteforce", "avg_ms": None, "p50_ms": None, "p95_ms": None, "std_ms": None, "qps": None}
    # Prepare matrix
    A = np.array([list(v.data) for v in vecs], dtype=np.float32)  # (n, d)
    Q = np.array([list(q.data) for q in queries], dtype=np.float32)
    # Warm-up
    if Q.shape[0] > 0:
        _ = A @ Q[0]
    t0 = time.time()
    # Batch compute all similarities
    sims = Q @ A.T  # (m, n)
    # Extract top-k per row (measure as one batched operation)
    if k < sims.shape[1]:
        idx = np.argpartition(-sims, k-1, axis=1)[:, :k]
        # stable sort within top-k indexes
        row_indices = np.arange(sims.shape[0])[:, None]
        top_vals = sims[row_indices, idx]
        order = np.argsort(-top_vals, axis=1)
        _ = idx[row_indices, order]
    else:
        _ = np.argsort(-sims, axis=1)
    total = time.time() - t0
    avg, p50, p95, std = _latency_stats([total])
    qps = (len(queries) / total) if total > 0 else None
    return {"label": "numpy_bruteforce", "avg_ms": avg, "p50_ms": p50, "p95_ms": p95, "std_ms": std, "qps": qps}


def bench_sklearn(vecs: List[Vector], queries: List[Vector], k: int) -> dict:
    if not _SKLEARN_AVAILABLE:
        return {"label": "sklearn_nn", "avg_ms": None, "p95_ms": None}
    X = [list(v.data) for v in vecs]
    nn = NearestNeighbors(n_neighbors=k, algorithm='auto', metric='cosine')
    t0 = time.time()
    nn.fit(X)
    build_time = time.time() - t0
    times = []
    for q in queries:
        t1 = time.time()
        _ = nn.kneighbors([list(q.data)], n_neighbors=k, return_distance=True)
        times.append(time.time() - t1)
    times_ms = [t * 1000.0 for t in times]
    times_ms.sort()
    p95 = times_ms[int(0.95 * (len(times_ms) - 1))] if times_ms else None
    return {"label": "sklearn_nn", "avg_ms": (sum(times_ms)/len(times_ms) if times_ms else None), "p95_ms": p95, "build_s": build_time}


def run_benchmark(n: int, dim: int, n_queries: int, k: int, metric: str,
                  M: int, efC: int, efS: int) -> dict:
    vecs, ids = make_dataset(n, dim)
    # pick queries randomly
    queries = random.sample(vecs, min(n_queries, len(vecs)))

    results = {"config": {"n": n, "dim": dim, "n_queries": n_queries, "k": k, "metric": metric,
                            "hnsw_params": {"M": M, "efConstruction": efC, "efSearch": efS},
                            "env": {
                                "numpy": bool(np is not None),
                                "hnswlib": _HNSW_AVAILABLE,
                                "sklearn": _SKLEARN_AVAILABLE
                            }}}

    # Exact index
    exact_idx, exact_build = bench_build_exact(vecs, ids, metric)
    results["build_seconds_exact"] = exact_build
    exact_stats = bench_knn_queries(exact_idx, queries, k, metric, "exact_index")
    results.setdefault("queries", []).append(exact_stats)

    # HNSW (if available)
    hnsw_stats = None
    hnsw_idx = None
    if _HNSW_AVAILABLE:
        hnsw_idx, hnsw_build = bench_build_hnsw(vecs, ids, metric, M, efC, efS)
        results["build_seconds_hnsw"] = hnsw_build
        if hnsw_idx is not None:
            hnsw_stats = bench_knn_queries(hnsw_idx, queries, k, metric, "hnsw_index")
            results["queries"].append(hnsw_stats)

    # NumPy brute force (if available)
    results["queries"].append(bench_numpy_bruteforce(vecs, queries, k))

    # scikit-learn baseline (optional)
    results["queries"].append(bench_sklearn(vecs, queries, k))

    # Recall@k if HNSW present: compare against exact
    try:
        # Compute exact top-k ids using batch if available
        if hasattr(exact_idx, 'search_batch'):
            exact_res = exact_idx.search_batch(queries, k=k, metric=metric)
        else:
            exact_res = [exact_idx.search(q, k=k, metric=metric) for q in queries]
        if hnsw_idx is not None:
            if hasattr(hnsw_idx, 'search_batch'):
                hnsw_res = hnsw_idx.search_batch(queries, k=k, metric=metric)
            else:
                hnsw_res = [hnsw_idx.search(q, k=k, metric=metric) for q in queries]
            # compute recall
            recs = []
            for a, b in zip(exact_res, hnsw_res):
                a_ids = {i for (i, _) in a}
                b_ids = {i for (i, _) in b}
                denom = max(1, len(a_ids))
                recs.append(len(a_ids & b_ids) / denom)
            recall = sum(recs) / len(recs) if recs else None
            results.setdefault("metrics", {})["recall_at_k"] = recall
    except Exception:
        pass

    # Derive speedups if both hnsw and exact are present
    try:
        exact_avg = exact_stats.get("avg_ms") if exact_stats else None
        hnsw_avg = hnsw_stats.get("avg_ms") if hnsw_stats else None
        if exact_avg and hnsw_avg:
            results["speedups"] = {"hnsw_vs_exact": exact_avg / hnsw_avg if hnsw_avg > 0 else None}
    except Exception:
        pass

    return results


def main():
    p = argparse.ArgumentParser(description="Vector ANN benchmark for ProtoBase")
    p.add_argument('--n', type=int, default=20000, help='Number of vectors')
    p.add_argument('--dim', type=int, default=128, help='Vector dimension')
    p.add_argument('--queries', type=int, default=100, help='Number of KNN queries to run')
    p.add_argument('--k', type=int, default=10, help='Top-k for KNN')
    p.add_argument('--metric', type=str, default='cosine', choices=['cosine', 'l2'])
    p.add_argument('--M', type=int, default=16, help='HNSW M parameter')
    p.add_argument('--efC', type=int, default=200, help='HNSW efConstruction')
    p.add_argument('--efS', type=int, default=64, help='HNSW efSearch')
    p.add_argument('--threads', type=int, default=1, help='Set OMP/MKL thread count for consistency')
    p.add_argument('--out', type=str, default='examples/benchmark_results_vectors.json')
    args = p.parse_args()

    # Control threads to avoid oversubscription
    os.environ['OMP_NUM_THREADS'] = str(args.threads)
    os.environ['MKL_NUM_THREADS'] = str(args.threads)

    res = run_benchmark(n=args.n, dim=args.dim, n_queries=args.queries, k=args.k,
                        metric=args.metric, M=args.M, efC=args.efC, efS=args.efS)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(res, f, indent=2)
    print(json.dumps(res, indent=2))


if __name__ == '__main__':
    main()
