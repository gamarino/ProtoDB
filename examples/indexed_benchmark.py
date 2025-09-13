import json
import os
import random
import time
import uuid
from datetime import datetime

import sys
# Ensure parent directory is on path to import proto_db when running from examples
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from proto_db.db_access import ObjectSpace
from proto_db.memory_storage import MemoryStorage
from proto_db.queries import ListPlan, FromPlan, WherePlan, Expression, SelectPlan


CATEGORIES = ["category1", "category2", "category3", "category4", "category5"]
STATUSES = ["active", "inactive", "pending", "archived"]


def make_item(i: int) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "name": f"Item-{i}",
        "value": random.randint(1, 100000),
        "category": random.choice(CATEGORIES),
        "status": random.choice(STATUSES),
        "created_at": datetime.now().isoformat(),
        "tags": [f"tag{j}" for j in range(random.randint(1, 5))],
    }


def build_dataset(n: int) -> list[dict]:
    random.seed(42)
    return [make_item(i) for i in range(n)]


def bench(fn, repeat=1):
    best = None
    for _ in range(repeat):
        t0 = time.time()
        fn()
        dt = time.time() - t0
        best = dt if best is None else min(best, dt)
    return best or 0.0


def percentile(xs, p):
    if not xs:
        return 0.0
    xs = sorted(xs)
    k = (len(xs)-1) * p
    f = int(k)
    c = min(f+1, len(xs)-1)
    if f == c:
        return xs[f]
    return xs[f] + (xs[c]-xs[f]) * (k - f)


def time_queries(fn, n=50, warmup=5):
    # Warmup
    for _ in range(max(0, warmup)):
        fn()
    lat = []
    t0 = time.time()
    for _ in range(n):
        t1 = time.time()
        fn()
        lat.append((time.time() - t1) * 1000.0)
    total = time.time() - t0
    qps = n / total if total > 0 else 0.0
    stats = {
        'total_seconds': total,
        'avg_ms': sum(lat)/len(lat) if lat else 0.0,
        'p50_ms': percentile(lat, 0.50),
        'p95_ms': percentile(lat, 0.95),
        'p99_ms': percentile(lat, 0.99),
        'qps': qps,
    }
    return stats


def run_benchmark(n_items=1000, n_queries=50, out_path="examples/benchmark_results_indexed.json", window=100, warmup=10, n_categories=50, n_statuses=20):
    global CATEGORIES, STATUSES
    # Generate domains with higher cardinality to increase selectivity for equality predicates
    CATEGORIES = [f"category{i+1}" for i in range(max(1, n_categories))]
    STATUSES = [f"status{i+1}" for i in range(max(1, n_statuses))]
    data = build_dataset(n_items)

    # Baseline competitor: pure Python list comprehension (filter by two fields + range)
    def py_query_once():
        cat = random.choice(CATEGORIES)
        st = random.choice(STATUSES)
        lo = random.randint(1, max(2, 100000 - window - 1))
        hi = lo + window
        return [r for r in data if (r.get('category') == cat and r.get('status') == st and lo < r.get('value', 0) < hi)]

    # Baseline PK lookup: find a single record by its id using list comprehension
    def py_query_single_item():
        # Choose an existing id to avoid misses with UUIDs
        target_id = data[random.randrange(n_items)].get('id') if data else None
        return [r for r in data if r.get('id') == target_id]

    py_stats = time_queries(lambda: py_query_once(), n=n_queries, warmup=warmup)
    py_pk_stats = time_queries(lambda: py_query_single_item(), n=n_queries, warmup=warmup)

    # ProtoBase setup: use ListPlan over dict rows
    space = ObjectSpace(storage=MemoryStorage())
    db = space.new_database('PerfDB')
    tr = db.new_transaction()

    # Store raw list as a ListPlan base (no need to persist objects for the benchmark)
    base_plan = ListPlan(base_list=data, transaction=tr)

    # Unindexed WherePlan (linear scan) using same window as baseline
    def pb_linear_query_once():
        cat = random.choice(CATEGORIES)
        st = random.choice(STATUSES)
        lo = random.randint(1, max(2, 100000 - window - 1))
        hi = lo + window
        flt = Expression.compile(['&', ['category', '==', cat], ['status', '==', st], ['value', 'between()', lo, hi]])
        plan = WherePlan(filter=flt, based_on=base_plan, transaction=tr)
        list(plan.execute())

    def pb_linear_query_single_item():
        target_id = data[random.randrange(n_items)].get('id') if data else None
        flt = Expression.compile(['id', '==', target_id])
        plan = WherePlan(filter=flt, based_on=base_plan, transaction=tr)
        plan = plan.optimize()
        list(plan.execute())

    pb_linear_stats = time_queries(lambda: pb_linear_query_once(), n=n_queries, warmup=warmup)
    pb_linear_pk_stats = time_queries(lambda: pb_linear_query_single_item(), n=n_queries, warmup=warmup)

    # Indexed path: build indexes for r.category, r.status, r.value, and r.id over wrapped rows
    class RowWrap:
        __slots__ = ('r', '_h')
        def __init__(self, row: dict):
            self.r = row
            self._h = hash(id(row))
        def __hash__(self):
            return self._h
    wrapped_records = [RowWrap(row) for row in data]
    base_indexed_plan = ListPlan(base_list=wrapped_records, transaction=tr)
    from proto_db.dictionaries import Dictionary, RepeatedKeysDictionary
    idx_map = {}
    for fld in ('r.category', 'r.status', 'r.value', 'r.id'):
        rkd = RepeatedKeysDictionary(transaction=tr)
        alias, attr = fld.split('.', 1)
        for rec in wrapped_records:
            row = getattr(rec, alias)
            key = None if row is None else row.get(attr)
            if key is not None:
                # Use native-type keys; do not convert to strings
                rkd = rkd.set_at(key, rec)
        idx_map[fld] = rkd
    indexes_dict = Dictionary(transaction=tr)
    for k, v in idx_map.items():
        indexes_dict = indexes_dict.set_at(k, v)
    from proto_db.queries import IndexedQueryPlan
    indexed = IndexedQueryPlan(indexes=indexes_dict, based_on=base_indexed_plan, transaction=tr)

    def pb_indexed_query_once():
        cat = random.choice(CATEGORIES)
        st = random.choice(STATUSES)
        lo = random.randint(1, max(2, 100000 - window - 1))
        hi = lo + window
        flt = Expression.compile(['&', ['r.category', '==', cat], ['r.status', '==', st], ['r.value', 'between()', lo, hi]])
        plan = WherePlan(filter=flt, based_on=indexed, transaction=tr)
        plan = plan.optimize()
        list(plan.execute())

    def pb_indexed_query_single_item():
        target_id = data[random.randrange(n_items)].get('id') if data else None
        flt = Expression.compile(['r.id', '==', target_id])
        plan = WherePlan(filter=flt, based_on=indexed, transaction=tr)
        plan = plan.optimize()
        list(plan.execute())

    pb_indexed_stats = time_queries(lambda: pb_indexed_query_once(), n=n_queries, warmup=warmup)
    pb_indexed_pk_stats = time_queries(lambda: pb_indexed_query_single_item(), n=n_queries, warmup=warmup)

    results = {
        "config": {"n_items": n_items, "n_queries": n_queries, "window": window, "warmup": warmup},
        "timings_seconds": {
            "python_list_baseline": py_stats['total_seconds'],
            "protodb_linear_where": pb_linear_stats['total_seconds'],
            "protodb_indexed_where": pb_indexed_stats['total_seconds'],
            "python_list_pk_lookup": py_pk_stats['total_seconds'],
            "protodb_linear_pk_lookup": pb_linear_pk_stats['total_seconds'],
            "protodb_indexed_pk_lookup": pb_indexed_pk_stats['total_seconds'],
        },
        "latency_ms": {
            "python_list_baseline": {k: v for k, v in py_stats.items() if k != 'total_seconds'},
            "protodb_linear_where": {k: v for k, v in pb_linear_stats.items() if k != 'total_seconds'},
            "protodb_indexed_where": {k: v for k, v in pb_indexed_stats.items() if k != 'total_seconds'},
            "python_list_pk_lookup": {k: v for k, v in py_pk_stats.items() if k != 'total_seconds'},
            "protodb_linear_pk_lookup": {k: v for k, v in pb_linear_pk_stats.items() if k != 'total_seconds'},
            "protodb_indexed_pk_lookup": {k: v for k, v in pb_indexed_pk_stats.items() if k != 'total_seconds'},
        },
        "speedups": {
            "indexed_over_linear": (pb_linear_stats['total_seconds'] / pb_indexed_stats['total_seconds']) if pb_indexed_stats['total_seconds'] > 0 else None,
            "indexed_over_python": (py_stats['total_seconds'] / pb_indexed_stats['total_seconds']) if pb_indexed_stats['total_seconds'] > 0 else None,
            "indexed_pk_over_linear": (pb_linear_pk_stats['total_seconds'] / pb_indexed_pk_stats['total_seconds']) if pb_indexed_pk_stats['total_seconds'] > 0 else None,
        },
    }

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(results, f, indent=2)
    return results


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='Indexed performance benchmark for ProtoBase')
    p.add_argument('--items', type=int, default=1000)
    p.add_argument('--queries', type=int, default=50)
    p.add_argument('--window', type=int, default=100, help='numeric range window size for value field (smaller → higher selectivity)')
    p.add_argument('--warmup', type=int, default=10, help='warmup query iterations before timing')
    p.add_argument('--categories', type=int, default=50, help='number of distinct categories (equality domain cardinality)')
    p.add_argument('--statuses', type=int, default=20, help='number of distinct statuses (equality domain cardinality)')
    p.add_argument('--out', type=str, default='examples/benchmark_results_indexed.json')
    args = p.parse_args()
    res = run_benchmark(n_items=args.items, n_queries=args.queries, out_path=args.out, window=args.window, warmup=args.warmup, n_categories=args.categories, n_statuses=args.statuses)
    print(json.dumps(res, indent=2))
