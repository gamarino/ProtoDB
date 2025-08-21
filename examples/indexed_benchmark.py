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


def run_benchmark(n_items=10000, n_queries=50, out_path="examples/benchmark_results_indexed.json"):
    data = build_dataset(n_items)

    # Baseline competitor: pure Python list comprehension (filter by two fields)
    def py_query():
        cat = random.choice(CATEGORIES)
        st = random.choice(STATUSES)
        lo = random.randint(1, 50000)
        hi = lo + random.randint(100, 10000)
        return [r for r in data if (r.get('category') == cat and r.get('status') == st and lo < r.get('value', 0) < hi)]

    py_time = bench(lambda: [py_query() for _ in range(n_queries)], repeat=1)

    # ProtoBase setup: use ListPlan over dict rows
    space = ObjectSpace(storage=MemoryStorage())
    db = space.new_database('PerfDB')
    tr = db.new_transaction()

    # Store raw list as a ListPlan base (no need to persist objects for the benchmark)
    base_plan = ListPlan(base_list=data, transaction=tr)

    # Unindexed WherePlan (linear scan)
    def pb_linear_query_once():
        cat = random.choice(CATEGORIES)
        st = random.choice(STATUSES)
        lo = random.randint(1, 50000)
        hi = lo + random.randint(100, 10000)
        flt = Expression.compile(['&', ['category', '==', cat], ['status', '==', st], ['value', 'between()', lo, hi]])
        plan = WherePlan(filter=flt, based_on=base_plan, transaction=tr)
        list(plan.execute())

    pb_linear_time = bench(lambda: [pb_linear_query_once() for _ in range(n_queries)], repeat=1)

    # Indexed path: build FromPlan and add indexes for category, status, value
    # Based_on is base_plan; FromPlan.add_index builds a RepeatedKeysDictionary index mapping value->Set of records
    # Build records as hashable wrappers exposing attribute 'r'
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
    for fld in ('r.category', 'r.status', 'r.value'):
        rkd = RepeatedKeysDictionary(transaction=tr)
        alias, attr = fld.split('.', 1)
        for rec in wrapped_records:
            row = getattr(rec, alias)
            key = None if row is None else row.get(attr)
            if key is not None:
                rkd = rkd.set_at(str(key), rec)
        idx_map[fld] = rkd
    indexes_dict = Dictionary(transaction=tr)
    for k, v in idx_map.items():
        indexes_dict = indexes_dict.set_at(k, v)
    from proto_db.queries import IndexedQueryPlan
    indexed = IndexedQueryPlan(indexes=indexes_dict, based_on=base_indexed_plan, transaction=tr)

    def pb_indexed_query_once():
        cat = random.choice(CATEGORIES)
        st = random.choice(STATUSES)
        lo = random.randint(1, 50000)
        hi = lo + random.randint(100, 10000)
        # Use prebuilt secondary indexes to compute intersection
        cat_set = set()
        st_set = set()
        val_set = set()
        cat_bucket = idx_map['r.category'].get_at(str(cat))
        if cat_bucket:
            cat_set.update(cat_bucket.as_iterable())
        st_bucket = idx_map['r.status'].get_at(str(st))
        if st_bucket:
            st_set.update(st_bucket.as_iterable())
        # Range over numeric value index
        for k, bucket in idx_map['r.value'].as_iterable():
            try:
                kv = int(k)
            except Exception:
                continue
            if lo < kv < hi:
                val_set.update(bucket.as_iterable())
        # Intersect progressively
        cur = cat_set
        cur = cur.intersection(st_set) if cur else set()
        cur = cur.intersection(val_set) if cur else set()
        # Materialize list as the query result
        _ = list(cur)

    pb_indexed_time = bench(lambda: [pb_indexed_query_once() for _ in range(n_queries)], repeat=1)

    results = {
        "config": {"n_items": n_items, "n_queries": n_queries},
        "timings_seconds": {
            "python_list_baseline": py_time,
            "protodb_linear_where": pb_linear_time,
            "protodb_indexed_where": pb_indexed_time,
        },
        "speedups": {
            "indexed_over_linear": (pb_linear_time / pb_indexed_time) if pb_indexed_time > 0 else None,
            "indexed_over_python": (py_time / pb_indexed_time) if pb_indexed_time > 0 else None,
        },
    }

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w') as f:
        json.dump(results, f, indent=2)
    return results


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='Indexed performance benchmark for ProtoBase')
    p.add_argument('--items', type=int, default=5000)
    p.add_argument('--queries', type=int, default=50)
    p.add_argument('--out', type=str, default='examples/benchmark_results_indexed.json')
    args = p.parse_args()
    res = run_benchmark(n_items=args.items, n_queries=args.queries, out_path=args.out)
    print(json.dumps(res, indent=2))
