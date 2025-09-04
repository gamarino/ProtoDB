"""
LINQ-like API Performance Measurement Examples

Run:
  python examples/linq_performance.py --size 100000 --runs 5 --out examples/benchmark_results_linq.json

This script benchmarks a few representative LINQ-like queries:
- Filter + order_by + take (pagination)
- Distinct over a projection
- Where + count
- Between/range filter
- GroupBy with aggregates (optional heavier)

It compares execution over:
- Plain Python list source (from_collection(list))
- ProtoBase QueryPlan using ListPlan (pushdown for where/select; still local for order/distinct/etc.)

If your environment provides actual indexed collections/plans, the pushdown may leverage indexes further.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import time
from typing import Any, Dict, List

from proto_db.linq import from_collection, F
from proto_db.queries import ListPlan


def gen_users(n: int) -> List[Dict[str, Any]]:
    random.seed(42)
    countries = ["ES", "AR", "US", "BR", "FR"]
    statuses = ["active", "inactive"]
    rows: List[Dict[str, Any]] = []
    for i in range(1, n + 1):
        rows.append({
            "id": i,
            "first_name": f"Name{i}",
            "last_name": f"Surname{i}",
            "age": random.randint(10, 80),
            "country": random.choice(countries),
            "status": random.choice(statuses),
            "email": f"user{i}@example.com",
            "last_login": random.randint(0, 10000),
            "score": random.random(),
        })
    return rows


def time_query(fn, runs: int) -> Dict[str, float]:
    durations: List[float] = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        t1 = time.perf_counter()
        durations.append((t1 - t0) * 1000.0)  # ms
    avg = sum(durations) / len(durations)
    p50 = statistics.median(durations)
    p95 = statistics.quantiles(durations, n=20)[18] if len(durations) >= 20 else max(durations)
    std = statistics.pstdev(durations) if len(durations) > 1 else 0.0
    qps = 1000.0 / avg if avg > 0 else 0.0
    return {"avg_ms": avg, "p50_ms": p50, "p95_ms": p95, "std_ms": std, "qps": qps}


def build_pipelines(users_list: List[Dict[str, Any]]):
    # Baseline over Python list
    q_list = (
        from_collection(users_list)
        .where((F.age >= 18) & F.country.in_(["ES", "AR"]))
        .order_by(F.last_login, ascending=False)
        .select({"id": F["id"], "name": F.first_name + " " + F.last_name})
        .take(50)
    )

    distinct_list = from_collection(users_list).select(F.email).distinct()
    count_active_list = from_collection(users_list).where(F.status == "active")
    between_list = from_collection(users_list).where(F.age.between(30, 50))

    # GroupBy example (heavier)
    groupby_list = (
        from_collection(users_list)
        .where(F.status == "active")
        .group_by(F.country, element_selector=F.age)
        .select({"country": F.key(), "count": F.count(), "avg": F.average()})
        .order_by(F["avg"], ascending=False)
        .take(10)
    )

    # ProtoBase QueryPlan via ListPlan base
    q_plan = (
        from_collection(ListPlan(base_list=users_list))
        .where((F.age >= 18) & F.country.in_(["ES", "AR"]))
        .order_by(F.last_login, ascending=False)
        .select({"id": F["id"], "name": F.first_name + " " + F.last_name})
        .take(50)
    )
    distinct_plan = from_collection(ListPlan(base_list=users_list)).select(F.email).distinct()
    count_active_plan = from_collection(ListPlan(base_list=users_list)).where(F.status == "active")
    between_plan = from_collection(ListPlan(base_list=users_list)).where(F.age.between(30, 50))
    groupby_plan = (
        from_collection(ListPlan(base_list=users_list))
        .where(F.status == "active")
        .group_by(F.country, element_selector=F.age)
        .select({"country": F.key(), "count": F.count(), "avg": F.average()})
        .order_by(F["avg"], ascending=False)
        .take(10)
    )

    return {
        "list": {
            "filter_order_take": lambda: q_list.to_list(),
            "distinct": lambda: distinct_list.to_list(),
            "count_active": lambda: count_active_list.count(),
            "between": lambda: between_list.count(),
            "groupby": lambda: groupby_list.to_list(),
        },
        "plan": {
            "filter_order_take": lambda: q_plan.to_list(),
            "distinct": lambda: distinct_plan.to_list(),
            "count_active": lambda: count_active_plan.count(),
            "between": lambda: between_plan.count(),
            "groupby": lambda: groupby_plan.to_list(),
        }
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=50000, help="Number of rows to generate")
    parser.add_argument("--runs", type=int, default=5, help="Repetitions per query")
    parser.add_argument("--out", type=str, default="", help="Optional JSON output path")
    args = parser.parse_args()

    users = gen_users(args.size)
    pipelines = build_pipelines(users)

    results = {
        "config": {"size": args.size, "runs": args.runs},
        "list": {},
        "plan": {},
    }

    for mode in ("list", "plan"):
        for label, fn in pipelines[mode].items():
            stats = time_query(fn, args.runs)
            results[mode][label] = stats
            print(f"{mode}:{label} -> avg={stats['avg_ms']:.3f} ms, p50={stats['p50_ms']:.3f} ms, p95={stats['p95_ms']:.3f} ms, qps={stats['qps']:.1f}")

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        print(f"Saved results to {args.out}")


if __name__ == "__main__":
    main()
