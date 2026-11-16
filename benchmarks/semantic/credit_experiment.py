"""The concurrency experiment of the semantic model: intents against direct mode (spec section 15, Task 10;
rulings R269, R273, R275).

The order-entry scenario of the demo (``docs/kit/semantic/acme_sales.py``): one company, ``K`` customers with a
credit limit, ``P`` products, and ``M`` orders of 1-3 lines each, placed by ``N`` threads.  Each order is one
``Database.run`` that creates the order, adds its lines through the ``add_line`` intent and commits:

- ``--mode direct``: today's transactions (``Database.run``, its retries on conflict; the R275 baseline);
- ``--mode intents``: the same work with ``intents=True``: the operations are logged and replayed at commit.

The plan (which customer, which lines, which prices) and the credit limits come from ``--seed``.  Each customer's
limit is ``--limit-fraction`` of the demand the plan places on it (rounded down), so some orders must be refused
by the credit rule (``committed <= credit_limit``).  The workload decides on nothing it reads: it never reads a
customer's committed amount, a query, ``.record`` or a computed attribute of the company or the product (in
intent mode those record whole cells, a sound coarsening, and the experiment would measure that coarsening
instead of the model).

The run is self-reporting.  It prints the work it did and verifies it before it reports a figure:

- :func:`reconcile` checks the outcome of every planned order against the final state, independently read: the
  committed orders are exactly the orders in the database, with their planned totals; each customer's maintained
  aggregate is the sum of its committed orders and within its limit; a refusal by the credit rule is explained by
  the final state; every retry was a conflict and the counts of attempts match the retry policy; every commit
  published exactly one version;
- :func:`~benchmarks.semantic.history_verifier.verify_history` walks every version of the history and recomputes
  each customer's committed amount from the lines (an independent oracle).

A failed check makes the run unverified: exit status 1, ``"verified": false`` in the JSON, which is written
anyway.  Usage, from the root of the tree to measure (a clean ``python -m benchmarks.export`` tree)::

    python -m benchmarks.semantic.credit_experiment --mode direct|intents --threads N --orders M \\
        --customers K --seed S --workdir DIR --out FILE
    python -m benchmarks.semantic.credit_experiment --summarize FILE... --out SUMMARY

``DIR`` must not exist; the space is created in it and left there.  ``--summarize`` gives, per mode and GIL
mode, the median and range of each figure over the verified runs.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import sys
import sysconfig
import time
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import ROUND_FLOOR, Decimal
from typing import NamedTuple

from benchmarks.harness import _cpu_model, _git_revision, cpu_frequency_policy, filesystem_of, gil_enabled, \
    latency_ms, stray_check
from benchmarks.semantic.history_verifier import verify_history
from benchmarks.support import run_threads

#: Registry module name -> import path of its code: the demo's model, imported from the measured tree.
MAPPING = {"acme_sales": "docs.kit.semantic.acme_sales"}
DATABASE = "acme"
ORDER, CUSTOMER, PRODUCT, COMPANY = "acme.sales.Order", "acme.sales.Customer", "acme.catalog.Product", \
    "acme.core.Company"
CREDIT_RULE = "committed <= credit_limit"
STATUSES = ("committed", "constraint", "conflict", "error")


class PlannedOrder(NamedTuple):
    """One order of the plan: its number, its customer (index) and its lines ``(product index, quantity,
    price)``."""

    number: int
    customer: int
    lines: tuple

    @property
    def total(self) -> Decimal:
        return sum((quantity * price for _, quantity, price in self.lines), Decimal(0))


@dataclass
class Outcome:
    """What happened to one planned order: ``status`` is ``committed``, ``constraint`` (refused by the credit
    rule), ``conflict`` (still conflicting after the last retry) or ``error`` (anything else, with ``error``);
    ``attempts`` counts the transactions run for it and ``conflicts`` those that ended in a conflict."""

    status: str
    attempts: int
    conflicts: int
    error: str = ""


@dataclass
class FinalState:
    """The final state as read from the database: ``orders`` maps an order's number to ``(customer index,
    stored total)``, ``committed`` a customer's index to its stored committed amount, and ``versions_added`` is
    the number of versions the orders' commits published."""

    orders: dict
    committed: dict
    versions_added: int = 0


def make_plan(*, orders: int, customers: int, products: int, seed: int, limit_fraction: float) -> tuple:
    """The orders to place and each customer's credit limit, from ``seed`` alone: ``(plan, limits)``."""
    rng = random.Random(seed)
    plan = []
    for number in range(1, orders + 1):
        customer = rng.randrange(customers)
        lines = tuple((rng.randrange(products), Decimal(rng.randint(1, 3)), Decimal(rng.randint(5, 50)))
                      for _ in range(rng.randint(1, 3)))
        plan.append(PlannedOrder(number, customer, lines))
    fraction = Decimal(str(limit_fraction))
    limits = {}
    for customer in range(customers):
        demand = sum((o.total for o in plan if o.customer == customer), Decimal(0))
        limits[customer] = (demand * fraction).quantize(Decimal(1), rounding=ROUND_FLOOR)
    return plan, limits


def reconcile(plan, limits: dict, outcomes: dict, final: FinalState, *, retries: int) -> list:
    """The problems found when the outcomes of ``plan`` are checked against ``final`` (empty: they reconcile).
    ``retries`` is the retry budget of ``Database.run``: an order rejected by conflict ran ``retries + 1``
    attempts, all conflicts; any other order ran one attempt after each conflict."""
    problems = []
    planned = {o.number: o for o in plan}
    for number in sorted(set(outcomes) - set(planned)):
        problems.append(f"order {number}: an outcome for an order not in the plan")
    for order in plan:
        outcome = outcomes.get(order.number)
        if outcome is None:
            problems.append(f"order {order.number}: no outcome")
            continue
        if outcome.status not in STATUSES or outcome.status == "error":
            problems.append(f"order {order.number}: {outcome.status} {outcome.error}")
        if outcome.status == "conflict":
            if not outcome.attempts == outcome.conflicts == retries + 1:
                problems.append(f"order {order.number}: rejected by conflict after {outcome.attempts} attempts "
                                f"({outcome.conflicts} conflicts); the retries allow {retries + 1}")
        elif outcome.conflicts != outcome.attempts - 1 or outcome.attempts < 1:
            problems.append(f"order {order.number}: {outcome.attempts} attempts with {outcome.conflicts} conflicts "
                            f"(each attempt but the last must have conflicted)")
    committed = {n for n, o in outcomes.items() if o.status == "committed"}
    if committed != set(final.orders):
        problems.append(f"the committed orders {sorted(committed - set(final.orders))} are missing from the "
                        f"database, and the database holds orders {sorted(set(final.orders) - committed)} "
                        f"that did not commit")
    for number in sorted(committed & set(final.orders) & set(planned)):
        customer, total = final.orders[number]
        if customer != planned[number].customer or total != planned[number].total:
            problems.append(f"order {number}: customer {customer} and total {total} in the database; planned "
                            f"customer {planned[number].customer}, total {planned[number].total}")
    for customer, limit in sorted(limits.items()):
        expected = sum((planned[n].total for n in committed if n in planned and planned[n].customer == customer),
                       Decimal(0))
        stored = final.committed.get(customer)
        if stored != expected:
            problems.append(f"customer {customer}: stored aggregate {stored}, sum of its committed orders "
                            f"{expected}")
        if stored is not None and stored > limit:
            problems.append(f"customer {customer}: committed {stored} above its limit {limit}")
    for number, outcome in sorted(outcomes.items()):
        if outcome.status != "constraint" or number not in planned:
            continue
        order = planned[number]
        # Orders are only added, never cancelled, in this workload: what the customer had committed when the
        # order was refused is at most its final amount, so a refusal needs final + total > limit.
        final_amount = final.committed.get(order.customer, Decimal(0)) or Decimal(0)
        if final_amount + order.total <= limits.get(order.customer, Decimal(0)):
            problems.append(f"order {number}: refused by the credit rule, but {final_amount} committed + "
                            f"{order.total} fits the limit {limits.get(order.customer)}")
    if final.versions_added != len(committed):
        problems.append(f"the orders' commits published {final.versions_added} versions for {len(committed)} "
                        f"committed orders")
    return problems


# -- the run ---------------------------------------------------------------------------------------------------

def _setup(plan_limits: dict, products: int):
    from proto_db.semantic import semantic

    def setup(tr):
        s = semantic(tr)
        company = s.create(COMPANY, name="ACME")
        customers = [s.create(CUSTOMER, name=f"Customer {k:03d}", credit_limit=limit, company=company)
                     for k, limit in sorted(plan_limits.items())]
        items = [s.create(PRODUCT, name=f"Product {p:03d}", list_price=Decimal("10"), company=company)
                 for p in range(products)]
        return company, customers, items
    return setup


def _place(db, world: tuple, order: PlannedOrder, *, intents: bool, retries: int, credit: str, latencies: list,
           attempt_latencies: list) -> Outcome:
    """Place ``order`` with ``Database.run``; the commit is timed inside ``fn``."""
    from proto_db.errors import ProtoConflictError, ProtoConstraintError
    from proto_db.semantic import semantic
    company, customers, products = world
    counts = {"attempts": 0, "conflicts": 0}

    def fn(tr):
        counts["attempts"] += 1
        try:
            s = semantic(tr)
            created = s.create(ORDER, number=order.number, date=date(2026, 7, 1) + timedelta(days=order.number % 28),
                               company=company, customer=customers[order.customer])
            version = s.get(created)
            for product, quantity, price in order.lines:
                version.add_line(products[product], quantity, price)
            start = time.perf_counter()
            try:
                tr.commit()
            finally:
                attempt_latencies.append(time.perf_counter() - start)
            latencies.append(attempt_latencies[-1])
        except ProtoConflictError:
            counts["conflicts"] += 1
            raise
    try:
        db.run(fn, retries=retries, intents=intents)
        status, error = "committed", ""
    except ProtoConstraintError as e:
        status, error = ("constraint", "") if e.constraint == credit else ("error", repr(e))
    except ProtoConflictError:
        status, error = "conflict", ""
    except Exception as e:                  # reported as an error outcome: reconcile fails the run
        status, error = "error", repr(e)
    return Outcome(status, counts["attempts"], counts["conflicts"], error)


def _final_state(db, world: tuple) -> FinalState:
    """The orders and aggregates of the current version, read raw (cell records), and nothing else."""
    _, customers, _ = world
    index = {c.name: k for k, c in enumerate(customers)}
    tr = db.new_transaction()
    try:
        orders, committed = {}, {}
        for name in tr.mutable_names():
            record = tr.get_mutable(name)
            if name.startswith(ORDER + "#"):
                orders[record.number] = (index.get(record.customer.name), record.total)
            elif name.startswith(CUSTOMER + "#"):
                committed[index[name]] = record.committed
        return FinalState(orders, committed)
    finally:
        tr.abort()


def environment(workdir: str) -> dict:
    """The machine and interpreter of the run."""
    import platform
    import proto_db
    return {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "cpu_model": _cpu_model(),
            "cpu_count": os.cpu_count(), "cpu_affinity": len(os.sched_getaffinity(0)),
            "cpu_frequency": cpu_frequency_policy(), "platform": platform.platform(),
            "interpreter": sys.executable, "python": sys.version.split()[0],
            "free_threaded_build": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
            "gil_enabled": gil_enabled(), "xoptions": {k: str(v) for k, v in sys._xoptions.items()},
            "proto_db": proto_db.__version__, "proto_db_path": os.path.dirname(proto_db.__file__),
            "workdir": workdir, "workdir_fs": filesystem_of(workdir)}


def run(args) -> dict:
    """One run of the experiment with the settings ``args``; the result as a JSON-able dict."""
    from proto_db import FileStorage, ObjectSpace
    from proto_db.semantic.descriptors import invariant_name
    from proto_db.semantic.expressions import parse
    from proto_db.storage.memory import MemoryStorage

    os.makedirs(args.workdir)
    check = stray_check(args.workdir)
    plan, limits = make_plan(orders=args.orders, customers=args.customers, products=args.products, seed=args.seed,
                             limit_fraction=args.limit_fraction)
    storage = MemoryStorage() if args.storage == "memory" else \
        FileStorage(os.path.join(args.workdir, "space"), durability=args.durability)
    space = ObjectSpace(storage, modules_mapping=MAPPING)
    intents = args.mode == "intents"
    credit = invariant_name(parse(CREDIT_RULE))
    try:
        db = space.new_database(DATABASE)
        db.run(lambda tr: tr.install_module("acme_sales"))
        world = db.run(_setup(limits, args.products))
        before = len(db.history())
        latencies = [[] for _ in range(args.threads)]
        attempt_latencies = [[] for _ in range(args.threads)]

        def worker(t: int):
            def work():
                return [(o.number, _place(db, world, o, intents=intents, retries=args.retries, credit=credit,
                                          latencies=latencies[t], attempt_latencies=attempt_latencies[t]))
                        for o in plan[t::args.threads]]
            return work
        wall, results = run_threads([worker(t) for t in range(args.threads)], timeout=args.timeout)
        outcomes = {number: outcome for result in results for number, outcome in result}
        final = _final_state(db, world)
        final.versions_added = len(db.history()) - before
        verify_start = time.perf_counter()
        report = verify_history(db)
        verify_s = time.perf_counter() - verify_start
    finally:
        space.close()
    problems = reconcile(plan, limits, outcomes, final, retries=args.retries)
    tally = {status: sum(1 for o in outcomes.values() if o.status == status) for status in STATUSES}
    commits = [x for per in latencies for x in per]
    attempts = [x for per in attempt_latencies for x in per]
    source = _git_revision()
    settings = {k: getattr(args, k) for k in ("mode", "threads", "orders", "customers", "products", "seed",
                                               "retries", "limit_fraction", "storage", "durability")}
    return {
        "experiment": "semantic credit: intents against direct mode",
        "commit": source.get("commit"), "clean": bool(source.get("clean")), "source": source,
        "mode": args.mode, "gil": gil_enabled(), **{k: v for k, v in settings.items() if k != "mode"},
        "settings": settings,
        "limits": {str(k): str(v) for k, v in limits.items()},
        "demand": str(sum((o.total for o in plan), Decimal(0))),
        "attempts": sum(o.attempts for o in outcomes.values()),
        "commits": tally["committed"],
        "conflicts": sum(o.conflicts for o in outcomes.values()),
        "rejected_by_conflict": tally["conflict"],
        "rejected_by_constraint": tally["constraint"],
        "errors": tally["error"],
        "error_samples": sorted({o.error for o in outcomes.values() if o.status == "error"})[:5],
        "violations": len(report.violations),
        "verifier": {"versions": report.versions, "seconds": round(verify_s, 3),
                     "violations": [list(map(str, v)) for v in report.violations],
                     "aggregate_mismatches": [list(map(str, m)) for m in report.aggregate_mismatches]},
        "commit_latency_ms": latency_ms(commits),
        "commit_attempt_latency_ms": latency_ms(attempts),
        "wall_s": round(wall, 4),
        "commits_per_s": round(tally["committed"] / wall, 2) if wall else None,
        "orders_per_s": round(len(plan) / wall, 2) if wall else None,
        "versions_added": final.versions_added,
        "reconciliation": problems,
        "verified": not problems and report.ok,
        "environment": environment(args.workdir),
        "check": check,
    }


def report_lines(result: dict) -> list:
    """The run's self-report, as printed."""
    lat = result["commit_latency_ms"]
    tail = f"p95 {lat['p95']}" if "p95" in lat else f"max {lat.get('max')}"
    lines = [
        f"credit experiment at {result['commit']} (clean: {result['clean']}), mode {result['mode']}, "
        f"gil={int(result['gil'])}, {result['threads']} threads, {result['customers']} customers, "
        f"{result['products']} products, seed {result['seed']}, storage {result['storage']}",
        f"{result['orders']} orders: {result['attempts']} attempts, {result['commits']} committed, "
        f"{result['conflicts']} conflicts (retries), {result['rejected_by_conflict']} rejected by conflict, "
        f"{result['rejected_by_constraint']} rejected by constraint, {result['errors']} errors",
        f"commit latency p50 {lat['p50']} ms, {tail} ms; {result['commits_per_s']} commits/s over {result['wall_s']} s",
        f"history verifier: {result['verifier']['versions']} versions, {result['violations']} violations, "
        f"{len(result['verifier']['aggregate_mismatches'])} aggregate mismatches",
    ]
    if result["verified"]:
        lines.append("verified: every order's outcome reconciles with the final state and the history")
    else:
        lines.append("NOT verified:")
        lines += [f"   {p}" for p in result["reconciliation"]]
        lines += [f"   violation {v}" for v in result["verifier"]["violations"][:10]]
        lines += [f"   mismatch {m}" for m in result["verifier"]["aggregate_mismatches"][:10]]
    return lines


# -- summaries -------------------------------------------------------------------------------------------------

_SUMMARY_FIELDS = ("commits", "attempts", "conflicts", "rejected_by_conflict", "rejected_by_constraint", "violations",
                   "orders_per_s", "commits_per_s", "wall_s")


def _spread(values: list) -> dict:
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def summarize(paths: list) -> dict:
    """Per cell (mode and GIL mode): the number of runs, of verified runs, and the median and range of each
    figure over the verified runs only (an unverified run is counted, never averaged in)."""
    every = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            every.append(json.load(f))
    by_cell = {}
    for result in every:
        by_cell.setdefault(f"{result['mode']} gil={int(result['gil'])}", []).append(result)
    cells = {}
    for label, results in sorted(by_cell.items()):
        good = [r for r in results if r.get("verified")]
        cell = {"runs": len(results), "verified_runs": len(good)}
        if good:
            for name in _SUMMARY_FIELDS:
                values = [r[name] for r in good if r.get(name) is not None]
                if values:
                    cell[name] = _spread(values)
            for key in ("p50", "p95"):
                values = [r["commit_latency_ms"][key] for r in good if key in r["commit_latency_ms"]]
                if values:
                    cell[f"commit_latency_{key}_ms"] = _spread(values)
        cells[label] = cell
    return {"cells": cells, "files": [os.path.basename(p) for p in paths],
            "commits_measured": sorted({r["commit"] for r in every}),
            "all_clean": all(r["clean"] for r in every)}


# -- command line ----------------------------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.semantic.credit_experiment",
                                     description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=("direct", "intents"))
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--orders", type=int, default=400)
    parser.add_argument("--customers", type=int, default=8)
    parser.add_argument("--products", type=int, default=4)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--retries", type=int, default=10, help="Database.run's retry budget (its default: 10)")
    parser.add_argument("--limit-fraction", type=float, default=0.75,
                        help="each customer's limit as a fraction of the demand the plan places on it")
    parser.add_argument("--storage", choices=("file", "memory"), default="file")
    parser.add_argument("--durability", choices=("none", "flush", "fsync"), default="none")
    parser.add_argument("--timeout", type=float, default=1800.0, help="seconds for the threads to finish")
    parser.add_argument("--workdir", help="directory for the space (must not exist; left in place)")
    parser.add_argument("--out", required=True, help="the results JSON file")
    parser.add_argument("--summarize", nargs="+", metavar="FILE", help="summarize results files instead of running")
    args = parser.parse_args(argv)
    if args.summarize is None:
        if args.mode is None or args.workdir is None:
            parser.error("--mode and --workdir are required to run the experiment")
        if os.path.exists(args.workdir):
            parser.error(f"{args.workdir} exists; choose a new directory")
        if min(args.threads, args.orders, args.customers, args.products) < 1 or args.retries < 0:
            parser.error("threads, orders, customers and products must be positive, retries non-negative")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.summarize is not None:
        summary = summarize(args.summarize)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=1, sort_keys=True)
        print(json.dumps(summary["cells"], indent=1, sort_keys=True))
        return 0
    result = run(args)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=1, sort_keys=True, default=str)
    for line in report_lines(result):
        print(line)
    return 0 if result["verified"] else 1


if __name__ == "__main__":
    sys.exit(main())
