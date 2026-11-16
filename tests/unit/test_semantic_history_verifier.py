"""The history verifier and the harness of the concurrency experiment (Task 10; preflight rows 35-37, 48).

Catches: a verifier that misses an over-limit state or a maintained aggregate that differs from the lines (both
planted with raw writes, which no constraint checks), or that reports a clean history as failing (a cancelled
order must stop weighing); a reconciliation that accepts counts that do not add up (an order with no outcome, a
committed order missing from the database, a retry count that does not match the attempts, a constraint refusal
that the final state cannot explain, a commit that published no version or two); and a command line whose JSON
lacks the fields the published section reads, or that reports a run as verified when its checks failed.  No sleep
decides anything (R82): the tiny command-line runs only check that the counts reconcile, whatever the
interleaving.
"""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from benchmarks.semantic import credit_experiment as experiment
from benchmarks.semantic.history_verifier import VerifierReport, verify_history
from proto_db.semantic import semantic
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace

pytestmark = [pytest.mark.timeout(120)]

ORDER, CUSTOMER = "acme.sales.Order", "acme.sales.Customer"


@pytest.fixture
def db():
    space = ObjectSpace(MemoryStorage(), modules_mapping=experiment.MAPPING)
    try:
        database = space.new_database("acme")
        database.run(lambda tr: tr.install_module("acme_sales"))
        yield database
    finally:
        space.close()


def _world(db, limit="100"):
    """A company, one customer (limit ``limit``), one product and one order of 50 (one line, 5 x 10)."""
    def load(tr):
        s = semantic(tr)
        company = s.create("acme.core.Company", name="ACME")
        customer = s.create(CUSTOMER, name="Bob", credit_limit=Decimal(limit), company=company)
        product = s.create("acme.catalog.Product", name="widget", list_price=Decimal("10"), company=company)
        order = s.create(ORDER, number=1, company=company, customer=customer)
        s.get(order).add_line(product, Decimal("5"), Decimal("10"))
        return company, customer, product, order
    return db.run(load)


def _raw(db, cell, **changes):
    """Write ``changes`` into the record of ``cell`` with a raw ``set_mutable`` (no semantic check runs);
    return the version it committed."""
    def write(tr):
        tr.set_mutable(cell.name, tr.get_mutable(cell.name).with_(**changes))
    db.run(write)
    return db.history()[0].version


# -- the verifier ----------------------------------------------------------------------------------------------

def test_verifier_finds_a_planted_violation(db):
    _, customer, _, _ = _world(db)
    over = _raw(db, customer, credit_limit=Decimal("10"))      # 50 committed against a limit of 10
    _raw(db, customer, credit_limit=Decimal("100"))            # back within the limit
    report = verify_history(db)
    assert isinstance(report, VerifierReport)
    assert report.violations == [(over, customer.name, Decimal("50"), Decimal("10"))]
    assert report.aggregate_mismatches == []
    assert report.versions == len(db.history())
    assert not report.ok


def test_verifier_finds_an_aggregate_mismatch(db):
    _, customer, _, order = _world(db)
    wrong = _raw(db, customer, committed=Decimal("999"))
    wrong_total = _raw(db, order, total=Decimal("7"))
    report = verify_history(db)
    assert report.violations == []          # the limit is compared with the recomputation, never the stored value
    assert (wrong, customer.name, "committed", Decimal("50"), Decimal("999")) in report.aggregate_mismatches
    assert (wrong_total, order.name, "total", Decimal("50"), Decimal("7")) in report.aggregate_mismatches
    assert not report.ok


def test_verifier_passes_a_clean_history(db):
    company, customer, product, first = _world(db)

    def more(tr):
        s = semantic(tr)
        order = s.create(ORDER, number=2, company=company, customer=customer)
        s.get(order).add_line(product, Decimal("3"), Decimal("10"))
        return order
    second = db.run(more, intents=True)
    db.run(lambda tr: semantic(tr).update(first, state="cancelled"))    # cancelling frees credit
    report = verify_history(db)
    assert report.violations == [] and report.aggregate_mismatches == []
    assert report.ok
    assert report.versions == len(db.history())

    tr = db.new_transaction()
    try:
        assert semantic(tr).get(customer).committed == Decimal("30")    # only the open order weighs
        assert semantic(tr).get(second).total == Decimal("30")
    finally:
        tr.abort()


def test_the_verifier_does_not_trust_the_customers_order_collection(db):
    """The recomputation starts from the order cells, so an order dropped from the customer's collection
    (raw) still weighs on that customer."""
    _, customer, _, _ = _world(db)
    from proto_db.collections.sets import Set
    dropped = _raw(db, customer, orders=Set(), committed=Decimal("0"))
    report = verify_history(db)
    assert (dropped, customer.name, "committed", Decimal("50"), Decimal("0")) in report.aggregate_mismatches


# -- the reconciliation ----------------------------------------------------------------------------------------

def _plan():
    lines = ((0, Decimal("1"), Decimal("30")),)
    return [experiment.PlannedOrder(1, 0, lines), experiment.PlannedOrder(2, 0, lines),
            experiment.PlannedOrder(3, 1, ((1, Decimal("2"), Decimal("10")),))]


def _consistent():
    """Customer 0 (limit 50) takes order 1 and refuses order 2 by its rule; customer 1 takes order 3 after one
    conflict.  Retries allowed: 2."""
    plan, limits = _plan(), {0: Decimal("50"), 1: Decimal("50")}
    outcomes = {1: experiment.Outcome("committed", attempts=1, conflicts=0),
                2: experiment.Outcome("constraint", attempts=1, conflicts=0),
                3: experiment.Outcome("committed", attempts=2, conflicts=1)}
    final = experiment.FinalState(orders={1: (0, Decimal("30")), 3: (1, Decimal("20"))},
                                  committed={0: Decimal("30"), 1: Decimal("20")}, versions_added=2)
    return plan, limits, outcomes, final


def test_reconcile_accepts_counts_that_add_up():
    assert experiment.reconcile(*_consistent(), retries=2) == []


def test_reconcile_flags_an_order_without_an_outcome():
    plan, limits, outcomes, final = _consistent()
    del outcomes[2]
    assert any("no outcome" in p for p in experiment.reconcile(plan, limits, outcomes, final, retries=2))


def test_reconcile_flags_an_error_outcome():
    plan, limits, outcomes, final = _consistent()
    outcomes[2] = experiment.Outcome("error", attempts=1, conflicts=0, error="RuntimeError('boom')")
    assert any("boom" in p for p in experiment.reconcile(plan, limits, outcomes, final, retries=2))


def test_reconcile_flags_a_committed_order_missing_from_the_database():
    plan, limits, outcomes, final = _consistent()
    del final.orders[3]
    final.committed[1] = Decimal("0")
    problems = experiment.reconcile(plan, limits, outcomes, final, retries=2)
    assert any("committed orders" in p for p in problems)


def test_reconcile_flags_a_wrong_total_and_a_wrong_aggregate():
    plan, limits, outcomes, final = _consistent()
    final.orders[3] = (1, Decimal("21"))
    final.committed[0] = Decimal("31")
    problems = experiment.reconcile(plan, limits, outcomes, final, retries=2)
    assert any("order 3" in p and "total" in p for p in problems)
    assert any("customer 0" in p and "aggregate" in p for p in problems)


def test_reconcile_flags_retry_counts_that_do_not_match_the_attempts():
    plan, limits, outcomes, final = _consistent()
    outcomes[3] = experiment.Outcome("committed", attempts=3, conflicts=1)
    assert any("order 3" in p and "attempts" in p for p in
               experiment.reconcile(plan, limits, outcomes, final, retries=2))
    plan, limits, outcomes, final = _consistent()
    outcomes[2] = experiment.Outcome("conflict", attempts=2, conflicts=2)      # retries=2 allows 3 attempts
    final.committed[0] = Decimal("30")
    assert any("order 2" in p and "attempts" in p for p in
               experiment.reconcile(plan, limits, outcomes, final, retries=2))


def test_reconcile_flags_a_constraint_refusal_the_final_state_cannot_explain():
    plan, limits, outcomes, final = _consistent()
    limits[0] = Decimal("100")              # 30 committed + 30 refused fit within 100: the refusal is wrong
    assert any("order 2" in p and "refused" in p for p in
               experiment.reconcile(plan, limits, outcomes, final, retries=2))


def test_reconcile_flags_a_customer_above_its_limit_and_a_missing_version():
    plan, limits, outcomes, final = _consistent()
    limits[1] = Decimal("10")
    final.versions_added = 3
    problems = experiment.reconcile(plan, limits, outcomes, final, retries=2)
    assert any("customer 1" in p and "limit" in p for p in problems)
    assert any("versions" in p for p in problems)


def test_the_plan_is_deterministic_and_some_demand_exceeds_the_limits():
    plan, limits = experiment.make_plan(orders=40, customers=3, products=2, seed=7, limit_fraction=0.75)
    again, same = experiment.make_plan(orders=40, customers=3, products=2, seed=7, limit_fraction=0.75)
    assert plan == again and limits == same
    assert [o.number for o in plan] == list(range(1, 41))
    demand = {c: sum(o.total for o in plan if o.customer == c) for c in range(3)}
    assert all(demand[c] > limits[c] for c in range(3) if demand[c])


# -- the command line ------------------------------------------------------------------------------------------

@pytest.mark.parametrize("mode", ["direct", "intents"])
def test_the_command_line_writes_a_verified_self_reporting_result(tmp_path, mode, capsys):
    out = tmp_path / f"{mode}.json"
    status = experiment.main(["--mode", mode, "--threads", "2", "--orders", "12", "--customers", "2",
                              "--products", "2", "--seed", "3", "--workdir", str(tmp_path / "work"),
                              "--out", str(out)])
    result = json.loads(out.read_text())
    assert status == 0, result["reconciliation"]
    for field in ("commit", "clean", "mode", "gil", "threads", "orders", "attempts", "commits", "conflicts",
                  "rejected_by_conflict", "rejected_by_constraint", "violations", "commit_latency_ms",
                  "verifier", "environment", "check", "settings"):
        assert field in result, field
    assert result["mode"] == mode and result["threads"] == 2 and result["orders"] == 12
    assert result["commits"] + result["rejected_by_conflict"] + result["rejected_by_constraint"] == 12
    assert result["attempts"] >= 12
    assert result["violations"] == 0 and result["verifier"]["aggregate_mismatches"] == []
    assert result["reconciliation"] == [] and result["verified"] is True
    assert set(result["commit_latency_ms"]) >= {"p50"}
    printed = capsys.readouterr().out
    assert f"{result['commits']} committed" in printed and "verified" in printed


def test_the_command_line_refuses_an_existing_workdir(tmp_path):
    (tmp_path / "work").mkdir()
    with pytest.raises(SystemExit):
        experiment.main(["--mode", "direct", "--workdir", str(tmp_path / "work"), "--out",
                         str(tmp_path / "r.json")])


def test_a_failed_check_makes_the_run_unverified(tmp_path, monkeypatch):
    """A reconciliation problem must fail the run (exit 1) and be written, never look like a result."""
    monkeypatch.setattr(experiment, "reconcile", lambda *a, **k: ["planted problem"])
    out = tmp_path / "r.json"
    status = experiment.main(["--mode", "intents", "--threads", "1", "--orders", "2", "--customers", "1",
                              "--products", "1", "--workdir", str(tmp_path / "work"), "--out", str(out)])
    result = json.loads(out.read_text())
    assert status == 1 and result["verified"] is False and result["reconciliation"] == ["planted problem"]


def test_summarize_gives_medians_and_ranges_per_cell(tmp_path):
    paths = []
    for i, (p50, commits) in enumerate([(1.0, 10), (3.0, 12), (2.0, 11)]):
        path = tmp_path / f"r{i}.json"
        path.write_text(json.dumps({"mode": "intents", "gil": False, "commit": "abc1234", "clean": True,
                                    "verified": True, "commits": commits, "attempts": 12, "conflicts": 0,
                                    "rejected_by_conflict": 0, "rejected_by_constraint": 12 - commits,
                                    "violations": 0, "commit_latency_ms": {"p50": p50, "p95": p50 * 2},
                                    "orders_per_s": 100.0 * (i + 1), "wall_s": 1.0}))
        paths.append(str(path))
    summary = experiment.summarize(paths)
    cell = summary["cells"]["intents gil=0"]
    assert cell["runs"] == 3 and cell["verified_runs"] == 3
    assert cell["commit_latency_p50_ms"] == {"median": 2.0, "min": 1.0, "max": 3.0}
    assert cell["commits"] == {"median": 11, "min": 10, "max": 12}
    assert summary["commits_measured"] == ["abc1234"] and summary["all_clean"] is True
