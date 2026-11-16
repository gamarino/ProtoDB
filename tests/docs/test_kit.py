"""The recruiting kit in docs/kit runs (Task 10.6, R109).

* The ERP demo runs from the repository with one command, checks its own results and prints
  what it shows; a demo that stops checking, or whose acts stop printing, fails here.
* The semantic-model demo (docs/kit/semantic) does the same for its steps.
* The C1 exercise test passes on this tree (the fix is in), and fails on the scratch copy that
  ``make_exercise.py`` plants (a planted bug that the test does not catch proves nothing).
"""
import os
import shutil
import subprocess
import sys
import sysconfig

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
KIT = os.path.join(REPO, "docs", "kit")
EXERCISE = os.path.join(KIT, "exercise-c1")


def _run(args, cwd, timeout):
    """Run ``python args`` in a fresh interpreter of this session's GIL mode (as test_public_api does)."""
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    free_threaded_build = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    flags = ["-X", "gil=1"] if free_threaded_build and sys._is_gil_enabled() else []
    return subprocess.run([sys.executable, *flags, *args], cwd=cwd, env=env, capture_output=True, text=True,
                          timeout=timeout)


@pytest.mark.timeout(300)
def test_the_erp_demo_runs_and_checks_itself(tmp_path):
    out = _run([os.path.join(KIT, "erp_demo.py"), "--workdir", str(tmp_path / "demo")], REPO, 240)
    assert out.returncode == 0, f"the demo failed\n--- stdout ---\n{out.stdout}\n--- stderr ---\n{out.stderr}"
    for act in ("Act 1", "Act 2", "Act 3"):
        assert act in out.stdout, f"{act} printed nothing:\n{out.stdout}"
    assert "every check passed" in out.stdout, out.stdout
    assert out.stdout.count("[check]") >= 9, f"the demo checks too little:\n{out.stdout}"


@pytest.mark.timeout(330)
def test_the_semantic_demo_runs_and_checks_itself(tmp_path):
    pytest.importorskip("mcp", reason="the demo's agent steps (1, 2, 4, 5) need the optional extra 'agent' (the MCP "
                                      "SDK), absent from this interpreter (preflight F30)")
    out = _run([os.path.join(KIT, "semantic", "semantic_demo.py"), "--workdir", str(tmp_path / "demo")], REPO, 300)
    assert out.returncode == 0, f"the demo failed\n--- stdout ---\n{out.stdout}\n--- stderr ---\n{out.stderr}"
    for step in ("Step 1", "Step 2", "Step 3", "Step 4", "Step 5", "Step 6"):
        assert step in out.stdout, f"{step} printed nothing:\n{out.stdout}"
    assert "external" in out.stdout and "protoBaseST-handoff.md" in out.stdout, \
        f"step 6 does not say it is external and where the handoff is:\n{out.stdout}"
    assert "every check passed" in out.stdout, out.stdout
    assert out.stdout.count("[check]") >= 12, f"the demo checks too little:\n{out.stdout}"
    assert "by_order_date" in out.stdout, out.stdout
    assert out.stdout.count("agent -> ") >= 9, f"the agent steps make too few tool calls:\n{out.stdout}"


@pytest.mark.timeout(300)
def test_the_c1_exercise_test_passes_on_the_fixed_tree():
    out = _run(["-m", "pytest", "-q", "-p", "no:cacheprovider", os.path.join(EXERCISE, "test_c1_exercise.py")],
               REPO, 240)
    assert out.returncode == 0, out.stdout + out.stderr


@pytest.mark.timeout(300)
def test_the_c1_exercise_test_fails_on_the_planted_copy(tmp_path):
    if shutil.which("git") is None or not os.path.isdir(os.path.join(REPO, ".git")):
        pytest.skip("the exercise is built with git archive from a git checkout")
    dest = tmp_path / "exercise"
    made = _run([os.path.join(EXERCISE, "make_exercise.py"), str(dest)], REPO, 120)
    assert made.returncode == 0, made.stdout + made.stderr
    assert not os.path.exists(dest / "exercise_c1" / "SOLUTION.md"), "the solution was handed out"
    assert not os.path.exists(dest / "docs"), "the copy keeps the documents that describe the fix"
    assert not os.path.exists(dest / "tests" / "docs" / "test_kit.py"), "the copy keeps the kit's test, which names the fix"
    with open(dest / "proto_db" / "transactions.py", encoding="utf-8") as f:
        assert "_phase_1_floor" not in f.read()
    out = _run(["-m", "pytest", "-q", "-p", "no:cacheprovider", "exercise_c1/test_c1_exercise.py"], str(dest), 240)
    assert out.returncode == 1, f"the planted bug was not caught\n{out.stdout}{out.stderr}"
    assert "not completed (safe)" not in out.stdout, out.stdout
    assert "an acknowledged root (R'" in out.stdout, out.stdout


def test_the_child_interpreter_runs_in_this_session_gil_mode():
    """The demo and the exercise runs above use this session's GIL mode, not the build's default."""
    out = _run(["-c", "import sys; print(getattr(sys, '_is_gil_enabled', lambda: True)())"], REPO, 60)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == str(getattr(sys, "_is_gil_enabled", lambda: True)()), out.stdout


def test_make_exercise_refuses_a_destination_inside_the_repository():
    if shutil.which("git") is None or not os.path.isdir(os.path.join(REPO, ".git")):
        pytest.skip("make_exercise.py needs a git checkout before it looks at the destination")
    out = _run([os.path.join(EXERCISE, "make_exercise.py"), os.path.join(REPO, "exercise-scratch")], REPO, 60)
    assert out.returncode != 0 and "inside the repository" in out.stderr, out.stdout + out.stderr
    assert not os.path.exists(os.path.join(REPO, "exercise-scratch"))


def test_the_semantic_demo_history_verifier_reports_planted_failures():
    """Step 4's done criterion rests on the demo's history walk (``_verify_history``): it must report an
    over-limit state and a maintained aggregate that differs from the lines, both planted with raw writes
    (preflight F36: a check is shown failing, not only passing).  No MCP needed."""
    import importlib.util
    from decimal import Decimal
    from proto_db import ObjectSpace
    from proto_db.semantic import semantic
    from proto_db.storage.memory import MemoryStorage
    spec = importlib.util.spec_from_file_location("semantic_demo_t9", os.path.join(KIT, "semantic",
                                                                                     "semantic_demo.py"))
    demo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo)
    space = ObjectSpace(MemoryStorage(), modules_mapping=demo.MAPPING)
    try:
        db = space.new_database("acme")
        db.run(lambda tr: tr.install_module("acme_sales"))

        def load(tr):
            s = semantic(tr)
            company = s.create("acme.core.Company", name="ACME")
            customer = s.create("acme.sales.Customer", name="Bob", credit_limit=Decimal("100"), company=company)
            product = s.create("acme.catalog.Product", name="widget", list_price=Decimal("10"), company=company)
            order = s.create(demo.ORDER, number=1, company=company, customer=customer)
            s.get(order).add_line(product, Decimal("1"), Decimal("50"))
            return company, customer
        company, customer = db.run(load)
        assert demo._verify_history(db, company.name) == (1, [], [])

        def raw(**changes):
            def write(tr):
                tr.set_mutable(customer.name, tr.get_mutable(customer.name).with_(**changes))
            db.run(write)
        raw(credit_limit=Decimal("10"))                     # over the limit: 50 > 10
        over = db.new_transaction()
        over_version = over._db_root.version
        over.abort()
        raw(credit_limit=Decimal("100"), committed=Decimal("999"))     # the aggregate differs from the lines
        versions, violations, mismatches = demo._verify_history(db, company.name)
        assert versions == 3
        assert violations == [(over_version, customer.name, Decimal("50"), Decimal("10"))]
        assert [(cell, recomputed, stored) for _, cell, recomputed, stored in mismatches] == \
            [(customer.name, Decimal("50"), Decimal("999"))]
    finally:
        space.close()
