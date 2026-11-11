"""The recruiting kit in docs/kit runs (Task 10.6, R109).

* The ERP demo runs from the repository with one command, checks its own results and prints
  what it shows; a demo that stops checking, or whose acts stop printing, fails here.
* The C1 exercise test passes on this tree (the fix is in), and fails on the scratch copy that
  ``make_exercise.py`` plants (a planted bug that the test does not catch proves nothing).
"""
import os
import shutil
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
KIT = os.path.join(REPO, "docs", "kit")
EXERCISE = os.path.join(KIT, "exercise-c1")


def _run(args, cwd, timeout):
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    return subprocess.run([sys.executable, *args], cwd=cwd, env=env, capture_output=True, text=True,
                          timeout=timeout)


@pytest.mark.timeout(300)
def test_the_erp_demo_runs_and_checks_itself(tmp_path):
    out = _run([os.path.join(KIT, "erp_demo.py"), "--workdir", str(tmp_path / "demo")], REPO, 240)
    assert out.returncode == 0, f"the demo failed\n--- stdout ---\n{out.stdout}\n--- stderr ---\n{out.stderr}"
    for act in ("Act 1", "Act 2", "Act 3"):
        assert act in out.stdout, f"{act} printed nothing:\n{out.stdout}"
    assert "every check passed" in out.stdout, out.stdout
    assert out.stdout.count("[check]") >= 9, f"the demo checks too little:\n{out.stdout}"


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
    with open(dest / "proto_db" / "transactions.py", encoding="utf-8") as f:
        assert "_phase_1_floor" not in f.read()
    out = _run(["-m", "pytest", "-q", "-p", "no:cacheprovider", "exercise_c1/test_c1_exercise.py"], str(dest), 240)
    assert out.returncode == 1, f"the planted bug was not caught\n{out.stdout}{out.stderr}"
    assert "an acknowledged root (R'" in out.stdout, out.stdout


def test_make_exercise_refuses_a_destination_inside_the_repository():
    out = _run([os.path.join(EXERCISE, "make_exercise.py"), os.path.join(REPO, "exercise-scratch")], REPO, 60)
    assert out.returncode != 0 and "inside the repository" in out.stderr, out.stdout + out.stderr
    assert not os.path.exists(os.path.join(REPO, "exercise-scratch"))
