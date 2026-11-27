"""The runnable examples under examples/ and the Python snippets of README.md (Task 8.5).

Catches: an example or a README snippet broken by an API change (each runs in a fresh
interpreter, as a user would run it, and must print its own success line; the examples
assert their own results), and an example added to examples/ without a test here.

The tutorial scripts under examples/tutorial/ (``NN_<slug>.py``, generated from the Markdown chapters of
docs/tutorial/ by tests/docs/test_markdown_tutorials.py) are run the same way: each compares what it prints
with the output its chapter shows and ends with ``<name>: OK``.
"""
import os
import re
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EXAMPLES = os.path.join(REPO, "examples")

#: Every example and a line its output must contain (besides its final "<name>: OK").
EXPECTED = {
    "simple_example.py": "versions, newest first: [2, 1, 0]",
    "indexed_query.py": "with indexes:    IndexLookup",
    "linq_queries.py": "distinct items: ['desk', 'ink', 'lamp', 'paper', 'pen']",
    "transactions_concurrency.py": "conflict detected",
    "vector_search.py": "[docs_ivf] plan: VectorSearch <- VectorIvfFlat (backend 'ivf_flat')",
    "file_storage.py": "plan: Sort <- IndexRange",
}


def run_python(args, cwd):
    """Run the interpreter of this test session on ``args``; return the completed process."""
    return subprocess.run([sys.executable, *args], cwd=cwd, capture_output=True, text=True, timeout=240)


def test_every_example_is_tested():
    scripts = sorted(name for name in os.listdir(EXAMPLES) if name.endswith(".py"))
    assert scripts == sorted(EXPECTED)


@pytest.mark.parametrize("script", sorted(EXPECTED))
def test_example_runs(script, tmp_path):
    if script == "vector_search.py":
        pytest.importorskip("numpy")
    # Run from an unrelated directory: an example must work wherever it is started from.
    proc = run_python([os.path.join(EXAMPLES, script)], cwd=str(tmp_path))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert EXPECTED[script] in proc.stdout, proc.stdout
    assert f"{script[:-3]}: OK" in proc.stdout.splitlines()[-1], proc.stdout


TUTORIAL = os.path.join(EXAMPLES, "tutorial")

#: The tutorial scripts that build a VectorIndex; its default backend needs numpy.
TUTORIAL_NEEDS_NUMPY = {"09_vector_search.py", os.path.join("queries", "05_range_vector_and_custom_indexes.py")}


def tutorial_scripts():
    """The generated tutorial scripts, as paths relative to examples/tutorial (the helpers there are not scripts)."""
    found = []
    for directory in ("", "queries", "semantic"):
        names = os.listdir(os.path.join(TUTORIAL, directory))
        found += [os.path.join(directory, name) for name in names if re.match(r"\d\d_.*\.py$", name)]
    return sorted(found)


def test_every_tutorial_has_scripts():
    scripts = tutorial_scripts()
    for directory in ("", "queries", "semantic"):
        assert sum(1 for s in scripts if os.path.dirname(s) == directory) >= 5, directory


@pytest.mark.parametrize("script", tutorial_scripts())
def test_tutorial_script_runs(script, tmp_path):
    if script in TUTORIAL_NEEDS_NUMPY:
        pytest.importorskip("numpy")
    # From an unrelated directory and without PYTHONPATH: a script finds the repository and its helpers itself.
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    proc = subprocess.run([sys.executable, os.path.join(TUTORIAL, script)], cwd=str(tmp_path), capture_output=True,
                          text=True, timeout=240, env=env)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    name = os.path.basename(script)[:-3]
    assert proc.stdout.splitlines()[-1] == f"{name}: OK", proc.stdout[-2000:]


def test_tutorial_scripts_share_one_directory(tmp_path):
    """All the scripts, run twice from ONE directory, as a reader runs them from a checkout: none may depend on
    a fresh directory, collide with another's files, or leave anything behind in the directory it ran from."""
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    scripts = [s for s in tutorial_scripts() if s not in TUTORIAL_NEEDS_NUMPY or _has_numpy()]
    for round_number in (1, 2):
        for script in scripts:
            proc = subprocess.run([sys.executable, os.path.join(TUTORIAL, script)], cwd=str(tmp_path),
                                  capture_output=True, text=True, timeout=240, env=env)
            assert proc.returncode == 0, f"round {round_number}, {script}:\n{proc.stdout[-2000:]}{proc.stderr}"
    assert os.listdir(tmp_path) == [], f"left behind: {sorted(os.listdir(tmp_path))}"


def _has_numpy() -> bool:
    try:
        import numpy  # noqa: F401
    except ImportError:
        return False
    return True


def readme_snippets():
    """The ```python blocks of README.md, in order."""
    with open(os.path.join(REPO, "README.md"), encoding="utf-8") as f:
        text = f.read()
    return re.findall(r"```python\n(.*?)```", text, flags=re.DOTALL)


def test_readme_has_snippets():
    assert len(readme_snippets()) >= 3


@pytest.mark.parametrize("number", range(len(readme_snippets())))
def test_readme_snippet_runs(number, tmp_path):
    snippet = readme_snippets()[number]
    env = dict(os.environ, PYTHONPATH=REPO)
    proc = subprocess.run([sys.executable, "-c", snippet], cwd=str(tmp_path), capture_output=True, text=True,
                          timeout=240, env=env)
    assert proc.returncode == 0, f"README snippet {number}:\n{snippet}\n{proc.stdout}{proc.stderr}"
