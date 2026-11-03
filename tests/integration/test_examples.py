"""The runnable examples under examples/ and the Python snippets of README.md (Task 8.5).

Catches: an example or a README snippet broken by an API change (each runs in a fresh
interpreter, as a user would run it, and must print its own success line; the examples
assert their own results), and an example added to examples/ without a test here.
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
