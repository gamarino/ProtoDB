"""Catches: bare except, swallowed broad exception handlers, and test-aware code (D16).

Run as a script with ``--all`` to list offenders across the whole package.
"""
import ast
import sys
from pathlib import Path

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.static import _scan

# New modules are appended as they are written; Phase 11 replaces this with the whole package.
CHECKED_ROOTS = [
    "proto_db/errors.py",
]


def checked_files():
    """Files covered by CHECKED_ROOTS."""
    return list(_scan.expand(CHECKED_ROOTS))

BROAD = {"Exception", "BaseException"}
FORBIDDEN = ("pytest", "unittest", "proto_db.tests")


def _is_broad(handler):
    t = handler.type
    if t is None:
        return True
    elts = t.elts if isinstance(t, ast.Tuple) else [t]
    return any(isinstance(x, ast.Name) and x.id in BROAD for x in elts)


def _trivial(body):
    for stmt in body:
        if isinstance(stmt, (ast.Pass, ast.Continue)):
            continue
        if isinstance(stmt, ast.Return) and (stmt.value is None or isinstance(stmt.value, ast.Constant)):
            continue
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant):
            continue  # docstring / ellipsis
        return False
    return True


def find_offenders(path):
    """Return a list of offence descriptions for one file."""
    out = []
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler):
            if node.type is None:
                out.append(f"{path}:{node.lineno}: bare except")
            elif _is_broad(node) and _trivial(node.body):
                out.append(f"{path}:{node.lineno}: swallowed broad exception")
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        for n in names:
            if any(f in n for f in FORBIDDEN):
                out.append(f"{path}:{node.lineno}: imports {n}")
    return out


def test_no_swallowed_exceptions():
    offenders = [o for p in checked_files() for o in find_offenders(p)]
    assert not offenders, "\n".join(offenders)


def test_checker_detects_offences(tmp_path):
    """Self-test: the checker must flag the patterns it claims to flag."""
    bad = tmp_path / "bad.py"
    bad.write_text(
        "def f():\n"
        "    try:\n        x()\n    except:\n        pass\n"
        "    try:\n        x()\n    except Exception:\n        return None\n"
        "    try:\n        x()\n    except (ValueError, BaseException):\n        pass\n"
        "import pytest\n"
    )
    assert len(find_offenders(bad)) == 4
    good = tmp_path / "good.py"
    good.write_text(
        "def f():\n    try:\n        x()\n    except Exception as e:\n        raise RuntimeError() from e\n"
        "    try:\n        x()\n    except KeyError:\n        pass\n"
    )
    assert find_offenders(good) == []


if __name__ == "__main__":
    if "--all" in sys.argv:
        files = _scan.all_package_files()
        found = [o for p in files for o in find_offenders(p)]
        print("\n".join(found))
        print(f"{len(found)} offenders in {len(files)} files")
    else:
        print("usage: test_no_swallowed_exceptions.py --all")
