"""Catches: production code that references pytest/unittest/tests (test-aware code, D16)."""
import ast

from tests.static.test_no_swallowed_exceptions import checked_files

FORBIDDEN = ("pytest", "unittest", "proto_db.tests")


def _mentions(tree):
    for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            names = [node.value]
        for n in names:
            if any(f in n for f in FORBIDDEN):
                yield node.lineno, n


def test_no_test_awareness():
    offenders = []
    for path in checked_files():
        for line, name in _mentions(ast.parse(path.read_text(encoding="utf-8"))):
            offenders.append(f"{path}:{line}: mentions {name!r}")
    assert not offenders, "\n".join(offenders)
