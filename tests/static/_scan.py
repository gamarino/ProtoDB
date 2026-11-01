"""Shared helpers for the static checks (D16)."""
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def expand(roots):
    """Yield the .py files under each root (file or directory)."""
    for root in roots:
        p = REPO / root
        if p.is_dir():
            yield from sorted(p.rglob("*.py"))
        elif p.exists():
            yield p


def all_package_files():
    """Every .py file of the proto_db package, excluding the 0.1 test directory."""
    return [f for f in expand(["proto_db"]) if "tests" not in f.relative_to(REPO).parts]
