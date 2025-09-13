"""
LINQ-like API: policies for unsupported expressions.
Run: python examples/linq_policies.py
"""

import os
import sys
# Ensure project root is on sys.path for direct execution
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from proto_db.linq import from_collection, F, Policy


def main():
    items = [{"score": 0.9}, {"score": 0.0}, {"score": None}]

    def py_check(x):
        # A custom Python predicate that is not directly translatable
        return (x.get("score") or 0) > 0.8

    # Error on unsupported (will raise)
    try:
        _ = (
            from_collection(items)
            .with_policy(Policy(on_unsupported="error"))
            .where(lambda x: py_check(x))
            .count()
        )
    except Exception as ex:
        print("Expected error (on_unsupported=error):", ex)

    # Warn and fallback to local
    res = (
        from_collection(items)
        .on_unsupported("warn")
        .where(lambda x: py_check(x))
        .take(10)
        .to_list()
    )
    print("Warn+fallback results:", res)

    # Fully supported using F DSL (no fallback needed)
    res2 = (
        from_collection(items)
        .where((F.score != None) & (F.score >= 0.8))
        .to_list()
    )
    print("DSL-supported results:", res2)


if __name__ == "__main__":
    main()
