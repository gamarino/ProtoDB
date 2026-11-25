"""Root-name helpers for tests."""
from __future__ import annotations


def reserved_root_names(db) -> set:
    """Every root name of the database's current version, reserved ``__*`` names included.

    It reads the DatabaseRoot's ``objects`` map directly: ``Transaction.root_names`` is the application view.
    """
    tr = db.new_transaction()
    try:
        return set(tr._objects.keys())
    finally:
        tr.abort()
