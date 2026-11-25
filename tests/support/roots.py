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


def counted_set_root(tr, name: str):
    """Root ``name``'s CountedSet as ``tr``'s linked version, or a new CountedSet when the root is
    absent.  Adds to the linked version compose with concurrent adds without a precondition (spec
    section 5, D26); the first creation of the root compares against its absence (two concurrent
    creators conflict once, and ``Database.run`` retries)."""
    counted = tr.get_root_object(name)
    return counted if counted is not None else tr.new_counted_set()
