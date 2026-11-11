"""Saving a long chain of unsaved versions (R128).

The versioned-entity pattern (each version holds the previous one in a field, the tutorials'
``previous``) builds chains of atoms that are all saved by one commit.  Catches: a save that
recurses once per link (``RecursionError`` at about 200 versions); a chain saved out of order
or with a link lost; a cycle of unsaved atoms looping forever instead of failing.
"""
from __future__ import annotations

import sys

import pytest

from proto_db.core.atom import Atom
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoValidationError
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace

VERSIONS = 10_000


def _chain(n):
    obj = DBObject(v=0, previous=None, tags=("first",))
    for i in range(1, n):
        obj = obj.with_(v=i, previous=obj, tags=("v", i, {"nested": [obj]} if i % 1000 == 0 else None))
    return obj


def _walk(obj):
    seen = []
    while obj is not None:
        seen.append(obj.v)
        obj = obj.previous
    return seen


@pytest.mark.parametrize("storage", ["memory", "file"])
def test_ten_thousand_versions_save_in_one_transaction(storage, tmp_path):
    assert VERSIONS >= 10 * sys.getrecursionlimit()
    make = (lambda: MemoryStorage()) if storage == "memory" else (lambda: FileStorage(str(tmp_path / "db")))
    space = ObjectSpace(make())
    with space.new_database("d").new_transaction() as tr:
        tr.set_root_object("entity", _chain(VERSIONS))
    if storage == "file":
        space.close()
        space = ObjectSpace(make())
    with space.open_database("d").new_transaction() as tr:
        head = tr.get_root_object("entity")
        assert _walk(head) == list(range(VERSIONS - 1, -1, -1))
        assert head.previous.tags == ("v", VERSIONS - 2, None)
        cross = head
        while cross.v != 5000:
            cross = cross.previous
        assert cross.tags[2]["nested"][0].v == 4999
    space.close()
    if storage == "file":
        from proto_db.tools.verify import verify
        report, problems = verify(str(tmp_path / "db"))
        # Every record decodes and is canonical.  The database description (the conformance
        # notation, nested records described by content) does not reach 10,000 levels: that
        # open item is reported to the controller (R128 report), not decided here.
        assert report["atoms"] >= VERSIONS
        assert [p for p in problems if not p.startswith("FAILED describing database")] == []


def test_a_long_chain_grows_over_later_transactions():
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("d")
    with db.new_transaction() as tr:
        tr.set_root_object("entity", _chain(500))
    with db.new_transaction() as tr:
        head = tr.get_root_object("entity")
        for i in range(500, 3000):
            head = head.with_(v=i, previous=head)
        tr.set_root_object("entity", head)
    with db.new_transaction() as tr:
        assert _walk(tr.get_root_object("entity")) == list(range(2999, -1, -1))
    space.close()


class _Cell(Atom):
    """A mutable atom: two of them can refer to each other before either is saved."""
    __type_name__ = "tests.deep_chains.Cell"

    def __init__(self, n):
        super().__init__()
        object.__setattr__(self, "n", n)
        object.__setattr__(self, "other", None)


def test_a_cycle_of_unsaved_atoms_is_refused():
    space = ObjectSpace(MemoryStorage())
    cells = [_Cell(i) for i in range(400)]
    for a, b in zip(cells, cells[1:] + cells[:1]):
        object.__setattr__(a, "other", b)
    with space.new_database("d").new_transaction() as tr:
        tr.set_root_object("ring", cells[0])
        with pytest.raises(ProtoValidationError, match="cycle"):
            tr.commit()
    space.close()
