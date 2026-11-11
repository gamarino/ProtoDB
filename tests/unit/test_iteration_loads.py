"""An in-order walk of a tree loads each node once, whatever the atom cache evicts (R128).

The walks keep the nodes still to visit on an explicit stack.  A node read when the walk went
down past it, and read again when the walk came back to it, is loaded twice when the atoms the
consumer loaded meanwhile evicted it: the CAD tutorial's 13 extra loads per pass with a cache of
50 entries (553 loads for 540 atoms).  Catches: a walk that reads a stacked node's fields after
it was pushed, so a small cache reloads it.
"""
from __future__ import annotations

import pytest

from proto_db.collections import avl
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace


@pytest.fixture(scope="module")
def directory(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("walks") / "db")
    space = ObjectSpace(FileStorage(path))
    with space.new_database("d").new_transaction() as tr:
        items = tr.new_list()
        table = tr.new_dictionary()
        for i in range(120):
            detail = tr.new_list()
            for j in range(6):
                detail = detail.append_last(tr.new_object(j=j, data=tuple(range(i, i + 10))))
            record = tr.new_object(i=i, detail=detail)
            items = items.append_last(record)
            table = table.set_at(i, record)
        tr.set_root_object("items", items)
        tr.set_root_object("table", table)
    space.close()
    return path


def _consume(record) -> int:
    """Read the record and everything under it: many loads between two yields of the walk."""
    return record.i + sum(d.j + len(d.data) for d in record.detail)


WALKS = {
    "pos_iter": lambda tr: avl.pos_iter(tr.get_root_object("items").root),
    "pos_iter_from": lambda tr: avl.pos_iter_from(tr.get_root_object("items").root, 7),
    "key_iter": lambda tr: (v for _, v in avl.key_iter(tr.get_root_object("table").root)),
    "key_iter_range": lambda tr: (v for _, v in avl.key_iter_range(tr.get_root_object("table").root, 5, 100)),
    "key_iter_reversed": lambda tr: (v for _, v in avl.key_iter_reversed(tr.get_root_object("table").root)),
    "key_iter_from": lambda tr: (v for _, v in avl.key_iter_from(tr.get_root_object("table").root, 9)),
}


def _pass_loads(directory, entries, walk) -> tuple:
    space = ObjectSpace(FileStorage(directory), atom_cache_entries=entries)
    try:
        db = space.open_database("d")
        before = space.atom_cache_stats()["loads"]
        tr = db.new_transaction()
        total = sum(_consume(record) for record in WALKS[walk](tr))
        tr.abort()
        return total, space.atom_cache_stats()["loads"] - before
    finally:
        space.close()


@pytest.mark.parametrize("walk", sorted(WALKS))
def test_a_small_cache_loads_each_node_once_per_walk(directory, walk):
    total, cold = _pass_loads(directory, 100_000, walk)
    small_total, small = _pass_loads(directory, 8, walk)
    assert small_total == total
    assert small == cold
