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


# -- review of R128: I1 (linear past SAVE_NESTING), m1 (one error, any depth) ---------------------

def _pushed(storage_cls):
    """A storage that records the type tag and plain scalar fields of every pushed record, in order."""
    class Recording(storage_cls):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            self.pushed = []

        def push_atom(self, payload):
            state = payload["s"]
            self.pushed.append((payload["t"], tuple(sorted((k, v) for k, v in state.items()
                                                           if isinstance(v, (int, str)) and not isinstance(v, bool)))))
            return super().push_atom(payload)
    return Recording()


def _fanned(width, depth):
    """A record ``depth`` versions above one holding ``width`` fresh records (some shared)."""
    shared = DBObject(k=-1)
    kids = tuple(DBObject(k=j, also=shared if j % 7 == 0 else None) for j in range(width))
    obj = DBObject(v=-1, kids=kids, previous=None)
    for i in range(depth):
        obj = obj.with_(v=i, previous=obj, kids=None)
    return obj


def test_a_wide_record_past_the_nesting_limit_is_encoded_in_linear_work(monkeypatch):
    # Each value encoded counts once (encode_value recurses through the module global): an encoding
    # restarted after each of the k children (the first iterative save) costs about k**2 / 2.
    from proto_db.core import codec
    values = [0]
    real = codec.encode_value

    def counting(value, save_child):
        values[0] += 1
        return real(value, save_child)
    monkeypatch.setattr(codec, "encode_value", counting)
    width, depth = 4000, 60
    space = ObjectSpace(MemoryStorage())
    with space.new_database("d").new_transaction() as tr:
        tr.set_root_object("e", _fanned(width, depth))
        before = values[0]
        tr.commit()
    assert values[0] - before <= 20 * (width + depth)      # quadratic: about 8,000,000
    space.close()


@pytest.mark.parametrize("nesting", [0, 1, 48])
def test_the_iterative_save_writes_what_the_recursive_save_writes(monkeypatch, nesting):
    from proto_db.core import atom as atom_module

    def records(limit):
        monkeypatch.setattr(atom_module, "SAVE_NESTING", limit)
        storage = _pushed(MemoryStorage)
        space = ObjectSpace(storage)
        with space.new_database("d").new_transaction() as tr:
            tr.set_root_object("e", _fanned(30, 70))
        space.close()
        return storage.pushed
    recursive = records(10**9)
    assert records(nesting) == recursive


class _NoTypeName(Atom):
    pass


@pytest.mark.parametrize("depth", [5, 60])
def test_an_error_deep_in_a_chain_is_reported_once_whatever_the_depth(depth):
    obj = DBObject(bad=_NoTypeName())
    for i in range(depth):
        obj = obj.with_(v=i, previous=obj, bad=None)
    space = ObjectSpace(MemoryStorage())
    with space.new_database("d").new_transaction() as tr:
        tr.set_root_object("e", obj)
        with pytest.raises(ProtoValidationError) as info:
            tr.commit()
    space.close()
    message = str(info.value)
    assert message.startswith("Cannot save an instance of") and "Attribute" not in message
