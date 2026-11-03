"""Large node values live in atoms of their own (D51, Task 8.3f item 2).

A ``bytes`` or ``str`` value of at least ``VALUE_BOX_MIN`` held by an AVL node is stored
through a :class:`~proto_db.collections.avl.ValueBox`: path copying re-writes a reference to
the box, never the value, so a commit writes a large value once.  Measured before D51 (probe
``amp.py``, one dictionary entry of 100 000 bytes added per commit after 1000 commits): a
commit wrote 1 207 609 bytes of atoms, about twelve copies of the value; with D51 it writes
104 840.
"""
import msgpack
import pytest

from proto_db import MemoryStorage, ObjectSpace
from proto_db.collections import avl
from proto_db.storage.file import FileStorage

BIG = b"v" * 100_000
TEXT = "t" * 1_000


def _bytes_of_next_commit(db, body) -> int:
    """Atom bytes pushed to storage by one ``db.run(body)``."""
    storage = db.space.storage
    pushed = []
    original = storage.push_atom

    def push_atom(payload):
        pushed.append(len(msgpack.packb(payload, use_bin_type=True)))
        return original(payload)
    storage.push_atom = push_atom
    try:
        db.run(body)
    finally:
        del storage.push_atom
    return sum(pushed)


def test_large_values_are_boxed_and_small_ones_are_not():
    node = avl.key_put(None, 1, BIG)[0]
    assert type(node.value) is avl.ValueBox and node.value.value is BIG
    assert avl.key_get(node, 1) is BIG
    small = b"s" * (avl.VALUE_BOX_MIN - 1)
    assert avl.key_get(avl.key_put(node, 2, small)[0], 2) is small
    assert avl.key_put(node, 2, small)[0].right.value is small
    assert type(avl.pos_insert(None, 0, TEXT).value) is avl.ValueBox
    assert list(avl.pos_iter(avl.pos_insert(None, 0, TEXT))) == [TEXT]


def test_path_copies_share_the_box():
    root = avl.key_from_sorted((i, BIG if i == 0 else i) for i in range(64))
    box = next(n for n in _nodes(root) if n.key == 0).value
    assert type(box) is avl.ValueBox
    copy = avl.key_put(root, 63, "changed")[0]
    assert next(n for n in _nodes(copy) if n.key == 0).value is box


def _nodes(root):
    stack = [root]
    while stack:
        n = stack.pop()
        if n is not None:
            yield n
            stack += [n.left, n.right]


@pytest.mark.parametrize("make", ["dictionary", "list"])
def test_a_large_value_round_trips_through_storage(tmp_path, make):
    space = ObjectSpace(FileStorage(str(tmp_path / "s"), durability="none"))
    space.new_database("db")
    db = space.open_database("db")

    def write(tr):
        if make == "dictionary":
            tr.set_root_object("c", tr.new_dictionary().set_at("k", BIG).set_at("t", TEXT).set_at("n", 5))
        else:
            tr.set_root_object("c", tr.new_list().append_last(BIG).append_last(TEXT).append_last(5))
    db.run(write)
    space.close()
    space = ObjectSpace(FileStorage(str(tmp_path / "s"), durability="none"))
    with space.open_database("db").new_transaction() as tr:
        c = tr.get_root_object("c")
        values = [c.get_at("k"), c.get_at("t"), c.get_at("n")] if make == "dictionary" else list(c)
        assert values == [BIG, TEXT, 5]
        assert all(type(v) is not avl.ValueBox for v in values)
    space.close()


def test_a_commit_writes_a_large_value_once():
    """Blind writes merged into a dictionary holding 200 large values (D33 merge path): each
    commit writes about one value, not one per copied ancestor, and the merge reuses the box
    the transaction saved before it (no second copy)."""
    space = ObjectSpace(MemoryStorage())
    space.new_database("db")
    db = space.open_database("db")
    for i in range(200):
        db.run(lambda tr, i=i: tr.set_root_object("blobs", tr.new_dictionary().set_at(i, BIG)))
    written = _bytes_of_next_commit(db, lambda tr: tr.set_root_object("blobs", tr.new_dictionary().set_at(200, BIG)))
    assert len(BIG) < written < 1.2 * len(BIG), written
    with db.new_transaction() as tr:
        blobs = tr.get_root_object("blobs")
        assert blobs.count == 201 and blobs.get_at(200) == BIG and blobs.get_at(0) == BIG


def test_a_value_box_is_immutable():
    from proto_db.errors import ProtoUserError
    box = avl.ValueBox(BIG)
    with pytest.raises(ProtoUserError):
        box.value = b"x"
    with pytest.raises(ProtoUserError):
        del box.value
