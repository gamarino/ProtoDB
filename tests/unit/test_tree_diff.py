"""Structural diffs of persistent trees (spec R65, Task 12).

``diff.changed_keys`` must yield exactly the keys whose presence or stored value (by token)
differs between two keyed trees, skipping shared subtrees without loading them; each collection's
``changed_units`` must cover every unit whose token differs between two versions.
"""
import random

import pytest

from proto_db.collections import avl, diff
from proto_db.collections.avl import KeyNode, PosNode
from proto_db.collections.dictionaries import Dictionary, RepeatedKeysDictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.collections.linking import lookup_key
from proto_db.collections.lists import List
from proto_db.collections.sets import CountedSet, Set
from proto_db.preconditions import ABSENT, UnitKind, same_token, token
from proto_db.storage.memory import MemoryStorage
from proto_db.values import order_key
from tests.support.counting_storage import CountingStorage
from tests.support.persistence import DirectContext


def _keyed(pairs):
    root = None
    for k, v in pairs:
        root = avl.key_put(root, k, v)[0]
    return root


def test_diff_equal_trees_yields_nothing_without_loading():
    storage = CountingStorage(MemoryStorage())
    ctx = DirectContext(storage)
    keyed_ptr = ctx.save(_keyed((k, f"v{k}") for k in range(500)))
    positional_ptr = ctx.save(avl.pos_from_iterable(range(500)))
    storage.reset()
    # Two distinct stub objects of the same saved tree: compared by pointer, never loaded.
    a, b = (DirectContext(storage).load(keyed_ptr, KeyNode.__type_name__) for _ in range(2))
    assert a is not b
    assert list(diff.changed_keys(a, b)) == []
    p, q = (DirectContext(storage).load(positional_ptr, PosNode.__type_name__) for _ in range(2))
    assert list(diff.changed_positions(p, q, 500)) == []
    assert storage.reads == []


def test_diff_of_one_change_loads_only_the_changed_paths():
    storage = CountingStorage(MemoryStorage())
    ctx = DirectContext(storage)
    n = 4096
    old_ptr = ctx.save(_keyed((k, k) for k in range(n)))
    new_ptr = ctx.save(avl.key_put(ctx.load(old_ptr, KeyNode.__type_name__), 1234, -1)[0])
    storage.reset()
    old = DirectContext(storage).load(old_ptr, KeyNode.__type_name__)
    new = DirectContext(storage).load(new_ptr, KeyNode.__type_name__)
    assert list(diff.changed_keys(old, new)) == [1234]
    # Two root-to-leaf paths and their left spines: far below the 2 * 4096 nodes of the trees.
    assert len(storage.pointers_read()) <= 4 * 2 * 14, len(storage.pointers_read())


def _expected_changed(model: dict, new_model: dict, root, new_root) -> list:
    """The order keys present in one tree only, or whose stored values differ by token (a large
    value's box by its pointer, D51: an equal value put again is a new box), by brute force."""
    changed = []
    for okey in sorted(set(model) | set(new_model)):
        key = (model.get(okey) or new_model.get(okey))[0]
        a, b = avl.key_get_stored(root, key, ABSENT), avl.key_get_stored(new_root, key, ABSENT)
        if a is ABSENT or b is ABSENT or not same_token(token(a), token(b)):
            changed.append(okey)
    return changed


def test_diff_yields_exactly_changed_keys():
    rng = random.Random(7)
    storage = MemoryStorage()
    for trial in range(60):
        model = {}
        root = None
        for _ in range(rng.randrange(0, 300)):
            k = rng.randrange(0, 400)
            v = rng.choice([rng.randrange(0, 50), "x" * rng.choice([1, 300])])
            root = avl.key_put(root, k, v)[0]
            model[order_key(k)] = (k, v)
        if trial % 2:                       # a saved, lazily loaded old tree: pointer skips
            ctx = DirectContext(storage)
            root = ctx.load(ctx.save(root), KeyNode.__type_name__) if root is not None else None
        new_root, new_model = root, dict(model)
        for _ in range(rng.randrange(0, 40)):
            k = rng.randrange(0, 400)
            op = rng.random()
            if op < 0.4:
                new_root = avl.key_remove(new_root, k)[0]
                new_model.pop(order_key(k), None)
            else:
                # Same value again, an exact-form change (1 vs 1.0) or a new value.
                old = new_model.get(order_key(k))
                v = (old[1] if old is not None and op < 0.6 else
                     float(old[1]) if old is not None and type(old[1]) is int and op < 0.7 else
                     rng.randrange(0, 50))
                new_root = avl.key_put(new_root, k, v)[0]
                new_model[order_key(k)] = (new_model.get(order_key(k), (k,))[0], v)
        got = [order_key(k) for k in diff.changed_keys(root, new_root)]
        assert got == _expected_changed(model, new_model, root, new_root), trial
        assert [order_key(k) for k in diff.changed_keys(new_root, root)] == got


def test_diff_yields_exactly_changed_positions():
    rng = random.Random(7)
    for trial in range(60):
        items = [rng.randrange(0, 20) for _ in range(rng.randrange(1, 300))]
        root = avl.pos_from_iterable(items)
        new_root, new_items = root, list(items)
        for _ in range(rng.randrange(0, 20)):
            if rng.random() < 0.5:
                i = rng.randrange(0, len(new_items))
                v = rng.choice([new_items[i], rng.randrange(0, 20)])
                new_root = avl.pos_set(new_root, i, v)
                new_items[i] = v
            else:
                v = rng.randrange(0, 20)
                new_root = avl.pos_insert(new_root, len(new_items), v)
                new_items.append(v)
        expected = [i for i in range(len(items)) if not same_token(token(items[i]), token(new_items[i]))]
        assert list(diff.changed_positions(root, new_root, len(items))) == expected, trial


# -- changed_units: every unit whose token differs is covered ---------------------------------

def _dictionary(rng, cls):
    d = cls()
    for _ in range(rng.randrange(0, 120)):
        d = d.set_at(rng.randrange(0, 150), rng.choice([rng.randrange(0, 5), 1.0, "s"]))
    return d


def _edit_dictionary(rng, d):
    for _ in range(rng.randrange(0, 15)):
        k = rng.randrange(0, 150)
        d = d.remove_at(k) if rng.random() < 0.3 and d.has(k) else d.set_at(k, rng.choice([rng.randrange(0, 5), 1.0]))
    return d


def _set(rng, cls):
    s = cls()
    for _ in range(rng.randrange(0, 120)):
        s = s.add(rng.choice([rng.randrange(0, 150), 7.0]))
    return s


def _edit_set(rng, s):
    for _ in range(rng.randrange(0, 15)):
        e = rng.choice([rng.randrange(0, 150), 7, 7.0])
        s = s.remove(e) if rng.random() < 0.4 else s.add(e)
    return s


def _rkd(rng, cls):
    d = cls()
    for _ in range(rng.randrange(0, 80)):
        d = d.set_at(rng.randrange(0, 40), rng.randrange(0, 4))
    return d


def _edit_rkd(rng, d):
    for _ in range(rng.randrange(0, 10)):
        k = rng.randrange(0, 40)
        d = d.remove_at(k) if rng.random() < 0.3 else d.set_at(k, rng.randrange(0, 4))
    return d


def _list(rng, cls):
    return cls.from_iterable(rng.randrange(0, 9) for _ in range(rng.randrange(1, 120)))


def _edit_list(rng, lst):
    for _ in range(rng.randrange(0, 10)):
        if rng.random() < 0.5:
            lst = lst.set_at(rng.randrange(0, lst.count), rng.randrange(0, 9))
        else:
            lst = lst.append_last(rng.randrange(0, 9))
    if rng.random() < 0.1:
        lst = lst.remove_at(0)
    return lst


CASES = [
    (Dictionary, UnitKind.KEY, _dictionary, _edit_dictionary, lambda c: list(c.keys())),
    (HashDictionary, UnitKind.KEY, _dictionary, _edit_dictionary, lambda c: list(c.keys())),
    (Set, UnitKind.ELEMENT, _set, _edit_set, lambda c: list(c.elements())),
    (CountedSet, UnitKind.COUNT, _set, _edit_set, lambda c: list(c.elements())),
    (RepeatedKeysDictionary, UnitKind.RECORDS, _rkd, _edit_rkd, lambda c: list(c.keys())),
    (List, UnitKind.POSITION, _list, _edit_list, lambda c: list(range(c.count))),
]


@pytest.mark.parametrize("cls, kind, build, edit, units_of", CASES, ids=[c[0].__name__ for c in CASES])
def test_changed_units_cover_every_changed_token(cls, kind, build, edit, units_of):
    rng = random.Random(7)
    storage = MemoryStorage()
    for trial in range(40):
        ctx = DirectContext(storage)
        old = ctx.load(ctx.save(build(rng, cls)), cls.__type_name__)
        new = edit(rng, old)
        ctx.save(new)
        units = list(cls.changed_units(old, new, "r"))
        if any(u.kind is UnitKind.WHOLE for u in units):
            assert cls is List and new.count < old.count, trial
            continue
        assert all(u.kind is kind and u.root == "r" for u in units)
        named = {order_key(lookup_key(u.key)) for u in units}
        for key in units_of(old) + units_of(new):
            if cls is List and key >= old.count:
                continue        # a position the old version lacks: no precondition held there (R65)
            if not same_token(old._unit_token(kind, key), new._unit_token(kind, key)):
                assert order_key(key) in named, (trial, key)
