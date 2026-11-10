"""Unit tests for proto_db.collections.hash_dictionaries.HashDictionary (spec section 8.2, D23, D24)."""
import os
import subprocess
import sys
import threading
from collections import Counter
from decimal import Decimal

import pytest

from proto_db.collections import avl
from proto_db.collections.base import ConcurrentOptimized
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.errors import ProtoUserError, ProtoValidationError
from proto_db.storage.memory import MemoryStorage
from proto_db.values import hash_key, order_key
from tests.support.persistence import DirectContext
from tests.support.recording_index import RecordingIndex


class CollidingHashDictionary(HashDictionary):
    """Four buckets only, so that collision handling is exercised on every operation."""

    __type_name__ = "tests.unit.CollidingHashDictionary"

    def _bucket_hash(self, key):
        return hash_key(key) % 4


def make(pairs=(), indexes=(), cls=HashDictionary):
    d = cls()
    for name in indexes:
        d = d.add_index(RecordingIndex(name))
    for k, v in pairs:
        d = d.set_at(k, v)
    return d


def reload(coll, context=None):
    context = context or DirectContext(MemoryStorage())
    return context.load(context.save(coll), type(coll).__type_name__)


both = pytest.mark.parametrize("cls", [HashDictionary, CollidingHashDictionary])


class TestBasics:
    def test_empty(self):
        d = HashDictionary()
        assert d.count == 0 and list(d.items()) == [] and not d.has("a") and d.get_at("a", 3) == 3

    def test_concurrent_optimized(self):
        assert isinstance(HashDictionary(), ConcurrentOptimized)

    @both
    def test_set_get_has_remove(self, cls):
        pairs = [(f"k{i}", i) for i in range(30)]
        d = make(pairs, cls=cls)
        assert d.count == 30 and dict(d.items()) == dict(pairs)
        assert sorted(d.values()) == list(range(30)) and Counter(d.elements()) == Counter(range(30))
        assert sorted(d.keys()) == sorted(k for k, _ in pairs)
        assert all(d.get_at(k) == v and d.has(k) and k in d for k, v in pairs)
        r = d.remove_at("k3").remove_at("k17")
        assert r.count == 28 and not r.has("k3") and not r.has("k17") and r.get_at("k4") == 4
        assert d.count == 30 and d.has("k3")

    @both
    def test_overwrite(self, cls):
        d = make([(1, "a"), (2, "b")], cls=cls).set_at(1.0, "c")
        assert d.count == 2 and d.get_at(1) == "c" and d.get_at(2) == "b"
        assert type(next(k for k in d.keys() if k == 1)) is int

    @both
    def test_remove_absent_key_never_deletes_another_entry(self, cls):
        d = make([(k, k) for k in range(0, 40, 2)], cls=cls)
        for absent in (-1, 1, 7, 39, 100, "x", None, True):
            r = d.remove_at(absent)
            assert dict(r.items()) == dict(d.items()) and r.count == d.count and r.root is d.root

    def test_collisions_share_buckets(self):
        d = make([(i, i) for i in range(20)], cls=CollidingHashDictionary)
        assert d.root.count <= 4  # tree nodes = buckets
        assert sum(len(bucket) for _, bucket in avl.key_iter(d.root)) == 20

    def test_key_constraints(self):
        """Keys must satisfy values.hash_key: lists, dicts and NaN are rejected (D24)."""
        for key in ([1], {"a": 1}, float("nan")):
            with pytest.raises(ProtoValidationError):
                HashDictionary().set_at(key, 1)
            with pytest.raises(ProtoValidationError):
                HashDictionary().has(key)
        with pytest.raises(ProtoValidationError):
            HashDictionary().set_at("k", object())

    def test_immutable(self):
        with pytest.raises(ProtoUserError):
            make([("a", 1)]).root = None


class TestKeyRegressions:
    @both
    def test_large_int_keys(self, cls):
        keys = [2**60 + i for i in range(50)]
        d = make([(k, i) for i, k in enumerate(keys)], cls=cls)
        assert d.count == 50
        loaded = reload(d)
        assert all(loaded.get_at(k) == i and loaded.has(k) for i, k in enumerate(keys))

    @both
    def test_bool_int_float_keys(self, cls):
        d = make([(1, "int"), (True, "bool"), (1.0, "float")], cls=cls)
        assert d.count == 2
        assert d.get_at(Decimal(1)) == "float" and d.get_at(True) == "bool"
        assert d.remove_at(1).get_at(True) == "bool" and d.remove_at(True).get_at(1) == "float"

    @both
    def test_has_on_reloaded(self, cls):
        context = DirectContext(MemoryStorage())
        d = make([("a", 1), ("b", 2)], cls=cls)
        assert reload(d, context).has("a") and not reload(d, context).has("z")
        assert reload(d, context).get_at("b") == 2 and "a" in reload(d, context)

    def test_persisted_layout_survives_a_process_with_another_hash_seed(self, tmp_path):
        """Bucket hashes are process-independent, so a reopened space finds every key (D24)."""
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        script = r"""
import sys
sys.path.insert(0, sys.argv[1])
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.storage.file import FileStorage
from proto_db.core.atom import AtomPointer
from tests.support.persistence import DirectContext
import uuid
storage = FileStorage(sys.argv[2], durability="none")
context = DirectContext(storage)
keys = ["alpha", b"beta", ("t", 1), 2**60, 1.5]
if sys.argv[3] == "write":
    d = HashDictionary()
    for i, k in enumerate(keys):
        d = d.set_at(k, i)
    ptr = context.save(d)
    storage.flush()
    print(ptr.segment.hex, ptr.offset)
else:
    seg, off = sys.argv[4], int(sys.argv[5])
    d = context.load(AtomPointer(uuid.UUID(seg), off), HashDictionary.__type_name__)
    assert [d.get_at(k) for k in keys] == list(range(5)), [d.get_at(k) for k in keys]
    print("ok")
storage.close()
"""
        space = str(tmp_path / "space")
        env1 = dict(os.environ, PYTHONHASHSEED="1")
        out = subprocess.run([sys.executable, "-c", script, root, space, "write"], env=env1,
                             check=True, capture_output=True, text=True).stdout.split()
        env2 = dict(os.environ, PYTHONHASHSEED="99")
        res = subprocess.run([sys.executable, "-c", script, root, space, "read", *out], env=env2,
                             capture_output=True, text=True)
        assert res.returncode == 0 and res.stdout.strip() == "ok", res.stderr


class TestIndexEvents:
    @both
    def test_events(self, cls):
        d = make([("a", 1), ("b", 2)], indexes=("i",), cls=cls)
        base = d.get_index("i")
        assert d.set_at("c", 3).get_index("i").events_since(base) == (("add", 3),)
        assert d.set_at("a", 10).get_index("i").events_since(base) == (("replace", 1, 10),)
        assert d.remove_at("b").get_index("i").events_since(base) == (("remove", 2),)
        assert d.remove_at("zz").indexes is d.indexes


class TestRebase:
    @both
    def test_rebase_per_key(self, cls):
        snapshot = reload(make([("k", 1), ("j", 5)], indexes=("i",), cls=cls))
        theirs = snapshot.set_at("k", 2).set_at("x", 9)
        mine = snapshot.set_at("k", 3).remove_at("j").set_at("y", 4).remove_at("nope")
        merged = mine.rebase_onto(theirs)
        assert dict(merged.items()) == {"k": 3, "x": 9, "y": 4}
        assert merged.get_index("i").live() == Counter(order_key(v) for v in merged.values())


class TestBulk:
    @both
    def test_from_items(self, cls):
        d = cls.from_items([("b", 1), ("a", 2), ("b", 3), (1, 0), (1.0, 9)])
        assert dict(d.items()) == {1: 9, "a": 2, "b": 3} and d.count == 3
        avl.check_invariants(d.root)
        assert cls.from_items([]).count == 0
        with pytest.raises(ProtoValidationError):
            cls.from_items([([1], 1)])

    def test_large_bulk(self):
        n = 100_000
        d = HashDictionary.from_items((i, i) for i in range(n))
        assert d.count == n
        loaded = reload(d)
        assert all(loaded.get_at(i) == i for i in range(0, n, 997))
        assert sum(1 for _ in loaded.values()) == n


class TestFreeThreading:
    def test_concurrent_readers_and_writers(self):
        n = 1_500
        base = reload(HashDictionary.from_items((f"k{i}", i) for i in range(n)))
        expected = {f"k{i}": i for i in range(n)}
        errors, barrier = [], threading.Barrier(12)

        def reader():
            barrier.wait()
            for _ in range(4):
                if dict(base.items()) != expected or base.count != n:
                    errors.append("items")
                for i in range(0, n, 29):
                    if base.get_at(f"k{i}") != i:
                        errors.append(i)

        def writer(seed):
            barrier.wait()
            d = base
            for i in range(200):
                d = d.set_at(f"w{seed}-{i}", i).remove_at(f"k{(seed * 11 + i) % n}")
            if d.count != n:
                errors.append(f"writer {seed}")

        threads = [threading.Thread(target=reader) for _ in range(8)]
        threads += [threading.Thread(target=writer, args=(s,)) for s in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
