"""Construction-cost guarantees of atoms (Task 1.5, decision D21).

Building an in-memory atom must not create a UUID or a lock: identity is assigned
lazily and locking uses a module-level striped pool.
"""
import threading
import uuid

from proto_db.collections.avl import KeyNode, PosNode
from proto_db.core import atom as atom_module
from proto_db.core.atom import Atom
from proto_db.core.dbobject import DBObject
from tests.support.dict_storage import DictStorage
from tests.support.persistence import DirectContext

_LOCK_TYPES = (type(threading.Lock()), type(threading.RLock()))


class Plain(Atom):
    """Atom with one persisted attribute."""

    def __init__(self, value, *, context=None):
        super().__init__(context=context)
        self.value = value


def _assert_cheap(atom):
    values = list(vars(atom).values())
    assert "oid" not in vars(atom)
    assert not any(isinstance(v, uuid.UUID) for v in values)
    assert not any(isinstance(v, _LOCK_TYPES) for v in values)


def test_bare_atoms_hold_no_uuid_and_no_lock():
    for atom in (Plain(1), PosNode(1), PosNode(2, PosNode(1), None), KeyNode("k", 1),
                 DBObject(a=1), DBObject(a=1).with_(a=2)):
        _assert_cheap(atom)


def test_construction_does_not_call_uuid4(monkeypatch):
    calls = []
    real = uuid.uuid4

    def counting():
        calls.append(1)
        return real()

    monkeypatch.setattr(atom_module.uuid, "uuid4", counting)
    nodes = [PosNode(i) for i in range(100)]
    DBObject(a=1).with_(b=2)
    assert calls == []
    _ = nodes[0].oid
    assert len(calls) == 1


def test_lock_for_is_a_shared_striped_pool():
    atoms = [Plain(i) for i in range(2000)]
    locks = {id(atom_module._lock_for(a)) for a in atoms}
    assert atom_module._lock_for(atoms[0]) is atom_module._lock_for(atoms[0])
    assert isinstance(atom_module._lock_for(atoms[0]), _LOCK_TYPES)
    assert len(locks) <= atom_module._STRIPES
    # Allocator alignment must not collapse the pool onto a few stripes.
    assert len(locks) >= atom_module._STRIPES // 2


def test_stubs_hold_no_lock():
    c = DirectContext(DictStorage())
    ptr = PosNode(1, context=c)._save()
    stub = c.load(ptr, PosNode.__type_name__)
    assert not any(isinstance(v, _LOCK_TYPES) for v in vars(stub).values())
    assert stub.value == 1
    assert not any(isinstance(v, _LOCK_TYPES) for v in vars(stub).values())


def test_concurrent_saves_of_cross_striped_trees_do_not_deadlock():
    # Many threads save distinct trees that share subtrees; with striped locks a
    # parent and an unrelated child often share or cross stripes.  Must finish.
    c = DirectContext(DictStorage())
    shared = [PosNode(i) for i in range(64)]
    roots = [PosNode(i, shared[i % 64], shared[(i * 7) % 64], context=c) for i in range(512)]
    barrier = threading.Barrier(16)
    errors = []

    def worker(k):
        barrier.wait()
        try:
            for r in roots[k::16] + roots[::-1][k::16]:
                r._save()
        except Exception as e:  # recorded; asserted empty below
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(k,), daemon=True) for k in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not any(t.is_alive() for t in threads), "deadlock"
    assert not errors
    assert c.storage.push_count == 512 + 64


def test_state_snapshot_tolerates_concurrent_oid_assignment():
    # _state must not iterate the live instance dict: a racing first oid read adds a key.
    for _ in range(200):
        atoms = [PosNode(i) for i in range(64)]
        barrier = threading.Barrier(8)
        errors = []

        def reader():
            barrier.wait()
            try:
                for a in atoms:
                    a._state()
            except Exception as e:  # recorded; asserted empty below
                errors.append(e)

        def observer():
            barrier.wait()
            for a in atoms:
                hash(a)

        threads = [threading.Thread(target=reader if k % 2 else observer) for k in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, errors[0]


def test_concurrent_loads_that_hash_child_stubs_do_not_deadlock():
    # Decoding a map with atom keys hashes (and so loads) child stubs; many threads
    # loading many such records exercise crossed stripes on the load path.
    c = DirectContext(DictStorage())
    children = [DBObject(context=c, n=i) for i in range(64)]
    ptrs = [DBObject(context=c, m={children[i % 64]: i, children[(i * 7 + 1) % 64]: -i})._save()
            for i in range(256)]
    barrier = threading.Barrier(16)
    errors = []

    def worker(k):
        barrier.wait()
        try:
            for i in list(range(k, 256, 16)) + list(range(255 - k, -1, -16)):
                stub = c.load(ptrs[i], DBObject.__type_name__)
                if stub.m[children[i % 64]] != i:
                    errors.append(i)
        except Exception as e:  # recorded; asserted empty below
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(k,), daemon=True) for k in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not any(t.is_alive() for t in threads), "deadlock"
    assert not errors, errors[0]
