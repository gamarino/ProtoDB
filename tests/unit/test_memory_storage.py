"""MemoryStorage: the shared storage contract plus memory-specific behaviour."""
import threading

import pytest

from proto_db.core.atom import Atom
from proto_db.errors import ProtoLockingError, ProtoTimeoutError, ProtoValidationError
from proto_db.storage.base import SharedStorage
from proto_db.storage.memory import MemoryStorage
from tests.contracts.storage_contract import StorageContract, atom_payload
from tests.support.persistence import DirectContext


class TestMemoryStorage(StorageContract):
    def make_storage(self):
        return MemoryStorage()


def test_is_a_shared_storage():
    assert isinstance(MemoryStorage(), SharedStorage)


def test_default_lock_timeout_is_applied():
    s = MemoryStorage(lock_timeout=0.05)
    held, release = threading.Event(), threading.Event()

    def holder():
        with s.root_lock():
            held.set()
            release.wait(10)

    t = threading.Thread(target=holder)
    t.start()
    try:
        assert held.wait(10)
        with pytest.raises(ProtoTimeoutError):
            with s.root_lock():
                pass
    finally:
        release.set()
        t.join()


def test_invalid_lock_timeout():
    with pytest.raises(ProtoValidationError):
        MemoryStorage(lock_timeout=-1)


def test_stores_encoded_bytes_not_live_objects():
    s = MemoryStorage()
    payload = atom_payload(1)
    ptr = s.push_atom(payload)
    assert s.get_atom(ptr) is not s.get_atom(ptr)


class Item(Atom):
    """Atom used to prove MemoryStorage works behind a DirectContext."""

    __type_name__ = "tests.memory_storage.Item"

    def __init__(self, value, *, context=None):
        super().__init__(context=context)
        self.value = value


def test_works_as_direct_context_storage():
    ctx = DirectContext(MemoryStorage())
    ptr = ctx.save(Item({"k": [1, 2, 3]}))
    loaded = ctx.load(ptr, Item.__type_name__)
    assert loaded.value == {"k": [1, 2, 3]}
