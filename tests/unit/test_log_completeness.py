"""ConcurrentOptimized.log_is_complete (Task 5.1, D33).

Catches: a transaction merging a written root by replaying an op log that does not
describe the written value's content (a bulk-built or loaded collection assigned to
a root it did not read), which would silently drop the written content.
"""
import pytest

from proto_db.collections.dictionaries import Dictionary, RepeatedKeysDictionary
from proto_db.collections.hash_dictionaries import HashDictionary
from proto_db.collections.sets import CountedSet, Set
from proto_db.storage.memory import MemoryStorage
from tests.support.persistence import DirectContext

FACTORIES = {
    "dictionary": (Dictionary, lambda c: c.set_at("k", 1), lambda cls: cls.from_items([("k", 1)])),
    "hash_dictionary": (HashDictionary, lambda c: c.set_at("k", 1), lambda cls: cls.from_items([("k", 1)])),
    "rkd": (RepeatedKeysDictionary, lambda c: c.set_at("k", 1), lambda cls: cls.from_items([("k", 1)])),
    "set": (Set, lambda c: c.add(1), lambda cls: cls.from_iterable([1])),
    "counted_set": (CountedSet, lambda c: c.add(1), lambda cls: cls.from_iterable([1])),
}


@pytest.fixture(params=sorted(FACTORIES))
def factory(request):
    return FACTORIES[request.param]


def test_new_empty_collection_and_logged_descendants_are_complete(factory):
    cls, mutate, _ = factory
    empty = cls()
    assert empty.log_is_complete
    assert mutate(empty).log_is_complete
    assert mutate(mutate(empty)).log_is_complete


def test_bulk_built_collection_is_not_complete(factory):
    cls, mutate, bulk = factory
    built = bulk(cls)
    assert built.count and not built.log_is_complete
    assert not mutate(built).log_is_complete


def test_loaded_collection_and_its_descendants_are_not_complete(factory):
    cls, mutate, _ = factory
    ctx = DirectContext(MemoryStorage())
    ptr = ctx.save(mutate(cls()))
    loaded = ctx.load(ptr, cls.__type_name__)
    assert not loaded.log_is_complete
    assert not mutate(loaded).log_is_complete


def test_rebased_version_is_not_complete(factory):
    cls, mutate, bulk = factory
    merged = mutate(cls()).rebase_onto(bulk(cls))
    assert not merged.log_is_complete


def test_index_changes_keep_completeness():
    from proto_db.indexing import EqualityIndex
    d = Dictionary().add_index(EqualityIndex("v", "v"))
    assert d.log_is_complete
    assert d.remove_index("v").log_is_complete
