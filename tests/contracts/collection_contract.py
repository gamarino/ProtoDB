"""Behavioural contract every IndexedCollection must satisfy (spec section 8.1).

The maintainer's requirement: every collection supports associated indexes and no
mutator ever returns a collection with stale indexes.  This module holds:

* :class:`CollectionFactory` and the registry :data:`COLLECTION_FACTORIES`.  Each
  task that adds a collection appends an entry in the REGISTRATIONS section at the
  bottom of this file.
* ``check_*`` functions, run by ``tests/unit/test_collection_contract.py`` for
  every registered factory, every mutator and several seeds.

A mutator is a callable ``(collection, rng) -> (new_collection, expected_events)``
or ``None`` when it does not apply to that collection (e.g. removing from an
empty collection).  ``expected_events`` is the exact sequence of element-level
index events the mutation implies: ``("add", e)``, ``("remove", e)``,
``("replace", old, new)``.  An empty sequence declares a no-op: the mutator may
then return the receiver itself or a new collection with the same content;
otherwise it must return a new object.
"""
from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable

from proto_db.collections.base import ConcurrentOptimized, IndexedCollection
from proto_db.query.plans import CollectionScanPlan
from proto_db.storage.memory import MemoryStorage
from proto_db.values import order_key
from tests.support.persistence import DirectContext
from tests.support.recording_index import RecordingIndex

#: Index names attached by the contract; two indexes prove every index gets every hook.
INDEX_NAMES = ("idx_a", "idx_b")
SEEDS = range(12)


@dataclass(frozen=True)
class CollectionFactory:
    """A collection kind under contract.

    ``empty()`` returns a new empty collection; ``populate(collection, rng)``
    returns a version with some random content, built through public mutators;
    ``mutators`` maps a name to a mutator (see the module docstring);
    ``element_key`` maps an element to a hashable key that is equal for equal
    elements (default ``order_key``; override for unorderable elements).
    """

    name: str
    empty: Callable[[], IndexedCollection]
    populate: Callable[[IndexedCollection, random.Random], IndexedCollection]
    mutators: dict = field(default_factory=dict)
    element_key: Callable = order_key


COLLECTION_FACTORIES: list[CollectionFactory] = []


def register(factory: CollectionFactory) -> CollectionFactory:
    """Add ``factory`` to the registry (names must be unique)."""
    assert all(f.name != factory.name for f in COLLECTION_FACTORIES), factory.name
    COLLECTION_FACTORIES.append(factory)
    return factory


def mutator_cases():
    """``(factory, mutator_name, seed)`` triples for parametrisation, with readable ids."""
    import pytest
    return [pytest.param(f, m, s, id=f"{f.name}-{m}-{s}")
            for f in COLLECTION_FACTORIES for m in f.mutators for s in SEEDS]


def factory_cases():
    """``(factory, seed)`` pairs for parametrisation, with readable ids."""
    import pytest
    return [pytest.param(f, s, id=f"{f.name}-{s}") for f in COLLECTION_FACTORIES for s in SEEDS]


# -- helpers ------------------------------------------------------------------------------

def _content(factory, coll) -> Counter:
    return Counter(factory.element_key(e) for e in coll.elements())


def _sequence(factory, coll) -> list:
    return [factory.element_key(e) for e in coll.elements()]


def indexed_sample(factory: CollectionFactory, rng: random.Random) -> IndexedCollection:
    """A populated collection with the contract indexes, attached before or after populating."""
    coll = factory.empty()
    if rng.random() < 0.5:
        for name in INDEX_NAMES:
            coll = coll.add_index(RecordingIndex(name))
        return factory.populate(coll, rng)
    coll = factory.populate(coll, rng)
    for name in INDEX_NAMES:
        coll = coll.add_index(RecordingIndex(name))
    return coll


def assert_indexes_consistent(factory, coll) -> None:
    """Every index's view of the elements equals the collection content (no stale index)."""
    content = _content(factory, coll)
    for name in coll.index_names():
        assert coll.get_index(name).live(factory.element_key) == content, f"stale index {name}"


def assert_round_trip(factory, coll) -> None:
    """Saving and lazily loading ``coll`` preserves elements, count and indexes; op logs are dropped."""
    context = DirectContext(MemoryStorage())
    ptr = context.save(coll)
    loaded = context.load(ptr, type(coll).__type_name__)
    assert type(loaded) is type(coll)
    assert _sequence(factory, loaded) == _sequence(factory, coll)
    assert loaded.count == coll.count == len(loaded)
    assert loaded.index_names() == coll.index_names()
    for name in coll.index_names():
        assert loaded.get_index(name).events == coll.get_index(name).events
    if isinstance(coll, ConcurrentOptimized):
        assert loaded._op_log == ()


# -- checks -------------------------------------------------------------------------------

def check_mutator(factory: CollectionFactory, mutator_name: str, seed: int) -> None:
    """One mutator applied to one random indexed collection satisfies the contract."""
    rng = random.Random(seed)
    coll = indexed_sample(factory, rng)
    assert_indexes_consistent(factory, coll)
    before_seq = _sequence(factory, coll)
    before_count = coll.count
    before_indexes = coll.indexes
    before_events = {n: coll.get_index(n).events for n in coll.index_names()}

    result = factory.mutators[mutator_name](coll, rng)
    if result is None:
        return
    new, expected = result
    expected = tuple(expected)

    assert isinstance(new, type(coll))
    if expected:
        assert new is not coll, "a mutation must return a new collection"
    elif new is not coll:
        assert _content(factory, new) == _content(factory, coll)

    # The receiver is unchanged: elements, count, index set and every index.
    assert _sequence(factory, coll) == before_seq
    assert coll.count == before_count
    assert coll.indexes is before_indexes
    for name, events in before_events.items():
        assert coll.get_index(name).events == events

    # Every index saw exactly the implied events, and none is stale.
    assert new.index_names() == list(INDEX_NAMES)
    for name in INDEX_NAMES:
        assert new.get_index(name).events_since(coll.get_index(name)) == expected, name
    assert new.count == len(list(new.elements())) == len(new)
    assert_indexes_consistent(factory, new)
    assert_round_trip(factory, new)


def check_index_management(factory: CollectionFactory, seed: int) -> None:
    """add_index / remove_index / get_index / as_query_plan behave on any content."""
    rng = random.Random(seed)
    coll = factory.populate(factory.empty(), rng)
    assert coll.index_names() == [] and len(coll.indexes) == 0
    assert coll.get_index("idx_a") is None

    with_a = coll.add_index(RecordingIndex("idx_a"))
    assert with_a is not coll and coll.index_names() == []
    assert _sequence(factory, with_a) == _sequence(factory, coll)
    built = with_a.get_index("idx_a")
    assert [factory.element_key(e) for e in built.events[0][1]] == _sequence(factory, coll)
    assert built.events[0][0] == "build" and len(built.events) == 1

    with_ab = with_a.add_index(RecordingIndex("idx_b"))
    assert with_ab.index_names() == ["idx_a", "idx_b"] and with_a.index_names() == ["idx_a"]

    without_a = with_ab.remove_index("idx_a")
    assert without_a.index_names() == ["idx_b"] and with_ab.index_names() == ["idx_a", "idx_b"]
    assert _sequence(factory, without_a) == _sequence(factory, coll)

    plan = with_ab.as_query_plan()
    assert isinstance(plan, CollectionScanPlan)
    assert [factory.element_key(e) for e in plan.execute()] == _sequence(factory, coll)
    explained = plan.explain()
    assert explained["plan"] == "CollectionScan"
    assert explained["indexes"] == ["idx_a", "idx_b"]
    assert explained["rows"] == coll.count
    assert plan.estimate().rows == coll.count
    assert_round_trip(factory, with_ab)


def check_mutator_sequence(factory: CollectionFactory, seed: int, steps: int = 40) -> None:
    """A random sequence of mutations keeps every index consistent at every step."""
    rng = random.Random(seed)
    coll = indexed_sample(factory, rng)
    names = sorted(factory.mutators)
    for _ in range(steps):
        result = factory.mutators[rng.choice(names)](coll, rng)
        if result is None:
            continue
        coll, _ = result
        assert_indexes_consistent(factory, coll)
        assert coll.count == len(list(coll.elements()))
    assert_round_trip(factory, coll)


# == REGISTRATIONS ========================================================================
# Tasks that add a collection append its factory below.

def _toy_factory() -> CollectionFactory:
    from tests.support.toy_collection import ToySet

    def populate(coll, rng):
        for _ in range(rng.randrange(0, 30)):
            coll = coll.add(rng.randrange(0, 60))
        return coll

    def fresh(coll, rng):
        while True:
            e = rng.randrange(0, 200)
            if not coll.has(e):
                return e

    def member(coll, rng):
        members = list(coll.elements())
        return rng.choice(members) if members else None

    def add_new(coll, rng):
        e = fresh(coll, rng)
        return coll.add(e), [("add", e)]

    def add_existing(coll, rng):
        e = member(coll, rng)
        return None if e is None else (coll.add(e), [])

    def discard_present(coll, rng):
        e = member(coll, rng)
        return None if e is None else (coll.discard(e), [("remove", e)])

    def discard_absent(coll, rng):
        return coll.discard(fresh(coll, rng)), []

    def replace(coll, rng):
        old = member(coll, rng)
        if old is None:
            return None
        new = fresh(coll, rng)
        return coll.replace(old, new), [("replace", old, new)]

    return CollectionFactory(
        name="ToySet", empty=ToySet, populate=populate,
        mutators={"add_new": add_new, "add_existing": add_existing, "discard_present": discard_present,
                  "discard_absent": discard_absent, "replace": replace})


register(_toy_factory())
