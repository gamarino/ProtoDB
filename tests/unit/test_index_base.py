"""Unit tests for proto_db.indexing.base (Index hooks and defaults) and IndexPlan (Task 4.1, D29).

Catches: an Index base that can be instantiated without its abstract hooks,
mutable or unvalidated index definitions, defaults that diverge from the spec
(with_replaced not remove-then-add, build keeping the receiver's content, empty
losing definition fields, explain missing fields, validate not detecting a
faulty index or checking unsupported terms), key specs that do not survive
persistence, and index plans that cannot be executed twice or misreport
themselves.
"""
import threading

import pytest

from proto_db.collections.lists import List
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from proto_db.indexing import Index, KeySpec, element_identity, register_extractor
from proto_db.query.plans import Estimate, IndexPlan, QueryPlan
from proto_db.query.terms import EQ, GT, IS_NONE, NOT_NONE, Term
from proto_db.storage.memory import MemoryStorage
from proto_db.values import MISSING
from tests.support.persistence import DirectContext
from tests.support.scan_index import ScanIndex


def _city(element):
    return KeySpec.path("city").extract(element)


register_extractor("tests.index_base.city", _city)


def city_index(name="by_city"):
    return ScanIndex(name, KeySpec.path("city"))


def results(index, term):
    return sorted(element_identity(e) for e in index.plan(term).execute())


# -- abstract hooks and construction ---------------------------------------------------------

def test_index_requires_every_abstract_hook():
    hooks = {"with_added": lambda self, e: self, "with_removed": lambda self, e: self,
             "supports": lambda self, t: False, "estimate": lambda self, t: None,
             "plan": lambda self, t: None}
    for missing in hooks:
        partial = type(f"Without_{missing}", (Index,), {k: v for k, v in hooks.items() if k != missing})
        with pytest.raises(TypeError):
            partial("i", KeySpec.element())
    complete = type("Complete", (Index,), dict(hooks, __type_name__="tests.unit.test_index_base.Complete"))
    assert complete("i", KeySpec.element()).name == "i"
    with pytest.raises(TypeError):
        Index("i", KeySpec.element())


@pytest.mark.parametrize("name, key_spec", [("", KeySpec.element()), (None, KeySpec.element()),
                                            (3, KeySpec.element()), ("i", "city"), ("i", None)])
def test_definition_is_validated(name, key_spec):
    with pytest.raises(ProtoValidationError):
        ScanIndex(name, key_spec)


def test_index_is_immutable():
    idx = city_index()
    for attr in ("name", "key_spec", "content", "anything"):
        with pytest.raises(ProtoUserError):
            setattr(idx, attr, 1)
    with pytest.raises(ProtoUserError):
        del idx.name


def test_definition_must_name_name_and_key_spec():
    with pytest.raises(ProtoUserError):
        type("BadDefinition", (ScanIndex,), {"_definition": ("name",),
                                             "__type_name__": "tests.unit.test_index_base.BadDefinition"})


def test_abstract_base_type_name():
    assert Index.__type_name__ == "proto_db.indexing.Index"


def test_concrete_index_classes_must_name_their_type():
    with pytest.raises(ProtoUserError, match="__type_name__"):
        type("Unnamed", (ScanIndex,), {})
    # An abstract intermediate class may rely on the default name.
    abstract = type("AbstractMiddle", (Index,), {"extra": lambda self: None})
    named = type("NamedLeaf", (abstract,), {
        "__type_name__": "tests.unit.test_index_base.NamedLeaf",
        "with_added": lambda self, e: self, "with_removed": lambda self, e: self,
        "supports": lambda self, t: False, "estimate": lambda self, t: None, "plan": lambda self, t: None})
    assert named("n", KeySpec.element()).name == "n"


def test_with_rejects_unknown_fields():
    with pytest.raises(ProtoValidationError):
        city_index()._with(bogus=1)


# -- defaults ------------------------------------------------------------------------------

CALLS = []


class SpyIndex(ScanIndex):
    """Records the order of add/remove calls in the module list CALLS."""

    __type_name__ = "tests.unit.test_index_base.SpyIndex"

    def with_added(self, element):
        CALLS.append(("add", element))
        return super().with_added(element)

    def with_removed(self, element):
        CALLS.append(("remove", element))
        return super().with_removed(element)


def test_with_replaced_defaults_to_remove_then_add():
    idx = SpyIndex("spy", KeySpec.element()).with_added(1)
    CALLS.clear()
    replaced = idx.with_replaced(1, 2)
    assert CALLS == [("remove", 1), ("add", 2)]
    assert results(replaced, Term(None, EQ, 2)) == [element_identity(2)]
    assert results(replaced, Term(None, EQ, 1)) == []


def test_build_starts_from_empty_and_folds_with_added():
    populated = city_index().with_added(DBObject(city="Rome"))
    elements = [DBObject(city="Oslo"), DBObject(city="Oslo"), DBObject()]
    built = populated.build(elements)
    assert results(built, Term("city", EQ, "Rome")) == []  # the receiver's content is not kept
    assert results(built, Term("city", EQ, "Oslo")) == sorted(element_identity(e) for e in elements[:2])
    assert results(built, Term("city", IS_NONE)) == [element_identity(elements[2])]
    assert results(populated, Term("city", EQ, "Rome")) != []  # receiver unchanged
    built.validate(elements)


def test_build_accepts_any_iterable():
    built = city_index().build(DBObject(city=c) for c in ("a", "b"))
    assert len(results(built, Term("city", EQ, "a"))) == 1


class Flavoured(ScanIndex):
    """An index with an extra definition field."""

    __type_name__ = "tests.unit.test_index_base.Flavoured"

    _definition = ScanIndex._definition + ("flavour",)

    def __init__(self, name, key_spec, flavour="plain", *, context=None):
        super().__init__(name, key_spec, context=context)
        object.__setattr__(self, "flavour", flavour)


def test_empty_keeps_the_whole_definition():
    idx = Flavoured("f", KeySpec.path("a"), flavour="spicy").with_added(DBObject(a=1))
    empty = idx.empty()
    assert type(empty) is Flavoured and empty is not idx
    assert (empty.name, empty.key_spec, empty.flavour) == ("f", KeySpec.path("a"), "spicy")
    assert empty.content.count == 0 and idx.content.count == 1
    assert results(empty, Term("a", EQ, 1)) == []


def test_explain_default():
    assert city_index().explain() == {"index": "by_city", "kind": "ScanIndex", "key": "city"}
    assert ScanIndex("e", KeySpec.element()).explain()["key"] == "<element>"


def test_supports_checks_the_key_path():
    idx = city_index()
    assert idx.supports(Term("city", EQ, "x")) and idx.supports(Term("city", IS_NONE))
    assert not idx.supports(Term("zip", EQ, "x"))
    assert not idx.supports(Term(None, EQ, "x"))
    assert not idx.supports(Term("city", GT, "x"))


def test_plan_on_unsupported_term_raises_not_supported():
    idx = city_index()
    for term in (Term("zip", EQ, 1), Term("city", GT, 1)):
        with pytest.raises(ProtoNotSupportedError):
            idx.plan(term)


def test_estimate_on_unsupported_term_raises_not_supported():
    with pytest.raises(ProtoNotSupportedError):
        city_index().estimate(Term("zip", EQ, 1))


def test_ordered_iteration_is_unsupported_by_default():
    idx = city_index()
    assert idx.supports_order(KeySpec.path("city")) is False
    with pytest.raises(ProtoNotSupportedError):
        idx.ordered_plan()
    with pytest.raises(ProtoNotSupportedError):
        idx.ordered_plan(descending=True)


def test_ordered_scan_index():
    from tests.support.scan_index import OrderedScanIndex
    a, b, c, d, e = (DBObject(city="b"), DBObject(city=None), DBObject(), DBObject(city=2), DBObject(city="a"))
    idx = OrderedScanIndex("o", KeySpec.path("city")).build([a, b, c, d, e])
    assert idx.supports_order(KeySpec.path("city")) and not idx.supports_order(KeySpec.path("zip"))
    assert list(idx.ordered_plan().execute()) == [c, b, d, e, a]  # MISSING < None < number < str
    assert list(idx.ordered_plan(descending=True).execute()) == [a, e, d, b, c]
    plan = idx.ordered_plan(descending=True)
    assert plan.explain() == {"plan": "IndexOrderedScan", "index": idx.explain(), "term": None,
                              "descending": True}
    assert plan.estimate() == Estimate(rows=5, cost=5)


class StaleIndex(ScanIndex):
    """Forgets removals: its content is stale after a remove."""

    __type_name__ = "tests.unit.test_index_base.StaleIndex"

    def with_removed(self, element):
        return self._with()


def test_validate_detects_an_inconsistent_index():
    a, b = DBObject(city="Rome"), DBObject(city="Rome")
    good = city_index().build([a, b]).with_removed(a)
    good.validate([b])
    stale = StaleIndex("s", KeySpec.path("city")).build([a, b]).with_removed(a)
    with pytest.raises(ProtoCorruptionError, match="'s'"):
        stale.validate([b])
    with pytest.raises(ProtoCorruptionError):
        city_index().validate([DBObject(city="Rome")])  # elements the index never saw


class StaleWithNotNone(StaleIndex):
    __type_name__ = "tests.unit.test_index_base.StaleWithNotNone"
    SUPPORTED_OPS = frozenset({EQ, IS_NONE, NOT_NONE})


def test_validate_covers_missing_keys_and_stale_keys():
    absent = DBObject()
    with pytest.raises(ProtoCorruptionError):
        city_index().with_added(absent).validate([])  # caught through ?N
    gone = DBObject(city="Gone")
    stale = StaleWithNotNone("s", KeySpec.path("city")).build([gone]).with_removed(gone)
    with pytest.raises(ProtoCorruptionError):
        stale.validate([])  # caught through ?!N even though no live element has the key "Gone"


class NotNoneOnly(ScanIndex):
    __type_name__ = "tests.unit.test_index_base.NotNoneOnly"
    SUPPORTED_OPS = frozenset({NOT_NONE})


def test_validate_only_checks_supported_terms():
    NotNoneOnly("n", KeySpec.path("city")).validate([])
    idx = NotNoneOnly("n", KeySpec.path("city")).with_added(DBObject())
    idx.validate([])  # the stray element has no key: only ?N could see it, and ?N is unsupported


def test_validate_skips_unorderable_keys():
    rec = DBObject(city=[1, 2])
    city_index().with_added(rec).validate([rec])


# -- persistence -----------------------------------------------------------------------------

@pytest.mark.parametrize("spec", [KeySpec.path("address.zip"), KeySpec.element(),
                                  KeySpec.extractor("tests.index_base.city")])
def test_key_spec_survives_persistence(spec):
    elements = [DBObject(city="x", address={"zip": 1}), DBObject()]
    idx = ScanIndex("p", spec).build(elements)
    context = DirectContext(MemoryStorage())
    loaded = context.load(context.save(idx), ScanIndex.__type_name__)
    assert type(loaded) is ScanIndex
    assert loaded.key_spec == spec and isinstance(loaded.key_spec, KeySpec)
    assert loaded.name == "p" and loaded.explain() == idx.explain()
    loaded.validate(elements)


def test_index_attached_to_a_collection_persists():
    lst = List().append_last(DBObject(city="a")).add_index(city_index())
    context = DirectContext(MemoryStorage())
    loaded = context.load(context.save(lst), List.__type_name__)
    loaded.get_index("by_city").validate(loaded.elements())


# -- IndexPlan -------------------------------------------------------------------------------

def test_index_plan():
    a, b = DBObject(city="Rome"), DBObject()
    idx = city_index().build([a, b])
    term = Term("city", EQ, "Rome")
    plan = idx.plan(term)
    assert isinstance(plan, IndexPlan) and isinstance(plan, QueryPlan)
    assert list(plan.execute()) == [a] and list(plan.execute()) == [a]  # re-executable
    assert plan.explain() == {"plan": "IndexScan", "index": idx.explain(), "term": term.describe()}
    assert plan.estimate() == idx.estimate(term) == Estimate(rows=2, cost=2)
    assert plan.index is idx and plan.term is term


def test_index_plan_without_term_needs_an_estimate():
    idx = city_index()
    with pytest.raises(ProtoValidationError):
        IndexPlan(idx, None, lambda: iter(()))
    plan = IndexPlan(idx, None, lambda: iter(()), estimate=Estimate(rows=0, cost=1), details={"x": 1})
    assert plan.estimate() == Estimate(rows=0, cost=1)
    assert plan.explain()["term"] is None and plan.explain()["x"] == 1
    with pytest.raises(ProtoValidationError):
        IndexPlan(idx, None, lambda: iter(()), estimate=(0, 1))
    with pytest.raises(ProtoValidationError):
        IndexPlan(idx, None, lambda: iter(()), estimate=Estimate(rows=0, cost=1), details={"plan": "x"})


def test_index_plan_default_kind_and_validation():
    idx = city_index()
    term = Term("city", EQ, 1)
    assert IndexPlan(idx, term, lambda: iter(())).explain()["plan"] == "IndexLookup"
    with pytest.raises(ProtoValidationError):
        IndexPlan(idx, term, "not callable")


def test_index_plan_concurrent_execution():
    elements = [DBObject(city=i % 3) for i in range(60)]
    plan = city_index().build(elements).plan(Term("city", EQ, 1))
    out, barrier = [], threading.Barrier(8)

    def run():
        barrier.wait()
        out.append(sorted(element_identity(e) for e in plan.execute()))

    threads = [threading.Thread(target=run) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(out) == 8 and all(r == out[0] for r in out) and len(out[0]) == 20


# -- element_identity ------------------------------------------------------------------------

def test_element_identity():
    r = DBObject(a=1)
    assert element_identity(1) == element_identity(1.0) != element_identity(True)
    assert element_identity(r) == element_identity(r) != element_identity(DBObject(a=1))
    assert element_identity([1, (2.0, "x")]) == element_identity([1.0, (2, "x")])
    assert element_identity([1]) != element_identity((1,))
    assert element_identity({"a": [1]}) == element_identity({"a": [1.0]})
    assert element_identity(float("nan")) == element_identity(float("nan"))
    assert element_identity(MISSING) != element_identity(None)
    hash(element_identity({"a": [1, {"b": 2}]}))
