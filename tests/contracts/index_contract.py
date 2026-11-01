"""Behavioural contract every Index implementation must satisfy (spec section 8.3, Task 4.1).

The maintainer's requirement: equality, range and vector indexes, and new index
kinds added by extending base classes, with every hook defined and verified.
An index class that passes this contract implements every hook correctly.

An :class:`IndexContract` describes one index kind under test:

* ``index_factory(name, key_spec) -> Index`` returns an EMPTY index;
* ``element_strategy(rng) -> element`` draws a random element (the contract
  also re-adds elements it already holds, so duplicates are exercised);
* ``term_strategy(rng, elements, key_spec) -> list[Term]`` (optional) adds
  kind-specific terms (e.g. ``near[]``) to the generic vocabulary the contract
  derives from the elements' own keys (every operator but ``near[]``, on the
  index key and on a foreign path);
* ``key_specs``: the key specs the element strategy is tested with;
* ``collection_key_spec``: the key spec used when the index is attached to every
  registered collection (whose elements are plain values), default
  ``KeySpec.element()``; ``None`` declares that the kind cannot index plain
  values, and check (f) is then reported as skipped, never as passed;
* ``oracle(term, elements) -> list`` (optional): the expected result of a term,
  default the scan oracle ``[e for e in elements if term.matches(e)]``; kinds
  answering terms the scan oracle refuses (``near[]``) supply their own;
* ``element_key``: identity used to compare result multisets (default
  :func:`proto_db.indexes.element_identity`).

Checks (each a method taking a seed; :meth:`IndexContract.checks` lists them):

(a) ``check_plan_matches_scan`` -- every supported term: plan results == oracle;
    plans are QueryPlans, re-executable, explainable, with a valid estimate.
(b) ``check_mutation_sequence`` -- random add/remove/replace sequences keep
    ``validate`` passing and plan == oracle at every step.
(c) ``check_persistence`` -- save + lazy load keeps type, definition, explain
    and every result; the loaded index keeps working.
(d) ``check_concurrent_readers`` -- 8 threads reading one index version (in
    memory and as a freshly loaded stub) get identical, correct results.
(e) ``check_unsupported_raises`` -- ``supports`` False => ``plan`` raises
    ``ProtoNotSupportedError``; terms on another path are never supported.
(f) ``check_collections`` -- attached through ``add_index`` to EVERY collection of
    ``COLLECTION_FACTORIES`` and driven by the collection mutators, the index
    stays consistent (validate passes, plan == oracle), also after a round trip.
(g) ``check_hooks_do_not_mutate`` -- no hook changes the receiver; hooks return
    an index of the same type and name.
(h) ``check_empty`` -- ``empty()`` of a populated index is empty and keeps the
    whole definition (``_definition``).
plus ``check_hook_equivalences`` -- ``with_replaced`` == remove + add, ``build``
== fold of ``with_added`` from empty, whatever the receiver holds; and
``check_explain`` -- ``explain()`` names the index, its kind and its key.

Registered kinds live in :data:`INDEX_CONTRACTS` (REGISTRATIONS section at the
bottom); ``tests/unit/test_index_contract.py`` runs every check for each.
"""
from __future__ import annotations

import random
import threading
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable

from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError
from proto_db.indexing import Index, KeySpec, element_identity, register_extractor
from proto_db.query.plans import Estimate, QueryPlan
from proto_db.query.terms import (BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_HIGH, BETWEEN_EXCL_LOW, EQ, GE,
                                  GT, IN, IS_NONE, LE, LT, NE, NOT_NONE, Term, is_orderable)
from proto_db.storage.memory import MemoryStorage
from proto_db.values import MISSING, order_key
from tests.contracts.collection_contract import COLLECTION_FACTORIES
from tests.support.persistence import DirectContext

SEEDS = range(8)
THREADS = 8
#: A path no contract key spec uses; terms on it must never be supported.
FOREIGN_PATH = "zz_contract_foreign_path"
_RANGE_OPS = (LT, LE, GT, GE)
_BETWEEN_OPS = (BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_LOW, BETWEEN_EXCL_HIGH)


def scan_oracle(term: Term, elements: list) -> list:
    """Default oracle: the elements a full scan returns for ``term``."""
    return [e for e in elements if term.matches(e)]


@dataclass(frozen=True)
class IndexContract:
    """One index kind under contract (see the module docstring)."""

    name: str
    index_factory: Callable[[str, KeySpec], Index]
    element_strategy: Callable[[random.Random], Any]
    term_strategy: Callable[[random.Random, list, KeySpec], list] | None = None
    key_specs: tuple = (KeySpec.element(),)
    collection_key_spec: KeySpec | None = KeySpec.element()
    oracle: Callable[[Term, list], list] = scan_oracle
    element_key: Callable = element_identity

    # -- helpers ------------------------------------------------------------------------

    @staticmethod
    def checks() -> list[str]:
        """Names of every check method, in a stable order."""
        return ["check_plan_matches_scan", "check_mutation_sequence", "check_persistence",
                "check_concurrent_readers", "check_unsupported_raises", "check_collections",
                "check_hooks_do_not_mutate", "check_empty", "check_hook_equivalences", "check_explain"]

    def run_all(self, seeds=SEEDS) -> None:
        """Run every check for every seed (raises on the first failure)."""
        for check in self.checks():
            for seed in seeds:
                getattr(self, check)(seed)

    def _new(self, key_spec: KeySpec, name: str = "idx") -> Index:
        index = self.index_factory(name, key_spec)
        assert isinstance(index, Index), f"index_factory returned {index!r}, not an Index"
        assert index.name == name and index.key_spec == key_spec
        return index

    def _elements(self, rng: random.Random, n: int | None = None) -> list:
        n = rng.randrange(0, 40) if n is None else n
        out: list = []
        for _ in range(n):
            if out and rng.random() < 0.2:
                out.append(rng.choice(out))  # the same element twice
            else:
                out.append(self.element_strategy(rng))
        return out

    def _multiset(self, elements) -> Counter:
        return Counter(self.element_key(e) for e in elements)

    def terms_for(self, key_spec: KeySpec, elements: list, rng: random.Random) -> list[Term]:
        """Generic terms derived from the elements' keys, plus the kind's own (``term_strategy``)."""
        path = key_spec.term_path
        keys: dict = {}
        for e in elements:
            k = key_spec.extract(e)
            if k is not MISSING and is_orderable(k):
                keys.setdefault(order_key(k), k)
        distinct = list(keys.values())
        rng.shuffle(distinct)
        pivots = distinct[:8]
        terms = [Term(path, IS_NONE), Term(path, NOT_NONE), Term(path, EQ, None),
                 Term(path, EQ, ("contract-absent-key", 0)), Term(FOREIGN_PATH, EQ, 1),
                 Term(FOREIGN_PATH, IS_NONE)]
        for k in pivots:
            terms.append(Term(path, EQ, k))
            terms.append(Term(path, NE, k))
            terms.append(Term(path, rng.choice(_RANGE_OPS), k))
        if distinct:
            terms.append(Term(path, IN, tuple(rng.sample(distinct, min(3, len(distinct))))))
        by_rank: dict = {}
        for k in distinct:
            by_rank.setdefault(order_key(k)[0], []).append(k)
        for group in by_rank.values():
            lo, hi = sorted((rng.choice(group), rng.choice(group)), key=order_key)
            terms.append(Term(path, rng.choice(_BETWEEN_OPS), (lo, hi)))
        if self.term_strategy is not None:
            terms.extend(self.term_strategy(rng, elements, key_spec))
        return terms

    def _results(self, index: Index, term: Term) -> Counter:
        return self._multiset(index.plan(term).execute())

    def _answers(self, index: Index, terms: list) -> dict:
        return {t: self._results(index, t) for t in terms if index.supports(t)}

    def assert_consistent(self, index: Index, elements: list, terms: list, where: str = "") -> int:
        """``validate`` passes and plan == oracle for every supported term; return how many were checked."""
        index.validate(elements)
        checked = 0
        for term in terms:
            if not index.supports(term):
                continue
            expected = self._multiset(self.oracle(term, elements))
            got = self._results(index, term)
            assert got == expected, (f"{self.name}{where}: plan({term}) returned {sorted(got.elements())!r}, "
                                     f"scan {sorted(expected.elements())!r}")
            checked += 1
        return checked

    # -- (a) ----------------------------------------------------------------------------

    def check_plan_matches_scan(self, seed: int) -> None:
        """(a) plan results == oracle for every supported term; plans are well-formed."""
        rng = random.Random(seed)
        for key_spec in self.key_specs:
            elements = self._elements(rng)
            index = self._new(key_spec).build(elements)
            terms = self.terms_for(key_spec, elements, rng)
            supported = [t for t in terms if index.supports(t)]
            assert supported, f"{self.name}: no generated term is supported on {key_spec}"
            for term in supported:
                plan = index.plan(term)
                assert isinstance(plan, QueryPlan), f"plan({term}) returned {plan!r}"
                first = self._multiset(plan.execute())
                assert self._multiset(plan.execute()) == first, "a plan must be re-executable"
                assert first == self._multiset(self.oracle(term, elements)), \
                    f"{self.name}: plan({term}) != scan on {key_spec}"
                assert isinstance(plan.explain(), dict)
                est = index.estimate(term)
                assert isinstance(est, Estimate), f"estimate({term}) returned {est!r}"

    # -- (b) ----------------------------------------------------------------------------

    def check_mutation_sequence(self, seed: int, steps: int = 30) -> None:
        """(b) add/remove/replace sequences keep validate passing and plan == oracle."""
        rng = random.Random(seed)
        for key_spec in self.key_specs:
            index = self._new(key_spec)
            model: list = []
            seen: list = []  # every element ever added: removed keys stay in the term pivots
            if rng.random() < 0.5:
                model = self._elements(rng, rng.randrange(0, 15))
                seen = list(model)
                index = index.build(model)
            for step in range(steps):
                r = rng.random()
                if not model or r < 0.45:
                    e = rng.choice(model) if model and rng.random() < 0.25 else self.element_strategy(rng)
                    index = index.with_added(e)
                    model.append(e)
                    seen.append(e)
                elif r < 0.75:
                    e = model.pop(rng.randrange(len(model)))
                    index = index.with_removed(e)
                else:
                    i = rng.randrange(len(model))
                    new = self.element_strategy(rng)
                    index = index.with_replaced(model[i], new)
                    model[i] = new
                    seen.append(new)
                self.assert_consistent(index, model, self.terms_for(key_spec, seen, rng),
                                       f" step {step} on {key_spec}")

    # -- (c) ----------------------------------------------------------------------------

    def check_persistence(self, seed: int) -> None:
        """(c) save + lazy load preserves the index; the loaded index keeps working."""
        rng = random.Random(seed)
        for key_spec in self.key_specs:
            elements = self._elements(rng)
            index = self._new(key_spec).build(elements)
            terms = self.terms_for(key_spec, elements, rng)
            context = DirectContext(MemoryStorage())
            loaded = context.load(context.save(index), type(index).__type_name__)
            assert type(loaded) is type(index)
            for field in type(index)._definition:
                assert getattr(loaded, field) == getattr(index, field), field
            assert loaded.explain() == index.explain()
            assert self._answers(loaded, terms) == self._answers(index, terms)
            self.assert_consistent(loaded, elements, terms, " (loaded)")
            extra = self.element_strategy(rng)
            grown = loaded.with_added(extra)
            self.assert_consistent(grown, elements + [extra], self.terms_for(key_spec, elements + [extra], rng),
                                   " (loaded, then added)")
            if elements:
                victim = rng.choice(elements)
                rest = list(elements)
                rest.remove(victim)
                self.assert_consistent(loaded.with_removed(victim), rest, terms, " (loaded, then removed)")

    # -- (d) ----------------------------------------------------------------------------

    def check_concurrent_readers(self, seed: int) -> None:
        """(d) 8 threads reading one version (in memory, and a fresh stub) agree and are correct."""
        rng = random.Random(seed)
        key_spec = self.key_specs[seed % len(self.key_specs)]
        elements = self._elements(rng, rng.randrange(10, 40))
        index = self._new(key_spec).build(elements)
        terms = [t for t in self.terms_for(key_spec, elements, rng) if index.supports(t)]
        expected = {t: self._multiset(self.oracle(t, elements)) for t in terms}
        context = DirectContext(MemoryStorage())
        stub = context.load(context.save(index), type(index).__type_name__)
        for version in (index, stub):
            barrier = threading.Barrier(THREADS)
            outputs: list = [None] * THREADS
            errors: list = []

            def read(slot, version=version, barrier=barrier, outputs=outputs, errors=errors):
                barrier.wait()
                try:
                    outputs[slot] = {t: self._results(version, t) for t in terms}
                except BaseException as e:  # reported to the main thread below
                    errors.append(e)
                    raise

            threads = [threading.Thread(target=read, args=(i,)) for i in range(THREADS)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            assert not errors, f"{self.name}: reader failed: {errors[0]!r}"
            assert all(out == expected for out in outputs), f"{self.name}: concurrent readers disagree"

    # -- (e) ----------------------------------------------------------------------------

    def check_unsupported_raises(self, seed: int) -> None:
        """(e) supports False => plan raises ProtoNotSupportedError; foreign paths unsupported."""
        rng = random.Random(seed)
        for key_spec in self.key_specs:
            elements = self._elements(rng)
            index = self._new(key_spec).build(elements)
            terms = self.terms_for(key_spec, elements, rng)
            for other in (FOREIGN_PATH, f"{FOREIGN_PATH}.x", None, "@zz_contract_foreign_extractor"):
                if other == key_spec.term_path:
                    continue
                terms.append(Term(other, EQ, 1))
                terms.append(Term(other, IS_NONE))
            for term in terms:
                if term.path != key_spec.term_path:
                    assert not index.supports(term), f"{self.name} on {key_spec} supports {term}"
                if not index.supports(term):
                    try:
                        index.plan(term)
                    except ProtoNotSupportedError:
                        continue
                    raise AssertionError(f"{self.name}: plan({term}) did not raise ProtoNotSupportedError")

    # -- (f) ----------------------------------------------------------------------------

    def check_collections(self, seed: int, steps: int = 20) -> None:
        """(f) attached to every registered collection, the index follows every mutator."""
        if self.collection_key_spec is None:
            import pytest
            pytest.skip(f"{self.name} declares it cannot index plain values (collection_key_spec=None)")
        key_spec = self.collection_key_spec
        assert COLLECTION_FACTORIES, "no collection is registered"
        for factory in COLLECTION_FACTORIES:
            rng = random.Random(seed)
            coll = factory.empty()
            early = rng.random() < 0.5
            if early:
                coll = coll.add_index(self._new(key_spec))
            coll = factory.populate(coll, rng)
            if not early:
                coll = coll.add_index(self._new(key_spec))
            names = sorted(factory.mutators)
            seen = list(coll.elements())
            for step in range(steps):
                result = factory.mutators[rng.choice(names)](coll, rng)
                if result is None:
                    continue
                coll = result[0]
                elements = list(coll.elements())
                seen.extend(elements)
                index = coll.get_index("idx")
                assert type(index).__name__ == type(self._new(key_spec)).__name__
                self.assert_consistent(index, elements, self.terms_for(key_spec, seen, rng),
                                       f" in {factory.name} step {step}")
            context = DirectContext(MemoryStorage())
            loaded = context.load(context.save(coll), type(coll).__type_name__)
            elements = list(loaded.elements())
            self.assert_consistent(loaded.get_index("idx"), elements, self.terms_for(key_spec, seen, rng),
                                   f" in loaded {factory.name}")

    # -- (g) ----------------------------------------------------------------------------

    def _snapshot(self, index: Index, terms: list):
        state = {k: v for k, v in vars(index).items() if not k.startswith("_") and k != "oid"}
        return state, self._answers(index, terms), index.explain()

    def _same_snapshot(self, before, after, hook: str) -> None:
        (state_b, answers_b, explain_b), (state_a, answers_a, explain_a) = before, after
        assert state_a.keys() == state_b.keys(), f"{hook} changed the receiver's attributes"
        for k in state_b:
            assert state_a[k] is state_b[k], f"{hook} replaced the receiver's attribute {k!r}"
        assert answers_a == answers_b, f"{hook} changed the receiver's answers"
        assert explain_a == explain_b, f"{hook} changed the receiver's explain()"

    def check_hooks_do_not_mutate(self, seed: int) -> None:
        """(g) no hook changes its receiver; hooks return a same-type, same-name index."""
        rng = random.Random(seed)
        for key_spec in self.key_specs:
            elements = self._elements(rng, rng.randrange(1, 30))
            index = self._new(key_spec).build(elements)
            new = self.element_strategy(rng)
            terms = self.terms_for(key_spec, elements + [new], rng)
            victim = rng.choice(elements)
            calls = {
                "with_added": lambda: index.with_added(new),
                "with_removed": lambda: index.with_removed(victim),
                "with_replaced": lambda: index.with_replaced(victim, new),
                "build": lambda: index.build([new]),
                "empty": lambda: index.empty(),
            }
            for hook, call in calls.items():
                before = self._snapshot(index, terms)
                result = call()
                self._same_snapshot(before, self._snapshot(index, terms), hook)
                assert type(result) is type(index), f"{hook} returned a {type(result).__name__}"
                assert result.name == index.name and result.key_spec == index.key_spec, hook
                assert result is not index, f"{hook} must return a new index"
            before = self._snapshot(index, terms)
            index.validate(elements)
            for term in terms:
                if index.supports(term):
                    list(index.plan(term).execute())
                    index.estimate(term)
            index.explain()
            self._same_snapshot(before, self._snapshot(index, terms), "a read hook")

    # -- (h) ----------------------------------------------------------------------------

    def check_empty(self, seed: int) -> None:
        """(h) empty() of a populated index is empty and keeps the definition."""
        rng = random.Random(seed)
        for key_spec in self.key_specs:
            elements = self._elements(rng, rng.randrange(1, 30))
            index = self._new(key_spec).build(elements)
            empty = index.empty()
            assert type(empty) is type(index)
            for field in type(index)._definition:
                assert getattr(empty, field) == getattr(index, field), f"empty() lost {field!r}"
            terms = self.terms_for(key_spec, elements, rng)
            self.assert_consistent(empty, [], terms, " (empty)")
            for term in terms:
                if empty.supports(term):
                    assert not list(empty.plan(term).execute()), f"empty() still answers {term}"
            self.assert_consistent(index, elements, terms, " (receiver of empty)")
            self.assert_consistent(empty.build(elements), elements, terms, " (empty, rebuilt)")

    # -- equivalences and explain --------------------------------------------------------

    def check_hook_equivalences(self, seed: int) -> None:
        """with_replaced == remove + add; build == fold of with_added from empty, whatever the receiver holds."""
        rng = random.Random(seed)
        for key_spec in self.key_specs:
            elements = self._elements(rng, rng.randrange(1, 30))
            index = self._new(key_spec).build(elements)
            old, new = rng.choice(elements), self.element_strategy(rng)
            terms = self.terms_for(key_spec, elements + [new], rng)
            assert self._answers(index.with_replaced(old, new), terms) == \
                self._answers(index.with_removed(old).with_added(new), terms), \
                f"{self.name}: with_replaced differs from with_removed + with_added"
            folded = self._new(key_spec)
            for e in elements:
                folded = folded.with_added(e)
            assert self._answers(index, terms) == self._answers(folded, terms), \
                f"{self.name}: build() differs from a fold of with_added"
            other = self._elements(rng, rng.randrange(0, 20))
            rebuilt = index.build(other)  # the receiver's content must not leak in
            assert self._answers(rebuilt, terms) == self._answers(self._new(key_spec).build(other), terms), \
                f"{self.name}: build() on a populated index kept the receiver's content"
            self.assert_consistent(rebuilt, other, self.terms_for(key_spec, other + elements, rng), " (rebuilt)")
            assert self._answers(self._new(key_spec).build(iter(elements)), terms) == self._answers(index, terms)

    def check_explain(self, seed: int) -> None:
        """explain() is a dict naming the index, its kind and its key."""
        for key_spec in self.key_specs:
            index = self._new(key_spec, name=f"explained_{seed}")
            info = index.explain()
            assert isinstance(info, dict)
            assert info["index"] == f"explained_{seed}"
            assert info["kind"] == type(index).__name__
            assert info["key"] == key_spec.describe()


INDEX_CONTRACTS: list[IndexContract] = []


def register(contract: IndexContract) -> IndexContract:
    """Add ``contract`` to the registry (names must be unique)."""
    assert all(c.name != contract.name for c in INDEX_CONTRACTS), contract.name
    INDEX_CONTRACTS.append(contract)
    return contract


def check_cases(contracts=None, seeds=SEEDS):
    """``(contract, check_name, seed)`` triples for parametrisation, with readable ids."""
    import pytest
    contracts = INDEX_CONTRACTS if contracts is None else contracts
    return [pytest.param(c, check, s, id=f"{c.name}-{check}-{s}")
            for c in contracts for check in c.checks() for s in seeds]


# == shared strategies ======================================================================

CITIES = ("Paris", "Rome", "Oslo", None, 3, 3.0, True, ("t", 1))
ZIPS = (1000, 2000, "A-1", None, 1000.0)


def record_strategy(rng: random.Random) -> DBObject:
    """A DBObject with a ``city`` (sometimes None or absent, mixed types) and an ``address``
    holding a ``zip`` (as a mapping or a nested record; sometimes absent at either level)."""
    attrs: dict = {"n": rng.randrange(0, 5)}
    if rng.random() < 0.8:
        attrs["city"] = rng.choice(CITIES)
    r = rng.random()
    if r < 0.35:
        attrs["address"] = {"zip": rng.choice(ZIPS)}
    elif r < 0.65:
        attrs["address"] = DBObject(zip=rng.choice(ZIPS))
    elif r < 0.75:
        attrs["address"] = {"street": "x"}
    elif r < 0.85:
        attrs["address"] = None
    return DBObject(**attrs)


def value_strategy(rng: random.Random):
    """A plain value of several type ranks, None included."""
    return rng.choice([rng.randrange(0, 12), float(rng.randrange(0, 12)), f"s{rng.randrange(0, 6)}", None,
                       rng.random() < 0.5, (rng.randrange(0, 3), "t")])


def _lower_city(element):
    city = KeySpec.path("city").extract(element)
    return city.lower() if isinstance(city, str) else city


register_extractor("tests.contracts.lower_city", _lower_city)

RECORD_KEY_SPECS = (KeySpec.path("city"), KeySpec.path("address.zip"),
                    KeySpec.extractor("tests.contracts.lower_city"), KeySpec.element())


# == REGISTRATIONS ==========================================================================
# Tasks that add an index kind append its contract below.

def _register_scan_index() -> None:
    from tests.support.scan_index import ScanIndex
    register(IndexContract(name="ScanIndex-records", index_factory=ScanIndex,
                           element_strategy=record_strategy, key_specs=RECORD_KEY_SPECS))
    register(IndexContract(name="ScanIndex-values", index_factory=ScanIndex,
                           element_strategy=value_strategy, key_specs=(KeySpec.element(),)))


_register_scan_index()
