"""Runs the Index contract over every registered index kind, and proves the contract has teeth.

Catches: any registered index kind that violates a hook's contract (see
tests/contracts/index_contract.py, checks (a)-(h)); and a contract that is too
weak -- each deliberately broken ScanIndex variant below must be rejected by the
check aimed at its defect, while the correct ScanIndex passes everything.
"""
import pytest

from proto_db.errors import ProtoCorruptionError
from proto_db.query.plans import Estimate, IndexPlan
from tests.contracts.index_contract import (INDEX_CONTRACTS, RECORD_KEY_SPECS, SEEDS, IndexContract,
                                            check_cases, record_strategy, value_strategy)
from tests.support.scan_index import OrderedScanIndex, ScanIndex


@pytest.mark.parametrize("contract, check, seed", check_cases())
def test_index_contract(contract, check, seed):
    getattr(contract, check)(seed)


def test_scan_index_is_registered():
    assert {c.name for c in INDEX_CONTRACTS} >= {"ScanIndex-records", "ScanIndex-values",
                                                 "OrderedScanIndex-records", "OrderedScanIndex-values"}


# -- broken variants -------------------------------------------------------------------------

class NoopRemoveIndex(ScanIndex):
    """Defect: with_removed returns a new index with unchanged content."""

    __type_name__ = "tests.unit.test_index_contract.NoopRemoveIndex"

    def with_removed(self, element):
        return self._with()


class DroppingPlanIndex(ScanIndex):
    """Defect: plan silently drops the first match."""

    __type_name__ = "tests.unit.test_index_contract.DroppingPlanIndex"

    def plan(self, term):
        inner = super().plan(term)

        def produce():
            matches = inner.execute()
            next(matches, None)
            yield from matches

        return IndexPlan(self, term, produce)


class PathBlindIndex(ScanIndex):
    """Defect: supports() ignores the term path (answers terms on other attributes with its own key)."""

    __type_name__ = "tests.unit.test_index_contract.PathBlindIndex"

    def supports(self, term):
        return term.op in self.SUPPORTED_OPS

    def plan(self, term):
        self._check_supported(term)
        content = self.content
        key_spec = self.key_spec

        def produce():
            from proto_db.query.terms import Term
            own = Term(key_spec.term_path, term.op, term.value)
            for element, n in content.items():
                if own.matches(element):
                    yield from [element] * n

        return IndexPlan(self, term, produce)


class LenientPlanIndex(ScanIndex):
    """Defect: plan on an unsupported term returns an empty plan instead of raising."""

    __type_name__ = "tests.unit.test_index_contract.LenientPlanIndex"

    def plan(self, term):
        if not self.supports(term):
            return IndexPlan(self, term, lambda: iter(()))
        return super().plan(term)


class MutatingIndex(ScanIndex):
    """Defect: with_added updates the receiver in place and returns it."""

    __type_name__ = "tests.unit.test_index_contract.MutatingIndex"

    def with_added(self, element):
        object.__setattr__(self, "content", self.content.add(element))
        return self


class ContentKeepingBuildIndex(ScanIndex):
    """Defect: build folds onto the receiver instead of an empty index."""

    __type_name__ = "tests.unit.test_index_contract.ContentKeepingBuildIndex"

    def build(self, elements):
        index = self
        for e in elements:
            index = index.with_added(e)
        return index


class ForgetfulEmptyIndex(ScanIndex):
    """Defect: empty() returns the receiver itself (content kept)."""

    __type_name__ = "tests.unit.test_index_contract.ForgetfulEmptyIndex"

    def empty(self):
        return self._with()


class WrongOrderIndex(OrderedScanIndex):
    """Defect: descending ordered iteration returns the ascending order."""

    __type_name__ = "tests.unit.test_index_contract.WrongOrderIndex"

    def ordered_plan(self, descending=False):
        return super().ordered_plan(descending=False)


class UnstableOrderIndex(OrderedScanIndex):
    """Defect: orders by the key's hash instead of its order key."""

    __type_name__ = "tests.unit.test_index_contract.UnstableOrderIndex"

    def ordered_plan(self, descending=False):
        content = self.content
        key_spec = self.key_spec

        def produce():
            items = sorted(content.items(), key=lambda item: hash(repr(key_spec.extract(item[0]))))
            for element, n in items:
                yield from [element] * n

        return IndexPlan(self, None, produce, estimate=Estimate(rows=0, cost=0))


class OrderClaimingIndex(ScanIndex):
    """Defect: claims ordered iteration on every key but cannot provide it."""

    __type_name__ = "tests.unit.test_index_contract.OrderClaimingIndex"

    def supports_order(self, key_spec):
        return True


def contracts_for(cls):
    return [IndexContract(name=f"{cls.__name__}-records", index_factory=cls, element_strategy=record_strategy,
                          key_specs=RECORD_KEY_SPECS),
            IndexContract(name=f"{cls.__name__}-values", index_factory=cls, element_strategy=value_strategy)]


def failing_seeds(cls, check):
    """Seeds (over both strategies) for which ``check`` rejects ``cls``."""
    failed = []
    for contract in contracts_for(cls):
        for seed in SEEDS:
            try:
                getattr(contract, check)(seed)
            except (AssertionError, ProtoCorruptionError):
                failed.append((contract.name, seed))
    return failed


@pytest.mark.parametrize("cls, check", [
    (NoopRemoveIndex, "check_mutation_sequence"),
    (NoopRemoveIndex, "check_collections"),
    (NoopRemoveIndex, "check_persistence"),
    (DroppingPlanIndex, "check_plan_matches_scan"),
    (DroppingPlanIndex, "check_mutation_sequence"),
    (DroppingPlanIndex, "check_collections"),
    (PathBlindIndex, "check_unsupported_raises"),
    (LenientPlanIndex, "check_unsupported_raises"),
    (MutatingIndex, "check_hooks_do_not_mutate"),
    (ContentKeepingBuildIndex, "check_hook_equivalences"),
    (ForgetfulEmptyIndex, "check_empty"),
    (WrongOrderIndex, "check_ordered_iteration"),
    (UnstableOrderIndex, "check_ordered_iteration"),
    (OrderClaimingIndex, "check_ordered_iteration"),
])
def test_contract_rejects_broken_index(cls, check):
    assert failing_seeds(cls, check), f"{check} did not catch {cls.__name__}"
