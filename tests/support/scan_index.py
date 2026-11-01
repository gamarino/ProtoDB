"""ScanIndex: a trivially correct reference index used to prove the index contract (Task 4.1).

It keeps every element it is given in a :class:`CountedSet` and answers ``==``
and ``?N`` terms on its key by filtering that multiset with
:meth:`Term.matches` (the scan oracle), so its answers are correct by
construction.  It implements only the abstract hooks of :class:`Index` and
relies on every default (``with_replaced``, ``build``, ``empty``, ``explain``,
``validate``); it is therefore also the reference for "the defaults are right".

:class:`OrderedScanIndex` adds ordered iteration (``supports_order`` /
``ordered_plan``) by sorting the stored multiset, the reference for that hook.

The broken variants used to show that the contract catches faulty indexes live
in ``tests/unit/test_index_contract.py``.
"""
from proto_db.collections.sets import CountedSet
from proto_db.indexing import Index
from proto_db.query.plans import Estimate, IndexPlan
from proto_db.query.terms import EQ, IS_NONE, is_orderable
from proto_db.values import order_key


class ScanIndex(Index):
    """Reference index: persisted ``content`` (a CountedSet of the indexed elements)."""

    __type_name__ = "tests.support.ScanIndex"
    SUPPORTED_OPS = frozenset({EQ, IS_NONE})

    def __init__(self, name, key_spec, *, context=None):
        super().__init__(name, key_spec, context=context)
        object.__setattr__(self, "content", CountedSet())

    def with_added(self, element):
        """One more occurrence of ``element``."""
        return self._with(content=self.content.add(element))

    def with_removed(self, element):
        """One occurrence fewer of ``element``."""
        return self._with(content=self.content.remove(element))

    def supports(self, term):
        """``==`` and ``?N`` on this index's key."""
        return self._on_key(term) and term.op in self.SUPPORTED_OPS

    def estimate(self, term):
        """Every stored occurrence is examined and may be returned."""
        self._check_supported(term)
        n = self.content.total_count
        return Estimate(rows=n, cost=n)

    def plan(self, term):
        """A plan filtering the stored multiset with the scan oracle."""
        self._check_supported(term)
        content = self.content

        def produce():
            for element, n in content.items():
                if term.matches(element):
                    for _ in range(n):
                        yield element

        return IndexPlan(self, term, produce, kind="IndexScan")


def _order_group(key) -> tuple:
    return (0, order_key(key)) if is_orderable(key) else (1,)


class OrderedScanIndex(ScanIndex):
    """Reference index with ordered iteration on its own key (sorts the stored multiset)."""

    __type_name__ = "tests.support.OrderedScanIndex"

    def supports_order(self, key_spec):
        """Ordered iteration by this index's own key."""
        return key_spec == self.key_spec

    def ordered_plan(self, descending=False):
        """Every stored occurrence, by ascending (or descending) key order; unorderable keys last."""
        if not self.supports_order(self.key_spec):  # pragma: no cover - always supported
            return super().ordered_plan(descending)
        content, key_spec = self.content, self.key_spec

        def produce():
            items = sorted(content.items(), key=lambda item: _order_group(key_spec.extract(item[0])),
                           reverse=descending)
            for element, n in items:
                for _ in range(n):
                    yield element

        n = content.total_count
        return IndexPlan(self, None, produce, kind="IndexOrderedScan", estimate=Estimate(rows=n, cost=n),
                         details={"descending": descending})
