"""ScanIndex: a trivially correct reference index used to prove the index contract (Task 4.1).

It keeps every element it is given in a :class:`CountedSet` and answers ``==``
and ``?N`` terms on its key by filtering that multiset with
:meth:`Term.matches` (the scan oracle), so its answers are correct by
construction.  It implements only the abstract hooks of :class:`Index` and
relies on every default (``with_replaced``, ``build``, ``empty``, ``explain``,
``validate``); it is therefore also the reference for "the defaults are right".

The broken variants used to show that the contract catches faulty indexes live
in ``tests/unit/test_index_contract.py``.
"""
from proto_db.collections.sets import CountedSet
from proto_db.indexing import Index
from proto_db.query.plans import Estimate, IndexPlan
from proto_db.query.terms import EQ, IS_NONE


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
