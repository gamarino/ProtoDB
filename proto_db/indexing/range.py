"""RangeIndex: answers equality, range and presence terms and yields elements in key order
(spec section 8.3, D28, D30).

Elements are filed by key in a keyed AVL of key groups (:mod:`.keyed`), ordered
by :func:`proto_db.values.order_key`; MISSING and None are real keys at the
bottom of the total order and elements with unorderable keys (list, dict, NaN)
sit in a side group.  Supported operators: ``==``, ``in``, ``!=``, ``<``,
``<=``, ``>``, ``>=``, the four ``between`` variants, ``?N`` and ``?!N``, with
the scan semantics of :meth:`proto_db.query.terms.Term.matches`: ordering
operators match only keys of the operand's type rank (``None < 5`` is false),
so range scans are bounded by :func:`proto_db.values.rank_bounds`; unorderable
keys match only ``!=`` and ``?!N``.

Ordered iteration (``supports_order`` / ``ordered_plan``, used by ``order_by``):
every occurrence ascending by key (MISSING < None < bool < number < ...),
unorderable keys after every orderable one; descending is the exact reverse
(unorderable keys first).  :meth:`RangeIndex.iter_ordered` is a convenience
iterator over the same plan.
"""
from __future__ import annotations

from collections.abc import Iterator

from ..query.plans import QueryPlan
from ..query.terms import (BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_HIGH, BETWEEN_EXCL_LOW, EQ, GE, GT, IN,
                           IS_NONE, LE, LT, NE, NOT_NONE, Term)
from .keyed import KeyGroupIndex
from .keyspec import KeySpec

__all__ = ["RangeIndex"]


class RangeIndex(KeyGroupIndex):
    """Range index: ``RangeIndex(name, key_spec)``; ``key_spec`` is a KeySpec or a dotted path str."""

    #: Plans read only the stored trees: cacheable by the index's pointer (D56).
    _plan_cacheable = True

    __type_name__ = "proto_db.indexing.RangeIndex"
    #: Operators this index answers exactly.
    SUPPORTED_OPS = frozenset({EQ, IN, NE, LT, LE, GT, GE, BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_LOW,
                               BETWEEN_EXCL_HIGH, IS_NONE, NOT_NONE})

    def supports(self, term: Term) -> bool:
        """Equality, range, ``between`` and presence operators on this index's key."""
        return self._on_key(term) and term.op in self.SUPPORTED_OPS

    def supports_order(self, key_spec: KeySpec) -> bool:
        """Ordered iteration by this index's own key."""
        return key_spec == self.key_spec

    def ordered_plan(self, descending: bool = False) -> QueryPlan:
        """Every stored occurrence in key order (descending: the exact reverse of the key-group order)."""
        return self._ordered_plan(descending)

    def iter_ordered(self, descending: bool = False) -> Iterator:
        """Iterate every stored occurrence in key order (``ordered_plan(descending).execute()``)."""
        return self.ordered_plan(descending).execute()
