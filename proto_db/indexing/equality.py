"""EqualityIndex: answers equality and presence terms on one key (spec section 8.3, D30).

Elements are filed by key in a keyed AVL of key groups (:mod:`.keyed`); each
group counts its elements, so duplicates in a List are kept.  Supported
operators: ``==``, ``in``, ``!=``, ``?N`` and ``?!N``, with the scan semantics
of :meth:`proto_db.query.terms.Term.matches` (D28): ``1 == 1.0``, ``True != 1``,
a MISSING key (absent attribute) matches only ``?N`` (never ``!=``), and an
unorderable key (list, dict, NaN) matches only ``!=`` and ``?!N``.

``!=`` is answered by walking every other key group; its estimate charges a
full pass (D30), so the planner prefers a scan unless a residual filter makes
the scan dearer.  No ordered iteration is offered: an equality index promises
lookups only (use :class:`proto_db.indexing.range.RangeIndex` for ``order_by``).
"""
from __future__ import annotations

from ..query.terms import EQ, IN, IS_NONE, NE, NOT_NONE, Term
from .keyed import KeyGroupIndex

__all__ = ["EqualityIndex"]


class EqualityIndex(KeyGroupIndex):
    """Equality index: ``EqualityIndex(name, key_spec)``; ``key_spec`` is a KeySpec or a dotted path str."""

    __type_name__ = "proto_db.indexing.EqualityIndex"
    #: Operators this index answers exactly.
    SUPPORTED_OPS = frozenset({EQ, IN, NE, IS_NONE, NOT_NONE})

    def supports(self, term: Term) -> bool:
        """``==``, ``in``, ``!=``, ``?N`` and ``?!N`` on this index's key."""
        return self._on_key(term) and term.op in self.SUPPORTED_OPS
