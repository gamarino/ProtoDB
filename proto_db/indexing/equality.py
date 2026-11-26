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

Uniqueness (spec R29, B6): ``EqualityIndex(name, key_spec, unique=True)`` holds at most one
occurrence per key.  Adding a second element under a key it already holds raises
``ProtoValidationError`` (at the write, against the collection version being written; building
the index over duplicates too).  MISSING (the element lacks the key) and None never count as
duplicates; ``1`` and ``1.0`` are one key (``order_key``), so they are duplicates; a key without
an order (a list, a dict, NaN) is refused with ``ProtoValidationError``.  Replacing an element
removes its occurrence before adding the new one, so a Dictionary key may be set again to a value
with the same index key.  In a transaction, two commits that add one key concurrently conflict
(``ProtoConflictError`` on ``Unit(INDEX_GROUP)``); the retry then sees the duplicate.

``unique`` is part of the index definition (``_definition``) and is persisted only when True: an
index written without it (by an older release, or not unique) reads as ``unique=False``.
"""
from __future__ import annotations

from collections.abc import Iterable

from ..collections import avl
from ..core.atom import PersistenceContext
from ..errors import ProtoValidationError
from ..query.terms import EQ, IN, IS_NONE, NE, NOT_NONE, Term
from ..values import MISSING
from .base import Index
from .keyed import EMPTY_GROUP, KeyGroupIndex
from .keyspec import KeySpec

__all__ = ["EqualityIndex"]


class EqualityIndex(KeyGroupIndex):
    """Equality index: ``EqualityIndex(name, key_spec, unique=False)``; ``key_spec`` is a KeySpec or
    a dotted path str (module docstring for ``unique``)."""

    #: Plans read only the stored trees: cacheable by the index's pointer (D56).
    _plan_cacheable = True
    #: The element hooks work from precomputed keys (R49).
    _keyed_reapplication = True

    __type_name__ = "proto_db.indexing.EqualityIndex"
    _definition = Index._definition + ("unique",)
    #: Operators this index answers exactly.
    SUPPORTED_OPS = frozenset({EQ, IN, NE, IS_NONE, NOT_NONE})

    def __init__(self, name: str, key_spec: KeySpec | str, unique: bool = False, *,
                 context: PersistenceContext | None = None):
        """Create an EMPTY index; ``unique`` (a bool) refuses two elements under one key."""
        if not isinstance(unique, bool):
            raise ProtoValidationError(f"EqualityIndex unique must be a bool, not {unique!r}")
        super().__init__(name, key_spec, context=context)
        if unique:
            object.__setattr__(self, "unique", True)    # persisted only when True (module docstring)

    def _absent_attribute(self, name: str):
        if name == "unique":
            return False        # not written: not unique (an older index included)
        return super()._absent_attribute(name)

    def _is_unique(self) -> bool:
        return self.unique is True

    def build(self, elements: Iterable) -> "EqualityIndex":
        """See :meth:`KeyGroupIndex.build`; a unique index refuses duplicate and unorderable keys."""
        built = super().build(elements)
        if self._is_unique():
            if built.side != EMPTY_GROUP:
                element = next(avl.key_iter(built.side[0]))[1][0]
                self._check_unique_key(self.key_spec.extract(element))
            for key, (_, total) in avl.key_iter(built.groups):
                if total > 1 and key is not MISSING and key is not None:
                    self._duplicate(key)
        return built

    def explain(self) -> dict:
        """See :meth:`KeyGroupIndex.explain`; a unique index adds ``unique: True``."""
        info = super().explain()
        if self._is_unique():
            info["unique"] = True
        return info

    def supports(self, term: Term) -> bool:
        """``==``, ``in``, ``!=``, ``?N`` and ``?!N`` on this index's key."""
        return self._on_key(term) and term.op in self.SUPPORTED_OPS

