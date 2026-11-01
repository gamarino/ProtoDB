"""Shared machinery of :class:`EqualityIndex` and :class:`RangeIndex`: key groups in a keyed AVL (D30).

Structure (persisted public fields of the index):

* ``groups`` -- a keyed AVL tree (:class:`proto_db.collections.avl.KeyNode`)
  ordered by ``order_key`` of the extracted key; MISSING and None are ordinary
  keys at the bottom of the total order.  Each node value is a *group*.
* ``side`` -- the group of every element whose key is unorderable (a list, a
  dict, NaN, or a tuple holding one); no comparison operator can match such a
  key (D28), only ``!=`` and ``?!N``.
* ``total_count`` -- the number of stored occurrences.

A group is the tuple ``(members, odd, total)``: ``members`` a keyed tree
mapping each orderable element (atoms by oid, values by ``order_key``) to its
occurrence count, ``odd`` a keyed tree mapping the *surrogate* of each
unorderable element (an orderable, injective, equality-preserving encoding,
see :func:`surrogate`) to ``(element, count)``, and ``total`` the occurrences in
the group.  Elements are counted, so a List holding an element twice stores it
with count 2.

Every operation is a pure function over immutable trees (path copying), so an
index version may be shared between threads, and plans capture only immutable
state.
"""
from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable, Iterator
from decimal import Decimal

from ..collections import avl
from ..core.atom import PersistenceContext
from ..errors import ProtoCorruptionError, ProtoValidationError
from ..query.plans import Estimate, IndexPlan
from ..query.terms import (BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_HIGH, BETWEEN_EXCL_LOW, EQ, GE, GT, IN,
                           IS_NONE, LE, LT, NE, NOT_NONE, Term, is_orderable)
from ..values import MISSING, order_key, rank_bounds
from .base import Index, element_identity
from .keyspec import KeySpec

__all__ = ["KeyGroupIndex", "surrogate", "EMPTY_GROUP"]

#: The group of no element: ``(members, odd, total)``.
EMPTY_GROUP = (None, None, 0)

_NONE_OKEY = order_key(None)
_MISSING_OKEY = order_key(MISSING)
_RANGE_OPS = frozenset({LT, LE, GT, GE, BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_LOW, BETWEEN_EXCL_HIGH})
_BETWEEN_INCLUSIVE = {BETWEEN: (True, True), BETWEEN_EXCL_BOTH: (False, False),
                      BETWEEN_EXCL_LOW: (False, True), BETWEEN_EXCL_HIGH: (True, False)}


def surrogate(value):
    """An orderable stand-in for an unorderable value: equal values give equal surrogates.

    Lists become ``("l", items)``, dicts ``("d", pairs sorted by key)``, tuples
    holding an unorderable value ``("t", items)``, NaN ``("n",)`` and every
    orderable value ``("v", value)``; items are encoded recursively.  The tags
    make the encoding injective up to value equality (``values_equal`` for
    orderable parts, every NaN equal to every NaN, as
    :func:`proto_db.indexing.element_identity`).  Only used inside the ``odd``
    trees, which never hold orderable elements, so a surrogate cannot collide
    with a real element.
    """
    if isinstance(value, list):
        return ("l", tuple(surrogate(v) for v in value))
    if isinstance(value, dict):
        pairs = [(surrogate(k), surrogate(v)) for k, v in value.items()]
        pairs.sort(key=lambda p: order_key(p[0]))
        return ("d", tuple(pairs))
    if isinstance(value, tuple) and not is_orderable(value):
        return ("t", tuple(surrogate(v) for v in value))
    if isinstance(value, (float, Decimal)) and not is_orderable(value):
        return ("n",)
    return ("v", value)


# -- groups ---------------------------------------------------------------------------------

def _group_add(group: tuple, element) -> tuple:
    members, odd, total = group
    if is_orderable(element):
        n = avl.key_get(members, element, 0)
        members = avl.key_put(members, element, n + 1)[0]
    else:
        s = surrogate(element)
        entry = avl.key_get(odd, s)
        odd = avl.key_put(odd, s, (element, 1) if entry is None else (entry[0], entry[1] + 1))[0]
    return members, odd, total + 1


def _group_remove(group: tuple, element) -> tuple | None:
    """The group with one occurrence of ``element`` fewer, or None when it holds none."""
    members, odd, total = group
    if is_orderable(element):
        n = avl.key_get(members, element, 0)
        if n == 0:
            return None
        members = (avl.key_remove(members, element)[0] if n == 1
                   else avl.key_put(members, element, n - 1)[0])
    else:
        s = surrogate(element)
        entry = avl.key_get(odd, s)
        if entry is None:
            return None
        odd = avl.key_remove(odd, s)[0] if entry[1] == 1 else avl.key_put(odd, s, (entry[0], entry[1] - 1))[0]
    return members, odd, total - 1


def _group_iter(group: tuple) -> Iterator:
    """Every occurrence stored in ``group`` (members first, then unorderable elements)."""
    members, odd, _ = group
    for element, n in avl.key_iter(members):
        for _ in range(n):
            yield element
    for _, (element, n) in avl.key_iter(odd):
        for _ in range(n):
            yield element


def _groups_iter(groups: Iterable[tuple]) -> Iterator:
    for group in groups:
        yield from _group_iter(group)


class _GroupBuilder:
    """Accumulates one group during a bulk build: elements by order key, unorderable ones by surrogate."""

    __slots__ = ("key", "members", "odd", "total")

    def __init__(self, key):
        self.key = key
        self.members: dict = {}
        self.odd: dict = {}
        self.total = 0

    def add(self, element) -> None:
        self.total += 1
        if is_orderable(element):
            slot = self.members.get(ok := order_key(element))
            if slot is None:
                self.members[ok] = [element, 1]
            else:
                slot[1] += 1
        else:
            s = surrogate(element)
            slot = self.odd.get(ok := order_key(s))
            if slot is None:
                self.odd[ok] = [s, element, 1]
            else:
                slot[2] += 1

    def group(self) -> tuple:
        members = avl.key_from_sorted((e, n) for _, (e, n) in sorted(self.members.items(), key=_first))
        odd = avl.key_from_sorted((s, (e, n)) for _, (s, e, n) in sorted(self.odd.items(), key=_first))
        return members, odd, self.total


def _first(item):
    return item[0]


def _count(root) -> int:
    return 0 if root is None else root.count


class KeyGroupIndex(Index):
    """Abstract base of the key-group indexes (see the module docstring); subclasses define ``supports``.

    ``__init__(name, key_spec, *, context=None)`` accepts a :class:`KeySpec` or
    a ``str`` (a dotted path, ``KeySpec.path``).  The plans and estimates of
    every operator live here; a subclass chooses which operators it answers
    (:meth:`supports`) and whether it offers ordered iteration.
    """

    def __init__(self, name: str, key_spec: KeySpec | str, *, context: PersistenceContext | None = None):
        """Create an EMPTY index on ``key_spec`` (a ``str`` means ``KeySpec.path(key_spec)``)."""
        if isinstance(key_spec, str):
            key_spec = KeySpec.path(key_spec)
        super().__init__(name, key_spec, context=context)
        object.__setattr__(self, "groups", None)
        object.__setattr__(self, "side", EMPTY_GROUP)
        object.__setattr__(self, "total_count", 0)

    # -- element hooks --------------------------------------------------------------------

    def with_added(self, element) -> "KeyGroupIndex":
        """The index after one more occurrence of ``element`` (filed under its key, MISSING included)."""
        key = self.key_spec.extract(element)
        if not is_orderable(key):
            return self._with(side=_group_add(self.side, element), total_count=self.total_count + 1)
        group = avl.key_get(self.groups, key, EMPTY_GROUP)
        groups = avl.key_put(self.groups, key, _group_add(group, element))[0]
        return self._with(groups=groups, total_count=self.total_count + 1)

    def with_removed(self, element) -> "KeyGroupIndex":
        """The index after one occurrence of ``element`` fewer; ``ProtoValidationError`` if it holds none."""
        key = self.key_spec.extract(element)
        if not is_orderable(key):
            side = _group_remove(self.side, element)
            if side is None:
                self._not_held(element, key)
            return self._with(side=side, total_count=self.total_count - 1)
        group = avl.key_get(self.groups, key)
        group = None if group is None else _group_remove(group, element)
        if group is None:
            self._not_held(element, key)
        groups = (avl.key_remove(self.groups, key)[0] if group[2] == 0
                  else avl.key_put(self.groups, key, group)[0])
        return self._with(groups=groups, total_count=self.total_count - 1)

    def _not_held(self, element, key):
        raise ProtoValidationError(
            f"Index {self.name!r} ({type(self).__name__} on {self.key_spec.describe()}) does not hold "
            f"{element!r} under the key {key!r}; an index can only remove an element it holds")

    def build(self, elements: Iterable) -> "KeyGroupIndex":
        """An index over exactly ``elements``, bulk-built (ruling R10): group, sort, then balanced trees.

        O(n log n) for the sort, O(n) tree construction; the receiver's content is never kept.
        """
        builders: dict = {}
        side = _GroupBuilder(None)
        total = 0
        extract = self.key_spec.extract
        for element in elements:
            total += 1
            key = extract(element)
            if is_orderable(key):
                builder = builders.get(ok := order_key(key))
                if builder is None:
                    builder = builders[ok] = _GroupBuilder(key)
            else:
                builder = side
            builder.add(element)
        groups = avl.key_from_sorted((b.key, b.group()) for _, b in sorted(builders.items(), key=_first))
        return self.empty()._with(groups=groups, side=side.group(), total_count=total)

    # -- plans ------------------------------------------------------------------------------

    def _lookup(self, value) -> tuple:
        return avl.key_get(self.groups, value, EMPTY_GROUP)

    def _distinct(self, values) -> list:
        seen: dict = {}
        for v in values:
            seen.setdefault(order_key(v), v)
        return list(seen.values())

    @staticmethod
    def _range(term: Term) -> tuple:
        """``(low okey, high okey, include low, include high)`` of a range term, within the operand's rank."""
        op = term.op
        if op in _BETWEEN_INCLUSIVE:
            low, high = term.value
            return (order_key(low), order_key(high)) + _BETWEEN_INCLUSIVE[op]
        ok = order_key(term.value)
        rank_low, rank_high = rank_bounds(term.value)
        if op == LT:
            return rank_low, ok, True, False
        if op == LE:
            return rank_low, ok, True, True
        if op == GT:
            return ok, rank_high, False, True
        return ok, rank_high, True, True  # GE

    def _plan_parts(self, term: Term) -> tuple:
        """``(kind, producer)`` for a supported term; producers capture immutable trees only."""
        op, groups, side = term.op, self.groups, self.side
        if op == EQ:
            found = [self._lookup(term.value)]
            return "IndexLookup", lambda: _groups_iter(found)
        if op == IN:
            found = [self._lookup(v) for v in self._distinct(term.value)]
            return "IndexLookup", lambda: _groups_iter(found)
        if op == IS_NONE:
            found = [self._lookup(MISSING), self._lookup(None)]
            return "IndexLookup", lambda: _groups_iter(found)
        if op == NOT_NONE:
            def present():
                for _, group in avl.key_iter_okey_range(groups, _NONE_OKEY, None, False):
                    yield from _group_iter(group)
                yield from _group_iter(side)
            return "IndexRange", present
        if op == NE:
            excluded = order_key(term.value)

            def complement():
                for key, group in avl.key_iter_okey_range(groups, _MISSING_OKEY, None, False):
                    if order_key(key) != excluded:
                        yield from _group_iter(group)
                yield from _group_iter(side)
            return "IndexComplement", complement
        if op in _RANGE_OPS:
            lok, hik, lo_inc, hi_inc = self._range(term)

            def in_range():
                for _, group in avl.key_iter_okey_range(groups, lok, hik, lo_inc, hi_inc):
                    yield from _group_iter(group)
            return "IndexRange", in_range
        raise ProtoValidationError(f"{type(self).__name__} has no plan for {op!r}")  # pragma: no cover

    def plan(self, term: Term) -> IndexPlan:
        """An :class:`IndexPlan` yielding every stored occurrence matching ``term`` (D28 semantics)."""
        self._check_supported(term)
        kind, producer = self._plan_parts(term)
        return IndexPlan(self, term, producer, kind=kind)

    # -- estimates (cost model: D30) --------------------------------------------------------

    def estimate(self, term: Term) -> Estimate:
        """Rows and cost of :meth:`plan` (D30): one unit per occurrence produced, per key group
        visited and per tree level descended; ``!=`` is charged a full pass so a bare scan wins."""
        self._check_supported(term)
        op = term.op
        groups_n = _count(self.groups)
        descent = math.log2(groups_n + 2)
        if op in (EQ, IN, IS_NONE):
            values = ([term.value] if op == EQ else self._distinct(term.value) if op == IN else [MISSING, None])
            rows = sum(self._lookup(v)[2] for v in values)
            return Estimate(rows=rows, cost=rows + len(values) * descent)
        side_total = self.side[2]
        if op == NOT_NONE:
            absent = [self._lookup(MISSING)[2], self._lookup(None)[2]]
            rows = self.total_count - sum(absent)
            visited = groups_n - sum(1 for n in absent if n) + (1 if side_total else 0)
            return Estimate(rows=rows, cost=rows + descent + visited)
        if op == NE:
            rows = self.total_count - self._lookup(MISSING)[2] - self._lookup(term.value)[2]
            return Estimate(rows=rows, cost=self.total_count + groups_n + 1)
        lok, hik, lo_inc, hi_inc = self._range(term)
        keys = (avl.key_count_below(self.groups, hik, inclusive=hi_inc)
                - avl.key_count_below(self.groups, lok, inclusive=not lo_inc))
        keys = max(keys, 0)  # an empty interval (low above high)
        per_key = (self.total_count - side_total) / groups_n if groups_n else 0.0
        rows = keys * per_key
        return Estimate(rows=rows, cost=rows + descent + keys)

    # -- ordered iteration (offered by subclasses through supports_order) ---------------------

    def _ordered_plan(self, descending: bool) -> IndexPlan:
        groups, side = self.groups, self.side

        def produce():
            if descending:
                yield from _group_iter(side)
                for _, group in avl.key_iter_reversed(groups):
                    yield from _group_iter(group)
            else:
                for _, group in avl.key_iter(groups):
                    yield from _group_iter(group)
                yield from _group_iter(side)

        n = self.total_count
        return IndexPlan(self, None, produce, kind="IndexOrderedScan",
                         estimate=Estimate(rows=n, cost=n + _count(groups) + 1),
                         details={"descending": bool(descending)})

    # -- explain and validate -------------------------------------------------------------------

    def explain(self) -> dict:
        """The base entries plus ``entries`` (occurrences), ``keys`` (distinct orderable keys) and
        ``unorderable`` (occurrences whose key is unorderable)."""
        info = super().explain()
        info.update(entries=self.total_count, keys=_count(self.groups), unorderable=self.side[2])
        return info

    @staticmethod
    def _slot(key) -> tuple:
        return ("key", order_key(key)) if is_orderable(key) else ("unorderable",)

    def _corrupt(self, detail: str):
        raise ProtoCorruptionError(f"Index {self.name!r} ({type(self).__name__} on "
                                   f"{self.key_spec.describe()}) is corrupt: {detail}")

    def _check_group(self, group, slot: tuple, stored: Counter) -> int:
        if not isinstance(group, tuple) or len(group) != 3:
            self._corrupt(f"malformed group {group!r}")
        members, odd, total = group
        avl.check_invariants(members)
        avl.check_invariants(odd)
        seen = 0
        entries = [(e, n) for e, n in avl.key_iter(members)]
        for s, entry in avl.key_iter(odd):
            if not isinstance(entry, tuple) or len(entry) != 2:
                self._corrupt(f"malformed entry {entry!r}")
            e = entry[0]
            if is_orderable(e) or order_key(surrogate(e)) != order_key(s):
                self._corrupt(f"element {e!r} is stored under the wrong surrogate {s!r}")
            entries.append(entry)
        for element, n in entries:
            if type(n) is not int or n < 1:
                self._corrupt(f"element {element!r} has the count {n!r}")
            if self._slot(self.key_spec.extract(element)) != slot:
                self._corrupt(f"element {element!r} is filed under {slot!r}, not under its own key")
            stored[(slot, element_identity(element))] += n
            seen += n
        if total != seen:
            self._corrupt(f"a group records {total!r} occurrences but holds {seen}")
        return seen

    def validate(self, elements: Iterable) -> None:
        """Structural check against ``elements`` (``ProtoCorruptionError`` on any mismatch).

        Checks the AVL invariants of every tree, that each stored element is
        filed under its own key with a positive count, that group and index
        totals add up, and that the stored multiset of ``(key, element)`` equals
        that of ``elements`` (no leftovers, no missing or miscounted entries).
        That determines every answer, so the default plan-vs-scan comparison
        (O(keys x n)) is not repeated.
        """
        avl.check_invariants(self.groups)
        stored: Counter = Counter()
        total = 0
        for key, group in avl.key_iter(self.groups):
            if not is_orderable(key):
                self._corrupt(f"unorderable key {key!r} in the key tree")
            n = self._check_group(group, self._slot(key), stored)
            if n == 0:
                self._corrupt(f"empty group under the key {key!r}")
            total += n
        total += self._check_group(self.side, ("unorderable",), stored)
        if total != self.total_count:
            self._corrupt(f"total_count is {self.total_count!r} but {total} occurrences are stored")
        expected = Counter((self._slot(self.key_spec.extract(e)), element_identity(e)) for e in elements)
        if stored != expected:
            extra, missing = stored - expected, expected - stored
            self._corrupt(f"{sum(extra.values())} stored occurrences no element accounts for, "
                          f"{sum(missing.values())} element occurrences not stored")
