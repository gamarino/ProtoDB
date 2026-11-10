"""Shared machinery of :class:`EqualityIndex` and :class:`RangeIndex`: key groups in a keyed AVL (D30).

Structure (persisted public fields of the index):

* ``groups`` -- a keyed AVL tree (:class:`proto_db.collections.avl.KeyNode`)
  ordered by ``order_key`` of the extracted key; MISSING and None are ordinary
  keys at the bottom of the total order.  Each node value is a *group*.
* ``side`` -- the group of every element whose key is unorderable (a list, a
  dict, NaN, or a tuple holding one); no comparison operator can match such a
  key (D28), only ``!=`` and ``?!N``.
* ``total_count`` -- the number of stored occurrences.

A group is the tuple ``(members, total)``: ``members`` a keyed tree mapping a
*member key* to ``(element, count)``, and ``total`` the occurrences in the
group.  A member key is ``(equality part, exact token)``: the equality part is
``(0, element)`` for an orderable element (atoms by oid, values by
``order_key``) and ``(1, surrogate(element))`` for an unorderable one (see
:func:`surrogate`); the exact token (:func:`exact_token`, the element's canonical encoding, D58) tells
apart values that are equal but not identical (``1``, ``1.0``, ``Decimal("1.00")``;
aware datetimes of one instant in different zones).  Plans therefore return exactly
the stored elements a scan returns, never an equal representative.  A removal takes
exactly the member whose member key is the removed element's; when there is none the
index does not hold the element (``ProtoValidationError``): it never takes an equal
member instead (D58, review C1).  Elements are counted, so a List holding an element
twice stores it with count 2.

``member_tokens`` (persisted) is :data:`MEMBER_TOKENS` on an index whose tokens are
canonical encodings; an index written by format 3 lacks it (its tokens were Python
text) and is rebuilt by its collection before its first change (D58).

Every operation is a pure function over immutable trees (path copying), so an
index version may be shared between threads, and plans capture only immutable
state.

Rank access (D56): both levels are :class:`~proto_db.collections.avl.CountedKeyNode`
trees, whose ``weight`` counts occurrences (a group's value is ``(members, total)``, a
member's ``(element, count)``).  Every plan's result is a concatenation of *segments*
(a key range of ``groups``, or one group's members) of known weight, so a plan offers
``window(start, stop)`` in O(log G + log M + rows) and ``exact_rows()`` in O(log G) per
segment.  An index written before weights existed (plain ``KeyNode`` trees) answers every
plan as before but offers no window; rebuilding it adds the weights.
"""
from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable, Iterator
from decimal import Decimal
from itertools import islice

from ..collections import avl
from ..collections.avl import CountedKeyNode
from ..core.atom import PersistenceContext
from ..errors import ProtoCorruptionError, ProtoMisfiledError, ProtoValidationError
from ..query.plans import Estimate, IndexPlan
from ..query.terms import (BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_HIGH, BETWEEN_EXCL_LOW, EQ, GE, GT, IN,
                           IS_NONE, LE, LT, NE, NOT_NONE, Term, is_orderable)
from ..values import MISSING, exact_token, is_atom, order_key, rank_bounds
from .base import Index, element_identity
from .keyspec import KeySpec

__all__ = ["KeyGroupIndex", "surrogate", "exact_token", "EMPTY_GROUP", "member_key", "equal_members_bounds",
           "group_add", "group_remove", "group_iter", "MEMBER_TOKENS"]

#: The member-token scheme of this revision (D58): an exact token is the canonical encoding.
#: Persisted as ``member_tokens`` by the key-group and vector indexes; absent on format-3 indexes.
MEMBER_TOKENS = 1

#: The group of no element: ``(members, total)``.
EMPTY_GROUP = (None, 0)

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
    :func:`proto_db.indexing.element_identity`).  Used as the equality part
    ``(1, surrogate)`` of member keys, tagged apart from orderable elements
    ``(0, element)``, so a surrogate cannot collide with a real element.
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


# -- exact member keys ------------------------------------------------------------------------

def _equality_part(element) -> tuple:
    """The part of a member key shared by every element equal to ``element`` (``element_identity`` classes)."""
    return (0, element) if is_orderable(element) else (1, surrogate(element))


def _member_key(element) -> tuple:
    """``(equality part, exact token)``: members equal to one another are adjacent in the members tree."""
    return (_equality_part(element), exact_token(element))


def _equal_members_bounds(element) -> tuple:
    """Order-key bounds enclosing every member key whose equality part equals ``element``'s."""
    eq = order_key(_equality_part(element))
    return (_R_TUPLE, (eq,)), (_R_TUPLE, (eq, (_ABOVE_EVERY_RANK,)))


_R_TUPLE = order_key(())[0]
#: A rank above every order-key rank: ``(_ABOVE_EVERY_RANK,)`` exceeds every order key (2-tuples, D17).
_ABOVE_EVERY_RANK = order_key(MISSING)[0] + 1000

#: Public names of the member-key scheme, shared with other index kinds (the vector index, D39).
member_key = _member_key
equal_members_bounds = _equal_members_bounds


def member_key_matches(key, element, current: bool) -> bool:
    """Whether ``key`` is the member key of ``element``.  On an index with current tokens
    (``current``) the whole key is compared; on one written by format 3, everything but the token,
    which format 3 computed differently and which is only checked to be a tuple (re-review I-1):
    such an index is valid, if stale, data."""
    if not isinstance(key, tuple) or len(key) != 2:
        return False
    if current:
        return order_key(key) == order_key(_member_key(element))
    return isinstance(key[1], tuple) and order_key(key[0]) == order_key(_equality_part(element))


# -- groups ---------------------------------------------------------------------------------

def _group_add(group: tuple, element) -> tuple:
    members, total = group
    key = _member_key(element)
    entry = avl.key_get(members, key)
    members = avl.key_put(members, key, (element, 1) if entry is None else (entry[0], entry[1] + 1),
                          node_class=CountedKeyNode)[0]
    return members, total + 1


def _group_remove(group: tuple, element) -> tuple | None:
    """The group with one occurrence of ``element`` fewer, or None when it holds none.

    Only the member whose member key is ``element``'s is removed: never an equal member with
    another token (D58, review C1).
    """
    members, total = group
    key = _member_key(element)
    entry = avl.key_get(members, key)
    if entry is None:
        return None
    members = avl.key_remove(members, key)[0] if entry[1] == 1 else avl.key_put(members, key,
                                                                                   (entry[0], entry[1] - 1))[0]
    return members, total - 1


def _group_iter(group: tuple) -> Iterator:
    """Every occurrence stored in ``group``, each the stored element itself."""
    for _, (element, n) in avl.key_iter(group[0]):
        for _ in range(n):
            yield element


#: Public names of the group operations, shared with other index kinds (the vector index, D39).
group_add = _group_add
group_remove = _group_remove
group_iter = _group_iter


def _groups_iter(groups: Iterable[tuple]) -> Iterator:
    for group in groups:
        yield from _group_iter(group)


# -- rank access (D56) ----------------------------------------------------------------------
#
# A segment is ``(tree, start, rows, descending, of_groups)``: ``rows`` occurrences from
# occurrence ``start`` of ``tree`` -- a groups tree (``of_groups``) or one group's members
# tree.  Members are always produced in ascending member order, as ``_group_iter`` does;
# ``descending`` reverses only the order of key groups (D30).

def _members_from(members, rank: int) -> Iterator:
    for _, (element, n), skip in avl.key_iter_from_weight(members, rank):
        for _ in range(n - skip):
            yield element


def _groups_from(groups, rank: int, descending: bool) -> Iterator:
    for _, (members, _total), skip in avl.key_iter_from_weight(groups, rank, descending):
        yield from _members_from(members, skip)


def _segment_rows(segment: tuple, offset: int, limit: int) -> Iterator:
    tree, start, _rows, descending, of_groups = segment
    rows = (_groups_from(tree, start + offset, descending) if of_groups
            else _members_from(tree, start + offset))
    return islice(rows, limit)


def _group_segment(group: tuple) -> tuple:
    return (group[0], 0, group[1], False, False)


def _window(segments: list, start: int, stop: int | None) -> Iterator:
    """Rows ``start .. stop-1`` of the concatenation of ``segments`` (``stop=None``: to the end)."""
    for segment in segments:
        rows = segment[2]
        if stop is not None and stop <= 0:
            return
        if start >= rows:
            start -= rows
            if stop is not None:
                stop -= rows
            continue
        end = rows if stop is None else min(rows, stop)
        if end > start:
            yield from _segment_rows(segment, start, end - start)
        if stop is not None:
            stop -= rows
        start = 0


def _reverse_segments(segments: list) -> list:
    """The segments' groups in reverse order; each group keeps its members' order."""
    out = []
    for tree, start, rows, descending, of_groups in reversed(segments):
        if of_groups:
            start = avl.key_weight(tree) - start - rows
            descending = not descending
        out.append((tree, start, rows, descending, of_groups))
    return out


def _rank_access(segments) -> tuple:
    """The ``(window, rows)`` pair an :class:`IndexPlan` takes, over lazily computed segments."""
    def window(start: int, stop: int | None) -> Iterator:
        return _window(segments(), start, stop)

    def rows() -> int:
        return sum(segment[2] for segment in segments())
    return window, rows


def _weighted(tree) -> bool:
    return tree is None or isinstance(tree, CountedKeyNode)


class _GroupBuilder:
    """Accumulates one group during a bulk build, by exact member key."""

    __slots__ = ("key", "members", "total")

    def __init__(self, key):
        self.key = key
        self.members: dict = {}
        self.total = 0

    def add(self, element) -> None:
        self.total += 1
        key = _member_key(element)
        slot = self.members.get(ok := order_key(key))
        if slot is None:
            self.members[ok] = [key, element, 1]
        else:
            slot[2] += 1

    def group(self) -> tuple:
        members = avl.key_from_sorted_okeys(((ok, k, (e, n)) for ok, (k, e, n)
                                             in sorted(self.members.items(), key=_first)),
                                            node_class=CountedKeyNode)
        return members, self.total


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

    __type_name__ = "proto_db.indexing.keyed.KeyGroupIndex"
    _required_fields = ("name", "key_spec", "groups", "side", "total_count")

    def __init__(self, name: str, key_spec: KeySpec | str, *, context: PersistenceContext | None = None):
        """Create an EMPTY index on ``key_spec`` (a ``str`` means ``KeySpec.path(key_spec)``)."""
        if isinstance(key_spec, str):
            key_spec = KeySpec.path(key_spec)
        super().__init__(name, key_spec, context=context)
        object.__setattr__(self, "groups", None)
        object.__setattr__(self, "side", EMPTY_GROUP)
        object.__setattr__(self, "total_count", 0)
        object.__setattr__(self, "member_tokens", MEMBER_TOKENS)

    def has_current_tokens(self) -> bool:
        """False for an index written by format 3 (Python-text member tokens): its collection
        rebuilds it before changing it (D58)."""
        return getattr(self, "member_tokens", None) == MEMBER_TOKENS

    # -- element hooks --------------------------------------------------------------------

    def with_added(self, element) -> "KeyGroupIndex":
        """The index after one more occurrence of ``element`` (filed under its key, MISSING included)."""
        key = self.key_spec.extract(element)
        if not is_orderable(key):
            return self._with(side=_group_add(self.side, element), total_count=self.total_count + 1)
        group = avl.key_get(self.groups, key, EMPTY_GROUP)
        groups = avl.key_put(self.groups, key, _group_add(group, element), node_class=CountedKeyNode)[0]
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
        groups = (avl.key_remove(self.groups, key)[0] if group[1] == 0
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
        groups = avl.key_from_sorted_okeys(((ok, b.key, b.group()) for ok, b in sorted(builders.items(), key=_first)),
                                           node_class=CountedKeyNode)
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
        """An :class:`IndexPlan` yielding every stored occurrence matching ``term`` (D28 semantics).

        On a weighted index the plan also offers rank access (``window``, ``exact_rows``, D56)."""
        self._check_supported(term)
        kind, producer = self._plan_parts(term)
        rank = sorted_rank = None
        if self._has_weights():
            rank = _rank_access(lambda: self._term_segments(term))
            sorted_rank = self._sorted_rank(lambda desc: self._sorted_segments(term, desc))
        return IndexPlan(self, term, producer, kind=kind, rank=rank, sorted_rank=sorted_rank, cache_args=())

    # -- rank access (D56) ----------------------------------------------------------------

    def _has_weights(self) -> bool:
        """True when both the key tree and the side group are weighted (written by this code)."""
        return _weighted(self.groups) and _weighted(self.side[0])

    def _range_segment(self, lok, hik, lo_inc: bool, hi_inc: bool) -> tuple:
        """The groups whose keys lie in the range, as one segment (an order key None = open side)."""
        groups = self.groups
        low = 0 if lok is None else avl.key_weight_below(groups, lok, inclusive=not lo_inc)
        high = avl.key_weight(groups) if hik is None else avl.key_weight_below(groups, hik, inclusive=hi_inc)
        return (groups, low, max(high - low, 0), False, True)

    def _term_segments(self, term: Term) -> list:
        """The segments of :meth:`plan`'s result, in the order its producer yields them."""
        op = term.op
        if op == EQ:
            return [_group_segment(self._lookup(term.value))]
        if op == IN:
            return [_group_segment(self._lookup(v)) for v in self._distinct(term.value)]
        if op == IS_NONE:
            return [_group_segment(self._lookup(MISSING)), _group_segment(self._lookup(None))]
        side = _group_segment(self.side)
        if op == NOT_NONE:
            return [self._range_segment(_NONE_OKEY, None, False, True), side]
        if op == NE:
            excluded = order_key(term.value)
            return [self._range_segment(_MISSING_OKEY, excluded, False, False),
                    self._range_segment(excluded, None, False, True), side]
        lok, hik, lo_inc, hi_inc = self._range(term)
        return [self._range_segment(lok, hik, lo_inc, hi_inc)]

    def _sorted_rank(self, segments_for):
        """The ``sorted_rank(key_spec, descending)`` hook of an IndexPlan: rank access to the plan's
        rows as a stable sort by this index's key would order them (None for another key)."""
        def sorted_rank(key_spec, descending: bool):
            if key_spec != self.key_spec:
                return None
            return _rank_access(lambda: segments_for(descending))
        return sorted_rank

    def _sorted_segments(self, term: Term, descending: bool) -> list:
        """The rows of :meth:`plan` in the order a stable sort by this index's key gives (D36):
        key groups ascending (unorderable keys last), each group's members in member order,
        which is the plan's own order for every operator but ``in`` (operand order); descending
        reverses the groups (unorderable first) and keeps each group's members in order."""
        if term.op == IN:
            values = sorted(self._distinct(term.value), key=order_key)
            segments = [_group_segment(self._lookup(v)) for v in values]
        else:
            segments = self._term_segments(term)
        return _reverse_segments(segments) if descending else segments

    def _ordered_segments(self, descending: bool) -> list:
        groups = self.groups
        whole = (groups, 0, avl.key_weight(groups), descending, True)
        side = _group_segment(self.side)
        return [side, whole] if descending else [whole, side]

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
            rows = sum(self._lookup(v)[1] for v in values)
            return Estimate(rows=rows, cost=rows + len(values) * descent)
        side_total = self.side[1]
        if op == NOT_NONE:
            absent = [self._lookup(MISSING)[1], self._lookup(None)[1]]
            rows = self.total_count - sum(absent)
            visited = groups_n - sum(1 for n in absent if n) + (1 if side_total else 0)
            return Estimate(rows=rows, cost=rows + descent + visited)
        if op == NE:
            rows = self.total_count - self._lookup(MISSING)[1] - self._lookup(term.value)[1]
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
        rank = sorted_rank = None
        if self._has_weights():
            rank = _rank_access(lambda: self._ordered_segments(descending))
            sorted_rank = self._sorted_rank(self._ordered_segments)
        return IndexPlan(self, None, produce, kind="IndexOrderedScan",
                         estimate=Estimate(rows=n, cost=n + _count(groups) + 1),
                         details={"descending": bool(descending)}, rank=rank, sorted_rank=sorted_rank,
                         cache_args=(("descending", bool(descending)),))

    # -- explain and validate -------------------------------------------------------------------

    def explain(self) -> dict:
        """The base entries plus ``entries`` (occurrences), ``keys`` (distinct orderable keys) and
        ``unorderable`` (occurrences whose key is unorderable)."""
        info = super().explain()
        info.update(entries=self.total_count, keys=_count(self.groups), unorderable=self.side[1])
        return info

    @staticmethod
    def _slot(key) -> tuple:
        return ("key", order_key(key)) if is_orderable(key) else ("unorderable",)

    def _corrupt(self, detail: str):
        raise ProtoCorruptionError(f"Index {self.name!r} ({type(self).__name__} on "
                                   f"{self.key_spec.describe()}) is corrupt: {detail}")

    def _misfiled(self, detail: str):
        raise ProtoMisfiledError(
            f"Index {self.name!r} ({type(self).__name__} on {self.key_spec.describe()}) holds a misfiled "
            f"entry: {detail}. Format 3 wrote built-in subclasses (a (str, Enum) member) through their own "
            f"conversion but filed them by their base value (D58); the collection must be rebuilt: "
            f"replace it with collection.rebuilt()")

    def _check_group(self, group, slot: tuple, stored: Counter) -> int:
        if not isinstance(group, tuple) or len(group) != 2:
            self._corrupt(f"malformed group {group!r}")
        current = self.has_current_tokens()
        members, total = group
        avl.check_invariants(members)
        seen = 0
        entries = []
        for key, entry in avl.key_iter(members):
            if not isinstance(entry, tuple) or len(entry) != 2:
                self._corrupt(f"malformed entry {entry!r}")
            if not member_key_matches(key, entry[0], current):
                self._corrupt(f"element {entry[0]!r} is stored under the wrong member key {key!r}")
            entries.append(entry)
        for element, n in entries:
            if type(n) is not int or n < 1:
                self._corrupt(f"element {element!r} has the count {n!r}")
            if self._slot(self.key_spec.extract(element)) != slot:
                self._misfiled(f"element {element!r} is filed under {slot!r}, not under its own key")
            stored[(slot, element_identity(element), exact_token(element))] += n
            seen += n
        if total != seen:
            self._corrupt(f"a group records {total!r} occurrences but holds {seen}")
        return seen

    def validate(self, elements: Iterable) -> None:
        """Structural check against ``elements`` (``ProtoCorruptionError`` on any mismatch).

        Checks the AVL invariants of every tree, that each stored element is
        filed under its own key and member key with a positive count, that group
        and index totals add up, and that the stored multiset of ``(key, element,
        exact token)`` equals that of ``elements`` (no leftovers, no missing or
        miscounted entries, no equal-but-different value in place of the real one).
        On an index written by format 3 (:meth:`has_current_tokens` false) the
        member keys' tokens are not compared: such an index is stale, not corrupt.
        That determines every answer, so the default plan-vs-scan comparison
        (O(keys x n)) is not repeated.
        """
        previous = None
        for key, _ in avl.key_iter(self.groups):
            okey = order_key(key) if is_orderable(key) else None
            if okey is not None and previous is not None and not previous[0] < okey:
                self._misfiled(f"the key {key!r} is stored after {previous[1]!r}, which its key order places "
                               f"after it")
            if okey is not None:
                previous = (okey, key)
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
        expected = Counter((self._slot(self.key_spec.extract(e)), element_identity(e), exact_token(e))
                           for e in elements)
        if stored != expected:
            extra, missing = stored - expected, expected - stored
            self._corrupt(f"{sum(extra.values())} stored occurrences no element accounts for, "
                          f"{sum(missing.values())} element occurrences not stored")
