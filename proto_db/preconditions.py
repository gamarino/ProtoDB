"""The canonical data of the precondition concurrency model (spec sections 4, 5, 6 and 14).

A transaction declares what its logic depends on as **preconditions**: pairs (conflict unit,
expected value), the expected value always being the unit's value in the transaction's base
(R8).  At commit every precondition must still hold against the current state (R6: compared by
exact token, atoms by storage pointer); one that does not raises
:class:`~proto_db.errors.ProtoConflictError` naming its unit (R36).

This module holds only data: :class:`Unit` (a conflict unit), :class:`KeyRange` (a range
invariant, R67), the :data:`ABSENT` token, :func:`token` (the comparable form of a value, R6) and
:class:`ReadLog`, the per-transaction record of preconditions with the first-observation rule
(R8, R9) and the escalation of R66.  Nothing here runs user code (P-C).
"""
from __future__ import annotations

import enum
import threading
from dataclasses import dataclass
from typing import Any, Callable, Optional

from .core.atom import Atom
from .values import exact_token, order_key

__all__ = ["ABSENT", "UnitKind", "Unit", "KeyRange", "PRECONDITION_ESCALATION_LIMIT", "ReadLog", "token",
           "same_token", "range_violated", "changed_keys", "range_holds", "INDEX_CATCH_UP_ROUNDS"]


class _AbsentToken:
    """The token of an absent unit: one object per process, kept by copy and pickling."""

    __slots__ = ()
    _instance: Optional["_AbsentToken"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "ABSENT"

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    def __reduce__(self):
        return (_AbsentToken, ())


ABSENT = _AbsentToken()
"""The expected value of a unit that is absent (a cell or root that does not exist, R4 "Absent")."""


class UnitKind(enum.Enum):
    """The class of a conflict unit (spec sections 5 and 6)."""

    ROOT = "root"                    # an object root: the whole value (R2)
    ROOT_PRESENCE = "root_presence"  # a collection root is present and of the same class
    ROOT_NAMES = "root_names"        # the set of root names
    CELL = "cell"                    # a mutable cell, by name (R3)
    CELL_NAMES = "cell_names"        # the set of mutable cell names
    KEY = "key"                      # a Dictionary / HashDictionary key
    ELEMENT = "element"              # the presence of a Set element
    COUNT = "count"                  # the count of a CountedSet element
    RECORDS = "records"              # the records of a RepeatedKeysDictionary key
    POSITION = "position"            # a List position
    WHOLE = "whole"                  # a whole collection root
    INDEX_DEFS = "index_defs"        # the set of index definitions of a collection
    INDEX_GROUP = "index_group"      # the group of one key in one index (section 7)
    RANGE = "range"                  # a range invariant (R67)


# The kinds R66 counts as fine preconditions of a collection root and replaces by WHOLE.
_FINE_KINDS = frozenset({UnitKind.KEY, UnitKind.ELEMENT, UnitKind.COUNT, UnitKind.RECORDS,
                         UnitKind.POSITION, UnitKind.INDEX_GROUP})


@dataclass(frozen=True)
class Unit:
    """A conflict unit.  ``root`` names the root (``None`` for cells and the name sets); for
    ``CELL`` ``key`` is the cell name; for ``INDEX_GROUP`` ``index`` names the index and ``key``
    is the index key."""

    kind: UnitKind
    root: str | None = None
    key: Any = None
    index: str | None = None


def _atom_token(atom: Atom) -> tuple:
    return ("@atom", atom.atom_pointer)


def token(value):
    """The comparable form of a unit's value (R6, D34): :data:`ABSENT` or an exact token.

    Plain values are compared exactly (:func:`proto_db.values.exact_token`: same type and same
    payload, so ``1`` and ``1.0``, ``Decimal("1")`` and ``Decimal("1.00")`` or one instant in two
    time zones differ); atoms, nested ones included, are identified by their storage pointer, so
    an atom that is the value itself (a collection, a ``ValueBox``) is never loaded.
    """
    if value is ABSENT:
        return ABSENT
    if isinstance(value, Atom):
        # Directly, not through exact_token: its is_atom() probe reads ``oid``, which
        # loads a stub (D21) -- under the root lock, for the commit-time check.
        return _atom_token(value)
    return exact_token(value, _atom_token)


def same_token(a, b) -> bool:
    """Whether two tokens of :func:`token` denote the same value."""
    if a is ABSENT or b is ABSENT:
        return a is b
    return a == b


@dataclass(frozen=True)
class KeyRange:
    """A range invariant (R67): the keys of root ``root`` between ``low`` and ``high``, from the
    Dictionary's own keys (``index=None``) or from the named RangeIndex.

    ``low`` and ``high`` are **order keys** (:func:`proto_db.values.order_key` of a key, or a
    :func:`proto_db.values.rank_bounds` bound), so a range is pure data compared without user
    code (P-C); ``None`` means unbounded on that side (an order key is never ``None``).  The
    precondition holds while every key inside the range, and its value (a Dictionary: the
    stored form, a ``ValueBox`` by pointer, D51; a RangeIndex: the group ``(members, total)``,
    the members by pointer), is the same in the current state as in the base (R68)."""

    root: str
    index: str | None
    low: Any
    high: Any
    low_inclusive: bool
    high_inclusive: bool


_NO_TOKEN = object()      # ReadLog.record: the escalation token is not computed yet

PRECONDITION_ESCALATION_LIMIT = 1024
"""At most this many fine preconditions per collection root; the next one escalates (R66)."""

INDEX_CATCH_UP_ROUNDS = 3
"""At most this many pre-pass rounds catch an index definition change up with the commits
published since its base (R69); a commit that would need one more conflicts on the whole
collection, and ``Database.run`` retries it."""


class ReadLog:
    """The preconditions and ranges one transaction recorded (thread-safe, R44).

    :meth:`record` keeps the first expected value given for a unit (R8, R9): later observations
    of the same unit add nothing.  Recording the ``PRECONDITION_ESCALATION_LIMIT + 1``-th fine
    precondition of one root (a key, element, count, records, position or index group unit, or a
    range: R66 bounds the read set's memory, and a range is a fine precondition) replaces that
    root's fine units and ranges by ``Unit(WHOLE, root)`` (R66, P-A); so does recording a
    ``WHOLE`` unit directly.  Once a root is whole, its fine units and ranges are covered and
    are not recorded.

    ``whole_token(root)`` gives the expected value of an escalated ``WHOLE`` unit: the token of
    the root's base value (R8).  A log built without it stores ``None`` for an escalation; the
    owner that validates the log supplies it.
    """

    def __init__(self, whole_token: Optional[Callable[[str], Any]] = None):
        self._lock = threading.Lock()
        self._whole_token = whole_token
        self._preconditions: dict = {}
        self._ranges: dict = {}             # KeyRange -> None: insertion-ordered set
        self._fine: dict = {}               # root -> number of fine units recorded
        self._whole: set = set()

    def record(self, unit: Unit, expected) -> None:
        """Record the precondition ``(unit, expected)`` unless the unit is already recorded or covered.

        The expected value of an escalation (``whole_token``) may load lazy tree nodes, so it is
        computed without holding the log's lock; the escalation is then decided again under it.
        """
        root = unit.root
        token = _NO_TOKEN
        while True:
            with self._lock:
                if unit in self._preconditions:
                    return
                if unit.kind is UnitKind.WHOLE:
                    self._make_whole(root, expected)
                    return
                if root is not None and unit.kind in _FINE_KINDS:
                    if root in self._whole:
                        return
                    count = self._fine.get(root, 0)
                    if count >= PRECONDITION_ESCALATION_LIMIT:
                        if token is not _NO_TOKEN or self._whole_token is None:
                            self._make_whole(root, None if token is _NO_TOKEN else token)
                            return
                    else:
                        self._fine[root] = count + 1
                        self._preconditions[unit] = expected
                        return
                else:
                    self._preconditions[unit] = expected
                    return
            # Escalation needed: compute the base token outside the lock, then check again (the
            # count of fine units only grows until the root becomes whole, so the next pass
            # escalates with this token or finds the root already whole).
            token = self._whole_token(root)

    def _make_whole(self, root, expected) -> None:
        """Under the lock: ``root`` becomes a whole-collection precondition."""
        self._whole.add(root)
        self._fine.pop(root, None)
        for unit in [u for u in self._preconditions if u.root == root and u.kind in _FINE_KINDS]:
            del self._preconditions[unit]
        for r in [r for r in self._ranges if r.root == root]:
            del self._ranges[r]
        self._preconditions[Unit(UnitKind.WHOLE, root)] = expected

    def record_range(self, r: KeyRange, covers: Optional[KeyRange] = None) -> None:
        """Record the range invariant ``r`` (once; covered when its root is whole).

        A range is a fine precondition of its root (R66): it counts toward
        ``PRECONDITION_ESCALATION_LIMIT`` with the root's fine units, and the range past the limit
        escalates the root to ``WHOLE``.  ``covers`` is a range ``r`` contains (a window read
        that grew: same start, a later end): it is replaced by ``r`` in place, so a window
        widening key by key holds one range.  Dropping ``covers`` is right even if another read
        recorded it too, since ``r`` covers it."""
        root = r.root
        token = _NO_TOKEN
        while True:
            with self._lock:
                if root in self._whole:
                    return
                old = covers is not None and covers != r and covers in self._ranges
                if r in self._ranges:
                    if old:
                        del self._ranges[covers]
                        self._fine[root] -= 1
                    return
                if old:
                    del self._ranges[covers]
                    self._ranges[r] = None
                    return
                count = self._fine.get(root, 0)
                if count < PRECONDITION_ESCALATION_LIMIT:
                    self._fine[root] = count + 1
                    self._ranges[r] = None
                    return
                if token is not _NO_TOKEN or self._whole_token is None:
                    self._make_whole(root, None if token is _NO_TOKEN else token)
                    return
            token = self._whole_token(root)         # outside the lock, as in record()

    def caught_up(self, unit: Unit, expected) -> None:
        """Replace the expected value of the recorded ``unit`` by ``expected``: the one exception
        to R8, for the ``WHOLE`` unit of a collection whose index definition change the pre-pass
        caught up with a newer published state (R69).  ``KeyError`` when ``unit`` is not recorded."""
        with self._lock:
            if unit not in self._preconditions:
                raise KeyError(unit)
            self._preconditions[unit] = expected

    def whole(self, root: str) -> bool:
        """Whether ``root`` has a whole-collection precondition."""
        with self._lock:
            return root in self._whole

    def __contains__(self, unit: Unit) -> bool:
        """Whether ``unit`` itself is recorded (not whether a WHOLE unit covers it)."""
        with self._lock:
            return unit in self._preconditions

    def preconditions(self) -> dict:
        """A copy of the recorded preconditions: ``{Unit: expected}`` in recording order."""
        with self._lock:
            return dict(self._preconditions)

    def ranges(self) -> tuple:
        """The recorded range invariants, in recording order."""
        with self._lock:
            return tuple(self._ranges)


# -- range invariants (R67, R68): data only, P-C ----------------------------------------------------

def _in_range(r: KeyRange, okey) -> bool:
    """Whether the order key ``okey`` lies inside ``r``."""
    low, high = r.low, r.high
    if low is not None and (okey < low or (okey == low and not r.low_inclusive)):
        return False
    if high is not None and (okey > high or (okey == high and not r.high_inclusive)):
        return False
    return True


def range_violated(r: KeyRange, changed_keys) -> bool:
    """Whether any key of ``changed_keys`` (keys that differ between the base and the current
    state: Dictionary keys, or index keys for a RangeIndex range) lies inside ``r``, by
    ``order_key`` comparisons only (R68, P-C)."""
    return any(_in_range(r, order_key(key)) for key in changed_keys)


def changed_keys(base_entries, current_entries):
    """The keys whose entry differs between two key-ordered streams of ``(order key, key, token)``
    (a key present in one only, or with another token): a merge, O(len(base) + len(current))."""
    base, current = iter(base_entries), iter(current_entries)
    b, c = next(base, None), next(current, None)
    while b is not None or c is not None:
        if c is None or (b is not None and b[0] < c[0]):
            yield b[1]
            b = next(base, None)
        elif b is None or c[0] < b[0]:
            yield c[1]
            c = next(current, None)
        else:
            if not same_token(b[2], c[2]):
                yield c[1]
            b, c = next(base, None), next(current, None)


def range_holds(r: KeyRange, base_tree, current_tree) -> bool:
    """Full validation of ``r`` (no pre-pass, R65): the keyed trees ``base_tree`` and
    ``current_tree`` (a Dictionary's ``root``, or a RangeIndex's ``groups``) hold the same keys
    with the same tokens inside ``r``.  The same tree (by pointer) holds at once; otherwise each
    tree is walked over the range only: O(log n + k) nodes per tree, k the entries inside the
    range, and no value is loaded (a ``ValueBox`` or a members tree is compared by pointer)."""
    if base_tree is current_tree:
        return True
    if isinstance(base_tree, Atom) and isinstance(current_tree, Atom):
        pointer = base_tree.atom_pointer
        if pointer is not None and pointer == current_tree.atom_pointer:
            return True
    from .collections import avl
    entries = [((okey, key, token(stored)) for okey, key, stored in
                avl.key_iter_okey_range_stored(tree, r.low, r.high, r.low_inclusive, r.high_inclusive))
               for tree in (base_tree, current_tree)]
    return not range_violated(r, changed_keys(*entries))
