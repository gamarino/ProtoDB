"""The canonical data of the precondition concurrency model (spec sections 4, 5, 6 and 14).

A transaction declares what its logic depends on as **preconditions**: pairs (conflict unit,
expected value), the expected value always being the unit's value in the transaction's base
(R8).  At commit every precondition must still hold against the current state (R6: compared by
exact token, atoms by storage pointer); one that does not raises
:class:`~proto_db.errors.ProtoConflictError` naming its unit (R36).

This module holds only data: :class:`Unit` (a conflict unit), :class:`KeyRange` (a range
invariant, R67), the :data:`ABSENT` token and :class:`ReadLog`, the per-transaction record of
preconditions with the first-observation rule (R8, R9) and the escalation of R66.  Nothing here
runs user code (P-C).
"""
from __future__ import annotations

import enum
import threading
from dataclasses import dataclass
from typing import Any, Callable, Optional

__all__ = ["ABSENT", "UnitKind", "Unit", "KeyRange", "PRECONDITION_ESCALATION_LIMIT", "ReadLog"]


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


@dataclass(frozen=True)
class KeyRange:
    """A range invariant (R67): the keys of root ``root`` between ``low`` and ``high`` (by
    ``order_key``), from the Dictionary's own keys (``index=None``) or from the named
    RangeIndex.  ``low``/``high`` ``None`` means unbounded on that side."""

    root: str
    index: str | None
    low: Any
    high: Any
    low_inclusive: bool
    high_inclusive: bool


PRECONDITION_ESCALATION_LIMIT = 1024
"""At most this many fine preconditions per collection root; the next one escalates (R66)."""


class ReadLog:
    """The preconditions and ranges one transaction recorded (thread-safe, R44).

    :meth:`record` keeps the first expected value given for a unit (R8, R9): later observations
    of the same unit add nothing.  Recording the ``PRECONDITION_ESCALATION_LIMIT + 1``-th fine
    unit of one root (key, element, count, records, position or index group) replaces that
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
        """Record the precondition ``(unit, expected)`` unless the unit is already recorded or covered."""
        with self._lock:
            if unit in self._preconditions:
                return
            root = unit.root
            if unit.kind is UnitKind.WHOLE:
                self._make_whole(root, expected)
                return
            fine = root is not None and unit.kind in _FINE_KINDS
            if fine:
                if root in self._whole:
                    return
                count = self._fine.get(root, 0)
                if count >= PRECONDITION_ESCALATION_LIMIT:
                    token = None if self._whole_token is None else self._whole_token(root)
                    self._make_whole(root, token)
                    return
                self._fine[root] = count + 1
            self._preconditions[unit] = expected

    def _make_whole(self, root, expected) -> None:
        """Under the lock: ``root`` becomes a whole-collection precondition."""
        self._whole.add(root)
        self._fine.pop(root, None)
        for unit in [u for u in self._preconditions if u.root == root and u.kind in _FINE_KINDS]:
            del self._preconditions[unit]
        for r in [r for r in self._ranges if r.root == root]:
            del self._ranges[r]
        self._preconditions[Unit(UnitKind.WHOLE, root)] = expected

    def record_range(self, r: KeyRange) -> None:
        """Record the range invariant ``r`` (once; covered when its root is whole)."""
        with self._lock:
            if r.root not in self._whole:
                self._ranges.setdefault(r, None)

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
