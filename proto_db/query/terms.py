"""Minimal term model and scan oracle (spec section 10, Task 4.1, decision D28).

A :class:`Term` is ``(path, op, value)``: the key named by ``path`` (see
:meth:`KeySpec.for_term_path`: a dotted path, ``None`` for the element itself,
``"@name"`` for a registered extractor) compared with ``value`` by ``op``.
:meth:`Term.matches` evaluates a term on one element with :mod:`proto_db.values`
semantics; it is the scan oracle every index must agree with.  Task 6.1 extends
this module with the full expression layer (``And``, ``Or``, ``Not``, ...).

Semantics (D28):

* ``==``/``in`` compare with :func:`values_equal` (``1 == 1.0``, ``True != 1``);
  ``None`` is an ordinary value (``== None`` matches an explicit None).
* A MISSING key (absent attribute) matches no comparison at all, ``!=``
  included; it matches only ``?N``.  ``?N`` (IS_NONE) is true for MISSING and
  None, ``?!N`` (NOT_NONE) for every other key.
* ``!=`` matches a present key that is not ``==`` to the value.
* Range operators (``<``, ``<=``, ``>``, ``>=`` and the ``between`` variants)
  only match keys of the value's type rank (``None < 5`` is false, ``"a" > 1``
  is false), ordered by :func:`order_key`.  ``between[]`` includes both bounds,
  ``between()`` excludes both, ``between(]`` excludes the lower, ``between[)``
  the upper; both bounds must share a type rank.
* Keys that cannot be ordered (lists, dicts, NaN) match only ``?!N``, ``!=``,
  ``contains``, ``?T`` and ``?!T``.
* ``contains`` (0.1 vocabulary, D36) takes one orderable operand: a str key
  matches a str operand it contains as a substring, a bytes key a bytes
  operand; a list or tuple key matches when one of its orderable items equals
  the operand (``values_equal``); a dict key matches when one of its orderable
  keys equals the operand (key membership, like Python's ``in``); a collection
  atom uses its own membership: List (and any collection with the default
  membership) by element equality (unorderable elements never match),
  Set/CountedSet by member, Dictionary/HashDictionary/RepeatedKeysDictionary
  by key; every other key (MISSING, None, scalars, other atoms) matches
  nothing.
* ``?T`` / ``?!T`` (0.1 vocabulary, D36) test the Python truthiness of a
  present key; MISSING matches neither (absence is not a value, like ``!=``).
* ``near[]`` takes a :class:`NearQuery` (or a mapping of its fields) and is
  answered by vector plans, never by the scan oracle: the result depends on the
  index's metric and on the whole collection (k nearest), so ``matches`` raises
  ``ProtoNotSupportedError``; the vector contract supplies an exact oracle.
* ``in`` over a set or frozenset stores its operands sorted by ``order_key``,
  so equal sets give equal terms; a list or tuple keeps the caller's order.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from ..errors import ProtoNotSupportedError, ProtoValidationError
from ..indexing.keyspec import KeySpec
from ..values import MISSING, canonical, is_atom, is_orderable, order_key

__all__ = ["Term", "NearQuery", "is_orderable", "OPERATORS", "NEAR_METRICS", "EQ", "NE", "LT", "LE", "GT", "GE", "IN", "BETWEEN",
           "BETWEEN_EXCL_BOTH", "BETWEEN_EXCL_LOW", "BETWEEN_EXCL_HIGH", "IS_NONE", "NOT_NONE", "NEAR", "CONTAINS",
           "IS_TRUE", "NOT_TRUE", "display_value"]

EQ = "=="
NE = "!="
LT = "<"
LE = "<="
GT = ">"
GE = ">="
IN = "in"
BETWEEN = "between[]"
BETWEEN_EXCL_BOTH = "between()"
BETWEEN_EXCL_LOW = "between(]"
BETWEEN_EXCL_HIGH = "between[)"
IS_NONE = "?N"
NOT_NONE = "?!N"
NEAR = "near[]"
CONTAINS = "contains"
IS_TRUE = "?T"
NOT_TRUE = "?!T"

_SCALAR_OPS = (EQ, NE, LT, LE, GT, GE)
#: ``op -> (include lower bound, include upper bound)``.
_BETWEEN_OPS = {BETWEEN: (True, True), BETWEEN_EXCL_BOTH: (False, False),
                BETWEEN_EXCL_LOW: (False, True), BETWEEN_EXCL_HIGH: (True, False)}
_PRESENCE_OPS = (IS_NONE, NOT_NONE)
_TRUTH_OPS = (IS_TRUE, NOT_TRUE)
#: Operators that take no value.
UNARY_OPERATORS = frozenset(_PRESENCE_OPS + _TRUTH_OPS)
#: Every operator of the term vocabulary.
OPERATORS = frozenset(_SCALAR_OPS + (IN, NEAR, CONTAINS) + tuple(_BETWEEN_OPS) + _PRESENCE_OPS + _TRUTH_OPS)

#: Metrics a ``near[]`` query may name (``None`` = the index's default metric).
NEAR_METRICS = frozenset({"cosine", "l2", "ip"})


def _finite_number(value, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)) or not math.isfinite(value):
        raise ProtoValidationError(f"{what} must be a finite number, not {value!r}")
    return float(value)


@dataclass(frozen=True)
class NearQuery:
    """Value of a ``near[]`` term: a query ``vector`` and ``k`` (nearest count) and/or a ``threshold``.

    ``vector`` is stored as a non-empty tuple of finite floats; ``k`` is an int
    >= 1 or None; ``threshold`` a finite float or None; at least one of ``k`` and
    ``threshold`` is given.  ``metric`` is ``"cosine"``, ``"l2"``, ``"ip"`` or
    None (the index's default).  Hashable, so terms holding it are hashable.
    """

    vector: tuple
    k: int | None = None
    threshold: float | None = None
    metric: str | None = None

    def __post_init__(self):
        if not isinstance(self.vector, (list, tuple)) or not self.vector:
            raise ProtoValidationError(f"near[] needs a non-empty vector, not {self.vector!r}")
        object.__setattr__(self, "vector", tuple(_finite_number(x, "A vector component") for x in self.vector))
        if self.k is None and self.threshold is None:
            raise ProtoValidationError("near[] needs k, threshold or both")
        if self.k is not None and (isinstance(self.k, bool) or not isinstance(self.k, int) or self.k < 1):
            raise ProtoValidationError(f"near[] k must be an int >= 1, not {self.k!r}")
        if self.threshold is not None:
            object.__setattr__(self, "threshold", _finite_number(self.threshold, "near[] threshold"))
        if self.metric is not None and self.metric not in NEAR_METRICS:
            raise ProtoValidationError(f"near[] metric must be one of {sorted(NEAR_METRICS)} or None, "
                                       f"not {self.metric!r}")


def _near_query(value) -> NearQuery:
    if isinstance(value, NearQuery):
        return value
    if isinstance(value, dict):
        unknown = set(value) - {"vector", "k", "threshold", "metric"}
        if unknown or "vector" not in value:
            raise ProtoValidationError(f"A near[] mapping needs 'vector' and only vector/k/threshold/metric, "
                                       f"got {sorted(map(str, value))}")
        return NearQuery(**value)
    raise ProtoValidationError(f"near[] needs a NearQuery (or a mapping of its fields), not {value!r}")


def _operand(value, op: str):
    """Validate one comparison operand: canonical, orderable, not MISSING."""
    value = canonical(value)
    if value is MISSING or not is_orderable(value):
        raise ProtoValidationError(f"Operator {op!r} needs an orderable value other than MISSING, not {value!r}")
    return value


def _rank(value) -> int:
    return order_key(value)[0]


@dataclass(frozen=True)
class Term:
    """A predicate on one key: ``path`` (str or None), ``op`` (an operator constant) and ``value``.

    Validated on construction (``ProtoValidationError``): ``in`` takes a
    collection of operands (stored as a tuple; a set sorted by ``order_key``),
    the ``between`` variants a pair ``(low, high)`` of one type rank (stored as
    a tuple), ``?N``/``?!N`` no value, the scalar comparisons one orderable
    operand, ``near[]`` a :class:`NearQuery` (a mapping of its fields is
    converted).  ``key_spec`` is the :class:`KeySpec` the path names.

    Equality and hashing are structural and follow :func:`values_equal` for the
    value (D36 fix round 1): ``Term("x", "==", 1) == Term("x", "==", 1.0)`` but
    ``Term("x", "==", True) != Term("x", "==", 1)``, recursively for ``in`` and
    ``between`` operand tuples; two terms are equal only if they match the same
    elements.
    """

    path: str | None
    op: str
    value: Any = None
    key_spec: KeySpec = field(init=False, repr=False, compare=False, default=None)

    def __post_init__(self):
        osa = object.__setattr__
        osa(self, "key_spec", KeySpec.for_term_path(self.path))
        op, value = self.op, self.value
        if op not in OPERATORS:
            raise ProtoValidationError(f"Unknown operator {op!r}; expected one of {sorted(OPERATORS)}")
        if op in _SCALAR_OPS or op == CONTAINS:
            osa(self, "value", _operand(value, op))
        elif op == IN:
            if not isinstance(value, (list, tuple, set, frozenset)):
                raise ProtoValidationError(f"Operator 'in' needs a list, tuple or set of values, not {value!r}")
            operands = tuple(_operand(v, op) for v in value)
            if isinstance(value, (set, frozenset)):
                operands = tuple(sorted(operands, key=order_key))
            osa(self, "value", operands)
        elif op in _BETWEEN_OPS:
            if not isinstance(value, (list, tuple)) or len(value) != 2:
                raise ProtoValidationError(f"Operator {op!r} needs a (low, high) pair, not {value!r}")
            low, high = _operand(value[0], op), _operand(value[1], op)
            if _rank(low) != _rank(high):
                raise ProtoValidationError(f"Operator {op!r} needs bounds of one type rank, not {low!r}, {high!r}")
            osa(self, "value", (low, high))
        elif op == NEAR:
            osa(self, "value", _near_query(value))
        elif op in UNARY_OPERATORS:
            if value is not None:
                raise ProtoValidationError(f"Operator {op!r} takes no value, got {value!r}")

    def _value_key(self):
        value = self.value
        return value if isinstance(value, NearQuery) else order_key(value)

    def __eq__(self, other):
        if type(other) is not type(self):
            return NotImplemented
        return self.path == other.path and self.op == other.op and self._value_key() == other._value_key()

    def __hash__(self):
        return hash((self.path, self.op, self._value_key()))

    def matches(self, element) -> bool:
        """Evaluate this term on ``element`` (the scan oracle; see the module docstring)."""
        op = self.op
        if op == NEAR:
            raise ProtoNotSupportedError("'near[]' terms are answered by vector plans, not by a scan")
        key = self.key_spec.extract(element)
        if op in _PRESENCE_OPS:
            return (key is MISSING or key is None) == (op == IS_NONE)
        if key is MISSING:
            return False
        if op in _TRUTH_OPS:
            return bool(key) == (op == IS_TRUE)
        if op == CONTAINS:
            return _contains(key, self.value)
        if op == NE:
            return not (is_orderable(key) and order_key(key) == order_key(self.value))
        if not is_orderable(key):
            return False
        k = order_key(key)
        if op == EQ:
            return k == order_key(self.value)
        if op == IN:
            return any(k == order_key(v) for v in self.value)
        if op in _BETWEEN_OPS:
            include_low, include_high = _BETWEEN_OPS[op]
            low, high = order_key(self.value[0]), order_key(self.value[1])
            if k[0] != low[0]:
                return False
            return (low <= k if include_low else low < k) and (k <= high if include_high else k < high)
        v = order_key(self.value)
        if k[0] != v[0]:
            return False
        if op == LT:
            return k < v
        if op == LE:
            return k <= v
        if op == GT:
            return k > v
        return k >= v  # GE

    def describe(self) -> dict:
        """``{"path", "op", "value"}`` for ``explain()``; the value is JSON-safe (:func:`display_value`)."""
        return {"path": self.path, "op": self.op, "value": display_value(self.value)}

    def to_list(self) -> list:
        """This term in the 0.1 prefix syntax accepted by ``Expression.compile``.

        ``[path, op]`` for unary operators, ``[path, op, low, high]`` for the
        between variants, ``[path, "near[]", vector, threshold]`` for a cosine
        threshold query (the 0.1 form) and ``[path, "near[]", NearQuery]``
        otherwise, ``[path, op, value]`` for the rest (an ``in`` operand list as
        a list).
        """
        op, value = self.op, self.value
        if op in UNARY_OPERATORS:
            return [self.path, op]
        if op in _BETWEEN_OPS:
            return [self.path, op, value[0], value[1]]
        if op == NEAR:
            if value.k is None and value.metric == "cosine":
                return [self.path, op, list(value.vector), value.threshold]
            return [self.path, op, value]
        if op == IN:
            return [self.path, op, list(value)]
        return [self.path, op, value]


def _contains(key, operand) -> bool:
    """``contains`` semantics (module docstring): substring for str/bytes, item equality for sequences."""
    if isinstance(key, str):
        return isinstance(operand, str) and operand in key
    if isinstance(key, bytes):
        return isinstance(operand, bytes) and operand in key
    if isinstance(key, (list, tuple)):
        target = order_key(operand)
        return any(is_orderable(item) and order_key(item) == target for item in key)
    if isinstance(key, dict):
        target = order_key(operand)
        return any(is_orderable(k) and order_key(k) == target for k in key)
    if is_atom(key):
        from ..collections.base import IndexedCollection  # deferred: collections import the query layer
        if isinstance(key, IndexedCollection):
            if type(key).__contains__ is IndexedCollection.__contains__:
                target = order_key(operand)
                return any(is_orderable(e) and order_key(e) == target for e in key.elements())
            return operand in key
    return False


def display_value(value):
    """A JSON-serialisable rendering of a term operand or key for ``explain()``.

    None, bool, int, str and finite floats are kept; tuples and lists become
    lists; a :class:`NearQuery` becomes a mapping of its fields; a dict becomes
    ``{"@dict": [[key, value], ...]}`` (keys stay distinct: ``1`` vs ``"1"``); every other
    value (Decimal, datetime, bytes, UUID, NaN, atoms, handles, ...) becomes its
    ``repr``, so distinct types stay distinguishable.
    """
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    if isinstance(value, (list, tuple)):
        return [display_value(v) for v in value]
    if isinstance(value, NearQuery):
        return {"vector": list(value.vector), "k": value.k, "threshold": value.threshold, "metric": value.metric}
    if isinstance(value, dict):
        # A JSON object would need str keys, and 1 and "1" would collide: keep pairs.
        return {"@dict": [[display_value(k), display_value(v)] for k, v in value.items()]}
    return repr(value)
