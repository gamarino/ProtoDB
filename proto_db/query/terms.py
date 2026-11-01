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
* Keys that cannot be ordered (lists, dicts, NaN) match only ``?!N`` and ``!=``.
* ``near[]`` takes a :class:`NearQuery` (or a mapping of its fields) and is
  answered by vector plans, never by the scan oracle: the result depends on the
  index's metric and on the whole collection (k nearest), so ``matches`` raises
  ``ProtoNotSupportedError``; the vector contract supplies an exact oracle.
* ``in`` over a set or frozenset stores its operands sorted by ``order_key``,
  so equal sets give equal terms; a list or tuple keeps the caller's order.
"""
from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from datetime import date, time, timedelta
from decimal import Decimal
from typing import Any

from ..errors import ProtoNotSupportedError, ProtoValidationError
from ..indexing.keyspec import KeySpec
from ..values import MISSING, canonical, is_atom, order_key

__all__ = ["Term", "NearQuery", "is_orderable", "OPERATORS", "NEAR_METRICS", "EQ", "NE", "LT", "LE", "GT", "GE", "IN", "BETWEEN",
           "BETWEEN_EXCL_BOTH", "BETWEEN_EXCL_LOW", "BETWEEN_EXCL_HIGH", "IS_NONE", "NOT_NONE", "NEAR"]

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

_SCALAR_OPS = (EQ, NE, LT, LE, GT, GE)
#: ``op -> (include lower bound, include upper bound)``.
_BETWEEN_OPS = {BETWEEN: (True, True), BETWEEN_EXCL_BOTH: (False, False),
                BETWEEN_EXCL_LOW: (False, True), BETWEEN_EXCL_HIGH: (True, False)}
_PRESENCE_OPS = (IS_NONE, NOT_NONE)
#: Every operator of the term vocabulary.
OPERATORS = frozenset(_SCALAR_OPS + (IN, NEAR) + tuple(_BETWEEN_OPS) + _PRESENCE_OPS)

_ORDERABLE_TYPES = (bool, int, str, bytes, bytearray, date, time, timedelta, uuid.UUID)


def is_orderable(value) -> bool:
    """True iff :func:`order_key` accepts ``value`` (no NaN, list or dict anywhere; a supported type)."""
    if value is None or value is MISSING or isinstance(value, _ORDERABLE_TYPES):
        return True
    if isinstance(value, float):
        return value == value
    if isinstance(value, Decimal):
        return not value.is_nan()
    if isinstance(value, tuple):
        return all(is_orderable(v) for v in value)
    if isinstance(value, (list, dict)):
        return False
    return is_atom(value)


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
    converted).  Terms are hashable.  ``key_spec`` is the :class:`KeySpec` the
    path names.
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
        if op in _SCALAR_OPS:
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
        elif op in _PRESENCE_OPS:
            if value is not None:
                raise ProtoValidationError(f"Operator {op!r} takes no value, got {value!r}")

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
        """``{"path", "op", "value"}`` for ``explain()``."""
        return {"path": self.path, "op": self.op, "value": self.value}
