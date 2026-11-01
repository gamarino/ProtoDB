"""ProtoDB value model.

Single source of truth for what a storable value is and how values order,
compare and hash.  Collections, indexes and the query evaluator all rely on
:func:`order_key`, :func:`values_equal` and :func:`hash_key`.

Rank order (see decision D17)::

    MISSING < None < bool < number < str < bytes < date < time-naive
    < time-aware < datetime-naive < datetime-aware < timedelta < UUID
    < tuple < atom

Numbers (``int``, ``float``, ``Decimal``) share one rank and are compared
exactly by Python's native mixed comparison; they are never converted to
``float``.  ``list`` and ``dict`` are storable (see :func:`canonical`) but are
not orderable keys.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from .errors import ProtoValidationError

__all__ = ["MISSING", "canonical", "order_key", "values_equal", "hash_key", "is_atom"]


class _Missing:
    """Type of the MISSING sentinel: an absent value, distinct from None."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "MISSING"

    def __reduce__(self):
        return "MISSING"

    def __bool__(self):
        return False


MISSING = _Missing()

(_R_MISSING, _R_NONE, _R_BOOL, _R_NUMBER, _R_STR, _R_BYTES, _R_DATE,
 _R_TIME_NAIVE, _R_TIME_AWARE, _R_DT_NAIVE, _R_DT_AWARE, _R_TIMEDELTA,
 _R_UUID, _R_TUPLE, _R_ATOM) = range(15)


_MICROSECOND = timedelta(microseconds=1)
_NAIVE_ORIGIN = datetime(1, 1, 1)


def is_atom(value) -> bool:
    """Return True if ``value`` looks like an atom: it has a ``uuid.UUID`` ``oid``."""
    return isinstance(getattr(value, "oid", None), uuid.UUID)


def _is_nan(value) -> bool:
    if isinstance(value, Decimal):
        return value.is_nan()
    return isinstance(value, float) and value != value


def _is_aware(value) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None


def canonical(value):
    """Validate ``value`` and return its canonical form.

    Supported: MISSING, None, bool, int, float (NaN allowed as a stored value),
    Decimal, str, bytes (``bytearray`` is converted), date, time, datetime,
    timedelta, UUID, atoms, tuples, and lists/dicts of supported values
    (processed recursively).  Raises ``ProtoValidationError`` otherwise.
    """
    if value is None or value is MISSING or isinstance(
            value, (bool, int, float, Decimal, str, bytes, date, time, timedelta, uuid.UUID)):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, tuple):
        return tuple(canonical(v) for v in value)
    if isinstance(value, list):
        return [canonical(v) for v in value]
    if isinstance(value, dict):
        return {canonical(k): canonical(v) for k, v in value.items()}
    if is_atom(value):
        return value
    raise ProtoValidationError(f"Unsupported value type: {type(value).__name__}")


def order_key(value) -> tuple:
    """Return ``(rank, payload)``, a key that totally orders orderable values.

    Raises ``ProtoValidationError`` for NaN, lists, dicts and unsupported types.
    """
    if value is MISSING:
        return (_R_MISSING, 0)
    if value is None:
        return (_R_NONE, 0)
    if isinstance(value, bool):
        return (_R_BOOL, int(value))
    if isinstance(value, (int, float, Decimal)):
        if _is_nan(value):
            raise ProtoValidationError("NaN cannot be used as a key")
        return (_R_NUMBER, value)
    if isinstance(value, str):
        return (_R_STR, value)
    if isinstance(value, (bytes, bytearray)):
        return (_R_BYTES, bytes(value))
    if isinstance(value, datetime):
        if _is_aware(value):
            # UTC microseconds from the fold-resolved offset; never compare the raw
            # aware datetime, whose comparison is fold-sensitive (PEP 495).
            local = (value.replace(tzinfo=None) - _NAIVE_ORIGIN) // _MICROSECOND
            return (_R_DT_AWARE, local - value.utcoffset() // _MICROSECOND)
        return (_R_DT_NAIVE, value)
    if isinstance(value, date):
        return (_R_DATE, value)
    if isinstance(value, time):
        micros = ((value.hour * 60 + value.minute) * 60 + value.second) * 1_000_000 + value.microsecond
        if _is_aware(value):
            off = value.utcoffset()
            return (_R_TIME_AWARE, micros - (off // timedelta(microseconds=1)))
        return (_R_TIME_NAIVE, micros)
    if isinstance(value, timedelta):
        return (_R_TIMEDELTA, value)
    if isinstance(value, uuid.UUID):
        return (_R_UUID, value.int)
    if isinstance(value, tuple):
        return (_R_TUPLE, tuple(order_key(v) for v in value))
    if is_atom(value):
        return (_R_ATOM, value.oid.int)
    if isinstance(value, (list, dict)):
        raise ProtoValidationError(f"{type(value).__name__} is not an orderable key")
    raise ProtoValidationError(f"Unsupported value type: {type(value).__name__}")


def values_equal(a, b) -> bool:
    """Return True iff ``order_key(a) == order_key(b)``."""
    return order_key(a) == order_key(b)


def hash_key(value) -> int:
    """Return a hash consistent with :func:`values_equal`."""
    return hash(order_key(value))
