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

import hashlib
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


#: Modulus of the exact numeric hash (a Mersenne prime, as in CPython's numeric hash).
_HASH_MODULUS = (1 << 61) - 1
_HASH_INV10 = pow(10, _HASH_MODULUS - 2, _HASH_MODULUS)


def _number_residue(x) -> bytes:
    """Encode the exact value of a non-NaN number for hashing: equal numbers give equal bytes.

    Finite numbers map to their rational value modulo the prime ``_HASH_MODULUS``
    (``m * n**-1 mod P`` for ``m / n``), computed without expanding huge
    exponents; infinities get their own tags.
    """
    p = _HASH_MODULUS
    if isinstance(x, Decimal):
        if x.is_infinite():
            return b"+inf" if x > 0 else b"-inf"
        sign, digits, exp = x.as_tuple()
        m = 0
        for d in digits:
            m = (m * 10 + d) % p
        m = m * (pow(10, exp, p) if exp >= 0 else pow(_HASH_INV10, -exp, p)) % p
        residue = (p - m) % p if sign else m
    elif isinstance(x, float):
        if x in (float("inf"), float("-inf")):
            return b"+inf" if x > 0 else b"-inf"
        n, d = x.as_integer_ratio()
        residue = n * pow(d, p - 2, p) % p
    else:
        residue = x % p
    return residue.to_bytes(8, "little")


def _feed_hash(h, key: tuple) -> None:
    """Feed an order key into the hash object ``h`` with an unambiguous, process-independent encoding."""
    rank, payload = key
    h.update(rank.to_bytes(1, "little"))
    if rank == _R_NUMBER:
        h.update(_number_residue(payload))
        return
    if rank == _R_TUPLE:
        h.update(len(payload).to_bytes(8, "little"))
        for item in payload:
            _feed_hash(h, item)
        return
    if isinstance(payload, str):
        data = payload.encode("utf-8", "surrogatepass")
    elif isinstance(payload, bytes):
        data = payload
    else:
        if isinstance(payload, datetime):  # naive datetime
            payload = (payload - _NAIVE_ORIGIN) // _MICROSECOND
        elif isinstance(payload, date):
            payload = payload.toordinal()
        elif isinstance(payload, timedelta):
            payload = payload // _MICROSECOND
        if type(payload) is not int:
            raise ProtoValidationError(f"Cannot hash order-key payload of type {type(payload).__name__}")
        data = payload.to_bytes(payload.bit_length() // 8 + 1, "little", signed=True)
    h.update(len(data).to_bytes(8, "little"))
    h.update(data)


def hash_key(value) -> int:
    """Return a signed 64-bit hash consistent with :func:`values_equal`, identical in every process.

    Python's ``hash`` of ``str``, ``bytes`` and date/time objects is salted per
    process (``PYTHONHASHSEED``), so it cannot order persisted structures.  This
    hash is a BLAKE2b digest of an exact encoding of :func:`order_key` (numbers
    by their exact rational value), stable across processes, platforms and
    releases (D24).  Raises ``ProtoValidationError`` for unorderable values.
    """
    h = hashlib.blake2b(digest_size=8)
    _feed_hash(h, order_key(value))
    return int.from_bytes(h.digest(), "little", signed=True)
