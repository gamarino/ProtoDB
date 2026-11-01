"""ProtoDB value model.

Single source of truth for what a storable value is and how values order,
compare and hash.  Collections, indexes and the query evaluator all rely on
:func:`order_key`, :func:`values_equal` and :func:`hash_key`.

Rank order (see decision D17)::

    MISSING < None < bool < number < str < bytes < date < time-naive
    < time-aware < datetime-naive < datetime-aware < timedelta < UUID
    < tuple < atom < mutable handle

The last rank holds :class:`proto_db.core.dbobject.MutableObject` handles,
ordered by name (D35); it was appended after ``atom`` so that the order keys
and hash keys of every other value are unchanged.

Numbers (``int``, ``float``, ``Decimal``) share one rank and are compared
exactly by Python's native mixed comparison; they are never converted to
``float``.  ``list`` and ``dict`` are storable (see :func:`canonical`) but are
not orderable keys.
"""
from __future__ import annotations

import copy
import hashlib
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from .errors import ProtoValidationError

__all__ = ["MISSING", "FrozenList", "FrozenDict", "canonical", "order_key", "rank_bounds", "values_equal", "hash_key",
           "is_atom", "exact_token"]


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
 _R_UUID, _R_TUPLE, _R_ATOM, _R_MUTABLE) = range(16)


_MICROSECOND = timedelta(microseconds=1)
_NAIVE_ORIGIN = datetime(1, 1, 1)


def is_atom(value) -> bool:
    """Return True if ``value`` looks like an atom: it has a ``uuid.UUID`` ``oid``."""
    return isinstance(getattr(value, "oid", None), uuid.UUID)


def _mutable_type() -> type:
    """The MutableObject handle class (imported lazily: ``core.dbobject`` imports this module)."""
    from .core.dbobject import MutableObject
    return MutableObject


def _is_nan(value) -> bool:
    if isinstance(value, Decimal):
        return value.is_nan()
    return isinstance(value, float) and value != value


def _is_aware(value) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None


def _frozen(kind: str):
    """Build a mutator that always refuses, pointing the user at ``.copy()``."""
    def refuse(self, *args, **kwargs):
        raise ProtoValidationError(
            f"Stored {kind} values are immutable; call .copy() to get a mutable copy")
    return refuse


class FrozenList(list):
    """Immutable ``list`` used for every list value stored in the database (D25).

    Reads behave like ``list`` and ``FrozenList([1]) == [1]``.  Every mutator
    raises ``ProtoValidationError``.  Like ``list`` it is unhashable.
    :meth:`copy` returns a plain mutable ``list`` (shallow: nested values stay
    frozen).  ``copy.copy``/``copy.deepcopy`` and pickling give an equal
    ``FrozenList``.
    """

    __slots__ = ()

    def __new__(cls, iterable=()):
        self = list.__new__(cls)
        list.extend(self, iterable)
        return self

    def __init__(self, iterable=()):
        """No-op: elements are set in ``__new__``, so ``__init__`` can never mutate."""

    def copy(self) -> list:
        """Return a plain, mutable, shallow copy."""
        return list(self)

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return FrozenList(copy.deepcopy(v, memo) for v in self)

    def __reduce__(self):
        return (FrozenList, (list(self),))

    append = extend = insert = pop = remove = clear = sort = reverse = _frozen("list")
    __setitem__ = __delitem__ = __iadd__ = __imul__ = _frozen("list")


class FrozenDict(dict):
    """Immutable ``dict`` used for every dict value stored in the database (D25).

    Same contract as :class:`FrozenList`: equal to the plain ``dict``, every
    mutator raises ``ProtoValidationError``, unhashable, :meth:`copy` returns
    a plain shallow ``dict``, deepcopy and pickle give an equal ``FrozenDict``.
    """

    __slots__ = ()

    def __new__(cls, *args, **kwargs):
        self = dict.__new__(cls)
        dict.update(self, *args, **kwargs)
        return self

    def __init__(self, *args, **kwargs):
        """No-op: items are set in ``__new__``, so ``__init__`` can never mutate."""

    def copy(self) -> dict:
        """Return a plain, mutable, shallow copy."""
        return dict(self)

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return FrozenDict((copy.deepcopy(k, memo), copy.deepcopy(v, memo)) for k, v in self.items())

    def __reduce__(self):
        return (FrozenDict, (dict(self),))

    update = setdefault = popitem = pop = clear = __setitem__ = __delitem__ = __ior__ = _frozen("dict")


def canonical(value):
    """Validate ``value`` and return its canonical form.

    Supported: MISSING, None, bool, int, float (NaN allowed as a stored value),
    Decimal, str, bytes (``bytearray`` is converted), date, time, datetime,
    timedelta, UUID, atoms, ``MutableObject`` handles, tuples, and lists/dicts of supported values
    (processed recursively).  Lists and dicts become :class:`FrozenList` and
    :class:`FrozenDict`; already-canonical input is returned as the same object.  Raises ``ProtoValidationError`` otherwise.
    """
    if value is None or value is MISSING or isinstance(
            value, (bool, int, float, Decimal, str, bytes, date, time, timedelta, uuid.UUID)):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, tuple):
        items = tuple(canonical(v) for v in value)
        return value if type(value) is tuple and all(a is b for a, b in zip(items, value)) else items
    if isinstance(value, list):
        items = [canonical(v) for v in value]
        if type(value) is FrozenList and all(a is b for a, b in zip(items, value)):
            return value
        return FrozenList(items)
    if isinstance(value, dict):
        pairs = [(canonical(k), canonical(v)) for k, v in value.items()]
        if type(value) is FrozenDict and all(
                ka is kb and va is vb for (ka, va), (kb, vb) in zip(pairs, value.items())):
            return value
        return FrozenDict(pairs)
    if is_atom(value) or isinstance(value, _mutable_type()):
        return value
    raise ProtoValidationError(f"Unsupported value type: {type(value).__name__}")


def order_key(value) -> tuple:
    """Return ``(rank, payload)``, a key that totally orders orderable values.

    Invariant (relied upon by :func:`rank_bounds` and by index range scans):
    every order key is a 2-element tuple ``(rank, payload)`` whose ``rank`` is
    an int in ``range(16)``; payloads are compared only between keys of one rank.

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
    if isinstance(value, _mutable_type()):
        return (_R_MUTABLE, value.name)
    if isinstance(value, (list, dict)):
        raise ProtoValidationError(f"{type(value).__name__} is not an orderable key")
    raise ProtoValidationError(f"Unsupported value type: {type(value).__name__}")


def rank_bounds(value) -> tuple:
    """Return ``(low, high)``: order keys bracketing every value of ``value``'s type rank (D30).

    For every orderable ``x``, ``low < order_key(x) < high`` holds exactly when
    ``x`` has the same rank as ``value`` (``low`` and ``high`` are never the
    order key of any value).  Range scans use them to stay within the operand's
    rank, since ordering comparisons match same-rank keys only (D28).  Relies on
    the :func:`order_key` invariant that every order key is a 2-tuple
    ``(rank, payload)``: the 1-tuple ``(rank,)`` sorts before every
    ``(rank, payload)`` and ``(rank + 1,)`` after it.  Raises
    ``ProtoValidationError`` for unorderable values, like :func:`order_key`.
    """
    rank = order_key(value)[0]
    return (rank,), (rank + 1,)


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


# -- exact tokens (D30, D34) -------------------------------------------------------------------

_EXACT_TAGS = {type(None): "N", bool: "b", int: "i", float: "f", Decimal: "D", str: "s", bytes: "y",
               date: "d", time: "t", datetime: "dt", timedelta: "td", uuid.UUID: "u", tuple: "T", list: "L",
               FrozenList: "FL", dict: "M", FrozenDict: "FM"}


def _tz_token(value):
    tz = value.tzinfo
    if tz is None:
        return None
    offset = value.utcoffset()
    return (None if offset is None else offset // timedelta(microseconds=1), repr(tz))


def exact_token(value, atom_token=None) -> tuple:
    """An orderable token that tells apart values a scan would return as different objects (D30).

    ``1``, ``1.0``, ``Decimal("1")`` and ``Decimal("1.00")`` are equal keys but
    four tokens; so are ``0.0``/``-0.0`` and aware datetimes of one instant in
    different time zones (offset and ``repr`` of the tzinfo are kept).  Tuples,
    lists and dicts are encoded item by item, dicts in their own order.  Atoms
    are identified elsewhere by default, so their token is ``("a",)``;
    ``atom_token(atom)``, when given, supplies the token of every atom, nested
    ones included (the transaction read set passes the storage pointer).  The token
    starts with a type tag (the qualified class name for subclasses), so values
    of different classes never share a token.  Best effort for exotic tzinfo
    classes whose ``repr`` is not stable: two tokens for one value only make
    the index store two entries, never a wrong answer (removal falls back to an
    equal entry).
    """
    if is_atom(value):
        return ("a",) if atom_token is None else atom_token(value)
    if isinstance(value, _mutable_type()):
        return ("@mut", value.name)
    cls = type(value)
    tag = _EXACT_TAGS.get(cls) or f"{cls.__module__}.{cls.__qualname__}"
    if value is None:
        return (tag,)
    if isinstance(value, bool):
        return (tag, int(value))
    if isinstance(value, int):
        return (tag, int(value))
    if isinstance(value, float):
        return (tag, float.hex(value))
    if isinstance(value, Decimal):
        return (tag, str(value))
    if isinstance(value, (str, bytes)):
        return (tag, value)
    if isinstance(value, datetime):
        return (tag, value.replace(tzinfo=None).isoformat(), value.fold, _tz_token(value))
    if isinstance(value, date):
        return (tag, value.toordinal())
    if isinstance(value, time):
        return (tag, value.replace(tzinfo=None).isoformat(), value.fold, _tz_token(value))
    if isinstance(value, timedelta):
        return (tag, value.days, value.seconds, value.microseconds)
    if isinstance(value, uuid.UUID):
        return (tag, value.int)
    if isinstance(value, (tuple, list)):
        return (tag, tuple(exact_token(v, atom_token) for v in value))
    if isinstance(value, dict):
        return (tag, tuple((exact_token(k, atom_token), exact_token(v, atom_token)) for k, v in value.items()))
    return (tag, repr(value))
