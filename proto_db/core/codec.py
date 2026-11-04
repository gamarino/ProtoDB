"""Atom state <-> msgpack-ready dict, with explicit type tags (spec section 5, D3, D7).

Wire form of a value:

* ``None``, ``bool``, ``float``, ``str``, ``bytes`` and ``int`` within the msgpack
  range (``-2**63 .. 2**64-1``) are written natively.  Strings are inline (D7).
* ``list`` is a msgpack array.
* Every other value is a single-key map whose key is a tag:

  ===========  ==================================================================
  ``@int``     ``format(n, "x")`` (signed hexadecimal) for integers outside the
               msgpack range; base 16 is not subject to the int/str digit limit
  ``@dec``     ``str(d)`` (keeps sign, exponent, NaN and infinities exactly)
  ``@date``    ``[year, month, day]``
  ``@time``    ``[hour, minute, second, microsecond, fold, tz]``
  ``@dt``      ``[year, month, day, hour, minute, second, microsecond, fold, tz]``
  ``@td``      ``[days, seconds, microseconds]``
  ``@uuid``    the 16 UUID bytes
  ``@tup``     array of encoded items
  ``@map``     array of ``[key, value]`` pairs in insertion order (keys may be any
               canonical value, so a msgpack map is never used for a dict)
  ``@missing`` the ``MISSING`` sentinel (value ``0``)
  ``@ref``     ``[segment, offset, type]`` for a nested atom: the 16 segment UUID bytes
               and the type tag of :func:`proto_db.core.atom.type_tag` (an alias for a
               built-in type, else the name).  Format version 2 wrote the segment as 32
               hex digits and the type as its name; both forms are still read (D51).
  ``@mut``     the cell name of a ``MutableObject`` handle (a non-empty str, D35)
  ===========  ==================================================================

  ``tz`` is ``None`` (naive), ``["zone", key]`` for a ``zoneinfo.ZoneInfo`` or
  ``["fixed", offset_microseconds, name]`` for a ``datetime.timezone``.  Other
  ``tzinfo`` implementations are rejected with ``ProtoValidationError``.

Decoding returns ``FrozenList``/``FrozenDict`` for arrays and ``@map`` (D25); encoding accepts
both frozen and plain containers.

Decoding validates the structure: an unknown tag or a malformed value raises
``ProtoCorruptionError``.
"""
from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..errors import ProtoCorruptionError, ProtoValidationError
from ..values import MISSING, FrozenDict, FrozenList
from .atom import Atom, AtomPointer, type_name_of, type_tag
from .dbobject import MutableObject

__all__ = ["encode_state", "decode_state", "encode_value", "decode_value", "redecode_value"]

_INT_MIN = -2**63
_INT_MAX = 2**64 - 1
_MICROSECOND = timedelta(microseconds=1)


# -- encoding ---------------------------------------------------------------------------

def encode_state(state: dict, save_child: Callable[[Atom], AtomPointer]) -> dict:
    """Encode an atom state (attribute name -> canonical value) into msgpack-ready form.

    ``save_child`` is called for every nested atom and must return its pointer.
    Raises ``ProtoValidationError`` for a non-``str`` attribute name or an
    unsupported value.
    """
    out = {}
    for name, value in state.items():
        if type(name) is not str:
            raise ProtoValidationError(f"Attribute names must be str, not {type(name).__name__}")
        out[name] = encode_value(value, save_child)
    return out


#: Classes the isinstance chain of :func:`_encode_other` would encode as an atom reference: an
#: Atom subclass that is no subclass of a type tested before ``Atom`` (Task 8.3i).  An atom is the
#: most frequent non-scalar value of a tree node and was tested last.
_ATOM_CLASSES: set = set()
_BEFORE_ATOM = (str, int, float, bytes, bytearray, list, tuple, dict, Decimal, datetime, date, time,
                timedelta, uuid.UUID, MutableObject)


def encode_value(value, save_child: Callable[[Atom], AtomPointer]):
    """Encode one canonical value (see the module docstring for the wire form).

    Values of the exact built-in types, and atoms, take fast paths that give the same result as
    the full isinstance chain (:func:`_encode_other`), which every other value goes through."""
    t = type(value)
    if t is int:
        return value if _INT_MIN <= value <= _INT_MAX else {"@int": format(value, "x")}
    if t is str or t is bytes or t is float or value is None or t is bool:
        return value
    if t is tuple:
        return {"@tup": [encode_value(v, save_child) for v in value]}
    if t in _ATOM_CLASSES:
        return _encode_ref(value, save_child)
    if isinstance(value, Atom) and not issubclass(t, _BEFORE_ATOM) and value is not MISSING:
        _ATOM_CLASSES.add(t)
        return _encode_ref(value, save_child)
    return _encode_other(value, save_child)


def redecode_value(value, load_ref: Callable[[AtomPointer, str], Atom], inline: Optional[list] = None):
    """``decode_value(encode_value(value, ...), load_ref)`` for a value whose nested atoms are all
    saved, without building the wire form (Task 8.3i: an atom just saved is cached decoded).

    Same result, type for type: the exact built-in scalars come back as themselves (decoding
    their wire form gives an equal object of the same type), tuples as tuples, lists as
    ``FrozenList`` and dicts as ``FrozenDict`` of redecoded items, an atom as
    ``load_ref(its pointer, its type name)``; any other value goes through the codec itself.
    ``inline``, a one-item list, accumulates the length of every ``str`` and ``bytes`` value met
    (nested atoms excluded): what the decoded value holds inline (review 8.3i I1)."""
    t = type(value)
    if t is str or t is bytes:
        if inline is not None:
            inline[0] += len(value)
        return value
    if t is int or t is float or value is None or t is bool:
        return value
    if t is tuple:
        return tuple([redecode_value(v, load_ref, inline) for v in value])
    if t in _ATOM_CLASSES:
        return load_ref(_saved_pointer(value), type(value).__type_name__)
    if t is list or t is FrozenList:
        return FrozenList([redecode_value(v, load_ref, inline) for v in value])
    if t is dict or t is FrozenDict:
        out = {}
        for k, v in value.items():
            out[redecode_value(k, load_ref, inline)] = redecode_value(v, load_ref, inline)
        return FrozenDict(out)
    if inline is not None and isinstance(value, (str, bytes, bytearray)):
        inline[0] += len(value)
    return decode_value(encode_value(value, _saved_pointer), load_ref)


def _saved_pointer(atom: Atom) -> AtomPointer:
    ptr = atom.atom_pointer
    if ptr is None:
        raise ProtoValidationError(f"{atom!r} is not saved: it has no pointer to refer to")
    return ptr


def _encode_ref(value, save_child):
    ptr = save_child(value)
    if not isinstance(ptr, AtomPointer):
        raise ProtoValidationError(f"save_child returned {type(ptr).__name__}, expected AtomPointer")
    return {"@ref": [ptr.segment.bytes, ptr.offset, type_tag(type(value).__type_name__)]}


def _encode_other(value, save_child: Callable[[Atom], AtomPointer]):
    """The full isinstance chain of :func:`encode_value`."""
    if value is None or value is True or value is False:
        return value
    if isinstance(value, str):
        return str(value)
    if isinstance(value, int):
        value = int(value)
        return value if _INT_MIN <= value <= _INT_MAX else {"@int": format(value, "x")}
    if isinstance(value, float):
        return float(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, list):
        return [encode_value(v, save_child) for v in value]
    if isinstance(value, tuple):
        return {"@tup": [encode_value(v, save_child) for v in value]}
    if isinstance(value, dict):
        return {"@map": [[encode_value(k, save_child), encode_value(v, save_child)]
                         for k, v in value.items()]}
    if isinstance(value, Decimal):
        return {"@dec": str(value)}
    if isinstance(value, datetime):
        return {"@dt": [value.year, value.month, value.day, value.hour, value.minute,
                        value.second, value.microsecond, value.fold, _encode_tz(value.tzinfo)]}
    if isinstance(value, date):
        return {"@date": [value.year, value.month, value.day]}
    if isinstance(value, time):
        return {"@time": [value.hour, value.minute, value.second, value.microsecond,
                          value.fold, _encode_tz(value.tzinfo)]}
    if isinstance(value, timedelta):
        return {"@td": [value.days, value.seconds, value.microseconds]}
    if isinstance(value, uuid.UUID):
        return {"@uuid": value.bytes}
    if value is MISSING:
        return {"@missing": 0}
    if isinstance(value, MutableObject):
        return {"@mut": value.name}
    if isinstance(value, Atom):
        return _encode_ref(value, save_child)
    raise ProtoValidationError(f"Unsupported value type: {type(value).__name__}")


def _encode_tz(tz):
    if tz is None:
        return None
    if isinstance(tz, ZoneInfo):
        if tz.key is None:
            raise ProtoValidationError("A ZoneInfo without a key cannot be stored")
        return ["zone", tz.key]
    if isinstance(tz, timezone):
        return ["fixed", tz.utcoffset(None) // _MICROSECOND, tz.tzname(None)]
    raise ProtoValidationError(f"Unsupported tzinfo type: {type(tz).__name__}")


# -- decoding ---------------------------------------------------------------------------

def decode_state(data: dict, load_ref: Callable[[AtomPointer, str], Atom]) -> dict:
    """Decode the output of :func:`encode_state`.

    ``load_ref(ptr, type_name)`` is called for every nested atom reference.
    Raises ``ProtoCorruptionError`` for malformed data or an unknown tag.
    """
    if not isinstance(data, dict):
        raise ProtoCorruptionError(f"Encoded state must be a map, not {type(data).__name__}")
    out = {}
    for name, value in data.items():
        if type(name) is not str:
            raise ProtoCorruptionError(f"Encoded attribute name must be str, not {type(name).__name__}")
        out[name] = decode_value(value, load_ref)
    return out


def decode_value(data, load_ref: Callable[[AtomPointer, str], Atom]):
    """Decode one value produced by :func:`encode_value`."""
    if data is None or type(data) in (bool, int, float, str, bytes):
        return data
    if type(data) is list:
        return FrozenList(decode_value(v, load_ref) for v in data)
    if type(data) is not dict or len(data) != 1:
        raise ProtoCorruptionError(f"Malformed encoded value: {_preview(data)}")
    (tag, body), = data.items()
    decoder = _DECODERS.get(tag)
    if decoder is None:
        raise ProtoCorruptionError(f"Unknown codec tag {_preview(tag)}")
    try:
        return decoder(body, load_ref)
    except (TypeError, ValueError, KeyError, IndexError, OverflowError, InvalidOperation,
            ZoneInfoNotFoundError) as e:
        raise ProtoCorruptionError(f"Malformed {tag} value: {_preview(body)}") from e


def _preview(x):
    text = repr(x)
    return text if len(text) <= 80 else text[:77] + "..."


def _fields(body, n, types):
    if type(body) is not list or len(body) != n:
        raise ValueError(f"expected an array of {n} items")
    for item, t in zip(body, types):
        if t is not None and type(item) is not t:
            raise TypeError(f"expected {t.__name__}, got {type(item).__name__}")
    return body


def _decode_tz(tz):
    if tz is None:
        return None
    if type(tz) is list and len(tz) == 2 and tz[0] == "zone" and type(tz[1]) is str:
        return ZoneInfo(tz[1])
    if type(tz) is list and len(tz) == 3 and tz[0] == "fixed" and type(tz[1]) is int \
            and type(tz[2]) is str:
        result = timezone(timedelta(microseconds=tz[1]))
        return result if result.tzname(None) == tz[2] else timezone(result.utcoffset(None), tz[2])
    raise ValueError("malformed tzinfo")


def _dec_int(body, _):
    if type(body) is not str:
        raise TypeError("expected str")
    return int(body, 16)


def _dec_decimal(body, _):
    if type(body) is not str:
        raise TypeError("expected str")
    return Decimal(body)


def _dec_date(body, _):
    return date(*_fields(body, 3, (int, int, int)))


def _dec_time(body, _):
    h, mi, s, us, fold, tz = _fields(body, 6, (int, int, int, int, int, None))
    return time(h, mi, s, us, tzinfo=_decode_tz(tz), fold=fold)


def _dec_datetime(body, _):
    y, mo, d, h, mi, s, us, fold, tz = _fields(body, 9, (int,) * 8 + (None,))
    return datetime(y, mo, d, h, mi, s, us, tzinfo=_decode_tz(tz), fold=fold)


def _dec_timedelta(body, _):
    return timedelta(*_fields(body, 3, (int, int, int)))


def _dec_uuid(body, _):
    if type(body) is not bytes:
        raise TypeError("expected bytes")
    return uuid.UUID(bytes=body)


def _dec_tuple(body, load_ref):
    if type(body) is not list:
        raise TypeError("expected an array")
    return tuple(decode_value(v, load_ref) for v in body)


def _dec_map(body, load_ref):
    if type(body) is not list:
        raise TypeError("expected an array")
    out = {}
    for pair in body:
        k, v = _fields(pair, 2, (None, None))
        out[decode_value(k, load_ref)] = decode_value(v, load_ref)
    return FrozenDict(out)


def _dec_missing(body, _):
    if body != 0 or type(body) is not int:
        raise ValueError("expected 0")
    return MISSING


def _dec_ref(body, load_ref):
    segment, offset, tag = _fields(body, 3, (None, int, None))
    if type(segment) is bytes and len(segment) == 16:
        segment = uuid.UUID(bytes=segment)
    elif type(segment) is str:                       # format version 2
        segment = uuid.UUID(hex=segment)
    else:
        raise TypeError("expected 16 segment bytes")
    try:
        ptr = AtomPointer(segment, offset)
    except ProtoValidationError as e:
        raise ValueError(str(e)) from e
    try:
        type_name = type_name_of(tag)
    except ProtoCorruptionError as e:
        raise ValueError(str(e)) from e
    return load_ref(ptr, type_name)


def _dec_mutable(body, _):
    if type(body) is not str or not body:
        raise ValueError("expected a non-empty str")
    return MutableObject(body)


_DECODERS = {
    "@int": _dec_int, "@dec": _dec_decimal, "@date": _dec_date, "@time": _dec_time,
    "@dt": _dec_datetime, "@td": _dec_timedelta, "@uuid": _dec_uuid, "@tup": _dec_tuple,
    "@map": _dec_map, "@missing": _dec_missing, "@ref": _dec_ref,
    "@mut": _dec_mutable,
}
