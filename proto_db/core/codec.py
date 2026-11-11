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

  ``tz`` is ``None`` (naive), ``["fixed", offset_microseconds, name]`` for a
  ``datetime.timezone``, and for a ``zoneinfo.ZoneInfo``: ``["zone", key]`` in a ``@time``
  (the wall clock; a time has no offset) and ``["instant", key]`` in a ``@dt``, whose fields
  are then the UTC instant with fold 0 (D58: the instant is fixed at write time, so its order
  and hash never depend on the reader's time-zone database).  ``["zone", key]`` in a ``@dt``
  (format 3: the wall clock, resolved with the reader's database) is still read.  Other
  ``tzinfo`` implementations are rejected with ``ProtoValidationError``.

Every text form has one spelling (D58): ``@int`` and ``@dec`` bodies must be the exact text
the encoder writes, ``@td`` must be normalized, and the keys of a ``@map`` must not conflict
(:func:`proto_db.values.map_key_ids`); otherwise decoding raises ``ProtoCorruptionError``, so a
decoded value re-encodes to the bytes it was read from.

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
from ..values import _EXACT_SCALARS as _ENCODED_AS_IS
from ..values import MISSING, FrozenDict, FrozenList, TokenAtoms, canonical, check_map_keys, check_text, plain_scalar
from .atom import Atom, AtomPointer, type_name_of, type_tag
from .dbobject import MutableObject

__all__ = ["encode_state", "decode_state", "encode_value", "decode_value", "redecode_value"]

_PLAIN_KEY_TYPES = frozenset({str, int, bytes})
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
        try:
            out[name] = encode_value(value, save_child)
        except ProtoValidationError as e:
            if type(e) is not ProtoValidationError:
                raise
            raise ProtoValidationError(f"Attribute {name!r}: {e}") from e
        if not name.isascii():
            check_text(name)
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
    if t is str:
        return value if value.isascii() else check_text(value)     # no UTF-8 for surrogates (R128)
    if t is bytes or t is float or value is None or t is bool:
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


#: What an atom encodes to inside an exact token (D58): not the encoding of any canonical value.
_TOKEN_REF = {"@ref": None}


def _encode_ref(value, save_child):
    if type(save_child) is TokenAtoms:
        save_child.atoms.append(value)
        return _TOKEN_REF
    ptr = save_child(value)
    if not isinstance(ptr, AtomPointer):
        raise ProtoValidationError(f"save_child returned {type(ptr).__name__}, expected AtomPointer")
    return {"@ref": [ptr.segment.bytes, ptr.offset, type_tag(type(value).__type_name__)]}


def _encode_other(value, save_child: Callable[[Atom], AtomPointer]):
    """The full isinstance chain of :func:`encode_value`."""
    if value is None or value is True or value is False:
        return value
    if type(value) not in _ENCODED_AS_IS:
        # A subclass of a scalar type is written as its base value, read with the base type's
        # methods (canonical() does the same): never through the subclass's __str__ or __float__.
        plain = plain_scalar(value)
        if plain is not None:
            value = plain
    if type(value) is str:
        return check_text(value)
    if type(value) is int:
        return value if _INT_MIN <= value <= _INT_MAX else {"@int": format(value, "x")}
    if type(value) is float or type(value) is bytes or type(value) is bool:
        return value
    if isinstance(value, list):
        return [encode_value(v, save_child) for v in value]
    if isinstance(value, tuple):
        return {"@tup": [encode_value(v, save_child) for v in value]}
    if isinstance(value, dict):
        if type(value) is not FrozenDict:
            # A raw dict: its keys are converted (built-in subclasses to their base values) and
            # checked by canonical(), so two keys that become one base value are refused.
            value = canonical(value)
        elif not all(type(k) in _PLAIN_KEY_TYPES for k in value):
            check_map_keys(value)
        return {"@map": [[encode_value(k, save_child), encode_value(v, save_child)]
                         for k, v in value.items()]}
    if isinstance(value, Decimal):
        return {"@dec": str(value)}
    if isinstance(value, datetime):
        if isinstance(value.tzinfo, ZoneInfo):
            return {"@dt": _instant_fields(value)}
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


def _instant_fields(value: datetime) -> list:
    """``@dt`` fields of a datetime in an IANA zone: its UTC instant, fold 0, ``["instant", key]``."""
    key = value.tzinfo.key
    if key is None:
        raise ProtoValidationError("A ZoneInfo without a key cannot be stored")
    try:
        utc = value.astimezone(timezone.utc)
    except (OverflowError, ValueError) as e:
        raise ProtoValidationError(f"{value!r} has no UTC instant between years 1 and 9999") from e
    return [utc.year, utc.month, utc.day, utc.hour, utc.minute, utc.second, utc.microsecond, 0,
            ["instant", key]]


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
    if type(tz) is list and len(tz) == 2 and tz[0] == "zone" and type(tz[1]) is str:  # wall clock
        return ZoneInfo(tz[1])
    if type(tz) is list and len(tz) == 3 and tz[0] == "fixed" and type(tz[1]) is int \
            and type(tz[2]) is str:
        result = timezone(timedelta(microseconds=tz[1]))
        return result if result.tzname(None) == tz[2] else timezone(result.utcoffset(None), tz[2])
    raise ValueError("malformed tzinfo")


def _dec_int(body, _):
    if type(body) is not str:
        raise TypeError("expected str")
    value = int(body, 16)
    if format(value, "x") != body or _INT_MIN <= value <= _INT_MAX:
        raise ValueError("not the canonical @int text of an integer outside the native range")
    return value


def _dec_decimal(body, _):
    if type(body) is not str:
        raise TypeError("expected str")
    value = Decimal(body)
    if str(value) != body:
        raise ValueError("not the canonical @dec text (the to-scientific-string form)")
    return value


def _dec_date(body, _):
    return date(*_fields(body, 3, (int, int, int)))


def _dec_time(body, _):
    h, mi, s, us, fold, tz = _fields(body, 6, (int, int, int, int, int, None))
    return time(h, mi, s, us, tzinfo=_decode_tz(tz), fold=fold)


def _dec_datetime(body, _):
    y, mo, d, h, mi, s, us, fold, tz = _fields(body, 9, (int,) * 8 + (None,))
    if type(tz) is list and len(tz) == 2 and tz[0] == "instant" and type(tz[1]) is str:
        if fold != 0:
            raise ValueError("an instant has fold 0")
        return datetime(y, mo, d, h, mi, s, us, tzinfo=timezone.utc).astimezone(ZoneInfo(tz[1]))
    return datetime(y, mo, d, h, mi, s, us, tzinfo=_decode_tz(tz), fold=fold)


def _dec_timedelta(body, _):
    days, seconds, micros = _fields(body, 3, (int, int, int))
    if not (0 <= seconds < 86400 and 0 <= micros < 1_000_000):
        raise ValueError("a timedelta is normalized: 0 <= seconds < 86400, 0 <= microseconds < 10**6")
    return timedelta(days, seconds, micros)


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
    pairs = []
    raw_keys = []
    for pair in body:
        k, v = _fields(pair, 2, (None, None))
        raw_keys.append(k)
        pairs.append((decode_value(k, load_ref), decode_value(v, load_ref)))
    if not all(type(k) is str for k, _ in pairs):
        try:
            check_map_keys((k for k, _ in pairs), [_holds_zone_form(k) for k in raw_keys])
        except ProtoValidationError as e:
            raise ProtoCorruptionError(f"Malformed @map: {e}") from e
    out = FrozenDict(pairs)
    if len(out) != len(pairs):
        raise ProtoCorruptionError("Malformed @map: a map key appears twice")
    return out


def _holds_zone_form(raw) -> bool:
    """Whether the wire form of a map key holds a format-3 ``["zone", key]`` datetime (re-review I-2)."""
    if type(raw) is not dict or len(raw) != 1:
        return False
    (tag, body), = raw.items()
    if tag == "@dt":
        return type(body) is list and len(body) == 9 and type(body[8]) is list and body[8][:1] == ["zone"]
    if tag == "@tup":
        return type(body) is list and any(_holds_zone_form(item) for item in body)
    return False


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
