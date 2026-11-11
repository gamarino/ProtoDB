"""The language-neutral notation of the conformance manifest (``tests/conformance/README.md``).

A *value description* is a one-key JSON object naming the value's kind: ``{"int": "7"}``,
``{"str": "a"}``, ``{"datetime": [...]}``, ``{"ref": {...}}``... (:func:`describe`).  A database
is described by :func:`describe_database`: its roots in name order (records as ``object`` with
their attributes sorted by name, collections as ``collection`` with their elements or items in
the collection's own order, atoms of a type this process has no class for as ``opaque``), its
mutables, its indexes and its origin.  :mod:`proto_db.tools.verify` prints it; the conformance
manifest holds it for the vector spaces.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from ..collections.base import IndexedCollection
from ..collections.dictionaries import Dictionary, RepeatedKeysDictionary
from ..collections.hash_dictionaries import HashDictionary
from ..collections.lists import List
from ..collections.sets import CountedSet, Set
from ..core.atom import Atom, OpaqueAtom
from ..core.dbobject import DBObject
from ..values import MISSING, FrozenDict, FrozenList, MutableObject

__all__ = ["describe", "describe_object", "describe_database"]

_MICROSECOND = timedelta(microseconds=1)


def _tz(tz):
    if tz is None:
        return None
    if isinstance(tz, ZoneInfo):
        return ["zone", tz.key]
    return ["fixed", tz.utcoffset(None) // _MICROSECOND, tz.tzname(None)]


def describe(value):
    """The value description of a decoded canonical value (types are matched exactly); an atom
    is described as the ``ref`` to its record.  Lists, tuples and maps are described with an
    explicit stack (any depth)."""
    root = [None]
    stack = [(value, root, 0)]
    while stack:
        item, parent, slot = stack.pop()
        parent[slot], children = _value_shape(item)
        stack.extend(reversed(children))
    return root[0]


def _value_shape(value) -> tuple:
    """``(description, children)`` of a canonical value, as :func:`_shape` gives them."""
    t = type(value)
    if t is FrozenList or t is list or t is tuple:
        out = [None] * len(value)
        return {"list" if t is not tuple else "tuple": out}, [(v, out, i) for i, v in enumerate(value)]
    if t is FrozenDict or t is dict:
        return _pairs({}, "map", value.items())
    return _describe_leaf(value), []


def _describe_leaf(value):
    t = type(value)
    if value is None:
        return {"nil": None}
    if t is bool:
        return {"bool": value}
    if t is int:
        return {"int": str(value)}
    if t is float:
        return {"float": float.hex(value) if value == value and abs(value) != float("inf") else repr(value)}
    if t is str:
        return {"str": value}
    if t is bytes:
        return {"bytes": value.hex()}
    if t is Decimal:
        return {"decimal": str(value)}
    if t is datetime and isinstance(value.tzinfo, ZoneInfo):
        u = value.astimezone(timezone.utc)
        return {"datetime": [u.year, u.month, u.day, u.hour, u.minute, u.second, u.microsecond, 0,
                             ["instant", value.tzinfo.key]]}
    if t is datetime:
        return {"datetime": [value.year, value.month, value.day, value.hour, value.minute, value.second,
                             value.microsecond, value.fold, _tz(value.tzinfo)]}
    if t is date:
        return {"date": [value.year, value.month, value.day]}
    if t is time:
        return {"time": [value.hour, value.minute, value.second, value.microsecond, value.fold, _tz(value.tzinfo)]}
    if t is timedelta:
        return {"timedelta": [value.days, value.seconds, value.microseconds]}
    if t is uuid.UUID:
        return {"uuid": value.hex}
    if value is MISSING:
        return {"missing": True}
    if t is MutableObject:
        return {"mutable": value.name}
    if isinstance(value, Atom):
        ptr = value.atom_pointer
        return {"ref": {"segment": ptr.segment.hex, "offset": ptr.offset, "type": t.__type_name__}}
    raise TypeError(f"no description for {t.__name__}")


def describe_object(o):
    """The content description of a stored object: records, collections and opaque atoms are
    described by their content (recursively), any other value by :func:`describe`.

    Nested content is described with an explicit stack, not by recursion, so a record at any depth
    the product writes (a chain of 10,000 versions, R128) is described; the notation is unchanged."""
    root = [None]
    stack = [(o, root, 0)]
    while stack:
        value, parent, slot = stack.pop()
        parent[slot], children = _shape(value)
        stack.extend(reversed(children))
    return root[0]


def _shape(o) -> tuple:
    """``(description, children)``: the description of ``o`` with ``None`` where the description
    of each nested value goes, and ``children``, the ``(value, container, index)`` to fill in."""
    if isinstance(o, DBObject):
        return _with_attrs("object", o)
    if isinstance(o, OpaqueAtom):
        return _with_attrs("opaque", o)
    if isinstance(o, (List, Set)):
        elements = list(o.elements())
        out = [None] * len(elements)
        return ({"collection": type(o).__type_name__, "elements": out},
                [(e, out, i) for i, e in enumerate(elements)])
    if isinstance(o, CountedSet):
        items, children = [], []
        for e, n in o.items():
            item = [None, {"int": str(n)}]
            items.append(item)
            children.append((e, item, 0))
        return {"collection": type(o).__type_name__, "items": items}, children
    if isinstance(o, (Dictionary, HashDictionary, RepeatedKeysDictionary)):
        return _pairs({"collection": type(o).__type_name__}, "items", o.items())
    if isinstance(o, tuple):
        out = [None] * len(o)
        return {"tuple": out}, [(v, out, i) for i, v in enumerate(o)]
    if isinstance(o, list):
        out = [None] * len(o)
        return {"list": out}, [(v, out, i) for i, v in enumerate(o)]
    if isinstance(o, dict):
        return _pairs({}, "map", o.items())
    return _value_shape(o)


def _with_attrs(kind: str, o) -> tuple:
    return _pairs({kind: type(o).__type_name__}, "attrs", sorted(o._state().items()), keys_described=False)


def _pairs(head: dict, field: str, pairs, keys_described: bool = True) -> tuple:
    items, children = [], []
    for k, v in pairs:
        item = [None, None] if keys_described else [k, None]
        items.append(item)
        if keys_described:
            children.append((k, item, 0))
        children.append((v, item, 1))
    head[field] = items
    return head, children


def describe_database(tx) -> dict:
    """The description of the database ``tx`` reads: ``roots`` and ``mutables`` (name order),
    ``indexes`` (per root collection: each index's type and whether its member tokens are
    current) and ``origin``."""
    out = {"roots": [[name, describe_object(tx.get_root_object(name))] for name in sorted(tx.root_names())],
           "mutables": [[name, describe_object(tx.get_mutable(name))] for name in sorted(tx.mutable_names())]}
    out["indexes"] = {}
    for name in sorted(tx.root_names()):
        root = tx.get_root_object(name)
        if isinstance(root, IndexedCollection) and root.index_names():
            out["indexes"][name] = {i: {"type": type(root.get_index(i)).__type_name__,
                                        "member_tokens": root.get_index(i).has_current_tokens()}
                                    for i in root.index_names()}
    out["origin"] = describe(tx._db_root.origin)
    return out
