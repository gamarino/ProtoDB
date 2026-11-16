"""The agent operations: neutral, JSON-ready answers over a database's catalog and its query engine (spec
section 11; Task 7; preflight F18, F19, F23).

Each operation takes a :class:`~proto_db.transactions.Database` and reads one version of it through a
read-only :class:`~proto_db.transactions.Snapshot` (``Database.snapshot_at``): the current version (the
snapshot of a fresh transaction's ``snapshot_ref``), or, for :func:`query`, the version its ``as_of`` names.
Every answer is JSON-ready (:func:`to_json`).  This module imports no third-party package; the MCP server
(:mod:`proto_db.agent.mcp_server`) serves these operations as tools of the same names.

- :func:`list_entities` says where an agent starts: the catalog's types (label, meaning) and, per type, the
  names of the cells holding its entities, a bounded page at a time (a ``limit`` and a ``next`` token that
  reads the same version).  :func:`describe` is the semantic session's, as JSON.
- :func:`check_answerable` says whether the catalog names every term of a question (R274): a **match** is a
  case-folded, exact equality with the name, a label or a synonym of an entity, an attribute or a role; a
  term without a match is **unmatched**; a **suggestion** (for an unmatched term) is a name, label or synonym
  that contains it, and is never presented as a match (S7).  ``answerable`` is true when there are terms and
  every one of them matched.
- :func:`query` runs the session's single query engine (preflight F18): in catalog mode through
  :meth:`SemanticSession.query <proto_db.semantic.session.SemanticSession.query>` on a session over the
  snapshot, in raw mode through :func:`~proto_db.semantic.entities.query_stored`, the same engine without the
  catalog.  ``of_type`` keeps the rows whose type is that type or one of its subtypes, ``where`` goes to the
  planner where it can answer it exactly and is a residual otherwise, and ``version`` is the version read,
  ``{"number", "ref"}`` (F23; ``as_of`` takes that ``ref``).  The tree's shape is checked by the engine's
  own check (:func:`~proto_db.semantic.entities.check_query`).

Rows
    A row of a query without an aggregate is ``{"entity": {"cell": name} | None, "type_name", "values"}``:
    the cell of the entity it reads (``None`` for a relationship atom, which has no identity), its type, and
    its values.  Without ``select`` the values are the type's catalogued attributes (fields, computed
    attributes, maintained aggregates) and its ``one`` roles (raw mode: the record's stored attributes); with
    it, the selected paths.  A value the element does not have (MISSING) is left out.  A role's value is the
    cell it designates (``{"cell": name}``); a version-bound role is ``{"entity": {"cell": name}, "version":
    {"type_name", "values"}}``, the version it binds (the product as sold), whose own roles, records and
    collections are shown as below; a record held inside another is ``{"record": type name}``, a collection
    ``{"collection": "list" | "set", "count": n}``; ``from`` reaches what they hold.  An aggregate query
    answers ``[{"sum": value}]`` or ``[{"count": n}]``.

Sensitivity (spec section 11; ``describe`` lists every attribute, ``query`` omits the personal ones)
    An attribute or role whose ``sensitivity`` is ``personal`` or ``secret`` never reaches the agent, neither
    as a value nor through what a query computes from it.  A computed attribute or an aggregate is withheld
    when its expression reads a withheld attribute, at any depth and across roles, and a computed attribute
    whose expression is Python-only is withheld, since what it reads cannot be checked.

    - A ``from``, ``where`` or ``aggregate`` that reads a withheld attribute is refused before anything is
      read (``ProtoValidationError`` naming it): a filter or a sum over a personal value discloses it.  The
      check is static and conservative: where the members' type is not known (a ``root`` origin), every type
      of the catalog is a candidate, and a step is withheld when it is withheld in any candidate type (a
      subtype included).
    - A value a row would show is checked against that row's own type: a selected path that reads a withheld
      attribute is omitted from that row (the default selection the same way), and ``terms`` lists the
      withheld element with ``"sensitivity"`` and ``"withheld": True``, so the agent knows what it did not
      get and why.

Raw mode (``raw=True``; the agent experiment's control group)
    The catalog is not reported.  :func:`list_entities` lists the names of the roots (reserved ``__`` roots
    excepted) and of the cells (spec section 11), paged as above; :func:`describe` gives what the stored
    records of a type hold (attribute names and the kinds of the values stored); :func:`check_answerable`
    matches stored type and attribute names; :func:`query` runs the same engine over the stored values alone:
    no catalogued defaults, no subtypes (``of_type`` is the stored type), no ``terms``, and index planning
    where the stored structure allows it (a collection of records; never the entries of a maintained key,
    whose key only the catalog explains).  Reserved roots are not available.  Sensitivity still holds: when
    the database has a catalog, its personal and secret attributes are withheld as above, and a refusal says
    only that an attribute is not available (it names no catalog type and no sensitivity).
"""
from __future__ import annotations

import base64
import binascii
import bisect
import datetime
import json
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation

from ..collections.lists import List
from ..collections.sets import Set
from ..core.dbobject import DBObject
from ..errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from ..semantic import semantic
from ..semantic import compose as _compose
from ..semantic.entities import check_query, query_stored
from ..semantic.expressions import unwrap_entry
from ..values import MutableObject, is_atom

__all__ = ["list_entities", "describe", "check_answerable", "query", "to_json", "from_json"]

#: The sensitivities kept from agents (spec section 11).
WITHHELD = frozenset({"personal", "secret"})
#: The one-key objects :func:`from_json` reads as typed values.
_TYPED = ("decimal", "date", "time", "datetime", "bytes", "cell")


# -- JSON ----------------------------------------------------------------------------------------------------

def to_json(value):
    """``value`` as JSON-ready data: a ``Decimal`` as its string, a date, time or datetime in ISO 8601, an
    identity handle as ``{"cell": name}``, bytes as ``{"bytes": base64}``, a map as an object with string
    keys, a list, tuple, ``List`` or ``Set`` as an array, a record as ``{"type_name", "values"}``.  Anything
    else is ``ProtoValidationError``."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ProtoValidationError(f"{value!r} has no JSON form")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ProtoValidationError(f"{value!r} has no JSON form")
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return {"bytes": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, MutableObject):
        return {"cell": value.name}
    if isinstance(value, Mapping):
        return {str(key): to_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json(item) for item in value]
    if isinstance(value, (List, Set)):
        return [to_json(item) for item in value.elements()]
    if isinstance(value, DBObject) and is_atom(value):
        return {"type_name": getattr(type(value), "__type_name__", None),
                "values": {name: to_json(item) for name, item in value._own.items()}}
    raise ProtoValidationError(f"{type(value).__name__} has no JSON form")


def from_json(value):
    """The inverse of :func:`to_json` for the values a client writes in a query's constants: a one-key
    object ``{"decimal" | "date" | "time" | "datetime" | "bytes" | "cell": text}`` is that typed value
    (JSON has no decimals or dates); arrays and other objects are read item by item; anything else is
    itself.  A malformed typed value is ``ProtoValidationError``."""
    if isinstance(value, list):
        return [from_json(item) for item in value]
    if not isinstance(value, dict):
        return value
    if len(value) == 1:
        (kind, text), = value.items()
        if kind in _TYPED:
            if not isinstance(text, str):
                raise ProtoValidationError(f"A typed value {{{kind!r}: text}} holds a string, not {text!r}")
            try:
                if kind == "decimal":
                    return Decimal(text)
                if kind == "date":
                    return datetime.date.fromisoformat(text)
                if kind == "time":
                    return datetime.time.fromisoformat(text)
                if kind == "datetime":
                    return datetime.datetime.fromisoformat(text)
                if kind == "bytes":
                    return base64.b64decode(text, validate=True)
                return MutableObject(text)
            except (ValueError, InvalidOperation, binascii.Error) as e:
                raise ProtoValidationError(f"{text!r} is not a {kind}") from e
    return {key: from_json(item) for key, item in value.items()}


def _bound(value) -> bool:
    return isinstance(value, dict) and value.keys() == {"entity", "version"} \
        and isinstance(value["entity"], MutableObject) and is_atom(value["version"])


def _flat(value):
    """A value inside a row: what a role, a record or a collection holds is reached with ``from``, not
    inlined (see "Rows")."""
    if _bound(value):
        return {"entity": {"cell": value["entity"].name}, "version": {"record": _stored_type(value["version"])}}
    if isinstance(value, (List, Set)):
        return {"collection": "list" if isinstance(value, List) else "set", "count": value.count}
    if isinstance(value, DBObject) and is_atom(value):
        return {"record": _stored_type(value)}
    return to_json(value)


def _value(value, policy: "_Withholding"):
    """A row's value: :func:`_flat`, except that a version-bound role shows the version it binds (the
    product as sold): ``{"entity": {"cell"}, "version": {"type_name", "values"}}``, its values withheld as a
    row's are."""
    if not _bound(value):
        return _flat(value)
    record = value["version"]
    type_name = _stored_type(record)
    return {"entity": {"cell": value["entity"].name},
            "version": {"type_name": type_name,
                        "values": {name: _flat(item) for name, item in record._own.items()
                                   if not policy.withheld(type_name, [name])}}}


# -- the version read ----------------------------------------------------------------------------------------

def _decode_ref(as_of) -> bytes:
    if isinstance(as_of, bytes):
        return as_of
    if not isinstance(as_of, str):
        raise ProtoValidationError(f"'as_of' is the 'ref' of a version an answer gave (a base64 string), "
                                   f"not {type(as_of).__name__}")
    try:
        return base64.b64decode(as_of, validate=True)
    except (binascii.Error, ValueError) as e:
        raise ProtoValidationError(f"'as_of' is the 'ref' of a version an answer gave (base64), not {as_of!r}") \
            from e


def _snapshot(db, as_of=None):
    """The snapshot of the version ``as_of`` names, or of the current version (preflight F23)."""
    if as_of is None:
        tr = db.new_transaction()
        try:
            ref = tr.snapshot_ref
        finally:
            tr.abort()
    else:
        ref = _decode_ref(as_of)
    return db.snapshot_at(ref)


def _version(snapshot) -> dict:
    return {"number": snapshot.version, "ref": base64.b64encode(snapshot.ref).decode("ascii")}


def _catalog_or_none(snapshot):
    """The verified catalog of the snapshot, or ``None`` when the database has none (raw mode over a
    database written before the semantic layer: nothing is known to withhold)."""
    try:
        return semantic(snapshot).catalog()
    except ProtoNotSupportedError:
        return None


# -- sensitivity ---------------------------------------------------------------------------------------------

class _Withholding:
    """The static sensitivity check of a query over a catalog model (see "Sensitivity")."""

    def __init__(self, model: Mapping | None, *, raw: bool = False):
        self.model = model
        self.raw = raw
        self.entities = {} if model is None else model["entities"]
        self.elements = {name: {e["name"]: e for e in entity.get("attributes", []) + entity.get("roles", [])}
                         for name, entity in self.entities.items()}
        self._subtypes: dict = {}
        self._computed: dict = {}

    def subtypes(self, type_name: str) -> frozenset:
        found = self._subtypes.get(type_name)
        if found is None:
            found = self._subtypes[type_name] = _compose.subtypes(self.model, type_name)
        return found

    def candidates(self, types) -> frozenset:
        return frozenset(self.entities) if types is None else types

    def cell_type(self, name: str):
        """The types an entity cell may hold, from its name (R260); ``None`` (any) when not catalogued."""
        type_name = name.rsplit("#", 1)[0] if "#" in name else None
        return self.subtypes(type_name) if type_name in self.entities else None

    def entry(self, type_name: str, name: str, sensitivity: str) -> dict:
        element = self.elements[type_name][name]
        certification = element.get("certification", self.entities[type_name].get("certification"))
        kind = "role" if "target" in element else element.get("kind", "field")
        return {"type_name": type_name, "name": name, "kind": kind, "certification": certification,
                "sensitivity": sensitivity, "withheld": True}

    def path(self, types, steps, seen: frozenset = frozenset()) -> tuple:
        """``(finding, types reached)`` for the path ``steps`` read on members of ``types`` (``None``: any):
        the first withheld ``(type, name, sensitivity)`` the path reads, else ``None``; and the types its
        last step reaches when every step is a role (``None`` when not known)."""
        for step in steps:
            following = set()
            for type_name in self.candidates(types):
                element = self.elements.get(type_name, {}).get(step)
                if element is None:
                    continue
                sensitivity = element.get("sensitivity")
                if sensitivity in WITHHELD:
                    return (type_name, step, sensitivity), None
                if element.get("kind") in ("computed", "computed_stored"):
                    finding = self.computed(type_name, step, seen)
                    if finding is not None:
                        return finding, None
                if "target" in element and element["target"] in self.entities:
                    following |= self.subtypes(element["target"])
            types = frozenset(following)
        return None, types

    def computed(self, type_name: str, name: str, seen: frozenset):
        """The withheld attribute the computed attribute ``name`` of ``type_name`` reads, at any depth;
        a Python-only expression is withheld (what it reads cannot be checked)."""
        key = (type_name, name)
        if key in self._computed:
            return self._computed[key]
        if key in seen:
            return None                         # a cycle: its other reads are checked where it started
        element = self.elements[type_name][name]
        if "expression" not in element:
            found = (type_name, name, "Python-only (what it reads cannot be checked for personal or secret "
                                      "attributes)")
        else:
            found = self.tree(element["expression"]["ast"], frozenset({type_name}), seen | {key})
        if found is not None or not seen:
            self._computed[key] = found     # a None below a cycle cut is not this element's whole answer
        return found

    def via_types(self, origin, types, seen):
        """``(finding, member types)`` of a ``via`` origin and its roles, or a bare root."""
        if origin[0] == "root":
            return None, None
        start, roles = origin[1], origin[2:]
        start_types = types if start[0] == "self" else self.cell_type(start[1]) if start[0] == "cell" else None
        return self.path(start_types, roles, seen)

    def tree(self, node, types, seen: frozenset = frozenset()):
        """The first withheld attribute the expression ``node`` reads on members of ``types``, or ``None``."""
        stack = [(node, types)]
        while stack:
            item, here = stack.pop()
            if not isinstance(item, list) or not item or not isinstance(item[0], str):
                continue
            op = item[0]
            if op == "attr":
                finding, _ = self.path(here, item[1:], seen)
            elif op in ("via", "root"):
                finding, _ = self.via_types(item, here, seen)
            elif op in ("sum", "count"):
                over = item[1]
                if isinstance(over, str):
                    finding, members = self.path(here, [over], seen)
                else:
                    finding, members = self.via_types(over, here, seen)
                stack.extend((operand, members) for operand in item[2:])
            elif op == "const":
                finding = None
            else:
                finding = None
                stack.extend((child, here) for child in item[1:])
            if finding is not None:
                return finding
        return None

    def withheld(self, type_name: str, steps) -> bool:
        """Whether the path ``steps`` read on a member of ``type_name`` reads a withheld attribute (a type
        the catalog does not describe may be any of its types)."""
        return self.path(frozenset({type_name}) if type_name in self.entities else None, steps)[0] is not None

    def refuse(self, part: str, finding) -> None:
        if finding is not None and self.raw:
            raise ProtoValidationError(f"The query's {part} reads an attribute that is not available")
        if finding is not None:
            type_name, name, sensitivity = finding
            raise ProtoValidationError(f"The query's {part} reads the attribute {name!r} of {type_name}, which is "
                                       f"{sensitivity}: personal and secret attributes are kept from agents "
                                       f"(spec section 11)")

    def allowed(self, type_name: str) -> list:
        """The attributes and ``one`` roles of ``type_name`` a row shows (none withheld)."""
        out = []
        for name, element in self.elements.get(type_name, {}).items():
            if "target" in element and element.get("cardinality") not in ("one", "0..1"):
                continue
            if self.path(frozenset({type_name}), [name])[0] is None:
                out.append(name)
        return out


# -- list_entities and describe ------------------------------------------------------------------------------

#: The default and the largest number of cell names one page of :func:`list_entities` gives.
LIST_LIMIT, LIST_LIMIT_MAX = 100, 1000


def _page_start(db, limit, after) -> tuple:
    """``(snapshot, the cell name the page starts after)``: a continuation token pins the version its first
    page read, so the pages of one listing read one version."""
    if type(limit) is not int or not 1 <= limit <= LIST_LIMIT_MAX:
        raise ProtoValidationError(f"limit is an int from 1 to {LIST_LIMIT_MAX}, not {limit!r}")
    if after is None:
        return _snapshot(db), None
    try:
        token = json.loads(base64.b64decode(after, validate=True)) if isinstance(after, str) else None
    except (binascii.Error, ValueError) as e:
        raise ProtoValidationError("after is the 'next' token a list_entities answer gave") from e
    if not isinstance(token, dict) or not isinstance(token.get("ref"), str) or not isinstance(token.get("after"), str):
        raise ProtoValidationError("after is the 'next' token a list_entities answer gave")
    return _snapshot(db, token["ref"]), token["after"]


def _page(snapshot, start, keep, limit: int) -> tuple:
    """``(the next cell names after start that keep accepts, at most limit, the continuation or None)``."""
    names = snapshot.mutable_names
    out, index = [], 0 if start is None else bisect.bisect_right(names, start)
    for index in range(index, len(names)):
        if keep(names[index]):
            if len(out) == limit:
                token = json.dumps({"ref": _version(snapshot)["ref"], "after": out[-1]})
                return out, base64.b64encode(token.encode("utf-8")).decode("ascii")
            out.append(names[index])
    return out, None


def _cell_type(name: str):
    return name.rsplit("#", 1)[0] if "#" in name else None


def list_entities(db, filter: str | None = None, *, limit: int = LIST_LIMIT, after: str | None = None,
                  raw: bool = False) -> dict:
    """Where an agent starts: ``{"entities": [{"type_name", "label", "meaning", "cells"}], "next"}``.  The
    types are those the catalog describes, filtered as :meth:`SemanticSession.list_entities
    <proto_db.semantic.session.SemanticSession.list_entities>` does; ``cells`` are the names of the cells
    holding entities of exactly that type (R260: ``<type name>#<hex>``), at most ``limit`` names in all, in
    name order.  ``next`` is ``None`` when the listing is complete, else the token to pass as ``after`` for
    the next page, which reads the same version.

    Raw mode lists the stored structure only (spec section 11): ``{"roots", "cells", "next"}``, the names of
    the roots (reserved ``__`` roots excepted) and of the cells, paged the same way; ``filter`` keeps the
    names that contain it."""
    if filter is not None and not isinstance(filter, str):
        raise ProtoUserError(f"The filter of list_entities is a str or None, not {type(filter).__name__}")
    snapshot, start = _page_start(db, limit, after)
    if raw:
        needle = None if filter is None else filter.casefold()

        def wanted(name: str) -> bool:
            return needle is None or needle in name.casefold()
        cells, token = _page(snapshot, start, wanted, limit)
        roots = [name for name in snapshot.root_names if not name.startswith("__") and wanted(name)]
        return {"roots": roots, "cells": cells, "next": token}
    listed = semantic(snapshot).list_entities(filter)
    types = {entity["type_name"] for entity in listed}
    cells, token = _page(snapshot, start, lambda name: _cell_type(name) in types, limit)
    by_type: dict = {}
    for name in cells:
        by_type.setdefault(_cell_type(name), []).append(name)
    return {"entities": [{**to_json(entity), "cells": by_type.get(entity["type_name"], [])} for entity in listed],
            "next": token}


def describe(db, type_name: str, *, raw: bool = False) -> dict:
    """The composed entity ``type_name``: attributes, roles, intents, constraints, provenance and
    certification (personal and secret attributes are listed, with their sensitivity).  Raw mode: what the
    stored records of the type hold, ``{"type_name", "cells", "records", "attributes": {name: [kinds]}}``
    (``records`` counts those held inside other records' collections)."""
    snapshot = _snapshot(db)
    if not raw:
        return to_json(semantic(snapshot).describe(type_name))
    if not isinstance(type_name, str):
        raise ProtoValidationError(f"A type name is a str, not {type(type_name).__name__}")
    cells, nested, attributes = 0, 0, {}

    def take(record):
        for name, value in record._own.items():
            attributes.setdefault(name, set()).add(_kind_of(value))
    for name in snapshot.mutable_names:
        record = snapshot.get_mutable(name)
        if _stored_type(record) == type_name:
            cells += 1
            take(record)
        if isinstance(record, DBObject) and is_atom(record):
            for value in record._own.values():
                if isinstance(value, (List, Set)):
                    for element in value.elements():
                        element = unwrap_entry(element)
                        if is_atom(element) and _stored_type(element) == type_name:
                            nested += 1
                            take(element)
    if not cells and not nested:
        raise ProtoValidationError(f"No stored record has the type {type_name!r}")
    return {"type_name": type_name, "cells": cells, "records": nested,
            "attributes": {name: sorted(kinds) for name, kinds in sorted(attributes.items())}}


def _stored_type(value) -> str:
    found = getattr(type(value), "__type_name__", None)
    return found if isinstance(found, str) else type(value).__name__


def _kind_of(value) -> str:
    """The kind of a stored value, as raw mode reports it."""
    if value is None:
        return "nil"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, Decimal):
        return "decimal"
    if isinstance(value, datetime.datetime):
        return "datetime"
    for kind in (datetime.date, datetime.time, str, bytes, float):
        if isinstance(value, kind):
            return kind.__name__
    if isinstance(value, MutableObject):
        return f"cell {value.name.rsplit('#', 1)[0]}"
    if isinstance(value, dict) and value.keys() == {"entity", "version"} and isinstance(value["entity"],
                                                                                         MutableObject):
        return f"version of cell {value['entity'].name.rsplit('#', 1)[0]}"
    if isinstance(value, (List, Set)):
        kinds = sorted({_kind_of(unwrap_entry(element)) for element in value.elements()})
        return f"{'list' if isinstance(value, List) else 'set'} of {' | '.join(kinds) or 'nothing'}"
    if isinstance(value, Mapping):
        return "map"
    if is_atom(value):
        return f"record {_stored_type(value)}"
    return type(value).__name__


# -- check_answerable ----------------------------------------------------------------------------------------

def _texts(item: Mapping) -> list:
    """``(via, text)`` of a catalog item: its name, its labels, its synonyms."""
    return ([("name", item["name"])] + [("label", text) for text in item.get("label", {}).values()]
            + [("synonym", text) for text in item.get("synonyms", ())])


def _vocabulary(snapshot, raw: bool) -> list:
    """``(candidate, [(via, text)])`` for everything a term may name."""
    out = []
    if raw:
        types: dict = {}
        for name in snapshot.mutable_names:
            record = snapshot.get_mutable(name)
            names = types.setdefault(_stored_type(record), set())
            if isinstance(record, DBObject) and is_atom(record):
                names.update(record._own.keys())
        for type_name, names in sorted(types.items()):
            out.append(({"kind": "entity", "type_name": type_name, "name": None},
                        [("stored", type_name), ("stored", type_name.rsplit(".", 1)[-1])]))
            out.extend(({"kind": "attribute", "type_name": type_name, "name": name}, [("stored", name)])
                       for name in sorted(names))
        return out
    for type_name, entity in sorted(semantic(snapshot).catalog()["entities"].items()):
        texts = [("name", type_name), ("name", type_name.rsplit(".", 1)[-1])]
        texts += [("label", text) for text in entity.get("label", {}).values()]
        texts += [("synonym", text) for text in entity.get("synonyms", ())]
        out.append(({"kind": "entity", "type_name": type_name, "name": None}, texts))
        for element in entity.get("attributes", []) + entity.get("roles", []):
            kind = "role" if "target" in element else "attribute"
            out.append(({"kind": kind, "type_name": type_name, "name": element["name"]}, _texts(element)))
    return out


def check_answerable(db, terms: list, *, raw: bool = False) -> dict:
    """``{"answerable", "matches", "unmatched", "suggestions"}`` for the terms of a question (see the module
    docstring).  ``matches`` maps every term to its candidates (empty when unmatched), ``suggestions`` every
    unmatched term to the candidates whose texts contain it; a candidate is ``{"kind", "type_name", "name",
    "via"}`` (``kind`` ``entity``, ``attribute`` or ``role``; ``name`` ``None`` for an entity; ``via``
    ``name``, ``label`` or ``synonym``, ``stored`` in raw mode)."""
    if isinstance(terms, str) or not isinstance(terms, (list, tuple)) \
            or not all(isinstance(term, str) and term.strip() for term in terms):
        raise ProtoValidationError(f"check_answerable takes a list of non-empty terms (str), not {terms!r}")
    vocabulary = _vocabulary(_snapshot(db), raw)
    matches, unmatched, suggestions = {}, [], {}
    for term in terms:
        wanted = term.strip().casefold()
        found, near = [], []
        for candidate, texts in vocabulary:
            exact = next((via for via, text in texts if text.casefold() == wanted), None)
            if exact is not None:
                found.append({**candidate, "via": exact})
                continue
            partial = next((via for via, text in texts if wanted in text.casefold()), None)
            if partial is not None:
                near.append({**candidate, "via": partial})
        matches[term] = found
        if not found:
            unmatched.append(term)
            suggestions[term] = near
    return {"answerable": bool(terms) and not unmatched, "matches": matches, "unmatched": unmatched,
            "suggestions": suggestions}


# -- query ---------------------------------------------------------------------------------------------------

def _screen(policy: _Withholding, tree) -> frozenset | None:
    """Refuse a ``from``, ``where`` or ``aggregate`` that reads a withheld attribute (see "Sensitivity");
    return the types the members may have (``None``: any), for the default selection."""
    origin = tree["from"]
    finding, types = (None, None) if origin[0] == "root" else policy.via_types(origin, None, frozenset())
    policy.refuse("'from'", finding)
    policy.refuse("'where'", None if tree.get("where") is None else policy.tree(tree["where"], types))
    aggregate = tree.get("aggregate")
    if aggregate is not None and aggregate[0] == "sum":
        policy.refuse("aggregate", policy.tree(aggregate[1], types))
    if tree.get("of_type") in policy.entities:
        types = policy.subtypes(tree["of_type"])
    return types


def query(db, tree: Mapping, *, raw: bool = False) -> dict:
    """Run the query ``tree`` (spec section 11) and answer ``{"rows", "explain", "version", "query",
    "terms"}`` as JSON-ready data (see the module docstring: rows, sensitivity, raw mode).  ``tree["as_of"]``
    is the ``ref`` of a version an earlier answer gave; without it the current version is read."""
    check_query(tree)
    snapshot = _snapshot(db, tree.get("as_of"))
    run = dict(tree)
    if "as_of" in tree:
        run["as_of"] = _version(snapshot)["ref"]
    session = None if raw else semantic(snapshot)
    policy = _Withholding(_catalog_or_none(snapshot) if raw else session.catalog(), raw=raw)
    types = _screen(policy, tree)
    aggregate, select = tree.get("aggregate"), tree.get("select")
    default = select is None and aggregate is None
    if default and not raw:
        run["select"] = sorted({name for type_name in policy.candidates(types) for name in policy.allowed(type_name)})
    answer = query_stored(snapshot, run, with_members=True) if raw else session.query(run, members=True)
    if aggregate is not None:
        rows = to_json(answer["rows"])
    else:
        rows = []
        for values, version in zip(answer["rows"], answer["members"]):
            record = version.record
            type_name = _stored_type(record)
            if default and raw:
                values = dict(record._own)
            shown = frozenset(policy.allowed(type_name)) if default and not raw else None
            values = {name: value for name, value in values.items()
                      if (shown is None or name in shown) and not policy.withheld(type_name, name.split("."))}
            rows.append({"entity": None if version.entity is None else {"cell": version.entity.name},
                         "type_name": type_name,
                         "values": {name: _value(value, policy) for name, value in values.items()}})
    terms = []
    for term in answer["terms"]:
        finding = policy.path(frozenset({term["type_name"]}), [term["name"]])[0]
        terms.append(term if finding is None else {**term, "sensitivity": finding[2], "withheld": True})
    return {"rows": rows, "explain": to_json(answer["explain"]), "version": to_json(answer["version"]),
            "query": to_json(dict(tree)), "terms": to_json(terms)}
