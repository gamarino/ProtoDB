"""The agent operations: neutral, JSON-ready answers over a database's catalog and its query engine (spec
section 11; Task 7; preflight F18, F19, F23).

Each operation takes a :class:`~proto_db.transactions.Database` and reads one version of it through a
read-only :class:`~proto_db.transactions.Snapshot` (``Database.snapshot_at``): the current version (the
snapshot of a fresh transaction's ``snapshot_ref``), or, for :func:`query`, the version its ``as_of`` names.
Every answer is JSON-ready (:func:`to_json`).  This module imports no third-party package; the MCP server
(:mod:`proto_db.agent.mcp_server`) serves these operations as tools of the same names.

- :func:`list_entities` and :func:`describe` are the semantic session's, as JSON.
- :func:`check_answerable` says whether the catalog names every term of a question (R274): a **match** is a
  case-folded, exact equality with the name, a label or a synonym of an entity, an attribute or a role; a
  term without a match is **unmatched**; a **suggestion** (for an unmatched term) is a name, label or synonym
  that contains it, and is never presented as a match (S7).  ``answerable`` is true when there are terms and
  every one of them matched.
- :func:`query` opens a semantic session over the snapshot and runs :meth:`SemanticSession.query
  <proto_db.semantic.session.SemanticSession.query>`, the single query engine (preflight F18): ``of_type``
  keeps the rows whose type is that type or one of its subtypes, ``where`` goes to the planner where it can
  answer it exactly and is a residual otherwise, and ``version`` is the version read, ``{"number", "ref"}``
  (F23; ``as_of`` takes that ``ref``).

Rows
    A row of a query without an aggregate is ``{"entity": {"cell": name} | None, "type_name", "values"}``:
    the cell of the entity it reads (``None`` for a relationship atom, which has no identity), its type, and
    its values.  Without ``select`` the values are the type's catalogued attributes (fields, computed
    attributes, maintained aggregates) and its ``one`` roles; with it, the selected paths.  A value the
    element does not have (MISSING) is left out.  A role's value is the cell it designates
    (``{"cell": name}``, also for a version-bound role), a record held inside another
    ``{"record": type name}``, a collection ``{"collection": "list" | "set", "count": n}``; ``from``
    reaches what they hold.  An aggregate query answers ``[{"sum": value}]`` or ``[{"count": n}]``.

Sensitivity (spec section 11; ``describe`` lists every attribute, ``query`` omits the personal ones)
    An attribute or role whose ``sensitivity`` is ``personal`` or ``secret`` never reaches the agent, neither
    as a value nor through what a query computes from it.  The check is static, over the catalog, before
    anything is read, and it is conservative: where the members' type is not known (a ``root`` origin), every
    type of the catalog is a candidate; a step is withheld when it is withheld in any candidate type (a
    subtype included); a computed attribute or an aggregate is withheld when its expression reads a withheld
    attribute, at any depth and across roles, and a computed attribute whose expression is Python-only is
    withheld, since what it reads cannot be checked.

    - A ``from``, ``where`` or ``aggregate`` that reads a withheld attribute is refused
      (``ProtoValidationError`` naming it): a filter or a sum over a personal value discloses it.
    - A selected path that reads one is omitted from every row; ``terms`` lists its element with
      ``"sensitivity"`` and ``"withheld": True``, so the agent knows what it did not get and why.

Raw mode (``raw=True``; the agent experiment's control group)
    The catalog is not reported: :func:`list_entities` lists the types of the records held by cells
    (``{"type_name", "cells"}``) and the roots that are not reserved (``{"root", "type_name"}``);
    :func:`describe` gives what the stored records of a type hold (attribute names and the kinds of the
    values stored); :func:`check_answerable` matches stored type and attribute names; :func:`query` runs the
    same tree with the expression evaluator over the stored values alone: no catalogued defaults, no
    subtypes (``of_type`` is the stored type), no index planning (``explain`` has no plan, every conjunct is
    a residual), no ``terms``.  Reserved roots (``__`` names) cannot be read.  Sensitivity still holds: when
    the database has a catalog, its personal and secret attributes are withheld exactly as above (the
    catalog decides what is withheld; nothing of it is reported).
"""
from __future__ import annotations

import base64
import binascii
import datetime
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation

from ..collections.lists import List
from ..collections.sets import Set
from ..core.dbobject import DBObject
from ..errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from ..indexing.keyspec import _step as _stored_step
from ..semantic import semantic
from ..semantic import compose as _compose
from ..semantic.expressions import (Reader, _arithmetic, _identity, _number, _truth, check_ast, evaluate,
                                    unwrap_entry)
from ..values import MISSING, MutableObject, is_atom

__all__ = ["list_entities", "describe", "check_answerable", "query", "to_json", "from_json"]

#: The sensitivities kept from agents (spec section 11).
WITHHELD = frozenset({"personal", "secret"})
#: The keys of a query tree (spec section 11).
_QUERY_KEYS = frozenset({"from", "of_type", "where", "select", "aggregate", "as_of"})
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


def _shallow(value):
    """A row's value: what a role, a record or a collection holds is reached with ``from``, not inlined
    (see "Rows")."""
    if isinstance(value, dict) and value.keys() == {"entity", "version"} and isinstance(value["entity"],
                                                                                         MutableObject):
        return {"cell": value["entity"].name}
    if isinstance(value, (List, Set)):
        return {"collection": "list" if isinstance(value, List) else "set", "count": value.count}
    if isinstance(value, DBObject) and is_atom(value):
        return {"record": getattr(type(value), "__type_name__", None)}
    return to_json(value)


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

    def __init__(self, model: Mapping | None):
        self.model = model
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

    def refuse(self, part: str, finding) -> None:
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

def list_entities(db, filter: str | None = None, *, raw: bool = False) -> list:
    """The types the catalog describes, ``{"type_name", "label", "meaning"}`` each, filtered as
    :meth:`SemanticSession.list_entities <proto_db.semantic.session.SemanticSession.list_entities>` does.
    Raw mode: see the module docstring."""
    snapshot = _snapshot(db)
    if not raw:
        return to_json(semantic(snapshot).list_entities(filter))
    if filter is not None and not isinstance(filter, str):
        raise ProtoUserError(f"The filter of list_entities is a str or None, not {type(filter).__name__}")
    needle = None if filter is None else filter.casefold()
    counts: dict = {}
    for name in snapshot.mutable_names:
        record = snapshot.get_mutable(name)
        type_name = _stored_type(record)
        counts[type_name] = counts.get(type_name, 0) + 1
    out = [{"type_name": t, "cells": n} for t, n in sorted(counts.items(), key=lambda item: str(item[0]))]
    out += [{"root": name, "type_name": _stored_type(snapshot.get_root_object(name))}
            for name in snapshot.root_names if not name.startswith("__")]
    if needle is not None:
        out = [item for item in out if any(needle in str(text).casefold()
                                           for text in (item.get("type_name"), item.get("root")) if text)]
    return out


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

def _check_tree(tree) -> None:
    if not isinstance(tree, Mapping):
        raise ProtoValidationError(f"A query is a mapping (spec section 11), not {type(tree).__name__}")
    unknown = sorted(set(tree) - _QUERY_KEYS)
    if unknown:
        raise ProtoValidationError(f"A query has the keys {sorted(_QUERY_KEYS)}; unknown keys {unknown}")
    origin = tree.get("from")
    if not isinstance(origin, list) or not origin or origin[0] not in ("via", "root"):
        raise ProtoValidationError(f"A query's 'from' is a via expression or a bare root, not {origin!r}")
    check_ast(origin, in_query=True)
    if origin[0] == "via" and origin[1][0] == "self":
        raise ProtoValidationError("A query has no current element: its 'from' starts at a root or a cell")
    if tree.get("where") is not None:
        check_ast(tree["where"], in_query=True)
    aggregate = tree.get("aggregate")
    if aggregate is not None:
        if not isinstance(aggregate, list) or not aggregate or aggregate[0] not in ("sum", "count") \
                or len(aggregate) != (2 if aggregate[0] == "sum" else 1):
            raise ProtoValidationError(f"A query's 'aggregate' is ['sum', term] or ['count'], not {aggregate!r}")
        if aggregate[0] == "sum":
            check_ast(aggregate[1], in_query=True)
    select = tree.get("select")
    if select is not None and (isinstance(select, str) or not isinstance(select, (list, tuple))
                               or not all(isinstance(n, str) and n and all(n.split(".")) for n in select)):
        raise ProtoValidationError(f"A query's 'select' is a list of attribute paths, not {select!r}")


def _from_types(policy: _Withholding, origin):
    """``(finding, member types)`` of a query's ``from``."""
    if origin[0] == "root":
        return None, None
    return policy.via_types(origin, None, frozenset())


def _screen(policy: _Withholding, tree) -> tuple:
    """Refuse a ``from``, ``where`` or ``aggregate`` that reads a withheld attribute; return ``(select to
    run or None, withheld term entries, the row types a default selection is computed for)``."""
    finding, types = _from_types(policy, tree["from"])
    policy.refuse("'from'", finding)
    policy.refuse("'where'", None if tree.get("where") is None else policy.tree(tree["where"], types))
    aggregate = tree.get("aggregate")
    if aggregate is not None and aggregate[0] == "sum":
        policy.refuse("aggregate", policy.tree(aggregate[1], types))
    if tree.get("of_type") is not None and tree["of_type"] in policy.entities:
        types = policy.subtypes(tree["of_type"])
    select, withheld, seen = tree.get("select"), [], set()
    if select is not None:
        kept = []
        for name in select:
            finding, _ = policy.path(types, name.split("."))
            if finding is None:
                kept.append(name)
            elif finding[:2] not in seen and finding[1] in policy.elements.get(finding[0], {}):
                seen.add(finding[:2])
                withheld.append(policy.entry(*finding))
        select = kept
    return select, withheld, types


def query(db, tree: Mapping, *, raw: bool = False) -> dict:
    """Run the query ``tree`` (spec section 11) and answer ``{"rows", "explain", "version", "query",
    "terms"}`` as JSON-ready data (see the module docstring: rows, sensitivity, raw mode).  ``tree["as_of"]``
    is the ``ref`` of a version an earlier answer gave; without it the current version is read."""
    _check_tree(tree)
    snapshot = _snapshot(db, tree.get("as_of"))
    if raw:
        return _raw_query(snapshot, tree)
    session = semantic(snapshot)
    policy = _Withholding(session.catalog())
    if tree.get("of_type") is not None and tree["of_type"] not in policy.entities:
        raise ProtoValidationError(f"The semantic catalog describes no type {tree['of_type']!r}")
    select, withheld, types = _screen(policy, tree)
    aggregate = tree.get("aggregate")
    default = select is None and aggregate is None
    if default:
        select = sorted({name for type_name in policy.candidates(types) for name in policy.allowed(type_name)})
    run = {**tree, "as_of": _version(snapshot)["ref"]} if "as_of" in tree else dict(tree)
    if aggregate is None:
        run["select"] = select
    answer = session.query(run, members=True)
    if aggregate is not None:
        rows = to_json(answer["rows"])
    else:
        rows = []
        for values, version in zip(answer["rows"], answer["members"]):
            type_name = type(version.record).__type_name__
            shown = set(policy.allowed(type_name)) if default else None
            rows.append({"entity": None if version.entity is None else {"cell": version.entity.name},
                         "type_name": type_name,
                         "values": {name: _shallow(value) for name, value in values.items()
                                    if shown is None or name in shown}})
    return {"rows": rows, "explain": to_json(answer["explain"]), "version": to_json(answer["version"]),
            "query": to_json(dict(tree)), "terms": to_json(answer["terms"]) + withheld}


# -- raw mode ------------------------------------------------------------------------------------------------

class _StoredReader(Reader):
    """What raw evaluation reads beyond the element: cells and roots of the snapshot; no catalogued
    defaults (a value not stored is MISSING)."""

    def __init__(self, snapshot):
        self._snapshot = snapshot

    def cell(self, name: str):
        return self._snapshot.get_mutable(name)

    def root(self, name: str):
        if name.startswith("__"):
            raise ProtoValidationError(f"The root {name!r} is reserved; raw mode reads the application's roots")
        return self._snapshot.get_root_object(name)


def _reserved_roots(node) -> None:
    stack = [node]
    while stack:
        item = stack.pop()
        if isinstance(item, list) and item and isinstance(item[0], str):
            if item[0] == "root" and len(item) == 2 and isinstance(item[1], str) and item[1].startswith("__"):
                raise ProtoValidationError(f"The root {item[1]!r} is reserved; raw mode reads the application's "
                                           f"roots")
            if item[0] != "const":
                stack.extend(item[1:])


def _stored_members(reader: _StoredReader, origin) -> list:
    """``(stored value, element)`` of each member a ``from`` reaches, as ``via`` reaches them (format spec,
    "Traversal"), keeping the stored value (an identity handle) beside the element it designates."""
    if origin[0] == "root":
        start, roles = origin, ()
    else:
        start, roles = origin[1], origin[2:]
    if start[0] == "cell":
        found = reader.cell(start[1])
        current = [] if found is None else [(MutableObject(start[1]), found)]
    else:
        current = _reach(reader, reader.members(reader.root(start[1])))
    for role in roles:
        current = _reach(reader, (unwrap_entry(raw) for _, element in current
                                  for raw in reader.members(_stored_step(element, role))))
    return current


def _reach(reader: _StoredReader, raws) -> list:
    out, seen = [], set()
    for raw in raws:
        key = _identity(raw)
        if key not in seen:
            seen.add(key)
            followed = reader.follow(raw)
            if followed is not MISSING:
                out.append((raw, followed))
    return out


def _raw_query(snapshot, tree) -> dict:
    """The query over the stored values alone (see "Raw mode")."""
    _reserved_roots([tree["from"], tree.get("where"), tree.get("aggregate")])
    policy = _Withholding(_catalog_or_none(snapshot))
    select, _, _ = _screen(policy, {**tree, "of_type": None})       # raw mode reports no catalog term
    reader = _StoredReader(snapshot)
    where, aggregate, of_type = tree.get("where"), tree.get("aggregate"), tree.get("of_type")
    conjuncts = [] if where is None else list(where[1:]) if where[0] == "and" else [where]
    members = []
    for raw, element in _stored_members(reader, tree["from"]):
        if not is_atom(element) or (of_type is not None and _stored_type(element) != of_type):
            continue
        if all(_truth("where", evaluate(c, element, reader=reader), "a conjunct of the where") for c in conjuncts):
            members.append((raw, element))
    if aggregate is not None and aggregate[0] == "count":
        rows = [{"count": len(members)}]
    elif aggregate is not None:
        total = 0
        for _, element in members:
            value = _number("sum", evaluate(aggregate[1], element, reader=reader))
            if value is not MISSING:
                total = _arithmetic("+", total, value)
        rows = [{"sum": to_json(total)}]
    else:
        rows = []
        for raw, element in members:
            type_name = _stored_type(element)
            if select is None:
                types = frozenset({type_name}) if type_name in policy.entities else None
                values = {name: value for name, value in element._own.items()
                          if policy.path(types, [name])[0] is None}
            else:
                values = {}
                for name in select:
                    value = evaluate(["attr", *name.split(".")], element, reader=reader)
                    if value is not MISSING:
                        values[name] = value
            handle = raw if isinstance(raw, MutableObject) else raw["entity"] \
                if isinstance(raw, dict) and isinstance(raw.get("entity"), MutableObject) else None
            rows.append({"entity": None if handle is None else {"cell": handle.name}, "type_name": type_name,
                         "values": {name: _shallow(value) for name, value in values.items()}})
    explain = {"from": tree["from"], "plan": None, "indexed": [], "residual": conjuncts}
    return {"rows": rows, "explain": to_json(explain), "version": _version(snapshot), "query": to_json(dict(tree)),
            "terms": []}

