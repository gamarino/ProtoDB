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

Refusals instead of silent empty answers (spec section 11, R283)
    While a query is evaluated, the expression language follows the agent-query rules
    (:func:`~proto_db.semantic.expressions.agent_rules`), in both modes: a comparison whose present operands
    (neither MISSING nor nil) have different D28 type ranks (a date with a text) raises ``ProtoValidationError``
    naming the operator and the two kinds, where the language answers false, also when the planner answers it
    through an index (then every element of the end is checked); a ``root`` or ``cell`` origin of the agent's
    tree that the version read does not list raises one naming it, where the language gives no members (checked
    on the tree before evaluation; a listed origin holding nil exists, and is empty).  Only the agent's own
    expressions follow these rules: a catalog-declared expression evaluated meanwhile (a computed attribute, an
    aggregate) keeps D28 (:func:`~proto_db.semantic.expressions.total_rules`).  Both refusals are decided after
    the withholding screen, by kinds of values the agent may see or by a name it supplied, so neither tests a
    withheld value.

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
      subtype included).  A step reaches known types only when every candidate declares it as a role to a
      catalogued type; after any other step (one the catalog does not declare, as on an uncatalogued record,
      or a field, whose value is not type-checked) the type is unknown again, so a step never hides what
      follows it: ``who.name`` through an uncatalogued record holding a customer is withheld, and a ``where``
      on it refused (no yes/no oracle).  The cost is on the safe side: an uncatalogued record's attribute that
      shares its name with a catalogued personal or secret one is withheld too.
    - The static check assumes that a declared role holds its declared target type (or a subtype) and that a
      cell holds the type its ``Type#`` name prefix names (R260).  The semantic layer keeps both true, but a
      write outside it (the plain transaction API, a legacy writer) can break them, so the guarantee does not
      rest on them: while a query is evaluated, every member it reaches through a declared role or a
      catalogued cell (in the ``from``, the ``where``, an aggregate, a selected path, the default selection) is
      checked against the type the static check assumed there, every member of a ``many`` end, not only those
      a ``where`` keeps.  A member of another type refuses the whole query with ``ProtoCorruptionError``, whose
      message names neither a value nor the member (the operator's log names the role and the types).  The
      cost: a type comparison per member reached; for a ``via`` ending in an indexed ``many`` end, one pass
      over the whole end, whatever the plan selects (agent queries only).  A damaged part of a database
      refuses agent queries that reach it (on the safe side).
    - A value a row would show is checked against that row's own type before anything is evaluated: a
      selected path that reads a withheld attribute is left out of that row's selection (the default
      selection the same way) and is never evaluated, so a withheld Python-only computed attribute never
      runs.  ``terms`` gives every element the query used with its own declared ``"sensitivity"`` (one of
      ``public``, ``internal``, ``personal``, ``secret``); a withheld element adds ``"withheld": True`` and
      ``"withheld_because"``, either ``{"reads": {"type_name", "name", "sensitivity"}}`` (the first withheld
      attribute it reads, itself included) or ``{"python_only": True}``, so the agent knows what it did not
      get and why.
    - No stored value reaches the agent through an error.  Each operation first opens the version (its
      snapshot and catalog) and checks the agent's own arguments, whose mistakes are explained plainly; then
      everything that reads the version runs inside one scrub (:func:`~proto_db.semantic.entities.scrubbed`):
      a failure of that evaluation (the ``from`` traversal, the ``where``, the aggregate, a selected path, a
      ``describe`` or a listing; a ``via`` or a ``root`` reaches values whatever the query names) is reported
      as ``ProtoValidationError`` by the part it arose in, its kind and its operand's type ("a str", "a record
      of type T"), never by its message or a value.  One caused by a ``LookupError`` or an ``AttributeError``
      (likely a programming error) is also logged, with the original exception, at WARNING.
    - Any other failure of an operation, opening included (a corruption, a storage or I/O error, a closed
      space: anything that is not an evaluation failure; and the code of the installed modules differing from
      the stored catalog, whose message quotes stored catalog values), keeps its type, so a client knows the store is
      damaged or unavailable, but its message is replaced by one that names only the operation and the kind
      (a corruption's message may preview the stored bytes), raised ``from None``; the original exception is
      logged at ERROR.  Both logs go to ``logging.getLogger("proto_db.agent")``, the operator's side (the MCP
      server keeps it off its stderr, :mod:`proto_db.agent.mcp_server`).

Raw mode (``raw=True``; the agent experiment's control group)
    The catalog is not reported.  :func:`list_entities` lists the names of the roots (reserved ``__`` roots
    excepted) and of the cells (spec section 11), paged as above; :func:`describe` gives what the stored
    records of a type hold (attribute names and the kinds of the values stored); :func:`check_answerable`
    matches stored type and attribute names; :func:`query` runs the same engine over the stored values alone:
    no catalogued defaults, no subtypes (``of_type`` is the stored type), no ``terms``, and index planning
    where the stored structure allows it (a collection of records; never the entries of a maintained key,
    whose key only the catalog explains); an attribute a row does not store is refused, never read as missing.
    Reserved roots are not available.  Sensitivity still holds: when
    the database has a catalog, its personal and secret attributes are withheld as above, and a refusal says
    only that an attribute is not available (it names no catalog type and no sensitivity).  The policy is
    read from the stored catalog alone, after the checks that need no code, so a server whose process cannot
    import the modules withholds exactly what a configured one does; only an absent ``__semanticCatalog``
    root means "nothing to withhold", and a stored catalog that cannot be decoded or checked makes the
    operation refuse (``ProtoCorruptionError`` or ``ProtoNotSupportedError``, by its kind only), never serve
    everything (fail closed).

The version read and today's policy
    Sensitivity is today's disclosure policy, not history (spec section 5 lets a later module redefine it).
    A :func:`query` with ``as_of`` withholds what the catalog of that version withholds and what the
    current version's catalog withholds: the stricter wins, element by element, and a term reports the
    stricter declared sensitivity.  In raw mode a version older than the catalog (data written before the
    first install) is read under the current catalog's policy.  ``describe`` stays a photograph of one
    version (R259): it shows what a version declares, never values.
"""
from __future__ import annotations

import base64
import binascii
import datetime
import functools
import json
import logging
import uuid
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation

from ..collections.lists import List
from ..collections.sets import Set
from ..core.atom import OpaqueAtom
from ..core.dbobject import DBObject
from ..catalog.atoms import SEMANTIC_CATALOG_ROOT, semantic_catalog_value
from ..errors import ProtoCorruptionError, ProtoUserError, ProtoValidationError
from ..semantic import semantic
from ..semantic import compose as _compose
from ..semantic.entities import check_query, is_evaluation_failure, query_stored, scrubbed
from ..semantic.store import checked_stored_model
from ..semantic.expressions import agent_rules, checking_reached, unwrap_entry
from ..values import MutableObject, check_text, is_atom

__all__ = ["list_entities", "describe", "check_answerable", "query", "to_json", "from_json"]

#: The sensitivities kept from agents (spec section 11).
WITHHELD = frozenset({"personal", "secret"})
#: The one-key objects :func:`from_json` reads as typed values.
_TYPED = ("decimal", "date", "time", "datetime", "bytes", "cell")
#: The operator's log: what the agent is not told (see "Sensitivity").
_LOG = logging.getLogger("proto_db.agent")
_LOG.addHandler(logging.NullHandler())      # a library's logger: the application decides where it goes


def _boundary(operation: str):
    """The agent boundary of an operation: a failure that is not an evaluation failure (a corruption, a storage
    or I/O error, a closed space) keeps its type, but its message names only ``operation`` and the kind; it is
    raised from None, and the original is logged at ERROR (see "Sensitivity")."""
    def wrap(function):
        @functools.wraps(function)
        def run(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except Exception as error:      # noqa: BLE001 -- every kind is re-raised, with its type
                if is_evaluation_failure(error):
                    raise
                _LOG.error("The agent operation %s failed with %s; the agent was told the kind only", operation,
                           type(error).__name__, exc_info=error)
                withheld = _content_free(error, operation)
            raise withheld from None
        return run
    return wrap


def _content_free(error: Exception, operation: str) -> Exception:
    """An exception of ``error``'s type whose message names only ``operation`` and that type."""
    kind = type(error).__name__
    message = (f"{operation} failed: {kind}; the message is withheld from agents, since it may hold stored data "
               f"(the operator's log 'proto_db.agent' has it)")
    try:
        withheld = type(error)(message)
    except Exception:                       # noqa: BLE001 -- a type that takes no message: the nearest that does
        withheld = (OSError if isinstance(error, OSError) else RuntimeError)(message)
    if hasattr(error, "code"):
        withheld.code = error.code
    return withheld


# -- JSON ----------------------------------------------------------------------------------------------------

def to_json(value):
    """``value`` as JSON-ready data: a ``Decimal`` as its string, a date, time or datetime in ISO 8601, an
    identity handle as ``{"cell": name}``, bytes as ``{"bytes": base64}``, a map as an object with string
    keys, a list, tuple, ``List`` or ``Set`` as an array.  Anything else is ``ProtoValidationError``, a record
    included: a record is reached with ``from`` and summarized in a row (:func:`_flat`), never written out
    whole, so no path can write a value the withholding policy has not seen (fail closed)."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ProtoValidationError("A float that is not finite has no JSON form")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ProtoValidationError("A Decimal that is not finite has no JSON form")
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return {"bytes": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, MutableObject):
        return {"cell": value.name}
    if isinstance(value, Mapping):
        return {_key(key, to_json): to_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json(item) for item in value]
    if isinstance(value, (List, Set)):
        return [to_json(item) for item in value.elements()]
    if _is_record(value):
        raise ProtoValidationError(f"A record ({_stored_type(value)}) has no JSON form: it is reached with 'from'")
    raise ProtoValidationError(f"{type(value).__name__} has no JSON form")


def _key(key, write) -> str:
    """A map key as a JSON object key.  A scalar, a UUID or a timedelta is its text; a handle its cell name; any
    other key (a date, bytes, a tuple, a record) is the text of what ``write`` (:func:`to_json` or
    :func:`_flat`) makes of it, so a key is never written through a ``str`` the withholding policy has not seen:
    a record key is refused by :func:`to_json` and summarized by :func:`_flat`."""
    if isinstance(key, str):
        return key
    if key is None or isinstance(key, (bool, int, float, Decimal, uuid.UUID, datetime.timedelta)):
        return str(key)
    if isinstance(key, MutableObject):
        return key.name
    written = write(key)
    return written if isinstance(written, str) else json.dumps(written, ensure_ascii=False, sort_keys=True)


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
    inlined (see "Rows").  A plain list, tuple or map is summarized item by item, so a record inside one is
    ``{"record": type}`` too."""
    if _bound(value):
        return {"entity": {"cell": value["entity"].name}, "version": {"record": _stored_type(value["version"])}}
    if isinstance(value, (List, Set)):
        return {"collection": "list" if isinstance(value, List) else "set", "count": value.count}
    if _is_record(value):
        return {"record": _stored_type(value)}
    if isinstance(value, (list, tuple)):
        return [_flat(item) for item in value]
    if isinstance(value, Mapping):
        return {_key(key, _flat): _flat(item) for key, item in value.items()}
    return to_json(value)


def _is_record(value) -> bool:
    """Whether ``value`` is a stored record: a :class:`DBObject`, or an :class:`OpaqueAtom` when this process
    has no class for its type (the raw server never imports the model's code)."""
    return isinstance(value, (DBObject, OpaqueAtom)) and is_atom(value)


def _own(record) -> Mapping:
    """The stored attributes of ``record``: a record's own fields (an opaque record's stored fields); a stored
    collection (an atom too) has none."""
    if isinstance(record, DBObject):
        return record._own
    if isinstance(record, OpaqueAtom):
        return record._state()
    return {}


def _value(value, policy: "_Strictest"):
    """A row's value: :func:`_flat`, except that a version-bound role shows the version it binds (the
    product as sold): ``{"entity": {"cell"}, "version": {"type_name", "values"}}``, its values withheld as a
    row's are."""
    if not _bound(value):
        return _flat(value)
    record = value["version"]
    type_name = _stored_type(record)
    return {"entity": {"cell": value["entity"].name},
            "version": {"type_name": type_name,
                        "values": {name: _flat(item) for name, item in _own(record).items()
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


def _stored_model(snapshot):
    """The stored catalog of ``snapshot`` after the checks that need no code
    (:func:`~proto_db.semantic.store.checked_stored_model`), or ``None`` when the version has no
    ``__semanticCatalog`` root: the withholding policy of raw mode, which never depends on the process having
    the modules' code (fail closed).  Only an absent root means "no catalog"; a catalog that cannot be decoded
    or checked raises (``ProtoCorruptionError``, ``ProtoNotSupportedError``) and the operation serves nothing."""
    return checked_stored_model(semantic_catalog_value(snapshot.get_root_object(SEMANTIC_CATALOG_ROOT)))


def _verified_catalog(session, operation: str) -> dict:
    """The verified catalog of ``session`` (catalog mode).  A ``ProtoValidationError`` here is the code of the
    installed modules differing from the stored catalog (R263): its message quotes stored catalog values, so
    the agent is told only the operation and the kind, as for a storage failure, and the operator's log keeps
    the message (see "Sensitivity")."""
    try:
        return session.catalog()
    except ProtoValidationError as error:
        _LOG.error("The agent operation %s could not open the semantic catalog (%s); the agent was told the kind "
                   "only", operation, type(error).__name__, exc_info=error)
        withheld = _content_free(error, operation)
    raise withheld from None


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

    def assumed(self, holder, step: str):
        """The types the static check assumes for a member reached through ``step`` of the record ``holder``
        (a declared role to a catalogued type: its target and subtypes) or, with no ``holder``, held by the
        cell named ``step`` (its ``Type#`` prefix, R260); ``None`` when it assumes nothing (an undeclared or
        uncatalogued step, a field: what follows is checked against every catalogued type, C1-b)."""
        if holder is None:
            return self.cell_type(step)
        element = self.elements.get(_stored_type(holder), {}).get(step)
        if element is not None and element.get("target") in self.entities:
            return self.subtypes(element["target"])
        return None

    def check_member(self, holder, step: str, member) -> None:
        """Refuse a member whose real type is not one :meth:`assumed` gives (see "Sensitivity": fail closed)."""
        expected = self.assumed(holder, step)
        if expected is not None and _stored_type(member) not in expected:
            where = "the cell it was read from" if holder is None else f"the role {_stored_type(holder)}.{step}"
            raise ProtoCorruptionError(f"A member reached through {where} is a {_stored_type(member)!r}, not of the "
                                       f"type the catalog declares there; the database holds data the semantic "
                                       f"layer did not write, and the agent query is refused")

    def sensitivity(self, type_name: str, name: str) -> str:
        """The element's own declared sensitivity (``internal`` when not stated)."""
        return self.elements.get(type_name, {}).get(name, {}).get("sensitivity", "internal")

    def path(self, types, steps, seen: frozenset = frozenset()) -> tuple:
        """``(finding, types reached)`` for the path ``steps`` read on members of ``types`` (``None``: any):
        the first withheld ``(type, name, sensitivity)`` the path reads (``sensitivity`` is ``None`` for a
        Python-only computed attribute), else ``None``; and the types its last step reaches (``None`` when not
        known).

        A step reaches known types only when every candidate type declares it as a role whose target the catalog
        describes.  Otherwise (a step some candidate does not declare, as on an uncatalogued record, or one that
        is not such a role: a field may hold anything, field values are not type-checked) what it reaches is
        unknown, ``None``, and the next step is checked against every catalogued type: a step never hides what
        follows it.  This is the safe side: an attribute name a non-sensitive record shares with a catalogued
        personal one is withheld too."""
        for step in steps:
            following, known = set(), True
            for type_name in self.candidates(types):
                element = self.elements.get(type_name, {}).get(step)
                if element is None:
                    known = False
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
                else:
                    known = False
            types = frozenset(following) if known and types is not None else None
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
            found = (type_name, name, None)
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
            why = (f"is {sensitivity}" if sensitivity is not None else
                   "is computed in Python only, so what it reads cannot be checked")
            raise ProtoValidationError(f"The query's {part} reads the attribute {name!r} of {type_name}, which {why}: "
                                       f"personal and secret attributes are kept from agents (spec section 11)")

    def term(self, term: dict) -> dict:
        """``term`` of the engine with the element's own ``sensitivity``; a withheld element adds
        ``"withheld": True`` and ``"withheld_because"``: ``{"reads": {"type_name", "name", "sensitivity"}}``,
        the first withheld attribute it reads (itself included), or ``{"python_only": True}``."""
        out = {**term, "sensitivity": self.sensitivity(term["type_name"], term["name"])}
        finding = self.path(frozenset({term["type_name"]}), [term["name"]])[0]
        if finding is not None:
            type_name, name, sensitivity = finding
            out["withheld"] = True
            out["withheld_because"] = {"python_only": True} if sensitivity is None else \
                {"reads": {"type_name": type_name, "name": name, "sensitivity": sensitivity}}
        return out

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
    """``(snapshot, where the page starts)``: ``None`` for the first page, else ``(kind, name)``, the last
    name a page gave.  A continuation token pins the version its first page read, so the pages of one
    listing read one version."""
    if type(limit) is not int or not 1 <= limit <= LIST_LIMIT_MAX:
        raise ProtoValidationError(f"limit is an int from 1 to {LIST_LIMIT_MAX}, not {limit!r}")
    if after is None:
        return _snapshot(db), None
    try:
        token = json.loads(base64.b64decode(after, validate=True)) if isinstance(after, str) else None
    except (binascii.Error, ValueError) as e:
        raise ProtoValidationError("after is the 'next' token a list_entities answer gave") from e
    if not isinstance(token, dict) or not isinstance(token.get("ref"), str) or token.get("kind") not in _KINDS \
            or not isinstance(token.get("after"), str) or not token["after"]:
        raise ProtoValidationError("after is the 'next' token a list_entities answer gave")
    try:
        check_text(token["after"])
    except ProtoValidationError:
        raise ProtoValidationError("after is the 'next' token a list_entities answer gave") from None
    return _snapshot(db, token["ref"]), (token["kind"], token["after"])


#: What a listing pages over, in this order: root names (raw mode only), then cell names.
_KINDS = ("root", "cell")


def _names(snapshot, kind: str, after):
    """The names of the roots or of the cells of ``snapshot`` after ``after`` (``None``: from the first), in
    name (code point) order: a cursor from the last name given, never a sort of every name."""
    names = snapshot.iter_root_names if kind == "root" else snapshot.iter_mutable_names
    return names() if after is None else names(after=after)


def _page(snapshot, kinds, start, keep, limit: int) -> tuple:
    """``({kind: names}, continuation or None)``: the next names after ``start`` that ``keep(kind, name)``
    accepts, at most ``limit`` in all, roots before cells."""
    out, last, given = {kind: [] for kind in kinds}, None, 0
    started = start is None
    for kind in kinds:
        if not started and kind != start[0]:
            continue
        after = None if started else start[1]
        started = True
        for name in _names(snapshot, kind, after):
            if not keep(kind, name):
                continue
            if given == limit:
                token = json.dumps({"ref": _version(snapshot)["ref"], "kind": last[0], "after": last[1]})
                return out, base64.b64encode(token.encode("utf-8")).decode("ascii")
            out[kind].append(name)
            last, given = (kind, name), given + 1
    return out, None


def _cell_type(name: str):
    return name.rsplit("#", 1)[0] if "#" in name else None


@_boundary("list_entities")
def list_entities(db, filter: str | None = None, *, limit: int = LIST_LIMIT, after: str | None = None,
                  raw: bool = False) -> dict:
    """Where an agent starts: ``{"entities": [{"type_name", "label", "meaning", "cells"}], "next"}``.  The
    types are those the catalog describes, filtered as :meth:`SemanticSession.list_entities
    <proto_db.semantic.session.SemanticSession.list_entities>` does; ``cells`` are the names of the cells
    holding entities of exactly that type (R260: ``<type name>#<hex>``), at most ``limit`` names in all, in
    name (code point) order.  ``next`` is ``None`` when the listing is complete, else the token to pass as
    ``after`` for the next page, which reads the same version.

    Raw mode lists the stored structure only (spec section 11): ``{"roots", "cells", "next"}``, the names of
    the roots (reserved ``__`` roots excepted), then of the cells, at most ``limit`` names in all per page,
    paged the same way; ``filter`` keeps the names that contain it."""
    if filter is not None and not isinstance(filter, str):
        raise ProtoUserError(f"The filter of list_entities is a str or None, not {type(filter).__name__}")
    snapshot, start = _page_start(db, limit, after)
    if raw:
        needle = None if filter is None else filter.casefold()

        def wanted(kind: str, name: str) -> bool:
            return not (kind == "root" and name.startswith("__")) and (needle is None or needle in name.casefold())

        def listing() -> dict:
            page, token = _page(snapshot, _KINDS, start, wanted, limit)
            return {"roots": page["root"], "cells": page["cell"], "next": token}
        return scrubbed("list_entities", listing, log=_LOG)
    if start is not None and start[0] != "cell":
        raise ProtoValidationError("after is the 'next' token a list_entities answer gave")
    session = semantic(snapshot)
    _verified_catalog(session, "list_entities")
    listed = session.list_entities(filter)
    types = {entity["type_name"] for entity in listed}

    def cells() -> dict:
        page, token = _page(snapshot, ("cell",), start, lambda kind, name: _cell_type(name) in types, limit)
        by_type: dict = {}
        for name in page["cell"]:
            by_type.setdefault(_cell_type(name), []).append(name)
        return {"entities": [{**to_json(entity), "cells": by_type.get(entity["type_name"], [])} for entity in listed],
                "next": token}
    return scrubbed("list_entities", cells, log=_LOG)


@_boundary("describe")
def describe(db, type_name: str, *, raw: bool = False) -> dict:
    """The composed entity ``type_name``: attributes, roles, intents, constraints, provenance and
    certification (personal and secret attributes are listed, with their sensitivity).  Raw mode: what the
    stored records of the type hold, ``{"type_name", "cells", "records", "attributes": {name: [kinds]}}``
    (``records`` counts those held inside other records' collections)."""
    if not isinstance(type_name, str):
        raise ProtoValidationError(f"A type name is a str, not {type(type_name).__name__}")
    snapshot = _snapshot(db)
    if not raw:
        model = _verified_catalog(semantic(snapshot), "describe")
        if type_name not in model["entities"]:
            raise ProtoValidationError(f"The semantic catalog describes no type {type_name!r}; list_entities gives "
                                       f"the types it describes")
        return scrubbed("describe", lambda: to_json(_compose.describe(model, type_name)), log=_LOG)
    answer = scrubbed("describe", _stored_description, snapshot, type_name, log=_LOG)
    if not answer["cells"] and not answer["records"]:
        raise ProtoValidationError(f"No stored record has the type {type_name!r}")
    return answer


def _stored_records(snapshot):
    """``(value, in_cell)`` for every cell's value and every atom held in a collection of a cell's record:
    what raw mode describes and matches."""
    for name in _names(snapshot, "cell", None):
        record = snapshot.get_mutable(name)
        yield record, True
        if _is_record(record):
            for value in _own(record).values():
                if isinstance(value, (List, Set)):
                    for element in value.elements():
                        element = unwrap_entry(element)
                        if is_atom(element):
                            yield element, False


def _stored_description(snapshot, type_name: str) -> dict:
    """What the stored records of ``type_name`` hold (raw :func:`describe`)."""
    cells, nested, attributes = 0, 0, {}
    for record, in_cell in _stored_records(snapshot):
        if _stored_type(record) != type_name:
            continue
        if in_cell:
            cells += 1
        else:
            nested += 1
        for name, value in _own(record).items():
            attributes.setdefault(name, set()).add(_kind_of(value))
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


def _vocabulary(snapshot, model: Mapping | None) -> list:
    """``(candidate, [(via, text)])`` for everything a term may name: in the catalog ``model``, or, without one
    (raw mode), in the stored records."""
    out = []
    if model is None:
        types: dict = {}
        for record, in_cell in _stored_records(snapshot):
            if in_cell or _is_record(record):           # a nested collection is not a type to name
                names = types.setdefault(_stored_type(record), set())
                names.update(_own(record).keys())
        for type_name, names in sorted(types.items()):
            out.append(({"kind": "entity", "type_name": type_name, "name": None},
                        [("stored", type_name), ("stored", type_name.rsplit(".", 1)[-1])]))
            out.extend(({"kind": "attribute", "type_name": type_name, "name": name}, [("stored", name)])
                       for name in sorted(names))
        return out
    for type_name, entity in sorted(model["entities"].items()):
        texts = [("name", type_name), ("name", type_name.rsplit(".", 1)[-1])]
        texts += [("label", text) for text in entity.get("label", {}).values()]
        texts += [("synonym", text) for text in entity.get("synonyms", ())]
        out.append(({"kind": "entity", "type_name": type_name, "name": None}, texts))
        for element in entity.get("attributes", []) + entity.get("roles", []):
            kind = "role" if "target" in element else "attribute"
            out.append(({"kind": kind, "type_name": type_name, "name": element["name"]}, _texts(element)))
    return out


@_boundary("check_answerable")
def check_answerable(db, terms: list, *, raw: bool = False) -> dict:
    """``{"answerable", "matches", "unmatched", "suggestions"}`` for the terms of a question (see the module
    docstring).  ``matches`` maps every term to its candidates (empty when unmatched), ``suggestions`` every
    unmatched term to the candidates whose texts contain it; a candidate is ``{"kind", "type_name", "name",
    "via"}`` (``kind`` ``entity``, ``attribute`` or ``role``; ``name`` ``None`` for an entity; ``via``
    ``name``, ``label`` or ``synonym``, ``stored`` in raw mode)."""
    if isinstance(terms, str) or not isinstance(terms, (list, tuple)) \
            or not all(isinstance(term, str) and term.strip() for term in terms):
        raise ProtoValidationError(f"check_answerable takes a list of non-empty terms (str), not {terms!r}")
    snapshot = _snapshot(db)
    model = None if raw else _verified_catalog(semantic(snapshot), "check_answerable")
    return scrubbed("check_answerable", _answerable, snapshot, model, terms, log=_LOG)


def _answerable(snapshot, model: Mapping | None, terms) -> dict:
    vocabulary = _vocabulary(snapshot, model)
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

def _origins(tree: Mapping) -> list:
    """The ``root`` and ``cell`` origins ``(kind, name)`` of the ``from``, ``where`` and aggregate of ``tree``, in
    the order they appear (a ``via``'s origin, a bare root, an aggregate's ``over``), each once."""
    found, stack = [], [tree.get("aggregate"), tree.get("where"), tree["from"]]
    while stack:
        node = stack.pop()
        if isinstance(node, list) and node and isinstance(node[0], str) and node[0] != "const":
            if node[0] in ("root", "cell") and len(node) > 1 and isinstance(node[1], str):
                if (node[0], node[1]) not in found:
                    found.append((node[0], node[1]))
            else:
                stack.extend(reversed(node[1:]))
    return found


def _reserved_root(tree: Mapping):
    """The first reserved (``__``) root the ``from``, ``where`` or aggregate of ``tree`` names, else ``None``."""
    return next((name for kind, name in _origins(tree) if kind == "root" and name.startswith("__")), None)


def _listed(names, name: str) -> bool:
    """Whether ``name`` is among ``names``, a version's root or cell names in code point order (stops past it)."""
    for listed in names:
        if listed >= name:
            return listed == name
    return False


def _check_origins(snapshot, tree: Mapping, version: str) -> None:
    """Refuse the first ``root`` or ``cell`` origin of ``tree`` that the version read does not list (spec section
    11, R283): R265 gives an absent origin no members, so a misspelled name would answer 0 rows.  Checked on the
    tree, before anything is evaluated, so the refusal depends neither on the data nor on the plan.  A listed name
    exists, also when it holds nil (an empty origin).  The names came from the agent; no stored value is named."""
    for kind, name in _origins(tree):
        if kind == "root":
            present = snapshot.get_root_object(name) is not None or _listed(snapshot.iter_root_names(), name)
        else:
            present = snapshot.get_mutable(name) is not None or _listed(snapshot.iter_mutable_names(), name)
        if not present:
            raise ProtoValidationError(f"The {kind} {name!r} does not exist in {version}; an agent query refuses an "
                                       f"absent origin instead of answering it as empty (spec section 11, R283)")


def _screen(policy: _Withholding, tree) -> frozenset | None:
    """Refuse a ``from``, ``where`` or ``aggregate`` that reads an attribute ``policy`` withholds (see
    "Sensitivity"); return the types the members may have (``None``: any), for the default selection."""
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


#: The sensitivities from the least to the most strict.
_STRICTNESS = ("public", "internal", "personal", "secret")


class _Strictest:
    """The withholding of a query: the policy of the catalog of the version it reads, made stricter by the
    current version's (spec section 11, ruling I3).  Sensitivity is today's disclosure policy, not history: a
    later module that marks an attribute personal binds the reads of earlier versions too.  Every check is
    made against each policy and the strictest answer wins; the types (subtypes, candidates) are those of the
    version read."""

    def __init__(self, read: _Withholding, current: _Withholding | None = None):
        self.read = read
        self.policies = (read,) if current is None or current.model is None else (read, current)
        self.raw = read.raw
        self.entities = read.entities

    def subtypes(self, type_name: str) -> frozenset:
        return self.read.subtypes(type_name)

    def candidates(self, types) -> frozenset:
        return self.read.candidates(types)

    def screen(self, tree) -> frozenset | None:
        """:func:`_screen` against every policy; the member types of the version read."""
        types = _screen(self.read, tree)
        for policy in self.policies[1:]:
            _screen(policy, tree)
        return types

    def withheld(self, type_name: str, steps) -> bool:
        return any(policy.withheld(type_name, steps) for policy in self.policies)

    def check_member(self, holder, step: str, member) -> None:
        """Each policy's :meth:`_Withholding.check_member`: a member must have a type every policy assumed."""
        for policy in self.policies:
            policy.check_member(holder, step, member)

    def allowed(self, type_name: str) -> list:
        return [name for name in self.read.allowed(type_name)
                if not any(policy.withheld(type_name, [name]) for policy in self.policies[1:])]

    def term(self, term: dict) -> dict:
        """The term of the version read (:meth:`_Withholding.term`), with the strictest declared sensitivity
        and, when only a stricter policy withholds it, that policy's reason."""
        out = self.read.term(term)
        for policy in self.policies[1:]:
            stricter = policy.term(term)
            if _STRICTNESS.index(stricter["sensitivity"]) > _STRICTNESS.index(out["sensitivity"]):
                out["sensitivity"] = stricter["sensitivity"]
            if "withheld" not in out and "withheld" in stricter:
                out["withheld"], out["withheld_because"] = True, stricter["withheld_because"]
        return out


@_boundary("query")
def query(db, tree: Mapping, *, raw: bool = False) -> dict:
    """Run the query ``tree`` (spec section 11) and answer ``{"rows", "explain", "version", "query",
    "terms"}`` as JSON-ready data (see the module docstring: rows, sensitivity, raw mode).  ``tree["as_of"]``
    is the ``ref`` of a version an earlier answer gave; without it the current version is read.  What is
    withheld is decided by the current catalog as well as by the catalog of the version read (the stricter
    wins), so a later tightening of a sensitivity binds every version."""
    check_query(tree)
    reserved = _reserved_root(tree) if raw else None
    if reserved is not None:
        raise ProtoValidationError(f"The root {reserved!r} is not available")
    snapshot = _snapshot(db, tree.get("as_of"))
    run = dict(tree)
    if "as_of" in tree:
        run["as_of"] = _version(snapshot)["ref"]
    session = None if raw else semantic(snapshot)
    read = _Withholding(_stored_model(snapshot) if raw else _verified_catalog(session, "query"), raw=raw)
    current = None if "as_of" not in tree else _Withholding(_stored_model(_snapshot(db)), raw=raw)
    policy = _Strictest(read, current)
    of_type = tree.get("of_type")
    if not raw and of_type is not None and of_type not in policy.entities:
        raise ProtoValidationError(f"The semantic catalog describes no type {of_type!r}")
    types = policy.screen(tree)
    _check_origins(snapshot, tree, "the version read (the one 'as_of' names)" if "as_of" in tree else
                   "the version read")
    aggregate, select = tree.get("aggregate"), tree.get("select")
    default = select is None and aggregate is None
    chosen: dict = {}

    def selection(type_name):
        """The paths a row of ``type_name`` evaluates: the withheld ones are left out before evaluation."""
        found = chosen.get(type_name)
        if found is None:
            if default:
                found = policy.allowed(type_name)
            else:
                found = [path for path in select if not policy.withheld(type_name, path.split("."))]
            chosen[type_name] = found
        return found
    if default and not raw:
        run["select"] = sorted({name for type_name in policy.candidates(types) for name in policy.allowed(type_name)})
    use = None if aggregate is not None or (default and raw) else selection

    def answered() -> dict:
        answer = query_stored(snapshot, run, with_members=True, selection=use) if raw else \
            session.query(run, members=True, selection=use)
        return _answer(answer, tree, policy, raw)
    with checking_reached(policy.check_member), agent_rules():
        return scrubbed("the query", answered, log=_LOG)


def _answer(answer: dict, tree: Mapping, policy: _Strictest, raw: bool) -> dict:
    """The engine's ``answer`` to ``tree`` as the agent receives it (see the module docstring, "Rows")."""
    aggregate = tree.get("aggregate")
    default = tree.get("select") is None and aggregate is None
    if aggregate is not None:
        rows = to_json(answer["rows"])
    else:
        rows = []
        for values, version in zip(answer["rows"], answer["members"]):
            record = version.record
            type_name = _stored_type(record)
            if default and raw:             # the record's stored attributes: read, never evaluated
                values = {name: value for name, value in _own(record).items()
                          if not policy.withheld(type_name, [name])}
            rows.append({"entity": None if version.entity is None else {"cell": version.entity.name},
                         "type_name": type_name,
                         "values": {name: _value(value, policy) for name, value in values.items()}})
    terms = [policy.term(term) for term in answer["terms"]]
    return {"rows": rows, "explain": to_json(answer["explain"]), "version": to_json(answer["version"]),
            "query": to_json(dict(tree)), "terms": to_json(terms)}
