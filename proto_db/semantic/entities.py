"""Entities and relations through the semantic session, in direct mode (spec sections 6, 7 and 10; ruling
R260, R262, R272; Task 6).

The semantic session (:mod:`proto_db.semantic.session`) delegates its operations here.  In direct mode each
operation reads and writes cells of its transaction at once, under today's read set and conflict rules:
every cell it reads enters the read set (``Transaction.get_mutable``), every cell it changes is written with
``Transaction.set_mutable``.  A session over a ``Snapshot`` reads only; its write operations raise
``ProtoUserError`` (preflight F24).

In intent mode (``Database.new_transaction(intents=True)``, :mod:`proto_db.semantic.intents`) the same
operations run on the transaction's record view when they are recorded, and on the cells current at commit
when they are replayed: the session gives them their store (``_store``), their lock (``_guard``, none at
replay) and the names of the cells they create (``_new_cell``, recorded once, never drawn at replay).

Storage (spec section 6)
    - An entity lives in a mutable cell named ``<type name>#<32 hex digits>`` (R260) whose value is a record
      of its semantic class; the cell's :class:`~proto_db.values.MutableObject` handle is its identity.
    - A ``one`` end (cardinality ``one`` or ``0..1``) is an attribute of the version holding the target's
      handle, or ``{"entity": handle, "version": record}`` for ``binding="version"`` (the product as sold).
      It is absent until first linked and holds nil after an unlink.  A ``one`` end that is linked is never
      overwritten: linking it again is refused ("unlink it first"); an unlink and a link in one transaction
      move it.
    - ``binding="version"`` binds the target's **field version**, current when the operation is applied (in
      direct mode the transaction's view, validated by its read set at commit; in intent mode at replay):
      the committed record the transaction read while the target's fields equal it; otherwise the
      transaction's kept record for the target while the fields equal it; otherwise the record in view,
      which becomes the kept record.  The rule reads the fields only, never how they were written (an
      update to the same value keeps the committed record bound; fields back to a kept state bind the
      kept record again).  Writes to the target's own ends and maintained aggregates
      never start a new bound version (each line joining ``Product.order_lines`` would otherwise bind a
      record no commit ever holds), and a later field change does not move an earlier binding.  The
      committed record of a target that holds the inverse end can never be bound: it holds the binder, and
      immutable records hold no cycle.
    - A relationship atom is created by ``relate`` with every one of its roles: an atom never changes, so a
      participant it lacks could never be added.
    - A ``many`` end (``many`` or ``1..*``) is an attribute holding a ``List`` (``ordered``) or a ``Set`` of
      its members: the handles of the entities, or the relationship atoms themselves.  It is absent until
      its first link, then keeps its collection (empty after the last unlink); it never holds nil.  A relationship atom
      lives once: every end that holds it references the same atom (one oid).
    - ``index_by`` (R262) is declared on the end that holds the collection or on its inverse ``one`` end (a
      relationship's ``Role``): a path of one member attribute is ``RangeIndex(f"by_{path}", path)`` on the
      collection (whose members must then be records: an index reads stored attributes, never through a
      handle).  A path ``role.attribute`` through an identity role of the member is a **maintained key**: the
      collection holds entry records ``{member, key}`` (plain ``DBObject``\\ s, format 4) indexed by
      ``RangeIndex("by_role_attribute", "key")``, and every change of the keyed attribute, or of the member's
      role, re-keys the entries in the same transaction.  Other shapes (a longer path, a path through a
      version-bound role or a non-stored attribute, two maintained keys, a maintained key beside member
      paths, a maintained key with no way back from the member to its collection) raise
      ``ProtoNotSupportedError`` when the end is written.
    - A maintained aggregate (``computed_stored``, R272) is stored in its owner's version, created as the int
      0, and moved by deltas (spec section 10): when a member joins or leaves its ``over`` end, by the
      member's contribution; when a member changes, by the contribution after minus the one before (its
      ``term`` where its ``where`` is true).  It is never recomputed over its collection, with one exception:
      an aggregate a later module added to a type whose entities existed before is absent from their records;
      it reads as computed from the relation's current members, and the first write that moves it stores it,
      seeded by that one computation (spec section 5: attributes are addable).  A change of an
      aggregate is a change of its owner, so it moves the aggregates the owner contributes to, along the
      inverses.  A maintained aggregate over a ``one`` end, or on a relationship (whose atoms never change),
      is ``ProtoNotSupportedError``.

Atomicity
    A transaction is meant for one thread at a time; a misuse from several threads sees each call happen
    entirely before or after each semantic operation.  The transaction's one lock gives that
    (``Transaction.semantic_operation``; "Locks" on ``Transaction``):

    - every write operation (create, update, link, unlink, relate, with the version bindings inside them)
      holds the lock throughout, and checks the transaction is active once it holds it, so an operation
      that waited for a commit or an abort fails before its first step;
    - every read of a session over a transaction (get, navigate, query, and a version's attribute, whose
      roles navigate) holds it for the call, so it sees an operation entirely or not at all; a session over
      a ``Snapshot`` reads an immutable version and takes no lock;
    - iterating :class:`Members` is many calls, not one read: each member's current version is read as the
      iteration reaches it, each read atomic on its own (like iterating any collection another thread
      writes);
    - the transaction's own methods (``get_mutable``, ``set_mutable`` and the others that read or write its
      state) hold the lock for the call, and so do ``commit`` and ``abort``: a commit from another thread
      waits for the operation in progress and publishes all of it, and one made from inside an operation
      on its own thread (a computed attribute, a hook) is refused (``ProtoUserError``).

Constraints (spec section 10; :mod:`proto_db.semantic.constraints`)
    In direct mode the outermost write operation (a primitive, or an intent with everything it runs) is all
    or nothing in the transaction (``Transaction.semantic_operation``): it checks the enforced invariants of
    the entities whose operands it changed, on the transaction's view, before it returns; a violation
    (``ProtoConstraintError``), like any failure, undoes its writes, and the transaction goes on.  The
    commit checks the cardinality of every entity the operations wrote.  In intent mode the replay checks
    both on the combined result (:mod:`proto_db.semantic.intents`).

Versions and navigation
    The session gives :class:`Version`\\ s: views over a stored record that navigate.  ``version.record`` is
    the stored ``DBObject``, ``version.entity`` the identity handle (``None`` for a relationship atom), and
    attribute access reads the record (fields, defaults, computed attributes, through the session's
    registry), except that a role navigates: a ``one`` end gives the target's current version (its bound
    version for ``binding="version"``), a ``many`` end gives :class:`Members`.  ``version["name"]`` reads an
    attribute named like ``entity`` or ``record``.

    An intent of the version's type (``order.add_line``) is a :class:`BoundIntent`: calling it invokes the
    intent through the session (:meth:`Operations.invoke`; logged in intent mode).  In intent mode, a read
    of a version records the fine read its caller decides on (:mod:`proto_db.semantic.intents`, "Fine reads
    and condition reads"); ``version._bound`` marks the version a role binds, whose reads record nothing.

    :class:`Members` is the member view of a ``many`` end (preflight row 44): it yields the members as
    versions, never the entries a maintained-key end stores.  ``members.collection`` is the stored ``List``
    or ``Set`` (``None`` while the end holds nothing), ``members.ordered`` says which.

The query engine (preflight F17, F18)
    :meth:`Operations.query` is the session's single query engine.  The tree is spec section 11's
    ``{"from", "of_type"?, "where"?, "select"?, "aggregate"?, "as_of"?}``.  When ``from`` ends in a ``many``
    end of one element (``["via", ["cell", name], ..., role]``), the conjuncts of ``where`` that the planner
    can answer exactly go to it over that end's stored collection, so its indexes answer them: over a
    maintained-key end, a conjunct on the key's path is rewritten to ``["attr", "key"]`` over the entries
    before :func:`~proto_db.semantic.expressions.to_planner` (which stays single-name); over an end of
    records, a conjunct on stored fields.  A conjunct on a field with a catalogued default, a computed
    attribute, a path, or over an end of handles runs as a residual on the members.  ``explain`` says which:
    ``{"from", "plan", "indexed", "residual"}``.  With ``members=True`` a query without an aggregate also
    answers ``"members"``: the version each row reads, row by row (the agent interface names each row's
    entity and type with it, :mod:`proto_db.agent.operations`).  With ``selection``, a callable from a member's
    type name to the paths to select for it, a member's row evaluates only those paths (the agent interface
    never evaluates a path it withholds).  An evaluation failure (see :data:`_EVALUATION_FAILURES`) propagates
    with its message, tagged with the part of the query it arose in (``semantic_part``: the ``from``, the
    ``where``, the aggregate or a selected path); :func:`scrubbed` reports it by that part, its kind and its
    operand's type only (the agent interface).  Raw planning (no catalog) reads the kind of every element of
    the collection once, and only when some conjunct could plan (see :meth:`Operations._indexed`).
    A term never reads an attribute that is not there as missing (a sum over it would answer 0): with a
    catalog, a path step the members' type does not declare is refused before anything is read
    (:meth:`Operations._check_declared`), and a row whose type the ``from`` does not tell is checked as it is
    read: by the catalog if its type is catalogued, by what it stores otherwise (:class:`_QueryReader`); over
    stored structure alone, a step a row does not store is refused when it is read (:class:`StoredReader`).  A
    declared attribute a member does not hold is its default or MISSING, as before.
"""
from __future__ import annotations

import base64
import contextlib
import copy
import functools
from collections.abc import Mapping
from typing import NamedTuple

from .. import resolution
from ..collections.lists import List
from ..collections.sets import Set
from ..core.atom import Atom
from ..core.dbobject import DBObject
from ..errors import ProtoClosedError, ProtoCorruptionError, ProtoNotSupportedError, ProtoUserError, \
    ProtoValidationError
from ..indexing.keyspec import _step as _stored_step
from ..indexing.range import RangeIndex
from ..query.expressions import And
from ..query.linq import from_collection
from ..query.terms import Term
from ..values import MISSING, MutableObject, canonical, is_atom, is_orderable, order_key, values_equal
from . import compose as _compose
from . import constraints as _constraints
from .classes import IntentContext
from .descriptors import RegistryReader
from .expressions import (Reader, _arithmetic, check_kinds, checking, reached, refusing, total_rules, _identity, _number, _operand_error, _truth, check_ast, describe_operand,
                          evaluate, to_planner, unwrap_entry)

__all__ = ["Version", "Members", "BoundIntent", "SessionContext", "Operations", "Schema", "StoredReader", "check_query",
           "query_stored", "scrubbed", "is_evaluation_failure"]

#: The end cardinalities that hold one target; the others hold a collection.
_SINGLE = frozenset({"one", "0..1"})
#: The keys of a query tree (spec section 11).
_QUERY_KEYS = frozenset({"from", "of_type", "where", "select", "aggregate", "as_of"})


def _invalid(detail: str) -> ProtoValidationError:
    return ProtoValidationError(detail)


#: What evaluating an expression over unexpected values raises: the expression language's own errors, and
#: what a Python-only computed attribute raises over a value it does not expect.  :func:`scrubbed` reports
#: these by their kind only; anything else (a storage or a corruption error, a closed space) propagates.
_EVALUATION_FAILURES = (ProtoValidationError, ProtoNotSupportedError, ProtoUserError, ArithmeticError, TypeError,
                        ValueError, LookupError, AttributeError)

#: The parts of a query an evaluation failure is tagged with (a selected path names itself).
_FROM, _WHERE, _AGGREGATE = "the query's 'from'", "the query's 'where'", "the query's aggregate"
#: The operators whose operands a term's declared-attribute check follows (their ``attr`` reads the member);
#: the others (``via``, ``root``, aggregates, constants) read other elements, or nothing.
_CHECKED_OPERATORS = frozenset({"+", "-", "*", "==", "!=", "<", "<=", ">", ">=", "and", "or", "not"})


def _tag(error: BaseException, part: str) -> None:
    """Record on ``error`` the part of the query it arose in, unless a part nearer to it already did."""
    if getattr(error, "semantic_part", None) is None:
        error.semantic_part = part


def _tagged(part: str, run, *args):
    """``run(*args)``; an evaluation failure propagates unchanged, tagged with ``part`` (:func:`_tag`)."""
    try:
        return run(*args)
    except _EVALUATION_FAILURES as error:
        _tag(error, part)
        raise


def _plain(error: ProtoValidationError) -> ProtoValidationError:
    """Mark ``error`` as one whose message holds no stored value (it names only what the query names and a
    type), so :func:`scrubbed` reports it as it is."""
    error.semantic_plain = True
    return error


def _not_stored(element, name: str) -> ProtoValidationError:
    """The refusal of a path step ``name`` that the record ``element`` does not store (raw mode)."""
    return _plain(_invalid(f"The attribute {name!r} is not stored in {describe_operand(element)}"))


def is_evaluation_failure(error: BaseException) -> bool:
    """Whether ``error`` is what evaluating over unexpected values raises (:data:`_EVALUATION_FAILURES`), not a
    storage, corruption or closed-space failure."""
    return isinstance(error, _EVALUATION_FAILURES) and not isinstance(error, ProtoClosedError)


def scrubbed(operation: str, run, *args, log=None):
    """``run(*args)``, reporting an evaluation failure (:func:`is_evaluation_failure`) without any value it may
    hold (the agent interface, :mod:`proto_db.agent.operations`): it becomes a ``ProtoValidationError`` that
    names the part it arose in (the query part the engine tagged, else ``operation``), the failure's kind and,
    when the evaluator named one, the operand's type (:func:`~proto_db.semantic.expressions.describe_operand`),
    never the failure's message.  It is raised outside the handler, so no exception that holds a value is
    chained to it.  A ``LookupError`` or an ``AttributeError`` (likely a programming error) is also logged, with
    the original exception, to ``log`` at WARNING.  Any other failure (a storage or a corruption error, a closed
    space) propagates unchanged."""
    try:
        return run(*args)
    except _EVALUATION_FAILURES as error:
        if not is_evaluation_failure(error):
            raise
        if log is not None and isinstance(error, (LookupError, AttributeError)):
            log.warning("Evaluating %s failed with %s; the agent was told the kind only", operation,
                        type(error).__name__, exc_info=error)
        part = getattr(error, "semantic_part", None)
        operand = getattr(error, "semantic_operand", None)
        kind = type(error).__name__
        plain = str(error) if getattr(error, "semantic_plain", False) else None
    if plain is not None:                   # names only what the query names and a type (see _plain)
        raise _invalid(f"{part}: {plain}" if isinstance(part, str) else plain)
    on = f", on {operand}" if isinstance(operand, str) else ""
    raise _invalid(f"Evaluating {part if isinstance(part, str) else operation} failed ({kind}{on}); the failure's "
                   f"message is not reported, since it may hold a stored value")


def _kind(element: Mapping) -> str:
    """``role``, or the attribute's ``kind`` (``field``, ``computed``, ``computed_stored``)."""
    return "role" if "target" in element else element.get("kind", "field")


def _single(element: Mapping) -> bool:
    return element["cardinality"] in _SINGLE


def _unset(value) -> bool:
    return value is MISSING or value is None


def _handle_of(value):
    """The identity handle a ``one`` end's stored value designates, or ``None``."""
    if isinstance(value, MutableObject):
        return value
    if isinstance(value, dict) and isinstance(value.get("entity"), MutableObject):
        return value["entity"]
    return None


def _atomic(method):
    """Run the write operation ``method`` holding its transaction's lock (see "Atomicity"): the session's
    guard, once its store is known to be writable (a session over a snapshot raises ``ProtoUserError``).  A
    replay (:mod:`proto_db.semantic.intents`) holds no lock.  The outermost operation of a direct-mode
    transaction checks the enforced invariants of the entities it changed before it returns, and is undone
    in the transaction when it raises (see "Constraints")."""
    @functools.wraps(method)
    def run(self, *args, **kwargs):
        self.session._store()
        with self.session._guard() as top:
            result = method(self, *args, **kwargs)
            if top:
                self._check_operation()
            return result
    return run


def _consistent(method):
    """Run the read ``method`` holding the lock of the session's transaction, if the session reads one (a
    snapshot never changes, a replay holds no lock): see "Atomicity"."""
    @functools.wraps(method)
    def run(self, *args, **kwargs):
        if self.session is None:
            return method(self, *args, **kwargs)
        with self.session._guard():
            return method(self, *args, **kwargs)
    return run


# -- the catalog, read for operations ------------------------------------------------------------------------

class _Key(NamedTuple):
    """A maintained key (R262): the member's identity ``role``, the ``attribute`` of its ``target`` type
    read through it, the declared ``path`` and the ``index`` name."""
    role: str
    attribute: str
    path: str
    index: str
    target: str


class _EndIndex:
    """How the ``many`` end ``end`` of ``owner`` stores its members (see "Storage")."""

    __slots__ = ("owner", "end", "member_type", "inverse", "ordered", "indexes", "key", "records", "refusal")

    def __init__(self, schema: "Schema", owner: str, end: str, element: Mapping):
        self.owner, self.end = owner, end
        self.member_type = element["target"]
        self.inverse = element.get("inverse")
        self.ordered = bool(element.get("ordered"))
        self.records = schema.is_relationship(self.member_type)
        self.indexes, self.key, self.refusal = [], None, None
        where = f"The end {end!r} of {owner!r}"
        if element.get("binding", "identity") != "identity":
            self.refusal = f"{where} is a many end bound by version; the MVP binds a one end only"
            return
        inverse = None if self.inverse is None else schema.element(self.member_type, self.inverse)
        paths = list(element.get("index_by", ()))
        if inverse is not None and "target" in inverse and _single(inverse):
            paths += [p for p in inverse.get("index_by", ()) if p not in paths]
        plain = []
        for path in paths:
            steps = path.split(".")
            name = "by_" + "_".join(steps)
            if len(steps) == 1:
                plain.append((name, path))
                continue
            role = schema.element(self.member_type, steps[0])
            target = None if role is None or "target" not in role else schema.element(role["target"], steps[1])
            if len(steps) != 2 or role is None or "target" not in role or not _single(role) \
                    or role.get("binding", "identity") != "identity" or target is None or _kind(target) != "field":
                self.refusal = (f"{where}: the index path {path!r} is neither a member attribute nor an identity "
                                f"role followed by a stored field of its target (R262)")
                return
            if self.key is not None:
                self.refusal = (f"{where} declares two maintained keys ({self.key.path!r}, {path!r}); an entry has "
                                f"one key")
                return
            self.key = _Key(steps[0], steps[1], path, name, role["target"])
        if self.key is not None and plain:
            self.refusal = (f"{where} declares a maintained key ({self.key.path!r}) beside member paths "
                            f"({[p for _, p in plain]}); its entries index the key only")
            return
        if self.key is not None and (inverse is None or schema.element(self.member_type, self.key.role).get(
                "inverse") is None):
            self.refusal = (f"{where} declares the maintained key {self.key.path!r}, and a re-key finds its entries "
                            f"through the inverses of this end and of the member's role {self.key.role!r}, which "
                            f"one of them lacks")
            return
        if plain and not self.records:
            self.refusal = (f"{where}: the index paths {[p for _, p in plain]} index stored attributes of records, and "
                            f"this end holds entity handles")
            return
        self.indexes = [(self.key.index, "key")] if self.key is not None else plain

    @property
    def entries(self) -> bool:
        return self.key is not None

    def check(self) -> None:
        if self.refusal is not None:
            raise ProtoNotSupportedError(self.refusal)


class Schema:
    """The composed catalog of a session, indexed for its operations."""

    def __init__(self, model: Mapping):
        self.model = model
        self._elements = {name: {e["name"]: e for e in entity.get("attributes", []) + entity.get("roles", [])}
                          for name, entity in model["entities"].items()}
        self._intents = {name: {i["name"]: i for i in entity.get("intents", [])}
                         for name, entity in model["entities"].items()}
        self._subtypes: dict = {}
        self._ends: dict = {}
        self._keyed: dict = {}

    def entity(self, type_name: str) -> dict:
        found = self.model["entities"].get(type_name) if isinstance(type_name, str) else None
        if found is None:
            raise _invalid(f"The semantic catalog describes no type {type_name!r}")
        return found

    def has(self, type_name) -> bool:
        return isinstance(type_name, str) and type_name in self.model["entities"]

    def is_relationship(self, type_name: str) -> bool:
        return "roles" in self.model["entities"].get(type_name, {})

    def element(self, type_name: str, name: str):
        return self._elements.get(type_name, {}).get(name)

    def elements(self, type_name: str) -> dict:
        return self._elements.get(type_name, {})

    def intent(self, type_name: str, name: str):
        """The composed intent ``name`` of ``type_name``, or ``None``."""
        return self._intents.get(type_name, {}).get(name)

    def subtypes(self, type_name: str) -> frozenset:
        found = self._subtypes.get(type_name)
        if found is None:
            found = self._subtypes[type_name] = _compose.subtypes(self.model, type_name)
        return found

    def is_a(self, type_name: str, supertype: str) -> bool:
        return self.has(supertype) and type_name in self.subtypes(supertype)

    def aggregates_over(self, type_name: str, end: str) -> list:
        """The maintained aggregates of ``type_name`` over its end ``end``: ``(name, tree)`` pairs."""
        return [(e["name"], e["expression"]["ast"]) for e in self.elements(type_name).values()
                if e.get("kind") == "computed_stored" and e["expression"]["ast"][1] == end]

    def has_aggregates_over(self, type_name: str, end: str) -> bool:
        """Whether ``type_name`` or one of its subtypes maintains an aggregate over its end ``end``."""
        return self.has(type_name) and any(self.aggregates_over(sub, end) for sub in self.subtypes(type_name))

    def single_ends(self, type_name: str) -> list:
        """The ``one`` ends of ``type_name`` that have an inverse: where its versions are members."""
        return [e for e in self.elements(type_name).values() if "target" in e and _single(e) and e.get("inverse")]

    def end_index(self, owner: str, end: str) -> _EndIndex:
        key = (owner, end)
        found = self._ends.get(key)
        if found is None:
            found = self._ends[key] = _EndIndex(self, owner, end, self.element(owner, end))
        return found

    def keyed_ends(self, type_name: str, attribute: str | None = None, role: str | None = None) -> list:
        """The maintained-key ends whose key reads ``attribute`` of an entity of ``type_name`` (the key's
        target), or whose key goes through the role ``role`` of a member of ``type_name``."""
        key = (type_name, attribute, role)
        found = self._keyed.get(key)
        if found is None:
            found = []
            for owner, elements in self._elements.items():
                for element in elements.values():
                    if "target" not in element or _single(element):
                        continue
                    info = self.end_index(owner, element["name"])
                    if info.refusal is not None or info.key is None:
                        continue
                    if attribute is not None and info.key.attribute == attribute \
                            and self.is_a(type_name, info.key.target) \
                            or role is not None and info.key.role == role and self.is_a(type_name, info.member_type):
                        found.append(info)
            self._keyed[key] = found
        return found

    def plannable(self, type_name: str) -> frozenset:
        """The attribute names of ``type_name`` whose evaluation is their stored value in every subtype: fields
        without a catalogued default, and maintained aggregates (preflight F16)."""
        names = None
        for sub in self.subtypes(type_name):
            here = {n for n, e in self.elements(sub).items()
                    if e.get("kind") == "computed_stored" or _kind(e) == "field" and "default" not in e}
            names = here if names is None else names & here
        return frozenset(names or ())


# -- versions -------------------------------------------------------------------------------------------------

class Version:
    """A version of an entity or a relationship atom, as a semantic session gives it (see "Versions and
    navigation" in the module docstring).  Immutable."""

    __slots__ = ("_session", "_record", "_entity", "_bound")

    def __init__(self, session, record, entity: MutableObject | None = None, bound: bool = False):
        object.__setattr__(self, "_session", session)
        object.__setattr__(self, "_record", record)
        object.__setattr__(self, "_entity", entity)
        object.__setattr__(self, "_bound", bound)       # the version a role binds, not the cell's

    @property
    def record(self):
        """The stored record (a ``DBObject`` of the type's semantic class).  In intent mode, reading what it
        holds may decide anything, so taking it is a fine read of the whole cell
        (:mod:`proto_db.semantic.intents`)."""
        if self._session is not None:
            self._session._record_read(self)
        return self._record

    @property
    def entity(self) -> MutableObject | None:
        """The handle of the entity this is a version of; ``None`` for a relationship atom."""
        return self._entity

    @property
    def oid(self):
        """The record's oid."""
        return self._record.oid

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return self._session._operations().attribute(self, name)

    def __getitem__(self, name: str):
        """The attribute ``name``, also one named like ``entity`` or ``record``."""
        if not isinstance(name, str) or not name or name.startswith("_"):
            raise ProtoValidationError(f"{name!r} is not an attribute name")
        return self._session._operations().attribute(self, name)

    def __setattr__(self, name, value):
        raise ProtoUserError("A version is immutable; change an entity through the semantic session (update, link)")

    def __eq__(self, other):
        if not isinstance(other, Version):
            return NotImplemented
        return self._entity == other._entity and self._record.oid == other._record.oid

    def __hash__(self):
        return hash(("Version", self._entity, self._record.oid))

    def __repr__(self):
        where = self._entity.name if self._entity is not None else type(self._record).__type_name__
        return f"<Version of {where}: {self._record!r}>"


class BoundIntent:
    """An intent of a version the session gave, as attribute access returns it (``order.add_line``): calling
    it binds the arguments to the intent's signature and invokes it through the session
    (:meth:`~proto_db.semantic.SemanticSession.invoke`), which applies it in direct mode and logs and applies
    it in intent mode.  Called while an intent runs, it runs as part of that intent."""

    __slots__ = ("_session", "_version", "_invoker")

    def __init__(self, session, version: Version, invoker):
        self._session = session
        self._version = version
        self._invoker = invoker

    @property
    def name(self) -> str:
        """The intent's name."""
        return self._invoker.name

    def __call__(self, *args, **kwargs):
        from .classes import _arguments
        arguments = _arguments(self._invoker._function, self._version._record, args, kwargs)
        return self._session.invoke(self._version, self.name, **arguments)

    def __repr__(self) -> str:
        return f"<intent {self.name!r} of {self._version!r}>"


class SessionContext(IntentContext):
    """The :class:`~proto_db.semantic.IntentContext` of an intent invoked through a session: ``target`` is the
    target's handle, and the operations are the session's (direct mode: the transaction's cells; intent mode:
    the record view, then the replay's cells).  The receiver passed to the implementation (``self``, the
    target's stored record) names the target wherever an entity is expected (``order=self``)."""

    def __init__(self, operations: "Operations", target: MutableObject, receiver):
        super().__init__(target)
        self._operations = operations
        self._receiver = receiver

    def _entity(self, value):
        return self.target if value is self._receiver else value

    def _given(self, given: Mapping) -> dict:
        return {name: self._entity(value) for name, value in given.items()}

    def create(self, type_name: str, **fields):
        return self._operations.create(type_name, self._given(fields))

    def get(self, handle):
        return self._operations.get(self._entity(handle))

    def update(self, handle, **changes):
        self._operations.update(self._entity(handle), changes)

    def link(self, source, role: str, target):
        self._operations.link(self._entity(source), role, self._entity(target))

    def unlink(self, source, role: str, target):
        self._operations.link(self._entity(source), role, self._entity(target), add=False)

    def relate(self, type_name: str, **roles_and_fields):
        return self._operations.relate(type_name, self._given(roles_and_fields))


class Members:
    """The members of a ``many`` end, as versions, in the stored collection's order (row 44: a
    maintained-key end yields its members, never its entries)."""

    __slots__ = ("_session", "_collection", "_ordered", "_entries")

    def __init__(self, session, collection, ordered: bool, entries: bool):
        self._session = session
        self._collection = collection
        self._ordered = ordered
        self._entries = entries

    @property
    def collection(self):
        """The stored ``List`` or ``Set`` (for a maintained-key end, of entries); ``None`` while the end
        holds nothing."""
        return self._collection

    @property
    def ordered(self) -> bool:
        """Whether the end is ordered (a ``List``) or not (a ``Set``)."""
        return self._ordered

    def _raw(self, element):
        return element.member if self._entries else element

    def __len__(self) -> int:
        return 0 if self._collection is None else self._collection.count

    def __iter__(self):
        if self._collection is None:
            return
        operations = self._session._operations()
        for element in self._collection.elements():
            found = operations.version_of(self._raw(element))
            if found is not None:
                yield found

    def __getitem__(self, index: int):
        if not self._ordered:
            raise ProtoUserError("The members of an unordered end have no positions")
        if self._collection is None:
            raise IndexError(index)
        return self._session._operations().version_of(self._raw(self._collection.get_at(index)))

    def __contains__(self, item) -> bool:
        raw = item.entity if isinstance(item, Version) and item.entity is not None else \
            item._record if isinstance(item, Version) else item
        try:
            wanted = _identity(raw)
        except ProtoValidationError:
            return False
        if self._collection is None:
            return False
        return any(_identity(self._raw(e)) == wanted for e in self._collection.elements())

    def __repr__(self):
        return f"<Members ({'ordered' if self._ordered else 'unordered'}, {len(self)})>"


# -- operations -----------------------------------------------------------------------------------------------

class _SessionReader(RegistryReader):
    """The evaluation reader of a session: cells through its source (so a transaction records the reads),
    roots likewise, defaults through the registry chain (preflight F15)."""

    def root(self, name: str):
        return self._source.get_root_object(name)


class _QueryReader(_SessionReader):
    """The session's reader while a catalog-mode query evaluates its terms: a path step a row does not hold is
    never read as missing unless the row's type declares it.  A row of a catalogued type follows the catalog
    (an attribute its type or one of its subtypes declares is its default or MISSING when the row does not hold
    it, as everywhere; an undeclared one is refused); a row of a type the catalog does not describe follows the
    raw rule (what it stores is read, and at a path's last step also what it inherits; anything else is
    refused, an inherited intermediate step with a message that says so).  The planning check
    (:meth:`Operations._check_declared`) refuses what it can before anything is read; this covers rows whose
    type the ``from`` does not tell."""

    def __init__(self, source, schema: "Schema"):
        super().__init__(source)
        self._schema = schema

    def _declared(self, element, name: str) -> bool:
        type_name = getattr(type(element), "__type_name__", None)
        if not self._schema.has(type_name):
            return False
        if all(self._schema.element(t, name) is None for t in self._schema.subtypes(type_name)):
            raise _plain(_invalid(f"The type {type_name!r} declares no attribute or role {name!r}"))
        return True             # declared by the type or one of its subtypes, as the planning check reads it

    def default(self, element, name: str):
        if self._declared(element, name):
            return super().default(element, name)
        found = super().default(element, name)
        if found is MISSING:
            raise _not_stored(element, name)
        return found

    def absent(self, element, name: str):
        if self._declared(element, name):
            return MISSING
        if super().default(element, name) is not MISSING:
            raise _plain(_invalid(f"The path step {name!r} of {describe_operand(element)} is inherited, not stored; "
                                  f"a query reads an inherited value only at the last step of a path"))
        raise _not_stored(element, name)


class StoredReader(Reader):
    """The evaluation reader of a query over stored structure alone (``schema=None``; the agent interface's
    raw mode): cells and roots of the source, no catalogued default, and no reserved root (``__`` names hold
    the database's own structures, catalog included).  A path step naming an attribute the record does not
    store is refused (:func:`_not_stored`), never read as missing: with no catalog, nothing says the attribute
    exists, and a sum over it would answer 0.  A stored ``None`` is a value."""

    def __init__(self, source):
        self._source = source

    def default(self, element, name: str):
        raise _not_stored(element, name)

    def absent(self, element, name: str):
        raise _not_stored(element, name)

    def cell(self, name: str):
        return self._source.get_mutable(name)

    def root(self, name: str):
        if name.startswith("__"):
            raise _invalid(f"The root {name!r} is not available")
        return self._source.get_root_object(name)


class Operations:
    """The operations of one semantic session (see the module docstring); built per call by the session.
    With ``schema=None`` it is the query engine over stored structure alone (:func:`query_stored`)."""

    __slots__ = ("session", "source", "schema", "reader", "tracker", "_resolver")

    def __init__(self, session, source, schema: Schema | None, reader: Reader | None = None, tracker=None):
        """``tracker``: the intent log whose fine reads this call's reads enter (intent mode, a read the caller
        decides on; :mod:`proto_db.semantic.intents`), else ``None``."""
        self.session = session
        self.source = source
        self.schema = schema
        self.tracker = tracker
        self._resolver = source
        if tracker is not None:
            from .intents import Tracking
            self._resolver = Tracking(source, tracker)
        self.reader = _SessionReader(self._resolver) if reader is None else reader

    @contextlib.contextmanager
    def reading(self):
        """Resolution and evaluation read with the session's source as the reader (its registry); a tracked
        read, through the view that records the cells it reads."""
        token = resolution.set_reader(self._resolver)
        try:
            yield
        finally:
            resolution.reset_reader(token)

    # -- cells ------------------------------------------------------------------------------------------

    def handle(self, value, where: str) -> MutableObject:
        """The identity handle of ``value``: a handle, or a version of an entity the session gave."""
        if isinstance(value, MutableObject):
            return value
        if isinstance(value, Version) and value.entity is not None:
            return value.entity
        if isinstance(value, Version):
            raise _invalid(f"{where}: a relationship atom has no identity; name an entity")
        raise _invalid(f"{where}: an entity is given by its handle (MutableObject) or a version obtained from the "
                       f"session, not {type(value).__name__}")

    def record(self, handle: MutableObject, where: str) -> tuple:
        """``(record, type name)`` of the entity ``handle`` in the source's view (the read enters a
        transaction's read set)."""
        record = self.source.get_mutable(handle.name)
        if record is None:
            raise _invalid(f"{where}: the cell {handle.name!r} does not exist")
        type_name = getattr(type(record), "__type_name__", None)
        if not self.schema.has(type_name) or not is_atom(record):
            raise _invalid(f"{where}: the cell {handle.name!r} holds no entity of the semantic catalog "
                           f"({type(record).__name__})")
        return record, type_name

    @_consistent
    def version_of(self, raw):
        """The version a stored role value designates: a handle's current version, a bound version, an
        atom; ``None`` for nothing linked or an absent cell.  It holds the transaction's lock (see
        "Atomicity"): a member read of an iteration (:class:`Members`) happens entirely before or after
        another thread's operation, also in intent mode, whose record view takes no lock of its own."""
        if _unset(raw):
            return None
        if isinstance(raw, MutableObject):
            record = self.source.get_mutable(raw.name)
            return None if record is None else Version(self.session, record, raw)
        if isinstance(raw, dict) and isinstance(raw.get("entity"), MutableObject):
            return Version(self.session, raw["version"], raw["entity"], bound=True)
        if is_atom(raw):
            return Version(self.session, raw, None)
        raise _operand_error(f"A role holds records, identity handles or version-bound references, not "
                             f"{describe_operand(raw)}", raw)

    def _write(self, handle: MutableObject, old, new) -> None:
        """Write ``new`` in ``handle``'s cell and move the aggregates ``old`` contributed to."""
        self.session._store().set_mutable(handle.name, new)
        self._propagate(old, new)

    def _same_fields(self, type_name: str, one, other) -> bool:
        """Whether the records ``one`` and ``other`` of ``type_name`` hold equal fields (roles, ends and
        maintained aggregates are not fields)."""
        return all(values_equal(_stored_step(one, name), _stored_step(other, name))
                   for name, element in self.schema.elements(type_name).items() if _kind(element) == "field")

    def _bound(self, handle: MutableObject, record, type_name: str) -> dict:
        """The value a version-bound role stores for ``handle``, whose record in the transaction's view is
        ``record`` (read with ``get_mutable``, so the read set validates the committed record at commit):
        ``{"entity", "version"}`` with its field version (see "Storage").  The rule is history-free: the
        committed record while the fields equal it; otherwise the record kept for the transaction while the
        fields equal it; otherwise ``record``, kept from now on."""
        tr = self.session._store()
        committed = tr.snapshot_mutable(handle.name)
        if committed is not None and self._same_fields(type_name, committed, record):
            return {"entity": handle, "version": committed}
        while True:
            kept = tr.semantic_field_version(handle.name)
            if kept is not None and self._same_fields(type_name, kept, record):
                return {"entity": handle, "version": kept}
            if tr.set_semantic_field_version(handle.name, kept, record) is record:
                return {"entity": handle, "version": record}

    # -- reading ----------------------------------------------------------------------------------------

    @_consistent
    def get(self, handle) -> Version:
        handle = self.handle(handle, "get")
        record, _ = self.record(handle, "get")
        return Version(self.session, record, handle)

    def _track(self, cell: str | None, attribute: str | None) -> None:
        """Record a fine read of ``attribute`` (``None``: the whole cell) of the entity ``cell`` when this
        call tracks (intent mode, a read the caller decides on); ``cell`` ``None`` (a bound version, an atom:
        nothing that changes) records nothing."""
        if self.tracker is not None and cell is not None:
            self.tracker.read(cell, attribute)

    @staticmethod
    def _cell_of(version: Version) -> str | None:
        """The cell ``version`` was read from, or ``None`` (a relationship atom, a bound version)."""
        return None if version.entity is None or version._bound else version.entity.name

    @_consistent
    def attribute(self, version: Version, name: str):
        record = version._record
        type_name = type(record).__type_name__
        if self.schema.intent(type_name, name) is not None:
            from .classes import IntentInvoker
            with self.reading():
                found = resolution.resolve(record, name)[0]
            if not isinstance(found, IntentInvoker):
                raise _invalid(f"The catalog describes the intent {name!r} of {type_name!r}, and the registry resolves "
                               f"{name!r} to {type(found).__name__}, not an intent")
            return BoundIntent(self.session, version, found)
        element = self.schema.element(type_name, name)
        if element is not None and "target" in element:
            return self.navigate(version, name)
        stored = element is not None and _kind(element) in ("field", "computed_stored")
        self._track(self._cell_of(version), name if stored else None)
        with self.reading():
            return record[name]

    @_consistent
    def navigate(self, source, role: str):
        if isinstance(source, Version):
            record, cell = source._record, self._cell_of(source)
        else:
            handle = self.handle(source, "navigate")
            (record, _), cell = self.record(handle, "navigate"), handle.name
        type_name = type(record).__type_name__
        element = self.schema.element(type_name, role)
        if element is None or "target" not in element:
            raise _invalid(f"{type_name!r} has no role {role!r}")
        self._track(cell, role)
        value = _stored_step(record, role)
        if _single(element):
            return self.version_of(value)
        info = self.schema.end_index(type_name, role)
        return Members(self.session, None if _unset(value) else value, info.ordered, info.entries)

    # -- evaluation ---------------------------------------------------------------------------------------

    def _evaluate(self, tree, element):
        """Evaluate a catalog-declared ``tree`` (an aggregate, its ``where``, a maintained key, a ``requires``):
        a total function, also during an agent query (:func:`~proto_db.semantic.expressions.total_rules`)."""
        with self.reading(), total_rules():
            return evaluate(tree, element, reader=self.reader)

    def _contribution(self, tree, member):
        """What ``member`` (a record) adds to the maintained aggregate ``tree``: its term where its
        ``where`` is true (1 for a count), else the int 0."""
        op = tree[0]
        where = tree[3] if op == "sum" and len(tree) > 3 else tree[2] if op == "count" and len(tree) > 2 else None
        if where is not None and not _truth(op, self._evaluate(where, member), "its where"):
            return 0
        if op == "count":
            return 1
        value = _number(op, self._evaluate(tree[2], member))
        return 0 if value is MISSING else value

    def _moved(self, record, type_name: str, end: str, delta_of, *, settled: bool = False) -> dict:
        """The changes of ``record``'s maintained aggregates over ``end`` by ``delta_of(tree)``.

        An aggregate ``record`` does not store was added by a later module to an entity that existed before
        it (spec section 5: attributes are addable).  The first write that moves it stores it, seeded by one
        full computation over the relation's current members (read in this operation, so its reads are
        validated as any other): the seed plus the delta, or, when ``settled`` (the members already show the
        change, as for a member that changed), the computation alone."""
        changes = {}
        for name, tree in self.schema.aggregates_over(type_name, end):
            delta = delta_of(tree)
            if delta == 0:
                continue
            stored = _stored_step(record, name)
            if stored is MISSING:
                seed = _number(tree[0], self._evaluate(tree, record))
                seed = 0 if seed is MISSING else seed
                changes[name] = seed if settled else _arithmetic("+", seed, delta)
            else:
                changes[name] = _arithmetic("+", stored, delta)
        return changes

    def _propagate(self, old, new) -> None:
        """An entity changed from ``old`` to ``new`` with the same roles: every maintained aggregate it is a
        member of moves by the change of its contribution, along its ``one`` ends (spec section 10)."""
        type_name = type(new).__type_name__
        for element in self.schema.single_ends(type_name):
            owner_handle = _handle_of(_stored_step(new, element["name"]))
            if owner_handle is None or not self.schema.has_aggregates_over(element["target"], element["inverse"]):
                continue
            owner, owner_type = self.record(owner_handle, f"{type_name!r}.{element['name']}")
            changes = self._moved(owner, owner_type, element["inverse"],
                                  lambda tree: _arithmetic("-", self._contribution(tree, new),
                                                           self._contribution(tree, old)), settled=True)
            if changes:
                self._write(owner_handle, owner, owner.with_(**changes))

    # -- collections --------------------------------------------------------------------------------------

    def _new_collection(self, info: _EndIndex):
        tr = self.session._store()
        collection = tr.new_list() if info.ordered else tr.new_set()
        for name, key in info.indexes:
            collection = collection.add_index(RangeIndex(name, key))
        return collection

    def _key(self, info: _EndIndex, member):
        """The maintained key of ``member`` (a record) in the end ``info``: ``role.attribute`` evaluated."""
        return self._evaluate(["attr", info.key.role, info.key.attribute], member)

    def _entry(self, raw, key):
        return DBObject(member=raw) if key is MISSING else DBObject(member=raw, key=key)

    def _find(self, collection, info: _EndIndex, raw, key=MISSING):
        """``(position, stored element)`` of the member ``raw`` in ``collection``, or ``None``.  An entry is
        looked up through the key index when its key is known."""
        if collection is None:
            return None
        wanted = _identity(raw)
        if info.entries and key is not MISSING and not isinstance(collection, List):
            try:
                candidates = from_collection(collection).where(Term("key", "==", key)).to_list()
            except ProtoValidationError:
                candidates = None
            for element in candidates or ():
                if _identity(element.member) == wanted:
                    return None, element
        elif not info.entries and not isinstance(collection, List):
            return (None, raw) if collection.has(raw) else None
        for position, element in enumerate(collection.elements()):
            if _identity(element.member if info.entries else element) == wanted:
                return position, element
        return None

    def _with_member(self, record, type_name: str, end: str, raw, member, add: bool, fresh: bool = False):
        """``record`` with the member ``raw`` (whose record is ``member``) added to or removed from its
        ``many`` end ``end``, and its aggregates over that end moved by the member's contribution.  A
        ``fresh`` member (an atom just created) is known not to be there."""
        info = self.schema.end_index(type_name, end)
        info.check()
        collection = _stored_step(record, end)
        collection = None if _unset(collection) else collection
        key = self._key(info, member) if info.entries else MISSING
        found = None if fresh else self._find(collection, info, raw, key)
        where = f"The end {end!r} of {type_name!r}"
        if add:
            if found is not None:
                raise _invalid(f"{where} already holds {describe_operand(raw)}")
            if collection is None:
                collection = self._new_collection(info)
            element = self._entry(raw, key) if info.entries else raw
            collection = collection.append_last(element) if info.ordered else collection.add(element)
        else:
            if found is None:
                raise _invalid(f"{where}: {describe_operand(raw)} is not linked to it")
            position, element = found
            collection = collection.remove_at(position) if info.ordered else collection.remove(element)
        sign = "+" if add else "-"
        changes = self._moved(record, type_name, end,
                              lambda tree: _arithmetic(sign, 0, self._contribution(tree, member)))
        return record.with_(**{end: collection}, **changes)

    def _rekey(self, owner_handle: MutableObject, info: _EndIndex, raw, old_key, new_key) -> None:
        """Replace the entry of ``raw`` in ``owner_handle``'s end ``info`` keyed ``old_key`` by one keyed
        ``new_key``."""
        if _same_key(old_key, new_key):
            return
        owner, owner_type = self.record(owner_handle, f"re-key of {info.owner!r}.{info.end}")
        collection = _stored_step(owner, info.end)
        found = self._find(None if _unset(collection) else collection, info, raw, old_key)
        if found is None:
            raise ProtoCorruptionError(f"The end {info.end!r} of {owner_handle.name!r} lacks the entry of "
                                       f"{describe_operand(raw)}, which its inverse end holds")
        position, element = found
        entry = self._entry(raw, new_key)
        collection = collection.set_at(position, entry) if info.ordered else collection.remove(element).add(entry)
        self.session._store().set_mutable(owner_handle.name, owner.with_(**{info.end: collection}))

    def _owner_of(self, info: _EndIndex, member):
        """The handle of the entity whose end ``info`` holds ``member`` (a record), through the inverse."""
        return _handle_of(_stored_step(member, info.inverse))

    # -- writing ----------------------------------------------------------------------------------------

    def _target(self, element: Mapping, value, where: str) -> tuple:
        """``(handle, record, type name)`` of the target ``value`` of the role ``element``, checked with
        ``is_a`` (R280: a subtype instance is accepted where its supertype is declared)."""
        handle = self.handle(value, where)
        record, type_name = self.record(handle, where)
        if not self.schema.is_a(type_name, element["target"]):
            raise _invalid(f"{where}: the target {handle.name!r} is a {type_name!r}, not a {element['target']!r} "
                           f"(or one of its subtypes)")
        return handle, record, type_name

    def _role(self, type_name: str, name: str, where: str) -> dict:
        element = self.schema.element(type_name, name)
        if element is None or "target" not in element:
            raise _invalid(f"{where}: {type_name!r} has no role {name!r}")
        if element.get("participant"):
            raise _invalid(f"{where}: {name!r} is a role of the relationship {type_name!r}; it is set by relate")
        if self.schema.is_relationship(element["target"]):
            raise _invalid(f"{where}: the end {name!r} of {type_name!r} holds atoms of the relationship "
                           f"{element['target']!r}; create them with relate")
        return element

    def _fields(self, type_name: str, given: Mapping, where: str) -> tuple:
        """Split ``given`` into ``(fields, roles)``; refuse what is never written and a missing required
        field."""
        fields, linked = {}, {}
        for name, value in given.items():
            element = self.schema.element(type_name, name)
            if element is None:
                raise _invalid(f"{where}: {type_name!r} has no attribute or role {name!r}")
            kind = _kind(element)
            if kind == "role":
                linked[name] = (element, value)
            elif kind == "field":
                fields[name] = value
            elif kind == "computed_stored":
                raise _invalid(f"{where}: {name!r} of {type_name!r} is a maintained aggregate; it is never written")
            else:
                raise _invalid(f"{where}: {name!r} of {type_name!r} is computed; it is never written")
        missing = [e["name"] for e in self.schema.elements(type_name).values()
                   if _kind(e) == "field" and e.get("required") and "default" not in e and e["name"] not in fields]
        if missing:
            raise _invalid(f"{where}: {type_name!r} has the required fields {missing} with no value; required fields "
                           f"are given at creation")
        return fields, linked

    def _semantic_class(self, type_name: str):
        try:
            cls = Atom._class_for(type_name)
        except ProtoCorruptionError:
            cls = None
        if cls is None or getattr(cls, "_implicit_parent", None) != type_name:
            raise ProtoNotSupportedError(f"{type_name!r} has no Python class in this process; a semantic session "
                                         f"creates records of the semantic class registered for the type (D57)")
        return cls

    def _initial_aggregates(self, type_name: str, where: str) -> dict:
        out = {}
        for element in self.schema.elements(type_name).values():
            if element.get("kind") != "computed_stored":
                continue
            over = self.schema.element(type_name, element["expression"]["ast"][1])
            if self.schema.is_relationship(type_name) or over is None or _single(over):
                raise ProtoNotSupportedError(f"{where}: the maintained aggregate {element['name']!r} of {type_name!r} "
                                             f"is maintained over a many end of an entity only")
            out[element["name"]] = 0
        return out

    @_atomic
    def create(self, type_name: str, given: Mapping) -> MutableObject:
        tr = self.session._store()
        where = f"create({type_name!r})"
        entity = self.schema.entity(type_name)
        if "roles" in entity:
            raise _invalid(f"{where}: {type_name!r} is a relationship; its atoms are created with relate")
        cls = self._semantic_class(type_name)
        fields, linked = self._fields(type_name, given, where)
        links = []
        for name, (element, value) in linked.items():
            element = self._role(type_name, name, where)
            if not _single(element):
                raise _invalid(f"{where}: {name!r} is a many end; link its members after the creation")
            links.append((element, self._target(element, value, f"{where}, {name}")))
        for element, (handle, record, target_type) in links:
            inverse = element.get("inverse")
            if inverse is not None:
                self._check_end(record, target_type, self.schema.element(target_type, inverse), None, add=True,
                                where=f"{where}, {element['name']}")
        with self.reading():
            record = cls(**fields, **self._initial_aggregates(type_name, where))
        handle = tr.new_mutable(self.session._new_cell(type_name), record)
        for element, (target, _, _) in links:
            self._apply_link(handle, element, target)
        return handle

    @_atomic
    def update(self, handle, changes: Mapping) -> None:
        handle = self.handle(handle, "update")
        where = f"update({handle.name!r})"
        old, type_name = self.record(handle, where)
        for name in changes:
            element = self.schema.element(type_name, name)
            if element is None:
                raise _invalid(f"{where}: {type_name!r} has no attribute or role {name!r}")
            kind = _kind(element)
            if kind == "role":
                raise _invalid(f"{where}: {name!r} is a role of {type_name!r}; change it with link and unlink")
            if kind != "field":
                what = "a maintained aggregate" if kind == "computed_stored" else "computed"
                raise _invalid(f"{where}: {name!r} of {type_name!r} is {what}; it is never written")
        if not changes:
            return
        new = old.with_(**changes)
        rekeys = []
        for name in changes:
            for info in self.schema.keyed_ends(type_name, attribute=name):
                back = self.schema.element(info.member_type, info.key.role)["inverse"]
                members = _stored_step(old, back)
                back_info = self.schema.end_index(type_name, back)
                old_key, new_key = self._evaluate(["attr", name], old), self._evaluate(["attr", name], new)
                for element in ([] if _unset(members) else members.elements()):
                    raw = element.member if back_info.entries else element
                    member = raw if is_atom(raw) else self.record(raw, where)[0]
                    owner = self._owner_of(info, member)
                    if owner is not None:
                        rekeys.append((owner, info, raw, old_key, new_key))
        self._write(handle, old, new)
        for owner, info, raw, old_key, new_key in rekeys:
            self._rekey(owner, info, raw, old_key, new_key)

    def _check_end(self, record, type_name: str, element, other: MutableObject | None, add: bool,
                   where: str) -> None:
        """Refuse a link that would overwrite a ``one`` end, or that is already there; an unlink of what is
        not linked.  ``other`` ``None`` means the new entity being created (never linked yet)."""
        current = _stored_step(record, element["name"])
        if _single(element):
            linked = _handle_of(current)
            if add and linked is not None:
                raise _invalid(f"{where}: the end {element['name']!r} of {type_name!r} is linked to {linked.name!r}; "
                               f"unlink it first")
            if not add and (linked is None or linked != other):
                raise _invalid(f"{where}: {other.name!r} is not linked to the end {element['name']!r} of "
                               f"{type_name!r}")
            return
        info = self.schema.end_index(type_name, element["name"])
        info.check()
        if other is None:
            return
        found = self._find(None if _unset(current) else current, info, other)
        if add and found is not None:
            raise _invalid(f"{where}: {other.name!r} is already linked to the end {element['name']!r} of {type_name!r}")
        if not add and found is None:
            raise _invalid(f"{where}: {other.name!r} is not linked to the end {element['name']!r} of {type_name!r}")

    def _set_end(self, handle: MutableObject, element: Mapping, other: MutableObject, add: bool) -> None:
        """Set (``add``) or clear one end ``element`` of ``handle`` to ``other``."""
        record, type_name = self.record(handle, "link")
        name = element["name"]
        if not _single(element):
            member, _ = self.record(other, "link")
            self._write(handle, record, self._with_member(record, type_name, name, other, member, add))
            return
        if add and element.get("binding", "identity") == "version":
            value = self._bound(other, *self.record(other, "link"))
        else:
            value = other if add else None
        new = record.with_(**{name: value})
        rekeys = []
        for info in self.schema.keyed_ends(type_name, role=name):
            if info.inverse == name:
                continue                    # the membership itself changes: the add or remove keys it
            owner = self._owner_of(info, new)
            if owner is not None:
                rekeys.append((owner, info, self._key(info, record), self._key(info, new)))
        self.session._store().set_mutable(handle.name, new)
        for owner, info, old_key, new_key in rekeys:
            self._rekey(owner, info, handle, old_key, new_key)

    def _apply_link(self, source: MutableObject, element: Mapping, target: MutableObject, add: bool = True) -> None:
        self._set_end(source, element, target, add)
        inverse = element.get("inverse")
        if inverse is not None:
            _, target_type = self.record(target, "link")
            self._set_end(target, self.schema.element(target_type, inverse), source, add)

    @_atomic
    def link(self, source, role: str, target, add: bool = True) -> None:
        verb = "link" if add else "unlink"
        handle = self.handle(source, verb)
        where = f"{verb}({handle.name!r}, {role!r})"
        record, type_name = self.record(handle, where)
        element = self._role(type_name, role, where)
        other, other_record, other_type = self._target(element, target, where)
        self._check_end(record, type_name, element, other, add, where)
        inverse = element.get("inverse")
        if inverse is not None:
            self._check_end(other_record, other_type, self.schema.element(other_type, inverse), handle, add, where)
        self._apply_link(handle, element, other, add)

    @_atomic
    def relate(self, type_name: str, given: Mapping) -> Version:
        where = f"relate({type_name!r})"
        entity = self.schema.entity(type_name)
        if "roles" not in entity:
            raise _invalid(f"{where}: {type_name!r} is not a relationship; create it with create")
        cls = self._semantic_class(type_name)
        fields, linked = self._fields(type_name, given, where)
        roles = [role["name"] for role in entity["roles"]]
        missing = [name for name in roles if name not in linked]
        if missing:
            raise _invalid(f"{where}: every role of a relationship is given; {missing} are missing")
        stray = [name for name in linked if name not in roles]
        if stray:
            raise _invalid(f"{where}: {stray} are ends of {type_name!r}, not its roles; a relationship atom is "
                           f"created with its roles and fields")
        self._initial_aggregates(type_name, where)
        values, owners = {}, []
        for name in roles:
            element = self.schema.element(type_name, name)
            handle, record, target_type = self._target(element, linked[name][1], f"{where}, {name}")
            values[name] = (self._bound(handle, record, target_type) if element.get("binding") == "version"
                            else handle)
            inverse = element.get("inverse")
            if inverse is not None:
                self.schema.end_index(target_type, inverse).check()
                owners.append((handle, inverse))
        with self.reading():
            atom = cls(**fields, **values)
        for handle, inverse in owners:
            record, target_type = self.record(handle, where)
            self._write(handle, record, self._with_member(record, target_type, inverse, atom, atom, True, fresh=True))
        return Version(self.session, atom, None)

    # -- intents (spec sections 4 and 9; R278) -----------------------------------------------------------

    @_atomic
    def invoke(self, target, name: str, arguments: Mapping):
        """Invoke the intent ``name`` on the entity ``target`` (a handle or a version) with ``arguments`` (by
        parameter name): its ``requires`` is checked on the target's current version, then its
        implementation, found by D62 resolution (so ``_super()`` works), runs with :func:`intent_context`
        bound to a :class:`SessionContext` over these operations.  A result that is a new version of the
        receiver (``self.with_(...)``) updates the target, and the target's current version is returned;
        any other result is returned as it is."""
        from .classes import _invoke
        if not isinstance(name, str) or not name:
            raise _invalid(f"An intent is named by a non-empty str, not {name!r}")
        handle = self.handle(target, f"invoke({name!r})")
        where = f"The intent {name!r} of {handle.name!r}"
        record, type_name = self.record(handle, where)
        element = self.schema.intent(type_name, name)
        if element is None:
            raise _invalid(f"{where}: {type_name!r} has no intent named {name!r}")
        self._check_requires(handle, record, type_name, element)
        with self.reading():
            result = _invoke(record, name, arguments, SessionContext(self, handle, record))
        if isinstance(result, DBObject) and type(result) is type(record):
            return self.get(handle)
        return result

    def _check_requires(self, handle: MutableObject, record, type_name: str, element: Mapping) -> None:
        """The intent's ``requires`` on ``record``, the target's current version: false (MISSING counts
        false) is ``ProtoConstraintError`` naming ``<type>.<intent>.requires``, with the intent's meaning and
        the values it read (spec section 9; preflight F25, F26); the replay keeps that class."""
        requires = element.get("requires")
        if requires is None:
            return
        if _truth("requires", self._evaluate(requires["ast"], record), "the requires of an intent"):
            return
        with self.reading():
            raise _constraints.requires_error(self.schema.model, type_name, element, handle.name, record,
                                              self.reader)

    # -- constraints (spec section 10) ------------------------------------------------------------------

    def check_constraints(self, touched: Mapping, before: Mapping | None, *, cardinality: bool = True,
                          invariants: bool = True) -> None:
        """:func:`proto_db.semantic.constraints.check_constraints` over the session's catalog, reading through
        its source (a transaction's reads enter its read set)."""
        with self.reading():
            _constraints.check_constraints(self.schema.model, touched, self.reader, before=before,
                                           cardinality=cardinality, invariants=invariants)

    def _check_operation(self) -> None:
        """At the end of the outermost operation of a direct-mode transaction, still inside it: the enforced
        invariants of the entities whose operands it changed (the cells it wrote, against their values before
        it), on the transaction's view."""
        tr = self.session._store()
        changes = tr.semantic_changes()
        touched = {}
        for name in changes:
            record = tr.get_mutable(name)
            if record is not None:
                touched[MutableObject(name)] = record
        if touched:
            self.check_constraints(touched, changes, cardinality=False)

    # -- the query engine (preflight F17, F18) ---------------------------------------------------------

    def _version(self) -> dict:
        """The version the engine reads, ``{"number", "ref"}`` (preflight F23)."""
        if self.session is not None:
            return self.session.version()
        return {"number": self.source.version, "ref": base64.b64encode(self.source.ref).decode("ascii")}

    def _selected(self, member, select, selection) -> dict:
        """The row of ``member``: the paths ``select`` names, or, with ``selection``, the paths
        ``selection(member's type name)`` gives -- the only paths evaluated; a failure is tagged with its path."""
        if selection is None:
            names = select
        else:
            names = selection(getattr(type(member), "__type_name__", None))
        row = {}
        for name in names:
            try:
                value = self._path(name, member)
            except _EVALUATION_FAILURES as error:
                _tag(error, f"the selected path {name!r}")
                raise
            if value is not MISSING:
                row[name] = value
        return row

    def _path(self, name: str, member):
        return evaluate(["attr", *name.split(".")], member, reader=self.reader)

    def _sum(self, members, node):
        total = 0
        for _, member in members:
            value = _number("sum", evaluate(node, member, reader=self.reader))
            if value is not MISSING:
                total = _arithmetic("+", total, value)
        return total

    @_consistent
    def query(self, tree: Mapping, *, with_members: bool = False, selection=None) -> dict:
        check_query(tree)
        version = self._version()
        if "as_of" in tree and tree["as_of"] != version["ref"]:
            raise ProtoUserError("This session reads one version; to read another, open a session over "
                                 "Database.snapshot_at(<ref>) and query it")
        origin, where, select, aggregate = tree["from"], tree.get("where"), tree.get("select"), tree.get("aggregate")
        of_type = tree.get("of_type")
        if of_type is not None and self.schema is not None:
            self.schema.entity(of_type)
        if self.schema is not None:
            self._check_declared(origin, of_type, where, aggregate, select)
        session_reader = self.reader
        if self.schema is not None and type(session_reader) is _SessionReader:
            self.reader = _QueryReader(self._resolver, self.schema)
        try:
            return self._query(tree, version, origin, where, select, aggregate, of_type, with_members, selection)
        finally:
            self.reader = session_reader

    def _query(self, tree, version, origin, where, select, aggregate, of_type, with_members, selection) -> dict:
        with self.reading():
            members, explain, member_type, used = self._members(origin, where)
            if of_type is not None:
                allowed = self.schema.subtypes(of_type) if self.schema is not None else frozenset({of_type})
                members = [(raw, m) for raw, m in members if getattr(type(m), "__type_name__", None) in allowed]
                member_type = of_type
            if aggregate is not None and aggregate[0] == "count":
                rows = [{"count": len(members)}]
            elif aggregate is not None:
                rows = [{"sum": _tagged(_AGGREGATE, self._sum, members, aggregate[1])}]
            elif select is not None:
                rows = [self._selected(member, select, selection) for _, member in members]
            else:
                rows = [self._row_version(raw, member) for raw, member in members]
        reads = [t for t in (where, aggregate[1] if aggregate and aggregate[0] == "sum" else None) if t is not None]
        reads += [["attr", *name.split(".")] for name in select or ()]
        types = [member_type] if member_type is not None else \
            sorted({getattr(type(m), "__type_name__", type(m).__name__) for _, m in members})
        terms = [] if self.schema is None else self._terms(used, types, reads)
        answer = {"rows": rows, "explain": explain, "version": version, "query": copy.deepcopy(dict(tree)),
                  "terms": terms}
        if with_members and aggregate is None:
            answer["members"] = [self._row_version(raw, member) for raw, member in members]
        return answer

    def _check_declared(self, origin, of_type, where, aggregate, select) -> None:
        """Refuse, before anything is read, a term whose path names an attribute or a role that the members'
        type does not declare (catalog mode): read as missing, it would make a sum answer 0.  The members' type
        is ``of_type`` or the type the ``from`` reaches, subtypes included; when the ``from`` says nothing
        about it (a root origin, or a cell the catalog does not type), nothing is refused here: each row is
        checked as it is read (:class:`_QueryReader`).  A step after an attribute that is not a role, or one
        starting with ``@``, is not checked."""
        base = of_type if of_type is not None else self._static_types(origin)[1]
        if base is None:
            return
        terms = [(_WHERE, where), (_AGGREGATE, aggregate[1] if aggregate and aggregate[0] == "sum" else None)]
        terms += [(f"the selected path {name!r}", ["attr", *name.split(".")]) for name in select or ()]
        for part, term in terms:
            stack = [] if term is None else [term]
            while stack:
                node = stack.pop()
                if not (isinstance(node, list) and node and isinstance(node[0], str)):
                    continue
                if node[0] == "attr":
                    self._check_path(part, node[1:], base)
                elif node[0] in _CHECKED_OPERATORS:
                    stack.extend(node[1:])

    def _check_path(self, part: str, steps, base: str) -> None:
        """Check the steps of one ``attr`` from members of the catalogued type ``base``."""
        for step in steps:
            if not isinstance(step, str) or step.startswith("@"):
                return
            found = [e for e in (self.schema.element(t, step) for t in self.schema.subtypes(base)) if e is not None]
            if not found:
                raise _plain(_invalid(f"{part}: the type {base!r} declares no attribute or role {step!r}"))
            targets = {e["target"] for e in found if "target" in e}
            if len(targets) != 1 or len(targets) != len(found):
                return                      # an attribute (or roles to different types): not followed further
            [base] = targets

    def _traverse(self, origin, roles) -> list:
        """The members ``(stored value, record)`` reached from ``origin`` (a ``cell`` or ``root`` origin, or
        a bare root) through ``roles``, as ``via`` reaches them (format spec, "Traversal"), keeping the
        stored value (the handle) beside the record it designates."""
        if origin[0] == "cell":
            found = self.reader.cell(origin[1])
            current = [] if found is None else [(MutableObject(origin[1]), found)]
            if found is not None:
                reached(None, origin[1], found)
        else:
            current = self._reached(self.reader.members(self.reader.root(origin[1])))
        for role in roles:
            out, seen = [], {}
            for _, record in current:
                self._reached((unwrap_entry(raw) for raw in self.reader.members(_stored_step(record, role))),
                              record, role, out, seen)
            current = out
        return current

    def _reached(self, raws, holder=None, step=None, out=None, seen=None) -> list:
        """Append to ``out`` each ``(stored value, member)`` of ``raws`` not yet ``seen``, followed; with a
        ``holder``, every member is reported as reached through its ``step``
        (:func:`~proto_db.semantic.expressions.reached`)."""
        out = [] if out is None else out
        seen = {} if seen is None else seen
        for raw in raws:
            key = _identity(raw)
            if key in seen:
                followed = seen[key]
            else:
                followed = seen[key] = self.reader.follow(raw)
                if followed is not MISSING:
                    out.append((raw, followed))
            if holder is not None and followed is not MISSING:
                reached(holder, step, followed)
        return out

    def _static_types(self, origin) -> tuple:
        """The roles a ``via`` from a cell crosses, ``[(owner type, role)]``, and its members' type, as the
        catalog declares them (``None`` when not known: a root origin)."""
        if self.schema is None or origin[0] != "via" or origin[1][0] != "cell":
            return [], None
        type_name = origin[1][1].rsplit("#", 1)[0]
        used = []
        for role in origin[2:]:
            element = self.schema.element(type_name, role)
            if element is None or "target" not in element:
                return used, None
            used.append((type_name, role))
            type_name = element["target"]
        return used, type_name

    def _members(self, origin, where) -> tuple:
        """``(members as (stored value, record) pairs, explain, members' type, roles crossed)``."""
        conjuncts = [] if where is None else list(where[1:]) if where[0] == "and" else [where]
        used, member_type = self._static_types(origin)
        source = {"from": copy.deepcopy(origin)}
        if origin[0] == "via":
            elements = _tagged(_FROM, self._traverse, origin[1], origin[2:-1])
            last = origin[-1]
            if len(elements) == 1 and is_atom(elements[0][1]):
                record = elements[0][1]
                collection = _stored_step(record, last)
                if self.schema is None:
                    if isinstance(collection, (List, Set)):
                        return (*self._indexed(collection, None, conjuncts, source, record, last), None, used)
                else:
                    owner_type = getattr(type(record), "__type_name__", None)
                    element = self.schema.element(owner_type, last) if self.schema.has(owner_type) else None
                    if element is not None and "target" in element and not _single(element):
                        info = self.schema.end_index(owner_type, last)
                        return (*self._indexed(None if _unset(collection) else collection, info, conjuncts,
                                               source, record, last),
                                member_type or info.member_type, used)
            pairs = _tagged(_FROM, self._traverse, origin[1], origin[2:])
        else:
            pairs = _tagged(_FROM, self._traverse, origin, ())
        kept = _tagged(_WHERE, self._satisfying, [(raw, m) for raw, m in pairs if is_atom(m)], conjuncts)
        return kept, {**source, "plan": None, "indexed": [], "residual": copy.deepcopy(conjuncts)}, member_type, used

    def _row_version(self, raw, member) -> Version:
        return Version(self.session, member, _handle_of(raw), bound=isinstance(raw, dict))

    def _satisfying(self, pairs, conjuncts) -> list:
        """The ``(stored value, member)`` pairs whose member satisfies every conjunct."""
        return [(raw, member) for raw, member in pairs if all(self._holds(c, member) for c in conjuncts)]

    def _holds(self, conjunct, member) -> bool:
        return _truth("where", evaluate(conjunct, member, reader=self.reader), "a conjunct of the where")

    def _indexed(self, collection, info: _EndIndex | None, conjuncts, source, holder=None, step=None) -> tuple:
        """The members of the stored ``collection`` of the end ``info`` that satisfy ``conjuncts``: what the
        planner can answer exactly goes to it over the collection (its indexes), the rest is a residual.
        With ``info=None`` (no catalog) the collection's stored structure decides: over records, a conjunct
        on one stored attribute plans (evaluation reads stored values only, as the planner does); over
        entries or handles, nothing does.  Deciding that costs one pass over the elements' kinds, made only
        when some conjunct could plan.  ``holder`` is the record whose attribute ``step`` holds the collection:
        while a check of reached members is held (:func:`~proto_db.semantic.expressions.checking_reached`),
        every member of the collection is reported, before the plan runs."""
        planned, candidates, indexed, residual = [], [], [], []
        allowed = None
        if info is None:
            # No catalog says what the collection holds, and no stored metadata does: its elements do.  It
            # plans only when every element is a record (a handle or an entry would be read by the planner
            # as itself, where evaluation follows it to its member); entries are unwrapped either way.
            entries = True
            if collection is not None and collection.count \
                    and any(to_planner(conjunct) is not None for conjunct in conjuncts) \
                    and _tagged(_FROM, _holds_records_only, collection):
                allowed = _ANY_NAME
        elif info.entries:
            entries = True
            if info.key.attribute in self.schema.plannable(info.key.target):
                allowed = frozenset({"key"})
        else:
            entries = False
            if info.records:
                allowed = self.schema.plannable(info.member_type)
        for conjunct in conjuncts:
            candidate = _rewrite(conjunct, info.key.role, info.key.attribute) if info is not None and info.entries \
                else conjunct
            term = to_planner(candidate) if allowed is not None and _reads_only(candidate, allowed) else None
            if term is None:
                residual.append(conjunct)
            else:
                planned.append(term)
                candidates.append(candidate)
                indexed.append(conjunct)
        if collection is None:
            return [], {**source, "plan": None, "indexed": copy.deepcopy(indexed), "residual": copy.deepcopy(residual)}
        kinds = _planned_comparisons(candidates) if refusing() else ()
        if (holder is not None and checking()) or kinds:
            elements = list(collection.elements())          # one walk of the end serves both checks
            if holder is not None and checking():
                # Every member of the end is checked, not only those the plan keeps: whether the check refuses
                # must not depend on what the where selects (no yes/no answer about a member it would refuse).
                _tagged(_FROM, self._reached, [unwrap_entry(element) if entries else element
                                               for element in elements], holder, step)
            if kinds:
                # Under the agent-query rules a comparison across kinds is refused (spec section 11, R283), but the
                # planner answers a planned comparison without the evaluator: it is checked here on every element
                # of the end, by the stored value the planner reads.  Agent queries only.
                _tagged(_WHERE, _check_planned_kinds, elements, kinds)
        query = from_collection(collection)
        if planned:
            query = query.where(planned[0] if len(planned) == 1 else And(*planned))
        plan = query.explain()
        listed = _tagged(_WHERE if planned else _FROM, query.to_list)
        reached = _tagged(_FROM, self._reached, [unwrap_entry(element) if entries else element for element in listed])
        members = _tagged(_WHERE, self._satisfying, reached, residual)
        return members, {**source, "plan": plan, "indexed": copy.deepcopy(indexed), "residual": copy.deepcopy(residual)}

    def _terms(self, used: list, types: list, reads: list) -> list:
        """The catalog elements the query used, with their certification: the roles it crossed and the
        attributes (and the roles of their paths) it read on its members."""
        out, seen = [], set()

        def add(type_name, name):
            element = self.schema.element(type_name, name)
            if element is None or (type_name, name) in seen:
                return element
            seen.add((type_name, name))
            certification = element.get("certification", self.schema.entity(type_name).get("certification"))
            out.append({"type_name": type_name, "name": name, "kind": _kind(element), "certification": certification})
            return element

        for type_name, role in used:
            add(type_name, role)
        paths = []
        for tree in reads:
            stack = [tree]
            while stack:
                node = stack.pop()
                if isinstance(node, list) and node and isinstance(node[0], str):
                    if node[0] == "attr":
                        paths.append(node[1:])
                    elif node[0] != "const":
                        stack.extend(child for child in node[1:] if isinstance(child, list))
        for type_name in types:
            for path in paths:
                owner = type_name
                for step in path:
                    element = add(owner, step) if owner is not None and self.schema.has(owner) else None
                    owner = element.get("target") if element is not None else None
        return out


def _planned_comparisons(candidates) -> tuple:
    """The comparisons of the planned conjuncts ``candidates`` (each a one-step ``attr`` against a ``const``, the
    shape :func:`~proto_db.semantic.expressions.to_planner` plans) whose constant is present and orderable, in
    the order they appear (conjunct by conjunct, left to right, under ``and``, ``or`` and ``not`` alike: the index
    answers every one of them over the whole end), as ``(operator, name, constant, attr_left, constant's rank)``."""
    found, stack = [], list(reversed(candidates))
    while stack:
        node = stack.pop()
        if not (isinstance(node, list) and node and isinstance(node[0], str)):
            continue
        if node[0] in ("==", "!=", "<", "<=", ">", ">="):
            left, right = node[1], node[2]
            attr_left = left[0] == "attr" and right[0] == "const"
            if not (attr_left or (right[0] == "attr" and left[0] == "const")):
                continue
            (name,), value = (left[1:], right[1]) if attr_left else (right[1:], left[1])
            value = canonical(value)
            if value is not MISSING and value is not None and is_orderable(value):
                found.append((node[0], name, value, attr_left, order_key(value)[0]))
        elif node[0] != "const":
            stack.extend(reversed(node[1:]))
    return tuple(found)


def _check_planned_kinds(elements, kinds) -> None:
    """Refuse a planned comparison across kinds (:func:`~proto_db.semantic.expressions.check_kinds`) on any of
    ``elements``, each read as the planner reads it: its stored value, MISSING when absent (no default).  Each
    attribute is read and ranked once per element; on an element, the first mismatching comparison in ``kinds``'
    order is the one the message names."""
    for element in elements:
        ranks: dict = {}
        for op, name, value, attr_left, value_rank in kinds:
            if name not in ranks:
                stored = _stored_step(element, name)
                orderable = stored is not MISSING and stored is not None and is_orderable(stored)
                ranks[name] = (stored, order_key(stored)[0] if orderable else None)
            stored, rank = ranks[name]
            if rank is not None and rank != value_rank:
                check_kinds(op, stored, value) if attr_left else check_kinds(op, value, stored)


def _same_key(old, new) -> bool:
    if old is MISSING or new is MISSING:
        return old is new
    return type(old) is type(new) and values_equal(old, new)


def _rewrite(node, role: str, attribute: str):
    """``node`` with every ``["attr", role, attribute]`` as ``["attr", "key"]`` (preflight F17)."""
    if not isinstance(node, list) or not node or not isinstance(node[0], str):
        return node
    if node[0] == "attr":
        return ["attr", "key"] if node[1:] == [role, attribute] else node
    if node[0] == "const":
        return node
    return [node[0], *(_rewrite(child, role, attribute) for child in node[1:])]


class _AnyName:
    """Every attribute name (a stored collection of records without a catalog: every stored value plans)."""

    def __contains__(self, name) -> bool:
        return True


_ANY_NAME = _AnyName()


def check_query(tree) -> None:
    """Check the shape of a query tree (spec section 11), before anything is read: its keys, a ``from`` that
    is a ``via`` from a root or a cell or a bare root, a ``where`` and an ``aggregate`` term that are
    expressions of the language, an ``aggregate`` that is ``["sum", term]`` or ``["count"]``, a ``select``
    that is a list of attribute paths.  ``ProtoValidationError`` names what is wrong."""
    if not isinstance(tree, Mapping):
        raise _invalid(f"A query is a mapping (spec section 11), not {type(tree).__name__}")
    unknown = sorted(set(tree) - _QUERY_KEYS)
    if unknown:
        raise _invalid(f"A query has the keys {sorted(_QUERY_KEYS)}; unknown keys {unknown}")
    origin = tree.get("from")
    if not isinstance(origin, list) or not origin or origin[0] not in ("via", "root"):
        raise _invalid(f"A query's 'from' is a via expression or a bare root, not {origin!r}")
    check_ast(origin, in_query=True)
    if origin[0] == "via" and origin[1][0] == "self":
        raise _invalid("A query has no current element: its 'from' starts at a root or a cell")
    if tree.get("where") is not None:
        check_ast(tree["where"], in_query=True)
    select = tree.get("select")
    if select is not None and (isinstance(select, str) or not isinstance(select, (list, tuple))
                               or not all(isinstance(n, str) and n and all(n.split(".")) for n in select)):
        raise _invalid(f"A query's 'select' is a list of attribute paths, not {select!r}")
    aggregate = tree.get("aggregate")
    if aggregate is not None:
        if not isinstance(aggregate, list) or not aggregate or aggregate[0] not in ("sum", "count") \
                or len(aggregate) != (2 if aggregate[0] == "sum" else 1):
            raise _invalid(f"A query's 'aggregate' is ['sum', term] or ['count'], not {aggregate!r}")
        if aggregate[0] == "sum":
            check_ast(aggregate[1], in_query=True)
    of_type = tree.get("of_type")
    if of_type is not None and (not isinstance(of_type, str) or not of_type):
        raise _invalid(f"A query's 'of_type' is a type name, not {of_type!r}")


def query_stored(snapshot, tree, *, with_members: bool = False, selection=None) -> dict:
    """Run the query ``tree`` with the session's engine over the stored structure of ``snapshot`` alone:
    no catalog (no defaults, ``of_type`` is the stored type, no terms), the stored collections' indexes
    where their structure allows (see :meth:`Operations._indexed`), and no reserved root.  The agent
    interface's raw mode (:mod:`proto_db.agent.operations`)."""
    return Operations(None, snapshot, None, reader=StoredReader(snapshot)).query(tree, with_members=with_members,
                                                                                 selection=selection)


def _holds_records_only(collection) -> bool:
    """Whether every element of the stored ``collection`` is a record, neither an identity handle nor an entry
    (raw planning, :meth:`Operations._indexed`): one pass over its elements."""
    return all(is_atom(element) and unwrap_entry(element) is element for element in collection.elements())


def _reads_only(node, names) -> bool:
    """Whether every ``attr`` of ``node`` is one step naming one of ``names``."""
    stack = [node]
    while stack:
        item = stack.pop()
        if isinstance(item, list) and item and isinstance(item[0], str):
            if item[0] == "attr":
                if len(item) != 2 or item[1] not in names:
                    return False
            elif item[0] != "const":
                stack.extend(item[1:])
    return True
