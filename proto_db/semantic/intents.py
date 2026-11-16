"""Intent transactions, on one node (spec section 9; rulings R260, R261, R269 to R271; Task 8; preflight F22,
F26).

``Database.new_transaction(intents=True)`` (or ``Database.run(fn, intents=True)``) starts a transaction in
**intent mode**: its semantic session (:func:`proto_db.semantic.semantic`) logs each top-level operation
instead of writing cells, and the commit replays the log on the state current at the serialized point.
Without the flag nothing changes.  A space with a cluster coordinator refuses the flag
(``ProtoNotSupportedError``).

Recording
    Each top-level operation of the session -- an intent invocation (``order.add_line(...)``,
    :meth:`~proto_db.semantic.SemanticSession.invoke`) or a primitive (``create``, ``update``, ``link``,
    ``unlink``, ``relate``) -- runs at once on the transaction's **record view**, :class:`Cells`: the
    snapshot's cells with the log's own effects over them, so the caller sees what it did (the handles it
    created, the totals it moved).  The view is not the transaction's cells: the operation writes nothing
    in them, and its own reads enter no read set (the replay reads again).  An operation that fails leaves
    nothing behind, in the view or in the log.  An intent's arguments are logged as they are given: an
    entity as its handle (in the record, its cell name), any other value as a canonical value.  The name of
    every cell a creation makes is drawn when the operation is recorded (R260): a ``create``'s cell is its
    target, the cells an intent creates travel in its arguments under ``"#created"`` (no parameter can be
    named so).  The replay draws nothing.

Fine reads and condition reads (R270, R261)
    What the caller reads through the session, outside the operations, is what its decisions rest on, so
    that is what the commit validates (never the cells' whole read set: the session's reads never enter
    ``Transaction._mreads``):

    - an attribute of an entity's version that is stored (a field, a maintained aggregate) or a role (a
      ``one`` end, or a ``many`` end, whose collection is then one read: a condition read of that end) is
      recorded as ``(cell, attribute) -> exact token`` of the attribute in the snapshot (ABSENT included);
    - an attribute whose value may depend on any attribute of the record (a computed attribute, a Python
      member), and taking ``version.record`` (the stored record itself), are recorded as
      ``(cell, None) -> exact token`` of the whole record; so is every cell a query or a computed attribute
      reads (the query's scanned collections and its members): coarse, never wrong;
    - the version a role binds (``binding="version"``) and a relationship atom never change, so reading them
      records nothing; the navigation that reached them did.

    Reads made while an intent runs record nothing: the replay reads again.  ``requires`` is checked when
    an intent is recorded, as a courtesy, without recording any read (F26); the replay's check decides.

The commit (spec section 9; :meth:`IntentLog.replay`)
    Under the root lock, in ``Transaction._build_root``, after the root writes and the registry merge and
    before the raw cell writes (F22): (1) every fine read is compared with the current cell (a change is
    ``ProtoConflictError``); so are the registry and catalog roots, since the replay composes with the
    snapshot's catalog; (2) the log is replayed on the current cells with the operations of direct mode
    (:class:`proto_db.semantic.entities.Operations`, over a fresh :class:`Cells`): both ends of every
    relation, maintained keys and aggregates, and each intent's ``requires`` on its target (false:
    ``ProtoValidationError`` naming ``<type>.<intent>.requires``; Task 9 makes it ``ProtoConstraintError``);
    (3) ``__intentLog`` is set to a ``List`` of the commit's :class:`~proto_db.catalog.atoms.IntentRecord`\\ s,
    whose ``transaction`` is the version the commit produces (R271).  A commit whose transaction logged
    nothing, or that is not in intent mode, writes no ``__intentLog`` and removes an older one: the root holds
    the records of the commit that wrote its version, and nothing older.

    Locks.  The replay runs in whichever thread builds the commit: its own, or a group-commit leader that
    builds the queued commits of other threads while each of those threads holds its own transaction's
    ``_lock`` and waits.  So the replay takes no transaction ``_lock``: it reads the log, which is frozen
    (it changes only under ``_lock`` while the transaction is active, and the transaction stopped being
    active, under ``_lock``, before its commit was queued); it reads and writes only the cells of its own
    :class:`Cells`; it resolves through the transaction's registry memo, filled before the root lock
    (``Transaction._prebuild_registries``); and ``intent_context()`` is a ``contextvars`` binding set around
    each replayed call in the building thread.  An intent that reaches another transaction's public method
    finds it either not active (it raises before taking that lock) or held by a call that never waits for
    the root lock.  The order stays ``_lock``, then the root lock, then the leaf locks (atom cache, registry
    cache); the replay adds no edge to it.

Mixed raw writes (spec section 9)
    A raw ``set_mutable``, ``new_mutable`` or ``remove_mutable`` of a cell the log's operations touched (read
    or wrote) is refused (``ProtoUserError``); so is a session access to a cell the transaction wrote raw
    (the session's view would not show the raw write), at record time and at replay.  Raw reads see the
    snapshot and the raw writes, never the log's effects, which exist from the commit on.  Other raw reads
    and writes keep today's rules.

Determinism
    An intent reads nothing external during the replay: external values travel as arguments.  This is
    documented, not policed (P2).  The registry, the catalog and the created cells' names are the
    recording's.
"""
from __future__ import annotations

import contextlib
import uuid
from collections.abc import Mapping

from .. import registry as _registry
from .. import resolution
from ..catalog.atoms import INTENT_LOG_ROOT, SEMANTIC_CATALOG_ROOT, IntentRecord
from ..collections.lists import List
from ..collections.sets import Set
from ..core.dbobject import DBObject
from ..errors import ProtoConflictError, ProtoUserError, ProtoValidationError
from ..indexing.keyspec import _step as _stored_step
from ..transactions import _ABSENT, _same_token, _token
from ..values import MISSING, MutableObject, canonical_for, is_atom
from .classes import _RUNNING
from .entities import Operations, Version

__all__ = ["IntentLog", "Cells", "CREATED"]

#: The argument key under which an intent's record lists the cells it created (no parameter name has ``#``).
CREATED = "#created"
#: The roots the replay's catalog comes from: a concurrent change of one is a conflict.
_CATALOG_ROOTS = (_registry.MODULES_ROOT, _registry.CATALOG_ROOT, SEMANTIC_CATALOG_ROOT)


# -- the cells an operation reads and writes ----------------------------------------------------------------

class _Layer:
    __slots__ = ("writes", "versions", "touched")

    def __init__(self):
        self.writes: dict = {}          # cell name -> value
        self.versions: dict = {}        # cell name -> the kept field version (entities, "Storage")
        self.touched: set = set()       # the cells the operations read or wrote


class Cells:
    """The cells of an intent transaction as its semantic operations see them: ``base`` (a version's
    ``mutables``) with the operations' writes over it.  The record view (over the snapshot) and each replay
    (over the cells current at commit) are one each.  It offers what
    :class:`~proto_db.semantic.entities.Operations` uses of a transaction (``get_mutable``, ``new_mutable``,
    ``set_mutable``, ``snapshot_mutable``, the field-version slot, ``new_list``, ``new_set``,
    ``get_root_object``) and the reader protocol of :mod:`proto_db.resolution` (the transaction's registry),
    and takes no lock: the record view is used under the transaction's ``_lock``, a replay by the one
    thread building the commit.

    :meth:`begin`, :meth:`keep` and :meth:`drop` make an operation all-or-nothing on the record view.  A cell
    the transaction wrote raw is refused (see the module docstring, "Mixed raw writes")."""

    __slots__ = ("_tr", "_base", "_objects", "_layers", "_replay")

    def __init__(self, tr, base, objects=None, *, replay: bool = False):
        self._tr = tr
        self._base = base
        self._objects = objects
        self._layers = [_Layer()]
        self._replay = replay

    # -- the reader protocol (proto_db.resolution.registry_of): the transaction's registry, never its lock --

    @property
    def _reader_space(self):
        return self._tr._space_registries

    @property
    def _reader_database(self) -> str:
        return self._tr._database.name

    def _reader_registry(self):
        return self._tr._resolution_registry()

    def _reader_names(self) -> tuple:
        return self._tr._listed

    @property
    def _reader_transaction(self):
        """The transaction these cells stand for (:func:`proto_db.semantic.descriptors._reader_for`)."""
        return self._tr

    # -- operations, all or nothing ---------------------------------------------------------------------

    def begin(self) -> None:
        self._layers.append(_Layer())

    def keep(self) -> None:
        top = self._layers.pop()
        under = self._layers[-1]
        under.writes.update(top.writes)
        under.versions.update(top.versions)
        under.touched |= top.touched

    def drop(self) -> None:
        self._layers.pop()

    def touches(self, name: str) -> bool:
        """Whether an operation read or wrote the cell ``name``."""
        return any(name in layer.touched for layer in self._layers)

    def written(self) -> dict:
        """The cells the operations wrote, by name (outside an operation)."""
        return self._layers[0].writes

    # -- cells --------------------------------------------------------------------------------------------

    def _touch(self, name: str) -> None:
        if name in self._tr._mwrites:
            raise ProtoUserError(
                f"The cell {name!r} was written raw in this transaction (set_mutable, new_mutable or remove_mutable); "
                f"the intent log does not read or write a raw-written cell (spec section 9): change it through "
                f"the semantic session only")
        if self._replay or len(self._layers) > 1:
            self._layers[-1].touched.add(name)

    def _lookup(self, name: str):
        for layer in reversed(self._layers):
            if name in layer.writes:
                return layer.writes[name]
        return self._base.get_at(name, _ABSENT)

    def get_mutable(self, name: str):
        self._touch(name)
        value = self._lookup(name)
        return None if value is _ABSENT else value

    def snapshot_mutable(self, name: str):
        """The cell's value in the base: the committed record the operation reads."""
        self._touch(name)
        value = self._base.get_at(name, _ABSENT)
        return None if value is _ABSENT else value

    def new_mutable(self, name: str, value) -> MutableObject:
        self._touch(name)
        value = canonical_for(value, f"Mutable {name!r}")
        if self._lookup(name) is not _ABSENT:
            if self._replay:
                raise ProtoConflictError(f"The cell {name!r}, which this transaction creates, was created by a "
                                         f"concurrent commit")
            raise ProtoValidationError(f"Mutable {name!r} already exists")
        self._layers[-1].writes[name] = value
        return MutableObject(name)

    def set_mutable(self, name: str, value) -> None:
        self._touch(name)
        value = canonical_for(value, f"Mutable {name!r}")
        if self._lookup(name) is _ABSENT:
            raise ProtoValidationError(f"Mutable {name!r} does not exist; create it with new_mutable")
        self._layers[-1].writes[name] = value

    def semantic_field_version(self, name: str):
        for layer in reversed(self._layers):
            if name in layer.versions:
                return layer.versions[name]
        return None

    def set_semantic_field_version(self, name: str, expected, record):
        current = self.semantic_field_version(name)
        if current is expected:
            self._layers[-1].versions[name] = current = record
        return current

    def new_list(self) -> List:
        return List(context=self._tr)

    def new_set(self) -> Set:
        return Set(context=self._tr)

    def get_root_object(self, name: str):
        """A root: at record time through the transaction (the read enters its root read set, validated at
        commit as today); at replay, the roots the commit is building on."""
        if self._replay:
            value = self._objects.get_at(name, _ABSENT)
            return None if value is _ABSENT else value
        return self._tr.get_root_object(name)


class Tracking:
    """The record view seen by a read the caller decides on: every cell read through it is recorded as a
    whole-cell read (see "Fine reads and condition reads")."""

    __slots__ = ("_cells", "_log")

    def __init__(self, cells: Cells, log: "IntentLog"):
        self._cells = cells
        self._log = log

    @property
    def _reader_space(self):
        return self._cells._reader_space

    @property
    def _reader_database(self) -> str:
        return self._cells._reader_database

    def _reader_registry(self):
        return self._cells._reader_registry()

    def _reader_names(self) -> tuple:
        return self._cells._reader_names()

    @property
    def _reader_transaction(self):
        return self._cells._reader_transaction

    def get_mutable(self, name: str):
        self._log.read(name)
        return self._cells.get_mutable(name)

    def get_root_object(self, name: str):
        return self._cells.get_root_object(name)


# -- the log ------------------------------------------------------------------------------------------------

def _value_token(record, attribute):
    """The exact token of the attribute ``attribute`` of the cell value ``record`` (``None``: the whole
    value), ABSENT when the cell or the stored attribute is absent."""
    if record is _ABSENT:
        return _ABSENT
    if attribute is None:
        return _token(record)
    if not isinstance(record, DBObject):
        return ("not a record", _token(record))
    value = _stored_step(record, attribute)
    return _ABSENT if value is MISSING else _token(value)


def _loggable(name: str, value, where: str) -> tuple:
    """``(value to run with, value to log)`` for the argument ``name``: an entity (a handle, or a version the
    session gave) as its handle and its cell name, any other value as a canonical value."""
    if isinstance(value, Version):
        if value.entity is None:
            raise ProtoValidationError(f"{where}: the argument {name!r} is a relationship atom, which has no "
                                       f"identity; an argument names an entity by its handle or version")
        return value.entity, value.entity.name
    if isinstance(value, MutableObject):
        return value, value.name
    if is_atom(value):
        raise ProtoValidationError(f"{where}: the argument {name!r} is a stored record; an argument names an "
                                   f"entity by its handle or the session's version of it, and is otherwise a "
                                   f"plain value")
    value = canonical_for(value, f"{where}, argument {name!r}")
    return value, value


def _loggables(arguments: Mapping, where: str) -> tuple:
    run, logged = {}, {}
    for name, value in arguments.items():
        run[name], logged[name] = _loggable(name, value, where)
    return run, logged


class Entry:
    """One logged operation: ``intent`` (the pair of type name and intent or primitive name), ``target`` (a
    cell name), ``arguments`` (logged, canonical), ``created`` (the names of the cells it created, in
    order), the ``schema`` it ran with, and ``run(operations)``, which applies it again."""

    __slots__ = ("intent", "target", "arguments", "created", "schema", "run")

    def __init__(self, intent: tuple, target: str, arguments: dict, created: list, schema, run):
        self.intent = intent
        self.target = target
        self.arguments = arguments
        self.created = created
        self.schema = schema
        self.run = run

    def record_arguments(self) -> dict:
        """The arguments of its IntentRecord: the logged ones, with ``"#created"`` when it created cells other
        than its target."""
        others = [name for name in self.created if name != self.target]
        return {**self.arguments, CREATED: list(others)} if others else dict(self.arguments)


class IntentLog:
    """The intent log of an intent transaction (see the module docstring): its record view, its operations
    and its fine reads.  Changed only under the transaction's ``_lock`` while it is active; read by the
    commit's build once it is not."""

    __slots__ = ("_tr", "cells", "entries", "_reads", "_drawn")

    def __init__(self, tr):
        self._tr = tr
        self.cells = Cells(tr, tr._mutables)
        self.entries: list = []
        self._reads: dict = {}          # (cell, attribute or None) -> exact token in the snapshot
        self._drawn = None              # the cell names drawn by the operation being recorded

    # -- recording ----------------------------------------------------------------------------------------

    def read(self, cell: str, attribute: str | None = None) -> None:
        """Record a fine read of ``attribute`` of ``cell`` (``None``: the whole cell), as the snapshot holds
        it; the first read of a key is kept."""
        key = (cell, attribute)
        if key not in self._reads:
            self._reads[key] = _value_token(self._tr._mutables.get_at(cell, _ABSENT), attribute)

    def touches(self, name: str) -> bool:
        return self.cells.touches(name)

    def new_cell(self, type_name: str) -> str:
        """Draw the name of a cell an operation being recorded creates (R260)."""
        if self._drawn is None:
            raise ProtoUserError("A semantic operation creates entities only while it is recorded")
        name = f"{type_name}#{uuid.uuid4().hex}"
        self._drawn.append(name)
        return name

    def _apply(self, session, intent_of, run):
        """Record one top-level operation: ``run(operations)`` on the record view, all or nothing, then
        ``intent_of(operations, result)`` gives its ``(intent pair, target)``."""
        tr = self._tr
        with tr.semantic_operation():
            if _RUNNING.get() is not None:
                raise _inside_an_intent()
            operations = session._operations(track=False)
            self.cells.begin()
            self._drawn = drawn = []
            try:
                result = run(operations)
                intent, target = intent_of(operations, result)
            except BaseException:
                self.cells.drop()
                raise
            finally:
                self._drawn = None
            self.cells.keep()
            return result, intent, target, drawn, operations.schema

    def _log(self, session, intent_of, run, arguments: dict):
        result, intent, target, drawn, schema = self._apply(session, intent_of, run)
        self.entries.append(Entry(intent, target, arguments, drawn, schema, run))
        return result

    def create(self, session, type_name: str, fields: Mapping):
        values, logged = _loggables(fields, f"create({type_name!r})")
        return self._log(session, lambda ops, handle: ((type_name, "create"), handle.name),
                         lambda ops: ops.create(type_name, values), logged)

    def update(self, session, target, changes: Mapping):
        handle = _handle(target, "update")
        values, logged = _loggables(changes, f"update({handle.name!r})")
        return self._log(session, lambda ops, _: (_type_of(ops, handle, "update"), handle.name),
                         lambda ops: ops.update(handle, values), logged)

    def link(self, session, source, role: str, target, add: bool):
        verb = "link" if add else "unlink"
        handle = _handle(source, verb)
        if not isinstance(role, str):
            raise ProtoValidationError(f"{verb}({handle.name!r}): a role is named by a str, not {role!r}")
        other, logged = _loggable("target", target, f"{verb}({handle.name!r}, {role!r})")
        return self._log(session, lambda ops, _: (_type_of(ops, handle, verb), handle.name),
                         lambda ops: ops.link(handle, role, other, add=add), {"role": role, "target": logged})

    def relate(self, session, type_name: str, given: Mapping):
        values, logged = _loggables(given, f"relate({type_name!r})")

        def intent_of(ops, _):
            first = ops.schema.entity(type_name)["roles"][0]["name"]
            return (type_name, "relate"), values[first].name
        return self._log(session, intent_of, lambda ops: ops.relate(type_name, values), logged)

    def invoke(self, session, target, name: str, arguments: Mapping):
        handle = _handle(target, f"invoke({name!r})")
        values, logged = _loggables(arguments, f"The intent {name!r} of {handle.name!r}")
        if CREATED in logged:
            raise ProtoValidationError(f"The intent {name!r}: {CREATED!r} is not a parameter name")
        return self._log(session, lambda ops, _: ((_type_of(ops, handle, name)[0], name), handle.name),
                         lambda ops: ops.invoke(handle, name, values), logged)

    # -- the commit ---------------------------------------------------------------------------------------

    def _validate(self, db_root) -> None:
        """Step 1 of the commit: the fine reads, and the catalog the replay composes with, against the
        current version ``db_root``."""
        current, database = db_root.mutables, self._tr._database.name
        for (cell, attribute), token in self._reads.items():
            if not _same_token(token, _value_token(current.get_at(cell, _ABSENT), attribute)):
                what = "the cell" if attribute is None else f"the attribute {attribute!r} of the cell"
                raise ProtoConflictError(
                    f"Intent transaction: {what} {cell!r} of database {database!r}, read through the semantic "
                    f"session, was changed by a concurrent commit")
        if self.entries:
            snapshot, objects = self._tr._objects, db_root.objects
            for name in _CATALOG_ROOTS:
                if not _same_token(_token(snapshot.get_at(name, _ABSENT)), _token(objects.get_at(name, _ABSENT))):
                    raise ProtoConflictError(
                        f"Intent transaction: the root {name!r} of database {database!r} was changed by a "
                        f"concurrent commit; the log was recorded with the catalog it replaced")

    def replay(self, tr, db_root, objects, mutables) -> tuple:
        """Under the root lock, in ``Transaction._build_root`` (preflight F22): validate the fine reads
        (``ProtoConflictError``), replay the log on ``mutables`` (the current version's cells), and set
        ``__intentLog`` in ``objects`` (the roots being committed).  Returns ``(objects, mutables)``.  Takes
        no transaction lock (see "Locks" in the module docstring)."""
        if db_root is not tr._db_root:
            self._validate(db_root)
        if not self.entries:
            return without_intent_log(objects), mutables
        version = db_root.version + 1
        cells = Cells(tr, mutables, objects, replay=True)
        token = resolution.set_reader(cells)
        try:
            for entry in self.entries:
                session = ReplaySession(entry, cells)
                entry.run(session._operations())
                session.check_created()
        finally:
            resolution.reset_reader(token)
        for name, value in cells.written().items():
            mutables = mutables.set_at(name, value)
        records = [IntentRecord(intent=entry.intent, target=entry.target, arguments=entry.record_arguments(),
                                transaction=version, context=tr) for entry in self.entries]
        return objects.set_at(INTENT_LOG_ROOT, List.from_iterable(records, context=tr)), mutables


def without_intent_log(objects):
    """``objects`` (the roots being committed) without ``__intentLog``: a commit that logged nothing holds
    no older commit's records (R271)."""
    return objects.remove_at(INTENT_LOG_ROOT) if objects.has(INTENT_LOG_ROOT) else objects


def _handle(value, where: str) -> MutableObject:
    if isinstance(value, Version) and value.entity is not None:
        return value.entity
    if isinstance(value, MutableObject):
        return value
    raise ProtoValidationError(f"{where}: an entity is given by its handle (MutableObject) or a version of it "
                               f"obtained from the session, not {type(value).__name__}")


def _type_of(ops, handle: MutableObject, verb: str) -> tuple:
    """``(type name of the entity handle, verb)``, the entity read on the record view."""
    _, type_name = ops.record(handle, verb)
    return type_name, verb


def _inside_an_intent() -> ProtoUserError:
    return ProtoUserError("A running intent changes entities through intent_context() (R278), not through the "
                          "semantic session")


# -- the replay ---------------------------------------------------------------------------------------------

class ReplaySession:
    """What the operations of a replayed entry see as their session: the replay's cells, the entry's schema,
    no lock, and the cell names the entry recorded (the replay draws none, R260).  A replayed intent that
    invokes an intent on a version runs it as part of itself."""

    __slots__ = ("_entry", "_cells", "_next")

    def __init__(self, entry: Entry, cells: Cells):
        self._entry = entry
        self._cells = cells
        self._next = 0

    def _store(self) -> Cells:
        return self._cells

    def _guard(self):
        return contextlib.nullcontext()

    def _operations(self, track: bool = False) -> Operations:
        return Operations(self, self._cells, self._entry.schema)

    def _new_cell(self, type_name: str) -> str:
        created, entry = self._entry.created, self._entry
        if self._next >= len(created):
            raise ProtoConflictError(
                f"The replay of {entry.intent[1]!r} on {entry.target!r} creates more entities than were recorded "
                f"({len(created)}): the state it was recorded on changed; retry the transaction")
        name = created[self._next]
        if name.rsplit("#", 1)[0] != type_name:
            raise ProtoConflictError(
                f"The replay of {entry.intent[1]!r} on {entry.target!r} creates an entity of {type_name!r} where "
                f"the recorded call created {name!r}: the state it was recorded on changed; retry the transaction")
        self._next += 1
        return name

    def check_created(self) -> None:
        entry = self._entry
        if self._next != len(entry.created):
            raise ProtoConflictError(
                f"The replay of {entry.intent[1]!r} on {entry.target!r} creates fewer entities ({self._next}) than "
                f"were recorded ({len(entry.created)}): the state it was recorded on changed; retry the transaction")

    def invoke(self, target, intent: str, /, **arguments):
        return self._operations().invoke(target, intent, arguments)

    def version(self) -> dict:
        raise ProtoUserError("A replayed intent reads through intent_context(); it has no session version")

    def _record_read(self, version) -> None:
        """A replay records no read (``Version.record``)."""
