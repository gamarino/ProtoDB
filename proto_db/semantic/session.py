"""The semantic session: the catalog of one database version, read through a transaction or a snapshot
(spec sections 5, 8 and 11; Task 5).

``semantic(source)`` opens a :class:`SemanticSession` over a :class:`~proto_db.transactions.Transaction`
or a :class:`~proto_db.transactions.Snapshot`.  The session reads the ``__semanticCatalog`` of that
source's version directly, never through the read set (as ``installed_modules`` reads the module list),
so reading the catalog never makes a commit conflict.  A transaction's session sees the catalog the
transaction would commit: its own pending installs merged in, as its registry has them.  An as-of
``Snapshot`` (``Database.snapshot_at``) gives that version's catalog, verified against that version's
registry: describing is a photograph of one version (R259).

The catalog is verified on first use (:func:`proto_db.semantic.store.verified_model`): a stored catalog
that is malformed or inconsistent is ``ProtoCorruptionError``; no catalog, or one that does not hold every
installed module, ``ProtoNotSupportedError`` ("no semantic catalog; install a module to add it"); code
that differs from it ``ProtoValidationError`` naming the module and the element (R263).

A session over a snapshot is read-only: every write operation goes through :meth:`SemanticSession._transaction`,
which raises ``ProtoUserError`` for it (preflight F24).

A session over a transaction started with ``intents=True`` is in **intent mode** (spec section 9;
:mod:`proto_db.semantic.intents`): its operations are logged and applied to the transaction's record view, the
reads its caller decides on are recorded as fine reads, and the commit replays the log.  Otherwise it is in
direct mode: operations write the transaction's cells at once, under today's rules.  One API serves both.
While an intent runs, its effects go through ``intent_context()`` (R278): the session's write operations raise
``ProtoUserError`` then, and an intent invoked on a version runs as part of the running one.
"""
from __future__ import annotations

import base64
import contextlib
import copy
import uuid

from ..catalog.atoms import SEMANTIC_CATALOG_ROOT, semantic_catalog_value
from ..errors import ProtoUserError
from . import compose as _compose
from .classes import _RUNNING
from . import entities as _entities
from . import store as _store

__all__ = ["SemanticSession", "semantic"]


def semantic(source) -> "SemanticSession":
    """The semantic session of ``source``, a ``Transaction`` or a ``Snapshot``; anything else is
    ``ProtoUserError``."""
    from ..transactions import Snapshot, Transaction
    if not isinstance(source, (Transaction, Snapshot)):
        raise ProtoUserError(f"A semantic session reads a read snapshot (a Transaction or a Snapshot), not a "
                             f"{type(source).__name__}")
    return SemanticSession(source)


class SemanticSession:
    """The semantic catalog of one database version (see the module docstring).  Open it with
    :func:`semantic`.  Its answers are neutral data (dicts, lists, strings and canonical values)."""

    __slots__ = ("_source", "_model", "_schema")

    def __init__(self, source):
        """Bind the session to ``source`` (use :func:`semantic`)."""
        self._source = source
        self._model = None          # (stamp, verified model): see _stamp
        self._schema = None         # (verified model, its Schema)

    # -- the source -------------------------------------------------------------------------------------

    def _log(self):
        """The intent log of the session's transaction (intent mode), else ``None``."""
        return getattr(self._source, "_intent_log", None)

    def _transaction(self):
        """The transaction a write operation writes in; a session over a snapshot is read-only."""
        from ..transactions import Snapshot
        if isinstance(self._source, Snapshot):
            raise ProtoUserError(f"A semantic session over {self._source!r} is read-only; write in a transaction "
                                 f"(Database.new_transaction)")
        return self._source

    def _store(self):
        """What a write operation writes in: the transaction (direct mode) or its record view (intent mode);
        a session over a snapshot is read-only (:meth:`_transaction`)."""
        transaction = self._transaction()
        log = self._log()
        return transaction if log is None else log.cells

    def _guard(self):
        """The lock an operation or a read holds (see "Atomicity" in :mod:`proto_db.semantic.entities`): the
        transaction's, none for a snapshot."""
        from ..transactions import Snapshot
        if isinstance(self._source, Snapshot):
            return contextlib.nullcontext()
        return self._source.semantic_operation()

    def _new_cell(self, type_name: str) -> str:
        """The name of the cell a creation makes, ``f"{type_name}#{uuid4().hex}"`` (R260), drawn now; in intent
        mode the log records it for the replay."""
        log = self._log()
        if log is not None:
            return log.new_cell(type_name)
        return f"{type_name}#{uuid.uuid4().hex}"

    def _record_read(self, version) -> None:
        """``version.record`` was taken: in intent mode, outside a running intent and while the transaction is
        active, a fine read of the whole cell the version was read from (none for a bound version or an
        atom)."""
        log = self._log()
        if log is None or _RUNNING.get() is not None or version._entity is None or version._bound:
            return
        source = self._source
        if not source.is_active:
            return                          # a read after the end decides nothing this transaction commits
        try:
            with source.semantic_operation():
                log.read(version._entity.name)
        except ProtoUserError:
            if source.is_active:
                raise

    def _writing(self):
        """The intent log a write operation is logged in (``None``: direct mode).  While an intent runs in
        this context its effects go through ``intent_context()``: ``ProtoUserError``."""
        if _RUNNING.get() is not None:
            raise ProtoUserError("A running intent changes entities through intent_context() (R278), not through "
                                 "the semantic session")
        self._store()
        return self._log()

    def _read(self) -> tuple:
        """``(stored model or the model to commit, registry, installed names, memo key)`` of the source's
        version, read without entering a read set."""
        from ..transactions import Snapshot
        source = self._source
        if isinstance(source, Snapshot):
            listed = source._reader_names()
            stored, key = _store.read_catalog(semantic_catalog_value(source.get_root_object(SEMANTIC_CATALOG_ROOT)))
            return stored, source.registry if listed else None, listed, key
        with source._lock:
            source._check_active()
            objects, listed, pending = source._objects, source._installed_names(), source._registry_pending()
        registry = source.registry if listed else None
        stored, key = _store.read_stored(objects)
        if pending and registry is not None:
            merged = _store.merged_model(stored, registry)
            if merged is not None:
                return merged, registry, listed, None           # this transaction's own, not memoized
        return stored, registry, listed, key

    def _stamp(self):
        """What the catalog a session sees depends on besides its version: for a transaction, its own
        installs (a session kept across ``install_module`` sees the new catalog); nothing for a snapshot."""
        from ..transactions import Snapshot
        source = self._source
        if isinstance(source, Snapshot):
            return None
        with source._lock:
            source._check_active()
            return source._installed_names(), source._semantic_pending

    def _verified(self) -> dict:
        stamp = self._stamp()
        memo = self._model
        if memo is None or memo[0] != stamp:
            stored, registry, listed, key = self._read()
            memo = self._model = (stamp, _store.verified_model(stored, registry, listed, key=key))
        return memo[1]

    def _operations(self, track: bool = True) -> "_entities.Operations":
        """The entity operations over the verified catalog of the session's version.  In intent mode they
        read the record view, and a read the caller decides on (``track``, outside a running intent) records
        its fine reads."""
        model = self._verified()
        memo = self._schema
        if memo is None or memo[0] is not model:
            memo = self._schema = (model, _entities.Schema(model))
        log = self._log()
        if log is None:
            return _entities.Operations(self, self._source, memo[1])
        tracker = log if track and _RUNNING.get() is None else None
        return _entities.Operations(self, log.cells, memo[1], tracker=tracker)

    # -- reading ----------------------------------------------------------------------------------------

    def catalog(self) -> dict:
        """The catalog model of the version (``module_set``, ``modules``, ``entities``; the neutral model of
        :mod:`proto_db.catalog.model`), verified on first use (see the module docstring).  A copy: changing
        it changes nothing stored."""
        return copy.deepcopy(self._verified())

    def describe(self, type_name: str) -> dict:
        """The composed entity ``type_name`` of the version: its fields, attributes, roles, intents,
        constraints, provenance and certification, plus ``kind`` (``entity`` or ``relationship``).  A type
        the catalog does not describe is ``ProtoValidationError``."""
        return _compose.describe(self._verified(), type_name)

    def list_entities(self, filter: str | None = None) -> list:
        """The types the catalog describes, by type name: ``{"type_name", "label", "meaning"}`` each
        (``None`` for what the type does not declare).  With ``filter``, only the types whose type name,
        label, synonyms or meaning contain it, case-folded."""
        if filter is not None and not isinstance(filter, str):
            raise ProtoUserError(f"The filter of list_entities is a str or None, not {type(filter).__name__}")
        needle = None if filter is None else filter.casefold()
        out = []
        for type_name, entity in sorted(self._verified()["entities"].items()):
            if needle is not None:
                texts = [type_name, entity.get("meaning", ""), *entity.get("label", {}).values(),
                         *entity.get("synonyms", ())]
                if not any(needle in text.casefold() for text in texts):
                    continue
            label = entity.get("label")
            out.append({"type_name": type_name, "label": None if label is None else dict(label),
                        "meaning": entity.get("meaning")})
        return out

    def version(self) -> dict:
        """The version the session reads: ``{"number": int, "ref": str}``, the ``ref`` being the base64 of
        the version's reference, which ``Database.snapshot_at`` takes once decoded (preflight F23)."""
        from ..transactions import Snapshot
        source = self._source
        if isinstance(source, Snapshot):
            number, ref = source.version, source.ref
        else:
            number, ref = source._db_root.version, source.snapshot_ref
        return {"number": number, "ref": base64.b64encode(ref).decode("ascii")}

    # -- entities and relations (Task 6; :mod:`proto_db.semantic.entities`) ----------------------------------

    def create(self, type_name: str, **fields):
        """Create an entity of ``type_name`` and return its handle (a ``MutableObject``): a cell named
        ``f"{type_name}#{uuid4().hex}"`` (R260) holding a record of the semantic class registered for the
        type (``ProtoNotSupportedError`` when this process has none).  ``fields`` are its fields and its
        ``one`` roles (handles or versions from the session), which are linked.  A missing required field,
        an unknown name, a computed attribute or a ``many`` end is ``ProtoValidationError``; so is a role
        target that is not of the declared type or one of its subtypes (R280)."""
        log = self._writing()
        if log is not None:
            return log.create(self, type_name, fields)
        return self._operations().create(type_name, fields)

    def get(self, handle) -> "_entities.Version":
        """The current version of the entity ``handle`` (a :class:`~proto_db.semantic.entities.Version`, whose
        ``record`` is the stored ``DBObject``)."""
        return self._operations().get(handle)

    def update(self, handle, **changes) -> None:
        """Change fields of the entity ``handle``.  Its maintained aggregates' owners move by deltas and the
        maintained keys over the changed fields are re-keyed, in this transaction.  A role is changed with
        :meth:`link` and :meth:`unlink`, never here (``ProtoValidationError``)."""
        log = self._writing()
        if log is not None:
            log.update(self, handle, changes)
        else:
            self._operations().update(handle, changes)

    def link(self, source, role: str, target) -> None:
        """Link ``target`` to the role ``role`` of ``source``: both ends and every maintained aggregate along
        the inverses are updated.  A ``one`` end already linked is refused (unlink it first)."""
        log = self._writing()
        if log is not None:
            log.link(self, source, role, target, add=True)
        else:
            self._operations().link(source, role, target)

    def unlink(self, source, role: str, target) -> None:
        """Undo :meth:`link`: both ends and the aggregates along the inverses are updated."""
        log = self._writing()
        if log is not None:
            log.link(self, source, role, target, add=False)
        else:
            self._operations().link(source, role, target, add=False)

    def relate(self, type_name: str, **roles_and_fields) -> "_entities.Version":
        """Create an atom of the relationship ``type_name`` with every role and its fields, and add it to the
        collection at each role's inverse end (one atom, referenced by every end).  A version-bound role
        stores ``{"entity": handle, "version": version}``."""
        log = self._writing()
        if log is not None:
            return log.relate(self, type_name, roles_and_fields)
        return self._operations().relate(type_name, roles_and_fields)

    def invoke(self, target, intent: str, /, **arguments):
        """Invoke the intent ``intent`` on the entity ``target`` (a handle, or a version from the session) with
        ``arguments`` by parameter name: what calling it on a version does (``order.add_line(product, 2,
        price)``; spec sections 4 and 9).  Its ``requires`` is checked on the target, its implementation runs
        through D62 resolution with ``intent_context()`` bound (R278), and a result that is a new version of
        the receiver updates the target (the target's current version is returned).  In direct mode it is
        applied at once; in intent mode it is logged, applied to the record view and replayed at commit, where
        its ``requires`` is decided.  Called while an intent runs, it runs as part of that intent."""
        if _RUNNING.get() is not None:
            return self._operations(track=False).invoke(target, intent, arguments)
        log = self._writing()
        if log is not None:
            return log.invoke(self, target, intent, arguments)
        return self._operations().invoke(target, intent, arguments)

    def navigate(self, source, role: str):
        """The role ``role`` of ``source`` (a handle or a version): a version for a ``one`` end (the bound
        version for ``binding="version"``), ``None`` when nothing is linked, or the
        :class:`~proto_db.semantic.entities.Members` of a ``many`` end (members, never entries)."""
        return self._operations().navigate(source, role)

    def query(self, tree, *, members: bool = False, selection=None) -> dict:
        """Run the query ``tree`` (spec section 11) on the session's version: ``{"rows", "explain", "version",
        "query", "terms"}``.  The session's single query engine (preflight F18): see
        :mod:`proto_db.semantic.entities`, "The query engine".  With ``members=True``, a query without an
        aggregate also answers ``"members"``, the :class:`~proto_db.semantic.entities.Version` each row reads.
        With ``selection`` (a callable from a member's type name to the paths to select), each row evaluates
        only the paths it gives for that member's type.  An evaluation failure propagates tagged with the part
        of the query it arose in (see :func:`~proto_db.semantic.entities.scrubbed`)."""
        return self._operations().query(tree, with_members=members, selection=selection)
