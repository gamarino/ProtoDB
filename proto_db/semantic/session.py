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
"""
from __future__ import annotations

import base64
import copy

from ..errors import ProtoUserError
from . import compose as _compose
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

    __slots__ = ("_source", "_model")

    def __init__(self, source):
        """Bind the session to ``source`` (use :func:`semantic`)."""
        self._source = source
        self._model = None          # (stamp, verified model): see _stamp

    # -- the source -------------------------------------------------------------------------------------

    def _transaction(self):
        """The transaction a write operation writes in; a session over a snapshot is read-only."""
        from ..transactions import Snapshot
        if isinstance(self._source, Snapshot):
            raise ProtoUserError(f"A semantic session over {self._source!r} is read-only; write in a transaction "
                                 f"(Database.new_transaction)")
        return self._source

    def _read(self) -> tuple:
        """``(stored model or the model to commit, registry, installed names, memo key)`` of the source's
        version, read without entering a read set."""
        from ..transactions import Snapshot
        source = self._source
        if isinstance(source, Snapshot):
            listed = source._reader_names()
            stored, key = _store.read_stored(source._root.objects)
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
