"""The semantic catalog in the database: what an install writes, and the verification of what a reader
reads (spec sections 5 and 8; ``docs/source/format-spec.rst``, "Semantic catalog (format 6)").

Writing
    ``install_module`` adds, add-if-absent at commit, the Module items the stored catalog lacks and the
    entities recomposed for the merged module set (:func:`merged_semantic_catalog`), under the root lock
    and from the registry of the module list committed then, never through the read set: concurrent
    installs never conflict over the catalog, and the stored composition is the composition of the union
    (never the last writer's).  The catalog's ``module_set`` is the full ``__currentModules`` list:
    ``base`` and the modules without contributions are Module items with no ``extensions`` (preflight
    F24).  A database none of whose modules contributes anything has no catalog at all, so its data
    stays format 4 or 5, byte for byte.  A stored Module item is kept as it is stored: an install never
    replaces another writer's contributions.

Reading (:func:`verified_model`)
    A stored catalog is checked before anything uses it: the model (:func:`~proto_db.catalog.model.from_atoms`),
    every expression tree (:func:`~proto_db.semantic.expressions.check_ast`), and the composition of its
    stored contributions, which must compose (maintained-aggregate neutrality included) and equal the
    stored entities, the cache.  A failure is ``ProtoCorruptionError``; an operator this revision does
    not implement ``ProtoNotSupportedError``.  Then the catalog must cover the installed list
    (``ProtoNotSupportedError`` until the next install, F24), and each stored Module item must equal the
    contributions of the code this process loaded: a difference is ``ProtoValidationError`` naming the
    module, the type, the element and the field (R263), never corrected silently.

    :func:`checked_stored_model` is the part of that verification that needs no code (the model, its
    expression trees and the composition of its contributions): what a reader without the modules' code (the
    agent interface's raw mode) can still trust, for instance to withhold what the catalog marks personal.
"""
from __future__ import annotations

import copy
import threading
from collections import OrderedDict

from ..catalog.atoms import semantic_catalog_root
from ..catalog.model import canonical_form, from_atoms, to_atoms
from ..errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoValidationError
from ..registry import Registry
from .compose import _semantic_extension, compose_modules, module_contributions
from .constraints import check_enforceable
from .expressions import check_ast

__all__ = ["has_contributions", "read_stored", "read_catalog",
           "merged_model", "merged_semantic_catalog", "verified_model", "checked_stored_model",
           "NO_CATALOG"]

#: The message of a database whose modules are installed and that has no semantic catalog (spec 5).
NO_CATALOG = "no semantic catalog; install a module to add it"


def has_contributions(registry: Registry) -> bool:
    """Whether some module of ``registry`` makes a semantic declaration."""
    return any(_semantic_extension(extension) for extension in registry.extensions)


def read_stored(objects) -> tuple:
    """``(model, record pointer)`` of the ``__semanticCatalog`` of the roots dictionary ``objects`` (a
    version's roots), ``(None, None)`` when the root is absent.  Read from the version itself, never
    through a read set.  A value of another type, or malformed records, are ``ProtoCorruptionError``."""
    return read_catalog(semantic_catalog_root(objects))


def read_catalog(catalog) -> tuple:
    """``(model, record pointer)`` of the checked ``__semanticCatalog`` value ``catalog`` (``None``: absent,
    ``(None, None)``)."""
    return (None, None) if catalog is None else (from_atoms(catalog), catalog.atom_pointer)


def merged_model(stored: dict | None, registry: Registry) -> dict | None:
    """The catalog model to store for the module list of ``registry`` over the ``stored`` model (or
    ``None``): the stored Module items, kept as stored, plus the contributions of the modules they lack,
    with the entities composed from the merged items; ``None`` when nothing changes, and when there is no
    stored catalog and no module contributes anything.  A composition that fails raises as
    :func:`~proto_db.semantic.compose.compose_modules` does (``ProtoValidationError``).  This is the writer's
    composition, so an enforced invariant this revision cannot enforce is refused
    (:func:`~proto_db.semantic.constraints.check_enforceable`, ``ProtoNotSupportedError``); verifying a stored
    catalog for use never refuses it (the invariant is published, not enforced)."""
    names = registry.key
    if stored is None:
        if not has_contributions(registry):
            return None
        items = {}
    else:
        items = dict(stored["modules"])
        if all(name in items for name in names):
            return None
    for name in names:
        if name not in items:
            items[name] = module_contributions(registry, name)
    model = compose_modules(items)
    check_enforceable(model["entities"])
    if stored is not None and "ext" in stored:
        model["ext"] = copy.deepcopy(stored["ext"])        # kept as written, never interpreted
    return model


def merged_semantic_catalog(current, registry: Registry, context):
    """The :class:`~proto_db.catalog.atoms.SemanticCatalog` to write for the module list of ``registry``
    (the list committed) over ``current``, the catalog current at commit (or ``None``): see
    :func:`merged_model`.  ``None`` when nothing changes.  Called under the root lock: it imports nothing
    (``registry`` is built), and reads ``current`` from the root, not through a read set."""
    model = merged_model(None if current is None else from_atoms(current), registry)
    return None if model is None else to_atoms(model, context)


# -- verification on use ------------------------------------------------------------------------------------

def _trees(value):
    """Every expression tree (the ``ast`` of an Expression item) in the model ``value``."""
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            for key, child in item.items():
                if key == "ast":
                    yield child
                elif key not in ("default", "examples", "ext"):     # canonical values, never expressions
                    stack.append(child)
        elif isinstance(item, list):
            stack.extend(item)


def _checked_stored(model: dict) -> None:
    """The checks of a stored catalog that do not depend on the code (see the module docstring)."""
    for tree in _trees(model):
        try:
            check_ast(tree)
        except ProtoValidationError as e:
            raise ProtoCorruptionError(f"The stored semantic catalog holds an invalid expression: {e}") from e
    try:
        recomposed = compose_modules(model["modules"])
    except ProtoValidationError as e:
        raise ProtoCorruptionError(f"The stored semantic catalog's contributions do not compose: {e}") from e
    if canonical_form({**model, "entities": recomposed["entities"]}) != canonical_form(model):
        different = sorted(name for name in set(model["entities"]) | set(recomposed["entities"])
                           if model["entities"].get(name) != recomposed["entities"].get(name))
        raise ProtoCorruptionError(f"The stored semantic catalog's entities are not the composition of its "
                                   f"modules (they differ for {different})")


def _plain(value):
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def _field_difference(stored: dict, code: dict):
    for field in sorted(set(stored) | set(code)):
        if field != "elements" and stored.get(field) != code.get(field):
            return field, stored.get(field), code.get(field)
    return None


def _code_difference(module: str, stored: dict, code: dict) -> str:
    """Where the stored Module item ``stored`` of ``module`` first differs from the code's ``code``."""
    found = _field_difference({k: v for k, v in stored.items() if k != "extensions"},
                              {k: v for k, v in code.items() if k != "extensions"})
    if found is not None:
        return f"module {module!r}, field {found[0]!r}: stored {found[1]!r}, code {found[2]!r}"
    stored_ext, code_ext = stored.get("extensions", []), code.get("extensions", [])
    for i in range(max(len(stored_ext), len(code_ext))):
        if i >= len(stored_ext) or i >= len(code_ext):
            extra = (code_ext if i >= len(stored_ext) else stored_ext)[i]
            side = "the code declares" if i >= len(stored_ext) else "the stored catalog holds"
            return f"module {module!r}, type {extra['extends']!r}: {side} an extension the other lacks"
        s, c = stored_ext[i], code_ext[i]
        where = f"module {module!r}, type {c['extends']!r}"
        found = _field_difference(s, c)
        if found is not None:
            return f"{where}, field {found[0]!r}: stored {found[1]!r}, code {found[2]!r}"
        s_elements = {e["name"]: e for e in s.get("elements", [])}
        c_elements = {e["name"]: e for e in c.get("elements", [])}
        for name in list(s_elements) + [n for n in c_elements if n not in s_elements]:
            if name not in c_elements or name not in s_elements:
                side = "only the stored catalog" if name not in c_elements else "only the code"
                return f"{where}, element {name!r}: declared by {side}"
            found = _field_difference(s_elements[name], c_elements[name])
            if found is not None:
                return f"{where}, element {name!r}, field {found[0]!r}: stored {found[1]!r}, code {found[2]!r}"
        if [e["name"] for e in s.get("elements", [])] != [e["name"] for e in c.get("elements", [])]:
            return f"{where}: the elements are declared in another order"
    return f"module {module!r}"


#: Verified models of stored catalogs, by (catalog record pointer, registry): a stored catalog and the
#: code a registry holds never change, so a verification holds for both.  Bounded; the oldest goes first.
_VERIFIED: OrderedDict = OrderedDict()
_VERIFIED_LOCK = threading.Lock()
_VERIFIED_SIZE = 64


#: Models of stored catalogs that passed the checks that need no code (:func:`checked_stored_model`), by
#: catalog record pointer.  Bounded; the oldest goes first.
_CHECKED: OrderedDict = OrderedDict()


def checked_stored_model(catalog) -> dict | None:
    """The model of the stored ``__semanticCatalog`` value ``catalog`` (``None``: absent, so ``None``), after the
    checks that do not depend on the code (see the module docstring): a malformed or inconsistent catalog is
    ``ProtoCorruptionError``, an operator this revision does not implement ``ProtoNotSupportedError``.  The
    model is shared: the caller must not change it."""
    if catalog is None:
        return None
    key = catalog.atom_pointer
    if key is not None:
        with _VERIFIED_LOCK:
            found = _CHECKED.get(key)
        if found is not None:
            return found
    model = from_atoms(catalog)
    _checked_stored(model)
    if key is not None:
        with _VERIFIED_LOCK:
            _CHECKED[key] = model
            while len(_CHECKED) > _VERIFIED_SIZE:
                _CHECKED.popitem(last=False)
    return model


def verified_model(stored: dict | None, registry: Registry | None, listed: tuple, *, key=None) -> dict:
    """``stored`` (the model a version stores, or the one a transaction would commit) verified against the
    installed list ``listed`` and the code of ``registry`` (its registry): see the module docstring.
    ``key`` (a stored record's identity) memoizes a successful verification per registry.  Returns the
    model, which the caller must not change."""
    if stored is None:
        if not listed:
            raise ProtoNotSupportedError("The database lists no modules, so it has no semantic catalog")
        raise ProtoNotSupportedError(f"The database lists the modules {list(listed)} and has {NO_CATALOG}")
    if key is not None:
        with _VERIFIED_LOCK:
            found = _VERIFIED.get((key, registry))
        if found is not None:
            return found
    _checked_stored(stored)
    lacking = sorted(set(listed) - set(stored["module_set"]))
    if lacking:
        raise ProtoNotSupportedError(
            f"The semantic catalog does not hold the contributions of the installed module(s) {lacking} (installed "
            f"by a writer without the semantic layer); install a module to add them")
    extra = sorted(set(stored["module_set"]) - set(listed))
    if extra:
        raise ProtoNotSupportedError(
            f"The semantic catalog holds the contributions of module(s) the database does not list {extra}; its "
            f"semantic model is not supported until those modules are installed")
    for name in stored["module_set"]:
        stored_item = _plain(stored["modules"][name])
        code_item = _plain(module_contributions(registry, name))
        if stored_item != code_item:
            raise ProtoValidationError(
                f"The code of the installed modules differs from the stored semantic catalog: "
                f"{_code_difference(name, stored_item, code_item)}.  The code is never applied over a stored "
                f"catalog silently; module evolution is designed, not built (R263)")
    if key is not None:
        with _VERIFIED_LOCK:
            _VERIFIED[(key, registry)] = stored
            while len(_VERIFIED) > _VERIFIED_SIZE:
                _VERIFIED.popitem(last=False)
    return stored
