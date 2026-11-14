"""DBObject, an immutable persistent record (spec section 5, decision D18), and
MutableObject, a handle on a named mutable cell (spec section 7, decisions D11, D35).

Reading an attribute that was never defined returns ``None``;
:meth:`DBObject.has_defined_attr` tells an explicit ``None`` apart from an
absent attribute.  A DBObject cannot be modified: :meth:`DBObject.with_`
returns a new version, which is a distinct atom with a new ``oid``
(assigned lazily, D21).

A DBObject may have **parents** (runtime registry, spec section 4): an ordered tuple whose items
are other DBObjects or registry names (non-empty ``str``), given as ``DBObject(parents=(...))``
and read as :attr:`DBObject.parents`.  :meth:`DBObject.with_` keeps them and
:meth:`DBObject.with_parents` returns a version with others.  They are persisted in the record's
envelope key ``"p"`` (the format-5 feature ``parents``); a DBObject without parents writes the
format-4 record, byte for byte.  ``parents`` and ``with_parents`` are therefore not attribute
names.

``list`` and ``dict`` values are stored as ``FrozenList`` and ``FrozenDict``
(by :func:`proto_db.values.canonical`, D25): attempts to mutate them raise
``ProtoValidationError``; use ``.copy()`` to get a mutable copy.

A :class:`MutableObject` is not an atom: it is a plain value naming a cell of the
database's mutable table, resolved through a transaction.  Stored inside an atom it
gives versions of a record a shared entity identity (D18).  The class is defined in
:mod:`proto_db.values` (it is part of the value model) and re-exported here.
"""
from __future__ import annotations

from ..errors import ProtoUserError, ProtoValidationError
from ..values import MutableObject, canonical, check_text
from .atom import _ABSENT, RESERVED_NAMES, Atom, PersistenceContext, _check_context, class_defines

__all__ = ["DBObject", "MutableObject"]

_osa = object.__setattr__


def _check_name(cls, name) -> None:
    if (not isinstance(name, str) or not name or name.startswith("_") or name in RESERVED_NAMES
            or class_defines(cls, name)):
        if isinstance(name, str) and class_defines(cls, name):
            raise ProtoValidationError(
                f"Invalid attribute name {name!r} for {cls.__name__}: {_SHADOW_RULE}")
        raise ProtoValidationError(f"Invalid attribute name {name!r} for {cls.__name__}")
    check_text(name)


#: Why a persisted field may not share its name with anything of its Python class (Task 10.P A4).
_SHADOW_RULE = ("the class defines an attribute of that name (a method, a property or a class "
                "attribute). In Python the stored field is its own accessor, read as a plain attribute, "
                "and a class attribute of the same name would hide it, so a property written as an "
                "accessor is refused too")


def _checked_parents(parents) -> tuple:
    """``parents`` as a tuple, each item validated: a DBObject (or an opaque atom read from
    storage, another implementation's type) or a non-empty ``str`` that :func:`check_text`
    accepts.  Anything else is ``ProtoValidationError``."""
    if not isinstance(parents, (tuple, list)):
        raise ProtoValidationError(
            f"parents must be a tuple of DBObjects and registry names (non-empty str), "
            f"not {type(parents).__name__}")
    for parent in parents:
        if type(parent) is str:
            if not parent:
                raise ProtoValidationError("A parent registry name must be a non-empty str")
            check_text(parent)
        elif not (isinstance(parent, Atom) and type(parent)._parented):
            raise ProtoValidationError(
                f"A parent must be a DBObject or a registry name (a non-empty str), not "
                f"{type(parent).__name__}")
    return tuple(parents)


class DBObject(Atom):
    """Immutable record of named canonical values, with optional parents.

    ``DBObject(context=None, parents=(), **attributes)``.  Attribute names must not start
    with an underscore, be one of ``oid``, ``atom_pointer``, ``context``, nor
    name an attribute of the class (``parents``, ``with_``, ``with_parents``,
    ``has_defined_attr``, or a subclass method or property).
    ``parents`` is a tuple (or list) of DBObjects and registry names (non-empty ``str``).
    Values are validated and normalised with :func:`proto_db.values.canonical`.
    Equality and hashing follow the ``oid`` (D10): two versions with the same
    content are different atoms.
    """

    __type_name__ = "proto_db.core.dbobject.DBObject"
    _parented = True
    #: Fields that format-4 records may hold under the names of the parents API (ruling R157):
    #: they load and are carried over by ``with_``, but no new value is ever written under them.
    _legacy_fields = {"parents": "_legacy_parents", "with_parents": "_legacy_with_parents"}

    def __init__(self, *, context: PersistenceContext | None = None, parents=(), **attributes):
        super().__init__(context=context)
        self._install(attributes)
        self._install_parents(parents)

    @classmethod
    def _from_state(cls, state: dict, context: PersistenceContext | None, parents=()) -> "DBObject":
        """Protected constructor: a new unsaved atom of ``cls`` with ``state`` and ``parents``,
        bypassing ``__init__``."""
        _check_context(context)
        obj = object.__new__(cls)
        osa = object.__setattr__
        osa(obj, "_loaded", True)
        osa(obj, "atom_pointer", None)
        osa(obj, "context", context)
        obj._install(state)
        obj._install_parents(parents)
        return obj

    def _install_parents(self, parents) -> None:
        """Validate ``parents`` and keep them as ``_parents``; none are kept for an empty tuple,
        so a DBObject without parents has exactly the attributes it had before parents existed."""
        if type(parents) is tuple and not parents:
            return
        checked = _checked_parents(parents)
        if checked:
            _osa(self, "_parents", checked)

    @property
    def parents(self) -> tuple:
        """The parents of this object, in order: DBObjects and registry names (``()`` for none)."""
        found = self._probe("_parents")
        return () if found is _ABSENT else found

    def _install(self, attributes: dict) -> None:
        values = {}
        for name, value in attributes.items():
            _check_name(type(self), name)
            try:
                values[name] = canonical(value)
            except ProtoValidationError as e:
                if type(e) is not ProtoValidationError:
                    raise
                raise ProtoValidationError(f"Field {name!r} of {type(self).__type_name__}: {e}") from e
        osa = object.__setattr__
        for name, value in values.items():
            osa(self, name, value)

    def _absent_attribute(self, name: str):
        return None

    def has_defined_attr(self, name: str) -> bool:
        """Return True if ``name`` is a persisted attribute of this record (even if ``None``)."""
        return self._own_probe(name) is not _ABSENT

    def _own(self, name: str):
        """The persisted attribute ``name`` of this record, or ``None`` if it has none (R150).

        Internal accessor (ruling R157): unlike ``getattr``, it also reads a legacy field that a
        format-4 record holds under a name the API now uses (``parents``, ``with_parents``).
        Task 4 replaces it with the public own-attribute access."""
        found = self._own_probe(name)
        return None if found is _ABSENT else found

    def _own_probe(self, name):
        if not isinstance(name, str) or name.startswith("_") or name in RESERVED_NAMES:
            return _ABSENT
        held = type(self)._legacy_fields.get(name)
        return self._probe(name if held is None else held)

    def with_(self, **changes) -> "DBObject":
        """Return a new version with ``changes`` applied: a new atom with a new ``oid`` (D18).

        Passing ``None`` defines the attribute as ``None``.  The new version has
        the same class and context and is not saved.
        """
        state = self._state()
        legacy = self._take_legacy(state)
        state.update(changes)
        return type(self)._from_state(state, self.context, self.parents)._carry(legacy)

    def with_parents(self, *parents) -> "DBObject":
        """Return a new version with the same attributes and ``parents`` instead of this one's
        (none when called without arguments): a new atom with a new ``oid``, not saved."""
        state = self._state()
        legacy = self._take_legacy(state)
        return type(self)._from_state(state, self.context, parents)._carry(legacy)

    def _take_legacy(self, state: dict) -> dict:
        """Remove from ``state`` (this record's ``_state()``) the legacy fields (R157) and return
        them, so that a change can never write one and a new version carries them unchanged."""
        legacy = type(self)._legacy_fields
        return {name: state.pop(name) for name in legacy if name in state}

    def _carry(self, legacy: dict) -> "DBObject":
        """Install the legacy fields ``legacy`` (already canonical: read from a record) on this
        new version, under their private names; return it."""
        held = type(self)._legacy_fields
        for name, value in legacy.items():
            _osa(self, held[name], value)
        return self

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        raise ProtoUserError(f"DBObject is immutable; use with_({name}=...) to create a new version")

    def __delattr__(self, name):
        if name.startswith("_"):
            object.__delattr__(self, name)
            return
        raise ProtoUserError("DBObject is immutable; attributes cannot be deleted")

    def __repr__(self):
        if not self._loaded_now():
            return super().__repr__()
        fields = [f"{k}={v!r}" for k, v in self._state().items()]
        parents = self.parents
        if parents:
            fields.insert(0, f"parents={parents!r}")
        return f"{type(self).__name__}({', '.join(fields)})"

