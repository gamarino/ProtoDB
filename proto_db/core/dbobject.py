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

Resolution (spec section 5, :mod:`proto_db.resolution`): a name Python does not find -- not a
member of the class, so a subclass's methods and properties come first, and not an own
attribute -- is looked up along the C3 linearization of the parents, registry names included
(their extension chains, newest first, through the current registry); a method found there is
bound to the object, and :meth:`DBObject._super` continues from the running method; a miss is
still ``None`` (R150).  ``o[name]`` reads what ``o.name`` reads, for a name computed at run time; ``name in o``
tells a definition (even ``None``) anywhere along the linearization from a miss;
:attr:`DBObject._own` holds the own attributes only, and :meth:`DBObject.has_defined_attr` and
index paths keep reading own attributes only.  Writes never reach a parent: ``with_`` sets own
attributes.

Semantic classes (spec section 4, :mod:`proto_db.semantic.classes`): a subclass whose body holds a
semantic element -- a semantic descriptor (``Field``, ``Computed``, ``Relation``, ...), an intent,
one of :data:`SEMANTIC_DUNDERS` or ``__validity__`` -- or one of whose bases is a semantic class is
handed, once created, to :func:`proto_db.semantic.classes.semantic_class`, which moves its semantic
elements into the registry.  Its instances resolve its ``__type_name__`` as an implicit last parent
(``_implicit_parent``), never persisted.  Every other subclass is untouched.

``list`` and ``dict`` values are stored as ``FrozenList`` and ``FrozenDict``
(by :func:`proto_db.values.canonical`, D25): attempts to mutate them raise
``ProtoValidationError``; use ``.copy()`` to get a mutable copy.

A :class:`MutableObject` is not an atom: it is a plain value naming a cell of the
database's mutable table, resolved through a transaction.  Stored inside an atom it
gives versions of a record a shared entity identity (D18).  The class is defined in
:mod:`proto_db.values` (it is part of the value model) and re-exported here.
"""
from __future__ import annotations

import sys
import types

from .. import resolution
from ..errors import ProtoAttributeNameError, ProtoUserError, ProtoValidationError
from ..values import FrozenDict, MutableObject, canonical, check_text
from .atom import _ABSENT, RESERVED_NAMES, Atom, PersistenceContext, _check_context, _forget_atom_class, class_defines

__all__ = ["DBObject", "MutableObject"]

#: The semantic dunders of a class body (spec section 4): entity-level semantic declarations.  The
#: registry keeps them apart from definitions (``proto_db.registry.Extension.semantic``).
SEMANTIC_DUNDERS = frozenset({"__meaning__", "__label__", "__help__", "__synonyms__", "__key__",
                              "__certification__", "__constraints__"})
#: A semantic dunder the MVP refuses (D72): its presence still makes the class semantic, so the
#: semantic layer names the refusal.
_DEFERRED_DUNDERS = frozenset({"__validity__"})

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
    seen = set()
    for parent in parents:
        if type(parent) is str:
            if not parent:
                raise ProtoValidationError("A parent registry name must be a non-empty str")
            check_text(parent)
            key = ("name", parent)
        elif isinstance(parent, Atom) and type(parent)._parented:
            key = ("atom", parent.oid)      # D10: one object per oid
        else:
            raise ProtoValidationError(
                f"A parent must be a DBObject or a registry name (a non-empty str), not "
                f"{type(parent).__name__}")
        if key in seen:
            raise ProtoValidationError(f"The parent {parent!r} appears more than once")
        seen.add(key)
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
    #: The registry name an instance resolves as its implicit last parent, never persisted: the
    #: ``__type_name__`` of a semantic class (set by :mod:`proto_db.semantic.classes`), ``None`` for
    #: every other class.
    _implicit_parent = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not _is_semantic(cls):
            return
        from ..semantic.classes import semantic_class
        try:
            semantic_class(cls)
        except BaseException:
            _forget_atom_class(cls)     # the class does not exist: its type name stays free
            raise
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
        """The parents of this object, in order: DBObjects and registry names (``()`` for none).

        An application subclass may define its own ``parents`` member, which then wins (Python
        members come first); the internal paths never read this property (review I1)."""
        return self._persisted_parents()

    def _persisted_parents(self) -> tuple:
        """The persisted parents (the private slot ``_parents``), whatever the class defines as
        ``parents``; ``()`` for none."""
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

    # -- resolution (spec section 5) ------------------------------------------------------

    #: Not iterable: ``__getitem__`` reads attributes by name, it is not a sequence protocol.
    __iter__ = None

    def _absent_attribute(self, name: str):
        # Python found no member and no own attribute: resolution over the parents (a miss is
        # None, R150; an object without parents answers None at once, as before parents existed).
        if self._probe("_parents") is _ABSENT and type(self)._implicit_parent is None:
            return None
        return resolution._resolve_inherited(self, name)[0]

    def __getitem__(self, name):
        """``o[name]``: what ``o.name`` reads, for a name computed at run time (``None`` on a miss).

        A legacy field named like the API (ruling R157: ``parents``, ``with_parents`` in a
        format-4 record) is read here as the field, own or inherited, while ``o.parents`` stays
        the property.  The other API names (``oid``, ``context``, ``atom_pointer``, ``with_``,
        ``has_defined_attr``, ...) and names starting with ``_`` are not attribute names:
        :class:`~proto_db.errors.ProtoAttributeNameError`, a ``ProtoValidationError`` that is also a
        ``KeyError`` (ruling R238: a mapping-first consumer, such as a Django template, then falls
        back to attribute access).  A name that is not a ``str`` is ``TypeError``."""
        kind = self._item_kind(name)
        if kind is _DATA:
            return resolution.resolve(self, name)[0]
        if kind is _MEMBER or class_defines(type(self), name):
            resolution.check_members(self)      # a Python member must not hide the registry (R240)
        return getattr(self, name)

    def __contains__(self, name) -> bool:
        """``name in o``: whether ``o.name`` (or ``o[name]``) finds a definition -- an own or
        inherited attribute, even one whose value is ``None``, or a member of the Python class --
        rather than missing.  False for anything that is not an attribute name."""
        try:
            kind = self._item_kind(name)
        except (TypeError, ProtoValidationError):
            return False
        if kind is _MEMBER or (kind is _ATTRIBUTE and class_defines(type(self), name)):
            resolution.check_members(self)      # a Python member must not hide the registry (R240)
            return True
        return resolution.resolve(self, name)[1] is not None

    def _item_kind(self, name):
        """How ``o[name]`` reads ``name``: ``_DATA`` (a legacy field name, R157), ``_MEMBER`` (an
        API name a subclass redefines: its member) or ``_ATTRIBUTE`` (``getattr``)."""
        if not isinstance(name, str):
            raise TypeError(f"DBObject attribute names are str, not {type(name).__name__}")
        if not name or name.startswith("_"):
            raise ProtoAttributeNameError(f"{name!r} is not an attribute name")
        if name in _API_NAMES:
            cls = type(self)
            if name in cls._legacy_held:
                return _DATA
            owner = next(k for k in cls.__mro__ if name in k.__dict__ or k is DBObject)
            if owner is DBObject or name in RESERVED_NAMES:
                raise ProtoAttributeNameError(
                    f"{name!r} is part of the DBObject API, not an attribute name: read it as o.{name}")
            return _MEMBER
        return _ATTRIBUTE

    def __setitem__(self, name, value):
        raise ProtoUserError(f"DBObject is immutable; use with_({name}=...) to create a new version")

    def __delitem__(self, name):
        raise ProtoUserError("DBObject is immutable; attributes cannot be deleted")

    @property
    def _own(self) -> FrozenDict:
        """This object's own attributes (never inherited ones), as a ``FrozenDict``; a legacy
        field (R157) appears under its field name."""
        return FrozenDict(self._state())

    def _resolve(self, name) -> tuple:
        """``(value, position)`` of ``name`` along :meth:`_linearization` (``(None, None)`` on a
        miss); see :func:`proto_db.resolution.resolve`."""
        return resolution.resolve(self, name)

    def _super(self) -> "resolution.SuperProxy":
        """The next definitions after the running registry method (spec section 6): a transient
        proxy that resolves a name from the position after the running extension in this object's
        linearization and binds a method it finds to this object (``self is o``).

        Called directly in the body of a registry method.  The running extension is found from
        the caller's code object (``sys._getframe(1).f_code``, read here, never through a
        wrapper); a call from anything else -- outside a method, from a closure, a helper, a
        generator expression, a generator method or a method bound under another registry -- or
        on an object whose linearization does not contain the running extension's registry name
        raises ``ProtoUserError``.  A coroutine method may call it (R176).  A miss through the
        proxy is ``None`` (R170): test the end of a chain with ``if self._super().f:``."""
        return resolution.super_proxy(self, sys._getframe(1).f_code)

    def _linearization(self) -> tuple:
        """This object, then the C3 order of its parents' nodes; see
        :func:`proto_db.resolution.linearize`."""
        return resolution.linearize(self)

    def has_defined_attr(self, name: str) -> bool:
        """Return True if ``name`` is an own persisted attribute of this record (even if ``None``).

        Own attributes only, as before parents existed: an inherited attribute is not defined
        here (``name in o`` answers along the whole linearization)."""
        return self._own_probe(name) is not _ABSENT

    def _own_probe(self, name):
        """The own persisted attribute ``name`` of this record, or ``_ABSENT``.  Also reads a legacy
        field held under a private name (R157); this is what index paths and resolution read."""
        if not isinstance(name, str) or name.startswith("_") or name in RESERVED_NAMES:
            return _ABSENT
        held = type(self)._legacy_held.get(name)
        return self._probe(name if held is None else held)

    def with_(self, **changes) -> "DBObject":
        """Return a new version with ``changes`` applied: a new atom with a new ``oid`` (D18).

        Passing ``None`` defines the attribute as ``None``.  The new version has
        the same class and context and is not saved.
        """
        state = self._state()
        legacy = self._take_legacy(state)
        state.update(changes)
        new = type(self)._from_state(state, self.context)._carry(legacy)
        parents = self._persisted_parents()
        if parents:
            # Already validated (by the constructor, with_parents or the record's decoding): kept as
            # they are, so a parent stub is not loaded to read its oid again (re-review 1).
            _osa(new, "_parents", parents)
        return new

    def with_parents(self, *parents) -> "DBObject":
        """Return a new version with the same attributes and ``parents`` instead of this one's
        (none when called without arguments): a new atom with a new ``oid``, not saved."""
        state = self._state()
        legacy = self._take_legacy(state)
        return type(self)._from_state(state, self.context, parents)._carry(legacy)

    def _take_legacy(self, state: dict) -> dict:
        """Remove from ``state`` (this record's ``_state()``) the legacy fields (R157) and return
        them, so that a change can never write one and a new version carries them unchanged."""
        legacy = type(self)._legacy_held
        return {name: state.pop(name) for name in legacy if name in state}

    def _carry(self, legacy: dict) -> "DBObject":
        """Install the legacy fields ``legacy`` (already canonical: read from a record) on this
        new version, under their private names; return it."""
        held = type(self)._legacy_held
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
        held = type(self)._legacy_held
        # A legacy field named like the API (R157) is labelled apart from the parents (review m1).
        fields = [f"<field {k}>={v!r}" if k in held else f"{k}={v!r}" for k, v in self._state().items()]
        parents = self._persisted_parents()
        if parents:
            fields.insert(0, f"parents={parents!r}")
        return f"{type(self).__name__}({', '.join(fields)})"


def _is_semantic(cls) -> bool:
    """Whether the new subclass ``cls`` is a semantic class (spec section 4, R280, preflight F2): one
    of its bases is one, or its own body holds a semantic element.  Only the class's own namespace
    and its bases are read."""
    if any(getattr(base, "_implicit_parent", None) is not None for base in cls.__bases__):
        return True
    for name, value in cls.__dict__.items():
        if name in SEMANTIC_DUNDERS or name in _DEFERRED_DUNDERS:
            return True
        if getattr(type(value), "__proto_descriptor__", False) or (
                type(value) is types.FunctionType and value.__dict__.get("__proto_intent__") is not None):
            return True
    return False


#: What :meth:`DBObject._item_kind` answers.
_DATA, _MEMBER, _ATTRIBUTE = object(), object(), object()

#: The names of the DBObject API (its members and Atom's, and the reserved names): never read as
#: attribute names through ``o[name]``, except the legacy fields of R157.
_API_NAMES = frozenset(
    name for cls in (DBObject, Atom) for name in vars(cls) if not name.startswith("_")) | RESERVED_NAMES
