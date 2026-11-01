"""DBObject: an immutable persistent record (spec section 5, decision D18).

Reading an attribute that was never defined returns ``None``;
:meth:`DBObject.has_defined_attr` tells an explicit ``None`` apart from an
absent attribute.  A DBObject cannot be modified: :meth:`DBObject.with_`
returns a new version, which is a distinct atom with a new ``oid``
(assigned lazily, D21).

Immutability is shallow: ``list`` and ``dict`` values are copied on
construction (by :func:`proto_db.values.canonical`) but are returned as is and
must not be mutated by callers.
"""
from __future__ import annotations

from ..errors import ProtoUserError, ProtoValidationError
from ..values import canonical
from .atom import RESERVED_NAMES, Atom, PersistenceContext, _check_context

__all__ = ["DBObject"]


def _check_name(cls, name) -> None:
    if (not isinstance(name, str) or not name or name.startswith("_") or name in RESERVED_NAMES
            or hasattr(cls, name)):
        raise ProtoValidationError(f"Invalid attribute name {name!r} for {cls.__name__}")


class DBObject(Atom):
    """Immutable record of named canonical values.

    ``DBObject(context=None, **attributes)``.  Attribute names must not start
    with an underscore, be one of ``oid``, ``atom_pointer``, ``context``, nor
    name an attribute of the class (``with_``, ``has_defined_attr``, or a
    subclass method or property).
    Values are validated and normalised with :func:`proto_db.values.canonical`.
    Equality and hashing follow the ``oid`` (D10): two versions with the same
    content are different atoms.
    """

    __type_name__ = "proto_db.core.dbobject.DBObject"

    def __init__(self, *, context: PersistenceContext | None = None, **attributes):
        super().__init__(context=context)
        self._install(attributes)

    @classmethod
    def _from_state(cls, state: dict, context: PersistenceContext | None) -> "DBObject":
        """Protected constructor: a new unsaved atom of ``cls`` with ``state``, bypassing ``__init__``."""
        _check_context(context)
        obj = object.__new__(cls)
        osa = object.__setattr__
        osa(obj, "_loaded", True)
        osa(obj, "atom_pointer", None)
        osa(obj, "context", context)
        obj._install(state)
        return obj

    def _install(self, attributes: dict) -> None:
        values = {}
        for name, value in attributes.items():
            _check_name(type(self), name)
            values[name] = canonical(value)
        osa = object.__setattr__
        for name, value in values.items():
            osa(self, name, value)

    def _absent_attribute(self, name: str):
        return None

    def has_defined_attr(self, name: str) -> bool:
        """Return True if ``name`` is a persisted attribute of this record (even if ``None``)."""
        if not isinstance(name, str) or name.startswith("_") or name in RESERVED_NAMES:
            return False
        self._load()
        return name in self.__dict__

    def with_(self, **changes) -> "DBObject":
        """Return a new version with ``changes`` applied: a new atom with a new ``oid`` (D18).

        Passing ``None`` defines the attribute as ``None``.  The new version has
        the same class and context and is not saved.
        """
        state = self._state()
        state.update(changes)
        return type(self)._from_state(state, self.context)

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
        if not self.__dict__.get("_loaded", True):
            return super().__repr__()
        fields = ", ".join(f"{k}={v!r}" for k, v in self._state().items())
        return f"{type(self).__name__}({fields})"
