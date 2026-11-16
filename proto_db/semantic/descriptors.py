"""The declarations of the semantic model in Python (spec sections 4, 6 and 10; the brief's blocks 1, 2,
4, 6, 7 and 8): descriptors, constraints and intents.

A descriptor is declared in a class body and named by ``__set_name__``::

    number = Field(int, required=True, label="Number", meaning="Sequential per company")
    total = Aggregate(over="lines", sum="amount", maintained=True, unit="currency:order")
    customer = Relation("acme.sales.Customer", cardinality="many-to-one", inverse="orders")

Each one is checked when it is created (``ProtoValidationError`` naming the offending argument) and
gives two things:

- :meth:`element`, its contribution to the catalog: the neutral item of an Attribute or a Role
  (``docs/source/format-spec.rst``, "Semantic catalog (format 6)") with the fields it states, none at
  its default;
- :meth:`resolve`, what resolution answers for an instance that does not hold the attribute (the
  registry calls it; :mod:`proto_db.resolution`).  A stored value is an own attribute and is found
  first, without calling it.

The semantic keyword arguments (``**semantic``) are ``meaning``, ``label`` (a ``str``, stored as
``{"und": text}``, or a dict from language tag to text, R267), ``help`` (likewise), ``group``,
``order``, ``synonyms``, ``examples``, ``caveats``, ``sensitivity`` (``public``, ``internal``,
``personal`` or ``secret``; ``internal`` when not stated), ``certification`` (a ``str``, for instance
:func:`Certified`), ``deprecated`` and ``ext`` (a map kept as written).  Any other key is
``ProtoValidationError``.

A text expression (``Computed("quantity * price")``, ``where="state != 'cancelled'"``,
``requires="state == 'open'"``) is parsed into the neutral tree (R277), which is what is stored; a
``Computed`` over a callable is a Python-only convenience, catalogued with ``languages: ["python"]``
and no expression (R276).
"""
from __future__ import annotations

import contextvars
import copy
import datetime
import hashlib
import inspect
import types
import uuid
from collections.abc import Mapping
from decimal import Decimal

import msgpack

from .. import resolution
from ..catalog.model import VALUE_KINDS
from ..core import codec
from ..core.atom import TYPE_NAME_PATTERN
from ..errors import ProtoValidationError
from ..values import MISSING, MutableObject, canonical, is_atom
from .expressions import Reader, check_ast, dependencies, evaluate, parse

__all__ = ["Field", "Enum", "Computed", "Aggregate", "Relation", "RelationTo", "Role", "Invariant", "intent",
           "Certified", "SENSITIVITIES"]

#: The values of ``sensitivity``; ``internal`` when not stated.
SENSITIVITIES = frozenset({"public", "internal", "personal", "secret"})
#: The values of ``on_delete`` (catalogued only: the MVP deletes no entity); ``restrict`` when not stated.
ON_DELETE = frozenset({"restrict", "cascade", "unlink"})
#: The values of ``binding``.
BINDINGS = frozenset({"identity", "version"})
#: The relation spellings (spec section 5): the end at the declaring entity, then the end at the target.
PAIR_CARDINALITIES = types.MappingProxyType({"many-to-one": ("one", "many"), "one-to-many": ("many", "one"),
                                             "one-to-one": ("one", "one"), "many-to-many": ("many", "many")})
#: The end spellings.
END_CARDINALITIES = frozenset({"one", "many", "0..1", "1..*"})

#: The Python types a ``Field`` takes, and the value kind each one is catalogued as (R268).
VALUE_TYPES = types.MappingProxyType({
    bool: "bool", int: "int", Decimal: "decimal", str: "str", bytes: "bytes", float: "float",
    datetime.date: "date", datetime.time: "time", datetime.datetime: "datetime", datetime.timedelta: "timedelta",
    uuid.UUID: "uuid", list: "list", tuple: "tuple", dict: "map"})

_SEMANTIC_KEYS = ("meaning", "label", "help", "group", "order", "synonyms", "examples", "caveats", "sensitivity",
                  "certification", "deprecated", "ext")


def _invalid(where: str, detail: str) -> ProtoValidationError:
    return ProtoValidationError(f"{where}: {detail}")


def _text(where: str, value) -> str:
    if type(value) is not str or not value:
        raise _invalid(where, f"a non-empty str, not {value!r}")
    return value


def _names(where: str, value) -> list:
    if isinstance(value, str) or not isinstance(value, (list, tuple)):
        raise _invalid(where, f"a list of names, not {value!r}")
    out = [_text(f"{where}[{i}]", name) for i, name in enumerate(value)]
    if len(set(out)) != len(out):
        raise _invalid(where, f"{out} names one item twice")
    return out


def language_map(where: str, value) -> dict:
    """A ``label`` or ``help``: a plain ``str`` declares ``{"und": text}`` (R267); a dict declares its
    languages explicitly."""
    if type(value) is str:
        return {"und": _text(where, value)}
    if isinstance(value, Mapping) and value:
        return {_text(f"{where} (a language tag)", tag): _text(f"{where}[{tag!r}]", text)
                for tag, text in value.items()}
    raise _invalid(where, f"a str or a dict from language tag to text, not {value!r}")


def _canonical(where: str, value):
    try:
        value = canonical(value)
    except ProtoValidationError as e:
        raise _invalid(where, str(e)) from e
    if _holds_atom(value):
        raise _invalid(where, "a catalogued value holds no atoms or identity handles")
    return value


def _holds_atom(value) -> bool:
    stack = [value]
    while stack:
        item = stack.pop()
        if is_atom(item) or isinstance(item, MutableObject):
            return True
        if isinstance(item, (tuple, list)):
            stack.extend(item)
        elif isinstance(item, dict):
            stack.extend(item.keys())
            stack.extend(item.values())
    return False


def semantic_fields(where: str, semantic: Mapping) -> dict:
    """The catalog fields of the ``**semantic`` keywords ``semantic``, checked, without those at their
    default; an unknown key is ``ProtoValidationError``."""
    unknown = sorted(key for key in semantic if key not in _SEMANTIC_KEYS)
    if unknown:
        raise _invalid(where, f"unknown semantic keywords {unknown}; they are {list(_SEMANTIC_KEYS)}")
    out = {}
    for key in _SEMANTIC_KEYS:
        if key not in semantic:
            continue
        value, here = semantic[key], f"{where}, {key}"
        if key in ("meaning", "group", "caveats", "certification"):
            out[key] = _text(here, value)
        elif key in ("label", "help"):
            out[key] = language_map(here, value)
        elif key == "order":
            if type(value) is not int:
                raise _invalid(here, f"an int, not {value!r}")
            out[key] = value
        elif key == "synonyms":
            if value:
                out[key] = _names(here, value)
        elif key == "examples":
            if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
                raise _invalid(here, f"a list of values, not {value!r}")
            if value:
                out[key] = [_canonical(f"{here}[{i}]", v) for i, v in enumerate(value)]
        elif key == "sensitivity":
            if value not in SENSITIVITIES:
                raise _invalid(here, f"{value!r} is not one of {sorted(SENSITIVITIES)}")
            if value != "internal":
                out[key] = value
        elif key == "deprecated":
            if type(value) is not bool:
                raise _invalid(here, f"a bool, not {value!r}")
            if value:
                out[key] = True
        else:                                   # ext
            if not isinstance(value, Mapping) or any(type(k) is not str for k in value):
                raise _invalid(here, f"a dict from str to a value, not {value!r}")
            if value:
                out[key] = {k: _canonical(f"{here}[{k!r}]", v) for k, v in value.items()}
    return out


def Certified(by: str, on: str) -> str:
    """The ``certification`` of a meaning certified by ``by`` on the date ``on``: the text
    ``"certified by <by> on <on>"`` (the catalog's ``certification`` is a ``str``)."""
    return f"certified by {_text('Certified, by', by)} on {_text('Certified, on', on)}"


def expression_tree(where: str, expression, *, allow_callable: bool = False):
    """The neutral tree of ``expression``: a text is parsed (R277), a tree is checked; a callable is
    returned as is when ``allow_callable``.  Anything else is ``ProtoValidationError``."""
    if isinstance(expression, str):
        try:
            return parse(expression)
        except ProtoValidationError as e:
            raise _invalid(where, str(e)) from e
    if isinstance(expression, list):
        try:
            check_ast(expression)
        except ProtoValidationError as e:
            raise _invalid(where, str(e)) from e
        return copy.deepcopy(expression)
    if allow_callable and callable(expression):
        return expression
    raise _invalid(where, f"an expression text or tree{', or a callable' if allow_callable else ''}, "
                          f"not {expression!r}")


# -- the evaluation reader of descriptors ------------------------------------------------------------

#: The (element oid or id, attribute) pairs a computed is being evaluated for in this context: a
#: computed that reads itself, directly or through others, is refused instead of recursing.
_EVALUATING: contextvars.ContextVar = contextvars.ContextVar("proto_db_semantic_evaluating", default=frozenset())


class RegistryReader(Reader):
    """What a descriptor's evaluation reads beyond the element (spec section 7, preflight F15): for an
    attribute the element does not hold, the first declaration along its chain that gives a value -- an
    object parent's attribute, an rc4 data default, a ``Field`` default, a computed attribute's value --
    else MISSING; and cells through the element's transaction or the running reader.  It reads the
    descriptors' raw values (:meth:`_Descriptor.value`), never the user-facing read, which answers
    ``None`` for MISSING."""

    def __init__(self, source=None):
        self._source = source

    def cell(self, name: str):
        source = self._source if self._source is not None else resolution.current_reader()
        if source is None or not hasattr(source, "get_mutable"):
            return super().cell(name)
        return source.get_mutable(name)

    def default(self, element, name: str):
        for kind, value in resolution.definitions(element, name):
            if kind in ("own", "data"):
                return value                # an object parent's attribute; an rc4 data default
            if kind != "descriptor":
                return MISSING              # a method is behaviour, not a value an expression reads
            found = value.value(element)
            if found is resolution.UNDECLARED:
                continue                    # a Field without a default: the lookup goes on
            return found
        return MISSING


def _reader_for(receiver) -> RegistryReader:
    context = getattr(receiver, "context", None)
    return RegistryReader(context if hasattr(context, "get_mutable") else None)


# -- descriptors -------------------------------------------------------------------------------------

class _Descriptor:
    """The common part of the semantic descriptors: a name given by ``__set_name__`` and the checked
    semantic fields."""

    __proto_descriptor__ = True

    def __init__(self, semantic: Mapping):
        self.name = None
        self._semantic = semantic_fields(self._where(), semantic)

    def _where(self) -> str:
        return f"{type(self).__name__}" + (f" {self.name!r}" if getattr(self, "name", None) else "")

    def __set_name__(self, owner, name):
        if self.name is not None and self.name != name:
            raise ProtoValidationError(f"{type(self).__name__} {self.name!r} is declared again as {name!r}; "
                                       f"declare one descriptor per attribute")
        self.name = name

    def element(self, name: str | None = None) -> dict:
        """The catalog item this descriptor contributes, with the fields it states (none at its
        default), named ``name`` (default: the name ``__set_name__`` gave)."""
        name = name or self.name
        if not name:
            raise ProtoValidationError(f"{type(self).__name__} has no name: declare it in a class body")
        return {"name": name, **self._fields(), **self._semantic}

    def _fields(self) -> dict:
        raise NotImplementedError

    def value(self, receiver):
        """The declared value of the attribute for ``receiver``, which does not hold it, as expressions
        read it: MISSING when there is none, :data:`resolution.UNDECLARED` when this declaration states
        nothing and the lookup goes on along the chain."""
        raise NotImplementedError

    def resolve(self, receiver):
        """What the user-facing read answers for ``receiver``, which does not hold the attribute:
        :meth:`value`, with ``None`` for MISSING."""
        found = self.value(receiver)
        return None if found is MISSING else found

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.name!r}>"


class Enum:
    """``Enum(*values)``: a ``str`` field whose values are the enumeration ``values`` (R268); the MVP
    catalogues them and does not enforce them."""

    __slots__ = ("values",)

    def __init__(self, *values: str):
        if not values:
            raise ProtoValidationError("Enum needs at least one value")
        self.values = tuple(_names("Enum", list(values)))

    def __repr__(self) -> str:
        return f"Enum{self.values!r}"


def value_kind(where: str, value_type) -> str:
    """The catalog's ``value_type`` of a Python type (R268)."""
    found = VALUE_TYPES.get(value_type) if isinstance(value_type, type) else None
    if found is None:
        raise _invalid(where, f"value_type is one of {[t.__name__ for t in VALUE_TYPES]} or Enum(...), not "
                              f"{value_type!r}")
    return found


class Field(_Descriptor):
    """A stored attribute: ``Field(value_type, *, required=False, default=MISSING, unit=None,
    indexed=False, **semantic)``.  An instance that does not hold it resolves to its ``default``, or,
    when this declaration states none, to the previous declaration's (``None`` when no declaration
    states one, R150)."""

    def __init__(self, value_type, *, required: bool = False, default=MISSING, unit: str | None = None,
                 indexed: bool = False, **semantic):
        super().__init__(semantic)
        where = self._where()
        if isinstance(value_type, Enum):
            self.value_type, self.values = "str", list(value_type.values)
        else:
            self.value_type, self.values = value_kind(where, value_type), None
        for flag, value in (("required", required), ("indexed", indexed)):
            if type(value) is not bool:
                raise _invalid(f"{where}, {flag}", f"a bool, not {value!r}")
        self.required, self.indexed = required, indexed
        self.unit = None if unit is None else _text(f"{where}, unit", unit)
        self.default = MISSING if default is MISSING else _canonical(f"{where}, default", default)

    def _fields(self) -> dict:
        out = {"kind": "field", "value_type": self.value_type}
        if self.values:
            out["values"] = list(self.values)
        if self.required:
            out["required"] = True
        if self.default is not MISSING:
            out["default"] = self.default
        if self.unit is not None:
            out["unit"] = self.unit
        if self.indexed:
            out["indexed"] = True
        return out

    def value(self, receiver):
        return resolution.UNDECLARED if self.default is MISSING else self.default


class Computed(_Descriptor):
    """A computed attribute: ``Computed(expression, *, unit=None, **semantic)``.  A text or a tree is
    neutral (stored as the tree, R277); a callable is Python only (R276: catalogued with
    ``languages: ["python"]`` and no expression).  An instance resolves it by evaluating it."""

    def __init__(self, expression, *, unit: str | None = None, **semantic):
        super().__init__(semantic)
        where = self._where()
        found = expression_tree(f"{where}, expression", expression, allow_callable=True)
        self.tree = found if isinstance(found, list) else None
        self.function = None if self.tree is not None else found
        self.unit = None if unit is None else _text(f"{where}, unit", unit)

    def _fields(self) -> dict:
        out = {"kind": "computed"}
        if self.tree is not None:
            out["expression"] = {"ast": self.tree}
        else:
            out["languages"] = ["python"]
        if self.unit is not None:
            out["unit"] = self.unit
        return out

    def value(self, receiver):
        if self.function is not None:
            return self.function(receiver)
        return _evaluate_guarded(self.tree, receiver, self.name)


def _evaluate_guarded(tree, receiver, name):
    key = (id(receiver), name)
    running = _EVALUATING.get()
    if key in running:
        raise ProtoValidationError(f"The computed attribute {name!r} reads itself, directly or through other "
                                   f"computed attributes")
    token = _EVALUATING.set(running | {key})
    try:
        value = evaluate(tree, receiver, reader=_reader_for(receiver))
    finally:
        _EVALUATING.reset(token)
    return value


class Aggregate(_Descriptor):
    """An aggregate over a role: ``Aggregate(*, over, sum=None, count=False, where=None,
    maintained=False, unit=None, **semantic)``, the tree ``["sum", over, ["attr", sum...], where]`` or
    ``["count", over, where]`` (spec section 7).  ``maintained=True`` catalogues it as a stored
    attribute (``computed_stored``, R272), updated by deltas; its terms must be neutral all the way
    down, which composition checks over the members' declarations."""

    def __init__(self, *, over: str, sum: str | None = None, count: bool = False, where=None,
                 maintained: bool = False, unit: str | None = None, **semantic):
        super().__init__(semantic)
        here = self._where()
        over = _text(f"{here}, over", over)
        if type(count) is not bool or type(maintained) is not bool:
            raise _invalid(here, "count and maintained are bool")
        if (sum is None) == (not count):
            raise _invalid(here, "state exactly one of sum=<attribute> and count=True")
        condition = None if where is None else expression_tree(f"{here}, where", where)
        if sum is not None:
            path = _text(f"{here}, sum", sum).split(".")
            if not all(path):
                raise _invalid(f"{here}, sum", f"{sum!r} is not an attribute path")
            tree = ["sum", over, ["attr", *path]]
        else:
            tree = ["count", over]
        if condition is not None:
            tree.append(condition)
        self.tree = expression_tree(here, tree)
        self.maintained = maintained
        self.unit = None if unit is None else _text(f"{here}, unit", unit)

    def _fields(self) -> dict:
        out = {"kind": "computed_stored" if self.maintained else "computed", "expression": {"ast": self.tree}}
        if self.unit is not None:
            out["unit"] = self.unit
        return out

    def value(self, receiver):
        return _evaluate_guarded(self.tree, receiver, self.name)


class _End(_Descriptor):
    """The common part of the relation descriptors: the role fields."""

    def _role(self, target, inverse, ordered, index_by, binding, on_delete) -> None:
        where = self._where()
        self.target = _type_name(f"{where}, target", target)
        self.inverse = None if inverse is None else _text(f"{where}, inverse", inverse)
        if type(ordered) is not bool:
            raise _invalid(f"{where}, ordered", f"a bool, not {ordered!r}")
        self.ordered = ordered
        if isinstance(index_by, str) or not isinstance(index_by, (list, tuple)):
            raise _invalid(f"{where}, index_by", f"a list of attribute paths, not {index_by!r}")
        self.index_by = _names(f"{where}, index_by", index_by)
        for path in self.index_by:
            if not all(path.split(".")):
                raise _invalid(f"{where}, index_by", f"{path!r} is not an attribute path")
        if binding not in BINDINGS:
            raise _invalid(f"{where}, binding", f"{binding!r} is not one of {sorted(BINDINGS)}")
        self.binding = binding
        if on_delete not in ON_DELETE:
            raise _invalid(f"{where}, on_delete", f"{on_delete!r} is not one of {sorted(ON_DELETE)}")
        self.on_delete = on_delete

    def _fields(self) -> dict:
        out = {"target": self.target}
        if self.inverse is not None:
            out["inverse"] = self.inverse
        out["cardinality"] = self.cardinality
        if self.inverse_cardinality is not None:
            out["inverse_cardinality"] = self.inverse_cardinality
        if self.participant:
            out["participant"] = True
        if self.ordered:
            out["ordered"] = True
        if self.index_by:
            out["index_by"] = list(self.index_by)
        if self.binding != "identity":
            out["binding"] = self.binding
        if self.on_delete != "restrict":                # the default is omitted ("Omission")
            out["on_delete"] = self.on_delete
        return out

    def value(self, receiver):
        # Nothing is linked: a stored role is an own attribute, found before this descriptor.  The
        # session navigates stored roles (Task 6).
        return MISSING


def _type_name(where: str, target) -> str:
    if isinstance(target, type):
        name = target.__dict__.get("__type_name__")
        if getattr(target, "_implicit_parent", None) is None or not isinstance(name, str):
            raise _invalid(where, f"the class {target.__qualname__} is not a semantic class with a declared "
                                  f"__type_name__")
        return name
    if not isinstance(target, str) or not TYPE_NAME_PATTERN.match(target):
        raise _invalid(where, f"a type name such as 'acme.sales.Order' or a semantic class, not {target!r}")
    return target


class Relation(_End):
    """A simple link without data: ``Relation(target, *, cardinality, inverse=None, ordered=False,
    index_by=(), binding="identity", on_delete="restrict", **semantic)`` (``on_delete`` is ``restrict`` when
    not stated).  ``cardinality`` is a relation spelling (``many-to-one``, ``one-to-many``,
    ``one-to-one``, ``many-to-many``), stored as its two ends, or an end spelling (``one``, ``many``,
    ``0..1``, ``1..*``) for a relation without an inverse: the inverse end's cardinality would be
    unknown."""

    participant = False

    def __init__(self, target: str, *, cardinality: str, inverse: str | None = None, ordered: bool = False,
                 index_by=(), binding: str = "identity", on_delete: str = "restrict", **semantic):
        super().__init__(semantic)
        self._role(target, inverse, ordered, index_by, binding, on_delete)
        where = f"{self._where()}, cardinality"
        if cardinality in PAIR_CARDINALITIES:
            self.cardinality, self.inverse_cardinality = PAIR_CARDINALITIES[cardinality]
        elif cardinality in END_CARDINALITIES:
            if self.inverse is not None:
                raise _invalid(where, f"{cardinality!r} gives one end only; a relation with an inverse is declared "
                                      f"with a relation spelling {sorted(PAIR_CARDINALITIES)}")
            self.cardinality, self.inverse_cardinality = cardinality, None
        else:
            raise _invalid(where, f"{cardinality!r} is not one of {sorted(PAIR_CARDINALITIES)} or "
                                  f"{sorted(END_CARDINALITIES)}")


class Role(_End):
    """A role of a relationship (Chen): ``Role(target, *, inverse=None, ordered=False, index_by=(),
    binding="identity", on_delete="restrict", **semantic)``.  It is ``one`` at its end; its inverse, when
    named, is a ``many`` end on the target holding the relationship's atoms."""

    participant = True
    cardinality = "one"

    def __init__(self, target: str, *, inverse: str | None = None, ordered: bool = False, index_by=(),
                 binding: str = "identity", on_delete: str = "restrict", **semantic):
        super().__init__(semantic)
        self._role(target, inverse, ordered, index_by, binding, on_delete)
        self.inverse_cardinality = "many" if self.inverse is not None else None


class RelationTo(_End):
    """The inverse end of the role ``role`` of the relationship ``target`` (a semantic class or its type
    name), declared on the other entity: ``RelationTo(target, *, role, ordered=False, **semantic)``.  It
    is a ``many`` end; composition checks that it agrees with the role."""

    participant = False
    cardinality = "many"
    inverse_cardinality = "one"

    def __init__(self, target, *, role: str, ordered: bool = False, **semantic):
        super().__init__(semantic)
        self._role(target, _text(f"{self._where()}, role", role), ordered, (), "identity", "restrict")


# -- constraints ---------------------------------------------------------------------------------------

def _refuse_atom(atom):
    raise ProtoValidationError(f"an expression holds no atoms, not a {type(atom).__name__}")


def invariant_name(tree) -> str:
    """The name of an unnamed invariant (R279): ``invariant_`` and the first 8 hex digits of the SHA-256
    of the canonical encoding of its tree (the msgpack bytes of the format's canonical value)."""
    encoded = msgpack.packb(codec.encode_value(canonical(tree), _refuse_atom), use_bin_type=True)
    return "invariant_" + hashlib.sha256(encoded).hexdigest()[:8]


class Invariant:
    """An entity-level constraint: ``Invariant(expression, *, meaning, enforced=True, message="",
    name=None)``, declared in ``__constraints__``.  Without ``name`` it is named by
    :func:`invariant_name` (R279).  Its scope is inferred where the declaration is composed:
    ``aggregate`` when the expression reads a maintained aggregate, else ``entity``."""

    __slots__ = ("tree", "meaning", "enforced", "message", "name")

    def __init__(self, expression, *, meaning: str, enforced: bool = True, message: str = "",
                 name: str | None = None):
        where = "Invariant" + (f" {name!r}" if name else "")
        self.tree = expression_tree(f"{where}, expression", expression)
        self.meaning = _text(f"{where}, meaning", meaning)
        if type(enforced) is not bool:
            raise _invalid(f"{where}, enforced", f"a bool, not {enforced!r}")
        self.enforced = enforced
        if type(message) is not str:
            raise _invalid(f"{where}, message", f"a str, not {message!r}")
        self.message = message
        self.name = invariant_name(self.tree) if name is None else _text(f"{where}, name", name)

    def element(self, scope: str) -> dict:
        """The Constraint item, with the ``scope`` the composition inferred."""
        out = {"name": self.name, "scope": scope, "expression": {"ast": self.tree}}
        if not self.enforced:
            out["enforced"] = False
        out["meaning"] = self.meaning
        if self.message:
            out["message"] = self.message
        return out

    def reads(self) -> frozenset:
        """The attribute and role names the expression reads from its own element."""
        return dependencies(self.tree)

    def __repr__(self) -> str:
        return f"<Invariant {self.name!r}>"


# -- intents ---------------------------------------------------------------------------------------------

class IntentSpec:
    """What :func:`intent` declares about a function: its ``meaning``, ``requires`` (a tree or
    ``None``), ``commutes_with`` and ``parameters`` (``(name, value_type)`` pairs, ``value_type`` a
    value kind, an entity type name or ``None`` when not declared)."""

    __slots__ = ("meaning", "requires", "commutes_with", "parameters", "signature", "name")

    def __init__(self, meaning, requires, commutes_with, parameters, signature):
        #: The definition name, recorded when a class body declares the intent.
        self.name = None
        self.meaning = meaning
        self.requires = requires
        self.commutes_with = commutes_with
        self.parameters = parameters
        self.signature = signature

    def element(self, name: str) -> dict:
        """The Intent item named ``name``."""
        out = {"name": name, "meaning": self.meaning}
        if self.parameters:
            out["parameters"] = [[p, t] for p, t in self.parameters]
        if self.requires is not None:
            out["requires"] = {"ast": self.requires}
        if self.commutes_with:
            out["commutes_with"] = list(self.commutes_with)
        return out


def _signature(fn) -> inspect.Signature:
    """``fn``'s signature with its annotations evaluated (a module with ``from __future__ import
    annotations`` states them as text); an annotation that names what is not defined yet stays text."""
    try:
        return inspect.signature(fn, eval_str=True)
    except NameError:
        pass
    try:
        import annotationlib                                # Python 3.14: deferred annotations
    except ImportError:
        return inspect.signature(fn)
    return inspect.signature(fn, annotation_format=annotationlib.Format.STRING)


def _parameter_type(where: str, value):
    """A parameter's ``value_type`` from an annotation or ``parameters=``: a Python type of a ``Field``,
    a semantic class (its type name), or a ``str`` naming a value kind or an entity type."""
    if value is inspect.Parameter.empty or value is None:
        return None
    if isinstance(value, type):
        if value in VALUE_TYPES:
            return VALUE_TYPES[value]
        return _type_name(where, value)
    if isinstance(value, str) and (value in VALUE_KINDS or TYPE_NAME_PATTERN.match(value)):
        return value
    raise _invalid(where, f"{value!r} is neither a value kind {sorted(VALUE_KINDS)}, a type of Field, a semantic "
                          f"class nor an entity type name (declare it with parameters=)")


def intent(*, meaning: str, requires=None, commutes_with=(), parameters: Mapping[str, str] | None = None):
    """Declare the decorated method an intent (spec sections 4 and 9): it returns the same function with
    ``__proto_intent__`` set.  Its parameters are those of its signature after the receiver; their types
    come from ``parameters`` or the annotations (undeclared: ``None``, an explicit unknown).  The
    effects go through :func:`proto_db.semantic.intent_context` (R278); returning a new version of the
    receiver (``self.with_(...)``) updates the target."""
    meaning = _text("intent, meaning", meaning)
    tree = None if requires is None else expression_tree("intent, requires", requires)
    commutes = _names("intent, commutes_with", commutes_with)
    if parameters is not None and not isinstance(parameters, Mapping):
        raise _invalid("intent, parameters", f"a mapping from parameter name to value type, not {parameters!r}")

    def decorate(fn):
        where = f"intent {getattr(fn, '__qualname__', fn)!r}"
        if type(fn) is not types.FunctionType:
            raise _invalid(where, f"intent decorates a plain function, not {type(fn).__name__}")
        if fn.__name__ == "<lambda>":
            raise _invalid(where, "a lambda is refused; define a named function")
        signature = _signature(fn)
        listed = list(signature.parameters.values())
        if not listed or listed[0].kind not in (inspect.Parameter.POSITIONAL_ONLY,
                                                inspect.Parameter.POSITIONAL_OR_KEYWORD):
            raise _invalid(where, "an intent takes its receiver (self) first")
        declared = dict(parameters or {})
        pairs = []
        for parameter in listed[1:]:
            if parameter.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD,
                                  inspect.Parameter.POSITIONAL_ONLY):
                raise _invalid(where, f"the parameter {parameter.name!r} is refused: an intent's arguments are "
                                      f"named and logged, so each is a named parameter")
            here = f"{where}, parameter {parameter.name!r}"
            given = declared.pop(parameter.name, inspect.Parameter.empty)
            found = _parameter_type(here, given if given is not inspect.Parameter.empty else parameter.annotation)
            pairs.append((parameter.name, found))
        if declared:
            raise _invalid(where, f"parameters= names {sorted(declared)}, which the signature does not have")
        fn.__proto_intent__ = IntentSpec(meaning, tree, tuple(commutes), tuple(pairs), signature)
        return fn

    return decorate
