"""Semantic classes, ``Extension`` and ``Relationship``: the brief's declarations mapped onto the rc4
registry (spec section 4, rulings R259, R280).

Semantic classes
    A ``DBObject`` subclass with a declared ``__type_name__`` whose body holds a semantic element (a
    descriptor of :mod:`proto_db.semantic.descriptors`, an intent, ``__constraints__`` or another
    semantic dunder, :data:`~proto_db.core.dbobject.SEMANTIC_DUNDERS`), or one of whose bases is a
    semantic class (R280), is a **semantic class**.  ``DBObject.__init_subclass__`` hands it, once
    created, to :func:`semantic_class`:

    - its semantic elements leave the Python class and become an implicit D62 extension of the
      registry name ``__type_name__``, declared by the registry module its Python module declared
      with :func:`proto_db.registry.module` (``__registry_module__ = "name"`` chooses when that Python
      module declares several; none is an error).  Its other members stay, so D62's rule R240 (a
      registry definition may not shadow a Python member) never forbids a later module from
      redefining a field or an intent;
    - the extension's ``parents`` are the type names of its semantic bases, in Python base order, so a
      subtype's chain comes before its supertypes' in the C3 linearization;
    - its instances resolve ``__type_name__`` as an implicit last parent (``_implicit_parent``, never
      persisted; :mod:`proto_db.resolution`);
    - it gains the class method ``describe(snapshot)``.

    Refused at class creation: a semantic element without a declared ``__type_name__`` (no silent
    legacy name, preflight F2), no registry module or an ambiguous one, ``__validity__``
    (``ProtoNotSupportedError``, D72), a ``Role`` outside a ``Relationship``, and malformed
    declarations.  A refused class leaves no trace: its type name stays free.

``Extension``
    ``class OrderDelivery(Extension): __extends__ = "acme.sales.Order"`` declares the D62 extension
    ``module.extends("acme.sales.Order")`` with the class body as definitions (descriptors, intents,
    plain methods and data defaults, the semantic dunders), optionally with ``__parents__`` (registry
    names) and ``__registry_module__``.  The decorator spelling stays valid and equivalent.  This is
    ``proto_db.semantic.Extension``; ``proto_db.registry.Extension`` is the built record and
    ``proto_db.catalog.Extension`` the catalog atom (preflight F40).

Intents (spec sections 4 and 9, R278)
    An intent method found by resolution is bound to an :class:`IntentInvoker`.  Every call is routed
    through :func:`_invoke`, which runs the implementation with :func:`intent_context` bound; a call on
    a version that no ``SemanticSession`` gave raises ``ProtoUserError`` (sessions arrive with Task 8,
    which supplies the context).  Inside a running intent, an intent called on any version (also
    through ``_super()``) runs directly as part of it.  An intent that returns a new version of its
    receiver updates the target through ``context.update``.
"""
from __future__ import annotations

import contextvars
import types
from collections.abc import Mapping

from .. import registry as _registry
from .. import resolution
from ..core.dbobject import _DEFERRED_DUNDERS, SEMANTIC_DUNDERS, DBObject
from ..errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from ..values import values_equal
from .descriptors import Invariant, Role, _names, _text, language_map

__all__ = ["Extension", "Relationship", "IntentContext", "IntentInvoker", "intent_context", "semantic_class",
           "bind_intent"]

#: Class-body names that configure a declaration and are not elements.
_CONFIG = frozenset({"__type_name__", "__type_aliases__", "__registry_module__"})
_EXTENSION_CONFIG = frozenset({"__extends__", "__parents__", "__registry_module__"})


class Relationship(DBObject):
    """The base class of a relationship with data (Chen; spec section 6): ``class OrderLine(Relationship)``.
    Its instances are atoms with their own fields; its :class:`~proto_db.semantic.descriptors.Role`
    descriptors are its roles.  It has no type name of its own."""


# -- checking a contribution ------------------------------------------------------------------------

def _check_semantic(where: str, semantic: Mapping) -> dict:
    """The semantic dunders ``semantic``, checked and normalized: text, language maps (R267), lists of
    names, and a list of :class:`~proto_db.semantic.descriptors.Invariant`."""
    out = {}
    for name, value in semantic.items():
        here = f"{where}: {name}"
        if name in ("__meaning__", "__certification__"):
            out[name] = _text(here, value)
        elif name in ("__label__", "__help__"):
            out[name] = language_map(here, value)
        elif name in ("__synonyms__", "__key__"):
            out[name] = tuple(_names(here, value))
        elif name == "__constraints__":
            if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
                raise ProtoValidationError(f"{here}: a list of Invariant, not {value!r}")
            for item in value:
                if not isinstance(item, Invariant):
                    raise ProtoValidationError(f"{here}: a list of Invariant, not one holding {item!r}")
            names = [item.name for item in value]
            if len(set(names)) != len(names):
                raise ProtoValidationError(f"{here}: two constraints are named alike ({names})")
            out[name] = tuple(value)
        else:                               # the semantic layer's own entries (kind, supertypes)
            out[name] = value
    return out


def _check_elements(where: str, definitions: Mapping) -> bool:
    """Check that each descriptor and intent of ``definitions`` is named by its definition name (an
    intent's name is recorded on its declaration); return whether a participant role is among them."""
    participant = False
    for name, (kind, value) in definitions.items():
        if kind == _registry.DESCRIPTOR:
            if value.name != name:
                raise ProtoValidationError(f"{where}: the descriptor {value!r} is declared as {name!r}; one "
                                           f"descriptor object declares one attribute")
            participant = participant or isinstance(value, Role)
        elif kind == _registry.METHOD and _registry._is_intent(value):
            spec = value.__proto_intent__
            named = getattr(spec, "name", None)
            if named is not None and named != name:
                raise ProtoValidationError(f"{where}: the intent function {value.__qualname__!r} is declared as "
                                           f"{named!r} and as {name!r}")
            spec.name = name
    return participant


def _check_decorated(where: str, registry_name: str, definitions: Mapping, semantic: Mapping) -> dict:
    """The decorator spelling's semantic declarations, checked (``ModuleHandle._definitions``, preflight
    F34): the normalized semantic dunders, with ``kind`` ``relationship`` when a participant role is
    declared."""
    out = _check_semantic(where, semantic)
    if _check_elements(where, definitions):
        out["kind"] = "relationship"
    return out


# -- semantic classes --------------------------------------------------------------------------------

def _registry_module(cls, where: str) -> "_registry.ModuleHandle":
    """The registry module declared by ``cls``'s Python module, chosen by ``__registry_module__`` when
    the class states one."""
    source = cls.__module__
    with _registry._LOCK:
        handles = [h for h in _registry._DECLARATIONS.values() if h.source == source and not h._superseded]
    wanted = cls.__dict__.get("__registry_module__")
    if wanted is not None:
        found = next((h for h in handles if h.name == wanted), None)
        if found is None:
            raise ProtoValidationError(
                f"{where}: __registry_module__ is {wanted!r}, but the Python module {source!r} declares no registry "
                f"module of that name (declared: {sorted(h.name for h in handles)})")
        return found
    if not handles:
        raise ProtoValidationError(
            f"{where}: the Python module {source!r} declares no registry module; call "
            f"proto_db.registry.module(...) before the class")
    if len(handles) > 1:
        raise ProtoValidationError(
            f"{where}: the Python module {source!r} declares the registry modules "
            f"{sorted(h.name for h in handles)}; state __registry_module__ = <name>")
    return handles[0]


def _is_element(value) -> bool:
    return getattr(type(value), "__proto_descriptor__", False) or (
        type(value) is types.FunctionType and _registry._is_intent(value))


def semantic_class(cls) -> None:
    """Make the new ``DBObject`` subclass ``cls`` a semantic class (see the module docstring); called
    by ``DBObject.__init_subclass__``.  Everything is checked before anything changes."""
    where = f"Semantic class {cls.__qualname__} (Python module {cls.__module__!r})"
    if not cls._type_name_declared:
        raise ProtoValidationError(
            f"{where}: a semantic class declares its __type_name__ (such as 'acme.sales.Order'); a legacy name is "
            f"never given to one")
    type_name = cls.__dict__["__type_name__"]
    for name in _DEFERRED_DUNDERS:
        if name in cls.__dict__:
            raise ProtoNotSupportedError(
                f"{where}: {name} declares the Validity descriptor (valid time, D72), which is designed and "
                f"deferred: it is not part of the MVP")
    handle = _registry_module(cls, where)
    definitions, semantic, moved = {}, {}, []
    for name, value in cls.__dict__.items():
        if name in SEMANTIC_DUNDERS:
            semantic[name] = value
            moved.append(name)
        elif _is_element(value):
            here = _registry._check_definition_name(f"{where}, extension of {type_name!r}", name,
                                                    _registry._api_names())
            definitions[name] = _registry._definition(here, value)
            moved.append(name)
    semantic = _check_semantic(where, semantic)
    participant = _check_elements(where, definitions)
    relationship = issubclass(cls, Relationship)
    if participant and not relationship:
        raise ProtoValidationError(
            f"{where}: a Role is a role of a relationship; declare the class as a Relationship, or use Relation")
    semantic["kind"] = "relationship" if relationship else "entity"
    supertypes = tuple(base.__dict__["__type_name__"] for base in cls.__bases__
                       if getattr(base, "_implicit_parent", None) is not None)
    if supertypes:
        semantic["supertypes"] = supertypes
    handle._extend_with(type_name, definitions, semantic, cls.__qualname__, supertypes, source=cls.__module__)
    for name in moved:
        delattr(cls, name)
    cls._implicit_parent = type_name
    cls.describe = classmethod(_describe)


def _describe(cls, snapshot=None) -> dict:
    """``<SemanticClass>.describe(snapshot)``: the composed entity of this class's type in the read
    snapshot ``snapshot`` (a ``Snapshot`` or a ``Transaction``; "describe as a photograph", R259), as
    neutral data.  Without a snapshot, ``ProtoUserError``."""
    from ..transactions import Snapshot, Transaction
    if not isinstance(snapshot, (Snapshot, Transaction)):
        raise ProtoUserError(
            f"{cls.__qualname__}.describe() takes a read snapshot (a Snapshot or a Transaction): the composition of "
            f"a type is per database and per version, so there is no description without one")
    registry = snapshot.registry
    if registry is None:
        raise ProtoNotSupportedError(
            f"{cls.__qualname__}.describe(): the database lists no modules, so no semantic catalog describes "
            f"{cls.__type_name__!r}")
    from .compose import compose, describe
    return describe(compose(registry), cls.__type_name__)


# -- Extension ---------------------------------------------------------------------------------------

class Extension:
    """The base class of a module's extension of a type (spec section 4), the equivalent of
    ``@module.extends(...)``: ``class OrderDelivery(Extension): __extends__ = "acme.sales.Order"``.
    The class is a container: it is never instantiated, and its Python MRO is not used."""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        _extension_class(cls)

    def __init__(self, *args, **kwargs):
        raise ProtoUserError(f"{type(self).__qualname__} declares an extension; it is never instantiated")


def _extension_class(cls) -> None:
    where = f"Extension {cls.__qualname__} (Python module {cls.__module__!r})"
    if cls.__bases__ != (Extension,):
        raise ProtoValidationError(f"{where}: an Extension subclass has Extension as its only base")
    extends = cls.__dict__.get("__extends__")
    if not isinstance(extends, str) or not extends:
        raise ProtoValidationError(f"{where}: __extends__ names the extended type (a non-empty str), not {extends!r}")
    parents = cls.__dict__.get("__parents__", ())
    handle = _registry_module(cls, where)
    where = f"Module {handle.name!r}, extension of {extends!r}"
    api = _registry._api_names()
    definitions, semantic = {}, {}
    for name, value in cls.__dict__.items():
        if name in _registry._CLASS_MACHINERY or name in _EXTENSION_CONFIG:
            continue
        if name in SEMANTIC_DUNDERS:
            semantic[name] = value
            continue
        if name in _DEFERRED_DUNDERS:
            raise ProtoNotSupportedError(
                f"{where}: {name} declares the Validity descriptor (valid time, D72), which is designed and "
                f"deferred: it is not part of the MVP")
        here = _registry._check_definition_name(where, name, api)
        definitions[name] = _registry._definition(here, value)
    semantic = _check_decorated(where, extends, definitions, semantic)
    handle._extend_with(extends, definitions, semantic, cls.__qualname__, parents, source=cls.__module__)


# -- intents -----------------------------------------------------------------------------------------

#: The context of the intent running in this context (thread, asyncio task), or ``None``.
_RUNNING: contextvars.ContextVar = contextvars.ContextVar("proto_db_intent_context", default=None)


class IntentContext:
    """What :func:`intent_context` returns while an intent runs (R278): ``target`` (the handle of the
    entity the intent was invoked on) and the operations ``create``, ``get``, ``update``, ``link``,
    ``unlink`` and ``relate``.  This base class defines the protocol; the semantic session implements
    it (Task 8), and every operation here raises ``NotImplementedError``."""

    def __init__(self, target):
        self.target = target

    def create(self, type_name: str, **fields):
        raise NotImplementedError

    def get(self, handle):
        raise NotImplementedError

    def update(self, handle, **changes):
        raise NotImplementedError

    def link(self, source, role: str, target):
        raise NotImplementedError

    def unlink(self, source, role: str, target):
        raise NotImplementedError

    def relate(self, type_name: str, **roles_and_fields):
        raise NotImplementedError


def intent_context() -> IntentContext:
    """The context of the running intent (R278); outside a running intent, ``ProtoUserError``."""
    found = _RUNNING.get()
    if found is None:
        raise ProtoUserError("intent_context() is valid only while an intent runs: invoke the intent on a version "
                             "obtained from a SemanticSession")
    return found


class IntentInvoker:
    """An intent bound to its receiver, as resolution returns it (``order.add_line``): calling it binds
    the arguments to the intent's signature and invokes it through :func:`_invoke`.  Inside a running
    intent it runs the implementation directly, as part of that intent."""

    __slots__ = ("_function", "_receiver")

    def __init__(self, function, receiver):
        self._function = function
        self._receiver = receiver

    @property
    def name(self) -> str:
        """The intent's name (its definition name)."""
        return self._function.__proto_intent__.name

    def __call__(self, *args, **kwargs):
        arguments = _arguments(self._function, self._receiver, args, kwargs)
        if _RUNNING.get() is not None:
            return self._function(self._receiver, **arguments)
        return _invoke(self._receiver, self.name, arguments, None)

    def __repr__(self) -> str:
        return f"<intent {self.name!r} of {self._receiver!r}>"


def bind_intent(function, receiver) -> IntentInvoker:
    """The intent ``function`` bound to ``receiver`` (what resolution returns for an intent)."""
    return IntentInvoker(function, receiver)


def _arguments(function, receiver, args, kwargs) -> dict:
    """The arguments of a call of the intent ``function``, by parameter name, defaults applied."""
    spec = function.__proto_intent__
    try:
        bound = spec.signature.bind(receiver, *args, **kwargs)
    except TypeError as e:
        raise ProtoValidationError(f"Intent {spec.name!r}: {e}") from e
    bound.apply_defaults()
    arguments = dict(bound.arguments)
    arguments.pop(next(iter(spec.signature.parameters)))
    return arguments


def _invoke(version, name: str, arguments: Mapping, context):
    """Invoke the intent ``name`` on ``version`` with ``arguments`` (by parameter name) under
    ``context`` (an :class:`IntentContext`): the one entry point of every intent call (preflight F21).

    The implementation is found by D62 resolution (so ``_super()`` works inside it) and runs with
    :func:`intent_context` bound to ``context``.  A result that is a new version of the receiver (a
    ``DBObject`` of its class, ``self.with_(...)``) updates the target: its changed attributes go to
    ``context.update(context.target, ...)``.  Returns the implementation's result.  Without a context
    (a version no ``SemanticSession`` gave), ``ProtoUserError``."""
    where = f"The intent {name!r} of {type(version).__type_name__!r}"
    if context is None:
        raise ProtoUserError(f"{where} is invoked on a version obtained from a SemanticSession (spec section 4); "
                             f"this version was not")
    if not isinstance(context, IntentContext):
        raise ProtoUserError(f"{where}: the context is an IntentContext, not {type(context).__name__}")
    found = resolution.resolve(version, name)[0]
    if not isinstance(found, IntentInvoker):
        raise ProtoValidationError(f"{where}: {type(version).__type_name__!r} has no intent named {name!r}")
    function = found._function
    arguments = _arguments(function, version, (), dict(arguments))
    token = _RUNNING.set(context)
    try:
        result = function(version, **arguments)
    finally:
        _RUNNING.reset(token)
    if isinstance(result, DBObject) and type(result) is type(version) and result is not version:
        changes = {key: value for key, value in result._own.items()
                   if not version.has_defined_attr(key) or not values_equal(version._own_probe(key), value)}
        if changes:
            context.update(context.target, **changes)
    return result
