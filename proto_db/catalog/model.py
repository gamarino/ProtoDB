"""The neutral model of a semantic catalog, its records and its canonical form (ruling R258).

The **model** is plain data, the same in every language: ``{"module_set": [str], "modules": {name:
module}, "entities": {type_name: entity}}``, plus ``"ext"`` when the SemanticCatalog record carries one.  Every item is a dict whose keys are exactly the field names
of ``docs/source/format-spec.rst``, "Semantic catalog (format 6)", and whose values have the field's
encoding, with lists where a record holds a ``@tup`` and nested items where it holds a ref (an
``expression`` is the item ``{"ast": tree}``).  A field that is not required is omitted when it is not
declared, and the model this module produces omits it at its default too (:func:`from_atoms`).

The type of an item is told from its place and its required fields: ``modules`` holds Module items, a
module's ``extensions`` Extension items, ``entities`` Relationship items (they have ``roles``) and Entity
items; among elements an item with ``kind`` is an Attribute, one with ``target`` a Role, one with
``scope`` a Constraint, and any other an Intent.  The required fields of the types make that unambiguous.
A Role with ``participant`` true is a role of a relationship: it is listed in the Relationship's ``roles``
(never in ``attributes``), and an Extension that declares one states ``"kind": "relationship"`` in its
``entity``, so a reader composing from the modules' contributions builds the same record.

- :func:`check_model` validates a model (``ProtoValidationError`` naming the path of the first invalid
  item);
- :func:`to_atoms` builds its records, writing no field at its default;
- :func:`from_atoms` reads them back into the model (``ProtoCorruptionError`` for malformed records);
- :func:`canonical_form` is the msgpack bytes of the canonical encoding of the expanded model: each item
  a map of its fields without the defaults, every map (items, ``modules``, ``entities``, ``label``,
  ``help``, ``entity`` and ``ext``) sorted by key in code-point order, no atoms.  Two implementations
  conform when their canonical forms are equal byte for byte; record bytes are never compared.
"""
from __future__ import annotations

import msgpack

from ..collections.dictionaries import Dictionary
from ..core import codec
from ..core.atom import TYPE_NAME_PATTERN
from ..errors import ProtoCorruptionError, ProtoValidationError
from . import atoms as _a

__all__ = ["check_model", "to_atoms", "from_atoms", "canonical_form", "VALUE_KINDS"]

#: The canonical value kinds a ``value_type`` names (ruling R268).
VALUE_KINDS = frozenset({"bool", "int", "decimal", "str", "bytes", "date", "time", "datetime", "timedelta",
                         "uuid", "float", "list", "tuple", "map"})
#: The values of the enumerated text fields.
_ENUMERATIONS = {
    "kind": frozenset({"field", "computed", "computed_stored"}),
    "scope": frozenset({"attribute", "entity", "relation", "aggregate"}),
    "binding": frozenset({"identity", "version"}),
    "cardinality": frozenset({"one", "many", "0..1", "1..*"}),
    "inverse_cardinality": frozenset({"one", "many", "0..1", "1..*"}),
    "value_type": VALUE_KINDS,
}
#: The fields an ``Extension.entity`` map may state, with their value kinds: ``kind`` (``entity`` or
#: ``relationship``, default ``entity``) and Entity fields, each with the encoding of the Entity field.
_ENTITY_MAP_FIELDS = {"kind": _a.TEXT, **{name: _a.Entity._fields[name] for name in (
    "meaning", "label", "help", "synonyms", "key", "supertypes", "certification")}}
_TOP_FIELDS = ("module_set", "modules", "entities")
_TOP_OPTIONAL = ("ext",)


def _element_types(cls, name: str) -> tuple:
    """The record types an element of the field ``name`` of ``cls`` may be."""
    if cls is _a.Module:
        return (_a.Extension,)
    if cls is _a.Extension:
        return (_a.Attribute, _a.Role, _a.Constraint, _a.Intent)
    if name == "attributes":
        return (_a.Attribute, _a.Role)
    return {"intents": (_a.Intent,), "constraints": (_a.Constraint,), "roles": (_a.Role,)}[name]


def _item_type(item: dict, allowed: tuple):
    """The record type of the element ``item`` (a dict) by its required fields."""
    if "kind" in item:
        found = _a.Attribute
    elif "target" in item:
        found = _a.Role
    elif "scope" in item:
        found = _a.Constraint
    else:
        found = _a.Intent
    return found if found in allowed else None


def _short(cls) -> str:
    return cls.__type_name__.rsplit(".", 1)[1]


def _is_default(name: str, kind: str, value) -> bool:
    """Whether ``value`` of the field ``name`` (of value kind ``kind``) is that field's default.  An
    ``Extension.entity`` map is at its default when every field it holds is (review I2)."""
    if kind == _a.ENTITY_MAP:
        return all(_is_entity_map_default(key, v) for key, v in value.items())
    if kind in _a.COLLECTION_KINDS:
        return len(value) == 0
    if name in _a.FIELD_DEFAULTS:
        default = _a.FIELD_DEFAULTS[name]
        return type(value) is type(default) and value == default
    return False


def _is_entity_map_default(key: str, value) -> bool:
    """Whether ``value`` of the ``Extension.entity`` field ``key`` is that field's default."""
    if key == "kind":
        return value == _a.ENTITY_KIND_DEFAULT
    return _is_default(key, _ENTITY_MAP_FIELDS[key], value)


def _refuse_atom(atom):
    raise ProtoValidationError(f"a catalog value holds no atoms, not a {type(atom).__name__}")


# -- checking -------------------------------------------------------------------------------------

def _invalid(path: str, detail: str) -> ProtoValidationError:
    return ProtoValidationError(f"Invalid catalog model at {path}: {detail}")


def check_model(model) -> None:
    """Validate the neutral model ``model``; ``ProtoValidationError`` names the path of the first invalid
    item (``entities['acme.sales.Order'].attributes[2].cardinality``)."""
    if type(model) is not dict:
        raise _invalid("the model", f"a model is a dict, not a {type(model).__name__}")
    unknown = sorted(str(k) for k in model if k not in _TOP_FIELDS + _TOP_OPTIONAL)
    if unknown:
        raise _invalid("the model", f"unknown keys {unknown}; a model has {list(_TOP_FIELDS + _TOP_OPTIONAL)}")
    if "ext" in model:
        _check_field(_a.SemanticCatalog, "ext", _a.EXT, model["ext"], "ext")
    missing = [k for k in _TOP_FIELDS if k not in model]
    if missing:
        raise _invalid("the model", f"it lacks {missing}")
    module_set = model["module_set"]
    _check_names("module_set", module_set)
    if list(module_set) != sorted(set(module_set)):
        raise _invalid("module_set", "the module names must be distinct and in code-point order")
    _check_dictionary("modules", model["modules"], _a.Module, "name")
    if set(model["modules"]) != set(module_set):
        raise _invalid("modules", f"the modules {sorted(model['modules'])} are not those of module_set "
                                  f"{list(module_set)}")
    entities = model["entities"]
    if type(entities) is not dict:
        raise _invalid("entities", f"a dict from type name to entity, not a {type(entities).__name__}")
    for key, item in entities.items():
        path = f"entities[{key!r}]"
        cls = _a.Relationship if type(item) is dict and "roles" in item else _a.Entity
        _check_record(cls, item, path)
        if item["type_name"] != key:
            raise _invalid(path, f"its type_name is {item['type_name']!r}, not its key")


def _check_dictionary(path: str, value, cls, key_field: str) -> None:
    if type(value) is not dict:
        raise _invalid(path, f"a dict from name to {_short(cls)}, not a {type(value).__name__}")
    for key, item in value.items():
        if type(key) is not str:
            raise _invalid(path, f"the key {key!r} is not a str")
        item_path = f"{path}[{key!r}]"
        _check_record(cls, item, item_path)
        if item[key_field] != key:
            raise _invalid(item_path, f"its {key_field} is {item[key_field]!r}, not its key")


def _check_record(cls, item, path: str) -> None:
    if type(item) is not dict:
        raise _invalid(path, f"a {_short(cls)} is a dict of its fields, not a {type(item).__name__}")
    for name in item:
        if name not in cls._fields:
            raise _invalid(f"{path}.{name}" if type(name) is str else path,
                           f"{name!r} is not a field of {cls.__type_name__}")
    for name in cls._required_fields:
        if name not in item:
            raise _invalid(path, f"a {_short(cls)} requires the field {name!r}")
    for name, kind in cls._fields.items():
        if name in item:
            _check_field(cls, name, kind, item[name], f"{path}.{name}")
    if cls is _a.Extension and item.get("entity", {}).get("kind", _a.ENTITY_KIND_DEFAULT) != "relationship":
        for i, element in enumerate(item.get("elements", ())):
            if "kind" not in element and "target" in element and element.get("participant"):
                raise _invalid(f"{path}.elements[{i}]", "a participant role belongs to an Extension whose "
                                                        "entity kind is relationship")


def _check_names(path: str, value) -> None:
    if type(value) not in (list, tuple):
        raise _invalid(path, f"a list of str, not a {type(value).__name__}")
    for i, name in enumerate(value):
        if type(name) is not str:
            raise _invalid(f"{path}[{i}]", f"a str, not a {type(name).__name__}")


def _check_text_map(path: str, value) -> None:
    if type(value) is not dict:
        raise _invalid(path, f"a dict from language tag to text, not a {type(value).__name__}")
    for tag, text in value.items():
        if type(tag) is not str or not tag or type(text) is not str:
            raise _invalid(path, f"{tag!r}: {text!r} is not a language tag and its text")


def _check_value(path: str, value) -> None:
    try:
        codec.encode_value(value, _refuse_atom)
    except ProtoValidationError as e:
        raise _invalid(path, f"not a canonical value without atoms: {e}") from e


def _check_value_type(path: str, value) -> None:
    """A parameter's ``value_type``: a value kind, an entity type name, or ``None`` (undeclared)."""
    if value is None:
        return
    if type(value) is not str or not (value in VALUE_KINDS or TYPE_NAME_PATTERN.match(value)):
        raise _invalid(path, f"{value!r} is neither a value kind {sorted(VALUE_KINDS)}, an entity type name "
                             f"nor None (undeclared)")


def _check_field(cls, name: str, kind: str, value, path: str) -> None:
    if kind == _a.TEXT:
        if type(value) is not str:
            raise _invalid(path, f"a str, not a {type(value).__name__}")
        allowed = _ENUMERATIONS.get(name)
        if allowed is not None and value not in allowed:
            raise _invalid(path, f"{value!r} is not one of {sorted(allowed)}")
    elif kind == _a.NAMES:
        _check_names(path, value)
    elif kind == _a.BOOL:
        if type(value) is not bool:
            raise _invalid(path, f"a bool, not a {type(value).__name__}")
    elif kind == _a.INT:
        if type(value) is not int:
            raise _invalid(path, f"an int, not a {type(value).__name__}")
    elif kind == _a.LANGUAGE_MAP:
        _check_text_map(path, value)
    elif kind == _a.VALUE:
        _check_value(path, value)
    elif kind == _a.VALUES:
        if type(value) not in (list, tuple):
            raise _invalid(path, f"a list of values, not a {type(value).__name__}")
        for i, v in enumerate(value):
            _check_value(f"{path}[{i}]", v)
    elif kind == _a.EXT or kind == _a.MAP:
        if type(value) is not dict or any(type(k) is not str for k in value):
            raise _invalid(path, "a dict from str to a value")
        for k, v in value.items():
            _check_value(f"{path}[{k!r}]", v)
    elif kind == _a.EXPRESSION:
        _check_record(_a.Expression, value, path)
    elif kind == _a.AST:
        if not isinstance(value, list) or not value or type(value[0]) is not str:      # a list or a FrozenList
            raise _invalid(path, "an expression tree is a list whose first item, the operator, is a str")
        _check_value(path, value)
    elif kind == _a.PARAMETERS:
        if type(value) not in (list, tuple):
            raise _invalid(path, f"a list of (name, value_type) pairs, not a {type(value).__name__}")
        for i, pair in enumerate(value):
            if type(pair) not in (list, tuple) or len(pair) != 2 or type(pair[0]) is not str or not pair[0]:
                raise _invalid(f"{path}[{i}]", f"{pair!r} is not a (name, value_type) pair")
            _check_value_type(f"{path}[{i}]", pair[1])
    elif kind == _a.ENTITY_MAP:
        if type(value) is not dict:
            raise _invalid(path, f"a dict of entity-level fields, not a {type(value).__name__}")
        for key, v in value.items():
            if key not in _ENTITY_MAP_FIELDS:
                raise _invalid(path, f"{key!r} is not an entity-level field; they are {list(_ENTITY_MAP_FIELDS)}")
            if key == "kind":
                if v not in _a.ENTITY_KINDS:
                    raise _invalid(f"{path}[{key!r}]", f"{v!r} is not one of {sorted(_a.ENTITY_KINDS)}")
                continue
            _check_field(_a.Entity, key, _ENTITY_MAP_FIELDS[key], v, f"{path}[{key!r}]")
    elif kind == _a.ELEMENTS:
        if type(value) not in (list, tuple):
            raise _invalid(path, f"a list of items, not a {type(value).__name__}")
        allowed = _element_types(cls, name)
        for i, item in enumerate(value):
            item_path = f"{path}[{i}]"
            if type(item) is not dict:
                raise _invalid(item_path, f"an item is a dict of its fields, not a {type(item).__name__}")
            element = _a.Extension if allowed == (_a.Extension,) else _item_type(item, allowed)
            if element is None:
                raise _invalid(item_path, f"not one of {[_short(c) for c in allowed]} (an Attribute has kind, "
                                          f"a Role target, a Constraint scope)")
            _check_record(element, item, item_path)
            if element is _a.Role:
                participant = item.get("participant", False)
                if name == "roles" and participant is not True:
                    raise _invalid(item_path, "a role of a relationship has participant true")
                if name == "attributes" and participant:
                    raise _invalid(item_path, "a participant role is listed in a Relationship's roles, not in "
                                              "attributes")
        if cls is _a.Relationship and name == "roles" and len(value) < 2:
            raise _invalid(path, "a relationship has two or more roles")
    else:                                   # DICTIONARY and NAME_PAIR are not fields of model items
        raise _invalid(path, f"the field kind {kind!r} has no model form")


# -- the records ----------------------------------------------------------------------------------

def to_atoms(model: dict, context) -> _a.SemanticCatalog:
    """The records of the checked ``model`` (:func:`check_model`), not yet saved, through ``context``:
    a :class:`~proto_db.catalog.atoms.SemanticCatalog` over Dictionaries of the Module and Entity or
    Relationship records.  No field is written at its default."""
    check_model(model)
    modules = Dictionary(context=context)
    for name in sorted(model["modules"]):
        modules = modules.set_at(name, _build(_a.Module, model["modules"][name], context))
    entities = Dictionary(context=context)
    for name in sorted(model["entities"]):
        item = model["entities"][name]
        entities = entities.set_at(name, _build(_a.Relationship if "roles" in item else _a.Entity, item, context))
    extra = {"ext": dict(model["ext"])} if model.get("ext") else {}
    return _a.SemanticCatalog(modules=modules, module_set=tuple(model["module_set"]), entities=entities,
                              context=context, **extra)


def _build(cls, item: dict, context):
    fields = {}
    for name, kind in cls._fields.items():
        if name not in item:
            continue
        value = item[name]
        if name not in cls._required_fields and _is_default(name, kind, value):
            continue
        fields[name] = _record_value(cls, name, kind, value, context)
    return cls(context=context, **fields)


def _record_value(cls, name: str, kind: str, value, context):
    if kind in (_a.NAMES, _a.VALUES):
        return tuple(value)
    if kind in (_a.LANGUAGE_MAP, _a.EXT, _a.MAP):
        return dict(value)
    if kind == _a.PARAMETERS:
        return tuple((pair[0], pair[1]) for pair in value)
    if kind == _a.EXPRESSION:
        return _build(_a.Expression, value, context)
    if kind == _a.ENTITY_MAP:
        return {key: _record_value(_a.Entity, key, _ENTITY_MAP_FIELDS[key], v, context)
                for key, v in value.items() if not _is_entity_map_default(key, v)}
    if kind == _a.ELEMENTS:
        allowed = _element_types(cls, name)
        return tuple(_build(_a.Extension if allowed == (_a.Extension,) else _item_type(item, allowed), item, context)
                     for item in value)
    return value                            # TEXT, BOOL, INT, VALUE, AST


def from_atoms(catalog) -> dict:
    """The neutral model of the stored catalog ``catalog``: its fields, without those at their default,
    and without fields its types do not list (a later revision's, which this one does not interpret).
    A record of the wrong type, a field of the wrong kind, or a model that :func:`check_model` refuses is
    ``ProtoCorruptionError``."""
    if not isinstance(catalog, _a.SemanticCatalog):
        raise ProtoCorruptionError(f"A semantic catalog is a {_a.SemanticCatalog.__type_name__}, not a "
                                   f"{type(catalog).__name__}")
    state = catalog._state()
    model = {"module_set": _read_value(_a.SemanticCatalog, "module_set", _a.NAMES, state["module_set"], "module_set"),
             "modules": _read_dictionary(state["modules"], (_a.Module,), "modules"),
             "entities": _read_dictionary(state["entities"], (_a.Entity, _a.Relationship), "entities")}
    if "ext" in state:
        ext = _read_value(_a.SemanticCatalog, "ext", _a.EXT, state["ext"], "ext")
        if ext:
            model["ext"] = ext
    try:
        check_model(model)
    except ProtoValidationError as e:
        raise ProtoCorruptionError(f"The stored semantic catalog is malformed: {e}") from e
    return model


def _malformed(path: str, detail: str) -> ProtoCorruptionError:
    return ProtoCorruptionError(f"Malformed semantic catalog record at {path}: {detail}")


def _read_dictionary(value, types: tuple, path: str) -> dict:
    if not isinstance(value, Dictionary):
        raise _malformed(path, f"a Dictionary, not a {type(value).__name__}")
    out = {}
    for key, record in value.items():
        out[key] = _read(record, types, f"{path}[{key!r}]")
    return out


def _read(record, types: tuple, path: str) -> dict:
    cls = type(record)
    if cls not in types:
        raise _malformed(path, f"a {' or '.join(t.__type_name__ for t in types)}, not a {cls.__name__}")
    state = record._state()
    out = {}
    for name, kind in cls._fields.items():
        if name not in state:
            continue
        value = _read_value(cls, name, kind, state[name], f"{path}.{name}")
        if name not in cls._required_fields and _is_default(name, kind, value):
            continue
        out[name] = value
    return out


def _read_value(cls, name: str, kind: str, value, path: str):
    if kind in (_a.NAMES, _a.VALUES, _a.ELEMENTS, _a.PARAMETERS):
        if type(value) is not tuple:
            raise _malformed(path, f"a tuple, not a {type(value).__name__}")
        if kind == _a.ELEMENTS:
            allowed = _element_types(cls, name)
            return [_read(record, allowed, f"{path}[{i}]") for i, record in enumerate(value)]
        if kind == _a.PARAMETERS:
            if any(type(pair) is not tuple for pair in value):
                raise _malformed(path, "a tuple of (name, value_type) tuples")
            return [list(pair) for pair in value]
        return list(value)
    if kind in (_a.LANGUAGE_MAP, _a.EXT, _a.MAP, _a.ENTITY_MAP):
        if not isinstance(value, dict):
            raise _malformed(path, f"a map, not a {type(value).__name__}")
        if kind == _a.ENTITY_MAP:
            out = {}
            for key, v in value.items():
                field_kind = _ENTITY_MAP_FIELDS.get(key)
                if field_kind is None:
                    raise _malformed(path, f"{key!r} is not an entity-level field")
                v = _read_value(_a.Entity, key, field_kind, v, f"{path}[{key!r}]")
                if not _is_entity_map_default(key, v):
                    out[key] = v
            return out
        return dict(value)
    if kind == _a.EXPRESSION:
        return _read(value, (_a.Expression,), path)
    return value                            # TEXT, BOOL, INT, VALUE, AST: checked by check_model


# -- the canonical form ---------------------------------------------------------------------------

def canonical_form(model: dict) -> bytes:
    """The canonical form of the checked ``model`` (ruling R258): the msgpack bytes of the canonical
    encoding of the expanded model, every item a map of its fields without the defaults, every map
    sorted by key in code-point order.  Oids, pointers and the placement of records never enter it."""
    check_model(model)
    expanded = {
        "entities": {name: _expand(_a.Relationship if "roles" in item else _a.Entity, item)
                     for name, item in sorted(model["entities"].items())},
        "module_set": tuple(model["module_set"]),
        "modules": {name: _expand(_a.Module, item) for name, item in sorted(model["modules"].items())},
    }
    if model.get("ext"):
        expanded = {"entities": expanded["entities"], "ext": dict(sorted(model["ext"].items())),
                    "module_set": expanded["module_set"], "modules": expanded["modules"]}
    return msgpack.packb(codec.encode_value(expanded, _refuse_atom), use_bin_type=True)


def _expand(cls, item: dict) -> dict:
    out = {}
    for name in sorted(item):
        kind = cls._fields[name]
        value = item[name]
        if name not in cls._required_fields and _is_default(name, kind, value):
            continue
        out[name] = _expanded_value(cls, name, kind, value)
    return out


def _expanded_value(cls, name: str, kind: str, value):
    if kind in (_a.NAMES, _a.VALUES):
        return tuple(value)
    if kind in (_a.LANGUAGE_MAP, _a.EXT, _a.MAP):
        return dict(sorted(value.items()))
    if kind == _a.PARAMETERS:
        return tuple((pair[0], pair[1]) for pair in value)
    if kind == _a.EXPRESSION:
        return _expand(_a.Expression, value)
    if kind == _a.ENTITY_MAP:
        return {key: _expanded_value(_a.Entity, key, _ENTITY_MAP_FIELDS[key], v)
                for key, v in sorted(value.items()) if not _is_entity_map_default(key, v)}
    if kind == _a.ELEMENTS:
        allowed = _element_types(cls, name)
        return tuple(_expand(_a.Extension if allowed == (_a.Extension,) else _item_type(item, allowed), item)
                     for item in value)
    return value                            # TEXT, BOOL, INT, VALUE, AST
