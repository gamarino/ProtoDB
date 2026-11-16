"""The semantic catalog's built-in atom types (format 6, feature ``catalog``).

The eleven types of ``docs/source/format-spec.rst``, "Semantic catalog (format 6)", aliases 24 to 34 in
this order: :class:`SemanticCatalog`, :class:`Module`, :class:`Extension`, :class:`Entity`,
:class:`Relationship`, :class:`Attribute`, :class:`Role`, :class:`Intent`, :class:`Constraint`,
:class:`Expression` and :class:`IntentRecord`.  Every record of them uses the format feature
``catalog`` and is version 6; none carries parents.

A record holds the fields its type lists (``_fields``, in the order they are written), each with the
value kind the chapter's "Field encodings" give it; a field that is not required is absent when it is
not declared or at its default ("Omission").  The records are plain data: building them from the
neutral model, reading them back and the canonical form are :mod:`proto_db.catalog.model`.

Two roots of a database's ``objects`` hold them, each identified by its name **and** by the type of
its value, as the registry roots are: :data:`SEMANTIC_CATALOG_ROOT` holds a :class:`SemanticCatalog`,
and :data:`INTENT_LOG_ROOT` a ``List`` of :class:`IntentRecord` records.  A value of another type is
``ProtoCorruptionError`` when it is read (:func:`semantic_catalog_root`, :func:`intent_log_root`,
:func:`intent_records`).  Applications never write a ``__`` root; the semantic layer writes its own at
commit.
"""
from __future__ import annotations

from collections.abc import Mapping

from ..collections.base import _Immutable
from ..collections.lists import List
from ..core.atom import Atom, PersistenceContext
from ..errors import ProtoCorruptionError, ProtoValidationError

__all__ = ["SEMANTIC_CATALOG_ROOT", "INTENT_LOG_ROOT", "SemanticCatalog", "Module", "Extension", "Entity",
           "Relationship", "Attribute", "Role", "Intent", "Constraint", "Expression", "IntentRecord",
           "CATALOG_RECORD_TYPES", "semantic_catalog_root", "intent_log_root", "intent_records"]

#: The root holding the semantic catalog (a :class:`SemanticCatalog`).
SEMANTIC_CATALOG_ROOT = "__semanticCatalog"
#: The root holding the intent log of the commit that wrote the version (a ``List`` of :class:`IntentRecord`).
INTENT_LOG_ROOT = "__intentLog"
_CATALOG_FEATURE = frozenset({"catalog"})

# -- value kinds of the fields (format-spec, "Field encodings") ------------------------------------
#: A ``str``.
TEXT = "text"
#: A ``@tup`` of ``str`` (the model holds a list).
NAMES = "names"
#: A ``@tup`` of refs to records (the model holds a list of items).
ELEMENTS = "elements"
#: A ``@map`` from a language tag to ``str``.
LANGUAGE_MAP = "language_map"
#: A ``bool``.
BOOL = "bool"
#: An ``int``.
INT = "int"
#: A canonical value.
VALUE = "value"
#: A ``@tup`` of canonical values (the model holds a list).
VALUES = "values"
#: A ref to an :class:`Expression` record (the model holds its item).
EXPRESSION = "expression"
#: The tree of the expression language: a ``list`` whose first item, the operator, is a ``str``.
AST = "ast"
#: A ``@tup`` of ``(name, value_type)`` pairs, each a ``@tup`` (the model holds a list of lists).
PARAMETERS = "parameters"
#: ``Extension.entity``: a ``@map`` of entity-level fields, each with the encoding of the Entity field.
ENTITY_MAP = "entity_map"
#: ``ext``: a ``@map`` from ``str`` to a canonical value, kept as written and never interpreted.
EXT = "ext"
#: A ``@map`` from ``str`` to a canonical value.
MAP = "map"
#: A ref to a ``Dictionary`` from ``str`` to a ref to a record (the model holds a dict of items).
DICTIONARY = "dictionary"
#: A ``@tup`` of two ``str``.
NAME_PAIR = "name_pair"

#: The kinds whose empty value is the default (omitted unless the field is required).
COLLECTION_KINDS = frozenset({NAMES, ELEMENTS, LANGUAGE_MAP, VALUES, PARAMETERS, ENTITY_MAP, EXT, MAP})
#: The fields that have a default other than "absent" or "empty" ("Omission").
FIELD_DEFAULTS = {"required": False, "indexed": False, "ordered": False, "deprecated": False, "enforced": True,
                  "participant": False, "sensitivity": "internal", "binding": "identity"}
#: The kind of the type an Extension contributes to, ``Extension.entity["kind"]``, and its default.
ENTITY_KINDS = frozenset({"entity", "relationship"})
ENTITY_KIND_DEFAULT = "entity"

_ENTITY_FIELDS = (("type_name", TEXT), ("supertypes", NAMES), ("meaning", TEXT), ("label", LANGUAGE_MAP),
                  ("help", LANGUAGE_MAP), ("synonyms", NAMES), ("key", NAMES), ("attributes", ELEMENTS))
_ENTITY_TAIL = (("intents", ELEMENTS), ("constraints", ELEMENTS), ("certification", TEXT),
                ("provenance", NAMES), ("ext", EXT))
#: The Attribute fields a Role does not carry: a role is stored and holds entities, not a value of a kind.
_NOT_ROLE_FIELDS = frozenset({"kind", "value_type", "values", "default", "unit", "expression"})
_ATTRIBUTE_FIELDS = (("name", TEXT), ("inherited_from", TEXT), ("kind", TEXT), ("value_type", TEXT),
                     ("values", NAMES), ("required", BOOL), ("default", VALUE), ("unit", TEXT),
                     ("indexed", BOOL), ("expression", EXPRESSION), ("languages", NAMES), ("meaning", TEXT),
                     ("label", LANGUAGE_MAP), ("help", LANGUAGE_MAP), ("group", TEXT), ("order", INT),
                     ("synonyms", NAMES), ("examples", VALUES), ("caveats", TEXT), ("sensitivity", TEXT),
                     ("certification", TEXT), ("deprecated", BOOL), ("provenance", NAMES), ("ext", EXT))
_ROLE_OWN = (("target", TEXT), ("inverse", TEXT), ("cardinality", TEXT), ("inverse_cardinality", TEXT),
             ("participant", BOOL), ("ordered", BOOL), ("index_by", NAMES), ("binding", TEXT), ("on_delete", TEXT))


class _CatalogRecord:
    """The construction of a catalog record (a mixin of every catalog type, not an atom type itself).

    The keyword arguments are the record's fields: a name the type does not list, or a missing required
    field, is ``ProtoValidationError``.  The fields are kept in the order of ``_fields`` (the order they
    are written); their values are taken as given (:func:`proto_db.catalog.model.to_atoms` builds them
    from a checked model)."""

    #: Field name -> value kind, in the order the fields are written.
    _fields: dict = {}
    _format_features = _CATALOG_FEATURE

    def __init__(self, *, context: PersistenceContext | None = None, **fields):
        super().__init__(context=context)
        cls = type(self)
        unknown = sorted(name for name in fields if name not in cls._fields)
        if unknown:
            raise ProtoValidationError(f"{cls.__type_name__} has no fields {unknown}; its fields are "
                                       f"{list(cls._fields)}")
        missing = [name for name in cls._required_fields if name not in fields]
        if missing:
            raise ProtoValidationError(f"{cls.__type_name__} lacks the required fields {missing}")
        for name in cls._fields:
            if name in fields:
                object.__setattr__(self, name, fields[name])


class SemanticCatalog(_CatalogRecord, _Immutable, Atom):
    """The value of :data:`SEMANTIC_CATALOG_ROOT`: ``modules`` (a Dictionary from module name to
    :class:`Module`), ``module_set`` (the module names in code-point order) and ``entities`` (a
    Dictionary from type name to :class:`Entity` or :class:`Relationship`, the composition for
    ``module_set``)."""

    __type_name__ = "proto_db.catalog.SemanticCatalog"
    _fields = dict((("modules", DICTIONARY), ("module_set", NAMES), ("entities", DICTIONARY), ("ext", EXT)))
    _required_fields = ("modules", "module_set", "entities")


class Module(_CatalogRecord, _Immutable, Atom):
    """A module's contributions: ``name``, ``version``, ``depends`` and its ``extensions`` in declaration
    order."""

    __type_name__ = "proto_db.catalog.Module"
    _fields = dict((("name", TEXT), ("version", TEXT), ("depends", NAMES), ("extensions", ELEMENTS),
                    ("ext", EXT)))
    _required_fields = ("name", "version")


class Extension(_CatalogRecord, _Immutable, Atom):
    """One contribution of a module to a type: ``module``, ``extends`` (the type name), ``entity`` (the
    entity-level fields it states, and ``kind``: ``relationship`` for a relationship type, omitted for an
    entity) and ``elements`` (the Attribute, Role, Intent and Constraint records it declares, with the
    fields it states)."""

    __type_name__ = "proto_db.catalog.Extension"
    _fields = dict((("module", TEXT), ("extends", TEXT), ("entity", ENTITY_MAP), ("elements", ELEMENTS),
                    ("ext", EXT)))
    _required_fields = ("module", "extends")


class Entity(_CatalogRecord, _Immutable, Atom):
    """A composed entity type."""

    __type_name__ = "proto_db.catalog.Entity"
    _fields = dict(_ENTITY_FIELDS + _ENTITY_TAIL)
    _required_fields = ("type_name",)


class Relationship(_CatalogRecord, _Immutable, Atom):
    """A composed relationship type (an entity whose instances link others): the Entity fields plus
    ``roles``, its Role records (two or more)."""

    __type_name__ = "proto_db.catalog.Relationship"
    _fields = dict(_ENTITY_FIELDS + (("roles", ELEMENTS),) + _ENTITY_TAIL)
    _required_fields = ("type_name", "roles")


class Attribute(_CatalogRecord, _Immutable, Atom):
    """An attribute: a stored field, a computed value or a maintained (stored) computed value."""

    __type_name__ = "proto_db.catalog.Attribute"
    _fields = dict(_ATTRIBUTE_FIELDS)
    _required_fields = ("name", "kind")


class Role(_CatalogRecord, _Immutable, Atom):
    """One end of a relation, or a role of a relationship: the Attribute fields except ``kind``,
    ``value_type``, ``values``, ``default``, ``unit`` and ``expression``, plus the role's own.
    ``participant`` is true for a role of a relationship (listed in its ``roles``), false (omitted) for
    the end of a plain relation."""

    __type_name__ = "proto_db.catalog.Role"
    _fields = dict(_ATTRIBUTE_FIELDS[:2] + _ROLE_OWN
                   + tuple(f for f in _ATTRIBUTE_FIELDS[2:] if f[0] not in _NOT_ROLE_FIELDS))
    _required_fields = ("name", "target", "cardinality")


class Intent(_CatalogRecord, _Immutable, Atom):
    """An operation with a meaning."""

    __type_name__ = "proto_db.catalog.Intent"
    _fields = dict((("name", TEXT), ("inherited_from", TEXT), ("meaning", TEXT), ("parameters", PARAMETERS),
                    ("requires", EXPRESSION),
                    ("commutes_with", NAMES), ("effects", NAMES), ("languages", NAMES), ("provenance", NAMES),
                    ("ext", EXT)))
    _required_fields = ("name",)


class Constraint(_CatalogRecord, _Immutable, Atom):
    """A rule the data must keep."""

    __type_name__ = "proto_db.catalog.Constraint"
    _fields = dict((("name", TEXT), ("inherited_from", TEXT), ("scope", TEXT), ("expression", EXPRESSION),
                    ("enforced", BOOL),
                    ("meaning", TEXT), ("message", TEXT), ("provenance", NAMES), ("ext", EXT)))
    _required_fields = ("name", "scope", "expression")


class Expression(_CatalogRecord, _Immutable, Atom):
    """An expression of the expression language, stored as its tree ``ast``."""

    __type_name__ = "proto_db.catalog.Expression"
    _fields = dict((("ast", AST), ("ext", EXT)))
    _required_fields = ("ast",)


class IntentRecord(_CatalogRecord, _Immutable, Atom):
    """One operation of the intent log: ``intent`` (the pair of type name and intent name), ``target``
    (the target's cell name), ``arguments`` (parameter name to canonical value) and ``transaction`` (the
    version of the DatabaseRoot the commit produced).  Its fields are checked when it is built
    (``ProtoValidationError``) and when it is read from the log (``ProtoCorruptionError``,
    :func:`intent_records`)."""

    __type_name__ = "proto_db.catalog.IntentRecord"
    _fields = dict((("intent", NAME_PAIR), ("target", TEXT), ("arguments", MAP), ("transaction", INT),
                    ("ext", EXT)))
    _required_fields = ("intent", "target", "arguments", "transaction")

    def __init__(self, *, context: PersistenceContext | None = None, **fields):
        super().__init__(context=context, **fields)
        problem = _intent_record_problem(self)
        if problem is not None:
            raise ProtoValidationError(f"Invalid intent record: {problem}")


#: The catalog's built-in types, in alias order (24 to 34).
CATALOG_RECORD_TYPES = (SemanticCatalog, Module, Extension, Entity, Relationship, Attribute, Role, Intent,
                        Constraint, Expression, IntentRecord)


def _intent_record_problem(record: IntentRecord) -> str | None:
    """What is wrong with the fields of ``record``, or ``None``."""
    intent = record.intent
    if type(intent) is not tuple or len(intent) != 2 or any(type(part) is not str or not part for part in intent):
        return f"intent is {intent!r}, not a pair of a type name and an intent name"
    if type(record.target) is not str or not record.target:
        return f"target is {record.target!r}, not a cell name"
    arguments = record.arguments
    if not isinstance(arguments, Mapping) or any(type(name) is not str for name in arguments):
        return f"arguments is {type(arguments).__name__}, not a map from parameter name to value"
    transaction = record.transaction
    if type(transaction) is not int or transaction < 0:
        return f"transaction is {transaction!r}, not a database version number"
    return None


def _corrupt(root: str, detail: str) -> ProtoCorruptionError:
    return ProtoCorruptionError(f"Semantic root {root!r}: {detail}")


def semantic_catalog_root(objects) -> SemanticCatalog | None:
    """The value of :data:`SEMANTIC_CATALOG_ROOT` in the roots dictionary ``objects`` (a version's roots),
    or ``None`` when it is absent; a value that is not a :class:`SemanticCatalog` is
    ``ProtoCorruptionError``."""
    value = objects.get_at(SEMANTIC_CATALOG_ROOT, None)
    if value is not None and not isinstance(value, SemanticCatalog):
        raise _corrupt(SEMANTIC_CATALOG_ROOT, f"its value is a {type(value).__name__}, not a "
                                              f"{SemanticCatalog.__type_name__}")
    return value


def intent_log_root(objects) -> List | None:
    """The value of :data:`INTENT_LOG_ROOT` in the roots dictionary ``objects``, or ``None`` when it is
    absent; a value that is not a ``List`` is ``ProtoCorruptionError``.  Its elements are checked when
    they are read (:func:`intent_records`)."""
    value = objects.get_at(INTENT_LOG_ROOT, None)
    if value is not None and not isinstance(value, List):
        raise _corrupt(INTENT_LOG_ROOT, f"its value is a {type(value).__name__}, not a List")
    return value


def intent_records(log: List) -> tuple:
    """The :class:`IntentRecord` records of the intent log ``log``, in order.  An element that is not an
    IntentRecord, or one whose fields are malformed, is ``ProtoCorruptionError`` naming its position."""
    out = []
    for position, record in enumerate(log.elements()):
        if not isinstance(record, IntentRecord):
            raise _corrupt(INTENT_LOG_ROOT, f"element {position} is a {type(record).__name__}, not an "
                                            f"{IntentRecord.__type_name__}")
        problem = _intent_record_problem(record)
        if problem is not None:
            raise _corrupt(INTENT_LOG_ROOT, f"element {position}: {problem}")
        out.append(record)
    return tuple(out)
