"""Composition of the semantic catalog from the modules' contributions (spec sections 4 and 5;
``docs/source/format-spec.rst``, "Composition and the canonical form").

- :func:`module_contributions` gives a registry module's contributions as the neutral Module item of
  the catalog model (:mod:`proto_db.catalog.model`): its semantic extensions, in declaration order,
  each with the entity-level fields and the elements it states;
- :func:`compose_modules` composes the entities from Module items (the stored ones, or the code's):
  this is the normative fold, so the code's composition and a stored catalog's are computed alike;
- :func:`compose` is the model of a registry: its modules' contributions and their composition;
- :func:`describe`, :func:`subtypes`, :func:`is_a` and :func:`index_paths` read a composed model.

The fold (normative)
    Contributions fold in the registry's load order (rulings R253-R255), module by module and, within
    a module, in declaration order.  An element (attribute, role or intent, one namespace; constraints
    a namespace of their own) is placed where it is first defined; a later definition modifies it:
    the fields a module may freely redefine (``label``, ``help``, ``group``, ``order``, ``meaning``,
    ``synonyms``, ``caveats``, ``examples``, ``default``, ``sensitivity``, ``deprecated``,
    ``certification``, ``ext``, the expression of a ``computed`` attribute with its ``languages``, the
    ``enforced`` and the ``meaning`` and ``message`` of a constraint, the ``meaning`` of an intent)
    take the newer value; every other field it states is structural and must equal the composed one,
    else ``ProtoValidationError`` naming the module, the type, the element and the field.  A field it
    omits is unchanged; a redefined intent's parameter whose type it does not declare keeps the
    composed type.  ``provenance`` lists the module that defined the element, then each module that
    modified it, once, in fold order.  Entity-level fields fold alike (``kind``, ``key`` and
    ``supertypes`` are structural).

Inverses
    A role or relation that names an inverse gets that end on its target, unless the target (or one
    of its supertypes) declares it: the created end is a role of the target with the two cardinalities
    swapped, the same ``ordered``, and the declaring module as provenance.  A declared end must agree
    with the role: target, inverse, both cardinalities and ``ordered``.

Subtypes (R280-R282)
    A type's linearization is the C3 order of its ``supertypes``.  The composed subtype is the fold of
    its linearization from the last node to the first: each node's own fold (its modules' contributions),
    then the subtype's.  An element first defined by another node carries ``inherited_from`` (the type
    that declared it, R281), keeps its provenance and gains the modules that modify it further.  A
    node may not change, in an element an earlier node defined, a structural field (``kind``,
    ``value_type``, ``cardinality``, ``target``, ``binding`` and the rest): ``ProtoValidationError``
    naming R282.  Of the entity-level fields only ``key`` is inherited, and a subtype may not redefine
    it; its ``meaning``, ``label``, ``help``, ``synonyms`` and ``certification`` are its own.

Maintained aggregates
    A ``computed_stored`` attribute's ``sum``/``count`` must read, on the members of its ``over`` role,
    only neutral declarations (fields, roles, computed attributes with an expression, other maintained
    aggregates): a Python-only computed there is ``ProtoValidationError``.
"""
from __future__ import annotations

import copy
from collections.abc import Mapping

from ..catalog.atoms import FIELD_DEFAULTS
from ..catalog.model import check_model
from ..errors import ProtoValidationError
from ..registry import DESCRIPTOR, METHOD, Registry, _is_intent, _load_order
from .descriptors import Aggregate

__all__ = ["module_contributions", "compose", "compose_modules", "describe", "subtypes", "is_a", "index_paths",
           "linearization"]

#: The ``Module.version`` the MVP writes (R263).
MODULE_VERSION = "1"

#: Fields any module may redefine (spec section 5); every other stated field is structural.
_FREE = frozenset({"label", "help", "group", "order", "meaning", "synonyms", "caveats", "examples", "default",
                   "sensitivity", "deprecated", "certification", "ext"})
#: Constraint fields that are not structural.
_FREE_CONSTRAINT = frozenset({"enforced", "meaning", "message", "ext"})
#: Entity-level fields any module may redefine.
_FREE_ENTITY = frozenset({"meaning", "label", "help", "synonyms", "certification"})
#: The Extension.entity keys and the Entity fields they give, in the order of the catalog.
_DUNDER_FIELDS = (("__meaning__", "meaning"), ("__label__", "label"), ("__help__", "help"),
                  ("__synonyms__", "synonyms"), ("__key__", "key"), ("__certification__", "certification"))
#: The fields omitted at their default (format spec, "Omission"): absent means this value.
_DEFAULTS = FIELD_DEFAULTS
#: A field an element does not state and that has no default.
_UNSTATED = object()


def _invalid(detail: str) -> ProtoValidationError:
    return ProtoValidationError(f"Semantic composition: {detail}")


# -- the contributions of a module ------------------------------------------------------------------

def _semantic_extension(extension) -> bool:
    """Whether the registry extension ``extension`` makes a semantic declaration."""
    if extension.semantic:
        return True
    return any(kind == DESCRIPTOR or (kind == METHOD and _is_intent(value))
               for kind, value in extension.definitions.values())


def _relationship_names(registry: Registry) -> frozenset:
    """The registry names some extension of ``registry`` declares as a relationship."""
    return frozenset(e.registry_name for e in registry.extensions if e.semantic.get("kind") == "relationship")


def _visible_modules(registry: Registry, module_name: str) -> frozenset:
    """``module_name`` and every module it depends on, transitively."""
    seen, stack = set(), [module_name]
    while stack:
        name = stack.pop()
        if name not in seen:
            seen.add(name)
            stack.extend(registry.depends_of(name))
    return frozenset(seen)


def _ancestor_names(registry: Registry, registry_name: str) -> frozenset:
    """``registry_name`` and its registry-name ancestors."""
    seen, stack = set(), [registry_name]
    while stack:
        name = stack.pop()
        if name not in seen:
            seen.add(name)
            stack.extend(registry.parents_of(name))
    return frozenset(seen)


def _maintained(registry: Registry, module_name: str, registry_name: str) -> frozenset:
    """The maintained aggregates declared for ``registry_name`` or its ancestors by ``module_name`` or a
    module it depends on: what an invariant of that module may read as an aggregate."""
    modules = _visible_modules(registry, module_name)
    names = _ancestor_names(registry, registry_name)
    return frozenset(name for e in registry.extensions if e.module in modules and e.registry_name in names
                     for name, (kind, value) in e.definitions.items()
                     if kind == DESCRIPTOR and isinstance(value, Aggregate) and value.maintained)


def _extension_item(registry: Registry, extension, relationships: frozenset) -> dict:
    """The catalog Extension item of the registry extension ``extension``."""
    semantic = extension.semantic
    entity = {}
    if extension.registry_name in relationships:
        entity["kind"] = "relationship"
    for dunder, field in _DUNDER_FIELDS:
        if dunder in semantic:
            value = semantic[dunder]
            entity[field] = list(value) if isinstance(value, tuple) else copy.deepcopy(value)
    if semantic.get("supertypes"):
        entity["supertypes"] = list(semantic["supertypes"])
    elements = []
    for name, (kind, value) in extension.definitions.items():
        if kind == DESCRIPTOR:
            elements.append(value.element(name))
        elif kind == METHOD and _is_intent(value):
            elements.append(value.__proto_intent__.element(name))
    constraints = semantic.get("__constraints__", ())
    if constraints:
        aggregates = _maintained(registry, extension.module, extension.registry_name)
        for constraint in constraints:
            scope = "aggregate" if constraint.reads() & aggregates else "entity"
            elements.append(constraint.element(scope))
    item = {"module": extension.module, "extends": extension.registry_name}
    if entity:
        item["entity"] = entity
    if elements:
        item["elements"] = elements
    return item


def module_contributions(registry: Registry, module_name: str) -> dict:
    """The contributions of the registry module ``module_name`` of ``registry``: its Module item
    (``name``, ``version``, ``depends``, ``extensions``), the extensions being its semantic ones, in
    declaration order.  A module ``registry`` does not contain is ``ProtoValidationError``."""
    if module_name not in registry.key:
        raise _invalid(f"the registry {list(registry.key)} does not contain the module {module_name!r}")
    relationships = _relationship_names(registry)
    extensions = [_extension_item(registry, e, relationships) for e in registry.extensions
                  if e.module == module_name and _semantic_extension(e)]
    item = {"name": module_name, "version": MODULE_VERSION}
    depends = list(registry.depends_of(module_name))
    if depends:
        item["depends"] = depends
    if extensions:
        item["extensions"] = extensions
    return item


def compose(registry: Registry) -> dict:
    """The catalog model of ``registry`` (the neutral model of :mod:`proto_db.catalog.model`): every
    module's contributions and the entities composed from them."""
    return compose_modules({name: module_contributions(registry, name) for name in registry.key})


# -- the fold ----------------------------------------------------------------------------------------

def _element_type(item: dict) -> str:
    if "kind" in item:
        return "attribute"
    if "target" in item:
        return "role"
    if "scope" in item:
        return "constraint"
    return "intent"


class _Own:
    """The own fold of one type: its modules' contributions, before inheritance."""

    __slots__ = ("type_name", "entity", "elements", "constraints", "provenance", "first")

    def __init__(self, type_name: str, first: int):
        self.type_name = type_name
        self.entity: dict = {}
        self.elements: dict = {}
        self.constraints: dict = {}
        self.provenance: list = []
        self.first = first


def _where(module: str, type_name: str, element: str | None = None, field: str | None = None) -> str:
    out = f"module {module!r}, type {type_name!r}"
    if element is not None:
        out += f", element {element!r}"
    if field is not None:
        out += f", field {field!r}"
    return out


def _merge(existing: dict, new: dict, *, module: str, type_name: str, subtype: bool, contributors=()) -> None:
    """Fold the definition ``new`` into the composed element ``existing`` (see "The fold")."""
    name = existing["name"]
    kind = _element_type(existing)
    if _element_type(new) != kind:
        raise _refusal(module, type_name, name, "kind", f"it is a {kind} and is redefined as a {_element_type(new)}",
                       subtype)
    free = _FREE_CONSTRAINT if kind == "constraint" else _FREE
    if kind == "intent":
        free = free | {"languages"}
    computed = kind == "attribute" and existing.get("kind") == "computed"
    if computed and new.get("kind") == "computed" and ("expression" in new or "languages" in new):
        existing.pop("expression", None)
        existing.pop("languages", None)
    for field, value in new.items():
        if field in ("name", "provenance", "inherited_from"):
            continue
        if field in free or (computed and field in ("expression", "languages")):
            existing[field] = copy.deepcopy(value)
            continue
        if field == "parameters" and kind == "intent" and "parameters" in existing:
            existing[field] = _merged_parameters(existing[field], value, module, type_name, name, subtype)
            continue
        current = existing.get(field, _DEFAULTS.get(field, _UNSTATED))
        if current is _UNSTATED:
            existing[field] = copy.deepcopy(value)      # stated for the first time: information added
        elif current != value or type(current) is not type(value):
            raise _refusal(module, type_name, name, field,
                           f"the composed value is {current!r} and the redefinition states {value!r}", subtype)
    provenance = existing.setdefault("provenance", [])
    for contributor in (contributors or [module]):
        if contributor not in provenance:
            provenance.append(contributor)


def _merged_parameters(existing: list, new: list, module, type_name, name, subtype) -> list:
    if [p for p, _ in existing] != [p for p, _ in new]:
        raise _refusal(module, type_name, name, "parameters",
                       f"the composed parameters are {[p for p, _ in existing]} and the redefinition's "
                       f"{[p for p, _ in new]}", subtype)
    out = []
    for (parameter, kind), (_, stated) in zip(existing, new):
        if stated is not None and kind is not None and stated != kind:
            raise _refusal(module, type_name, name, "parameters",
                           f"the parameter {parameter!r} is a {kind!r} and the redefinition states {stated!r}", subtype)
        out.append([parameter, kind if kind is not None else stated])
    return out


def _refusal(module, type_name, element, field, detail, subtype) -> ProtoValidationError:
    if subtype:
        return _invalid(f"{_where(module, type_name, element, field)}: a subtype may not change an inherited "
                        f"element's structure (R282: a role, query, aggregate or invariant typed by the supertype "
                        f"reads and writes the subtype's instances through the supertype's declaration); {detail}")
    return _invalid(f"{_where(module, type_name, element, field)}: a structural field is refused without a "
                    f"declared migration (spec section 5); {detail}")


def _fold_entity_fields(own: _Own, entity: Mapping, module: str) -> None:
    for field, value in entity.items():
        if field in _FREE_ENTITY:
            own.entity[field] = copy.deepcopy(value)
        elif field in own.entity and own.entity[field] != value:
            raise _invalid(f"{_where(module, own.type_name, field=field)}: the composed value is "
                           f"{own.entity[field]!r} and the extension states {value!r} (a structural field)")
        else:
            own.entity[field] = copy.deepcopy(value)


def _own_folds(modules: Mapping, order: tuple) -> dict:
    """The own fold of every type, in the order of its first contribution."""
    owns: dict = {}
    position = 0
    for module in order:
        for extension in modules[module].get("extensions", ()):
            type_name = extension["extends"]
            own = owns.get(type_name)
            if own is None:
                own = owns[type_name] = _Own(type_name, position)
                position += 1
            if module not in own.provenance:
                own.provenance.append(module)
            _fold_entity_fields(own, extension.get("entity", {}), module)
            for item in extension.get("elements", ()):
                target = own.constraints if _element_type(item) == "constraint" else own.elements
                found = target.get(item["name"])
                if found is None:
                    target[item["name"]] = {**copy.deepcopy(item), "provenance": [module]}
                else:
                    _merge(found, item, module=module, type_name=type_name, subtype=False)
    for own in owns.values():
        kind = own.entity.get("kind", "entity")
        for name, element in own.elements.items():
            if element.get("participant") and kind != "relationship":
                raise _invalid(f"type {own.type_name!r}, element {name!r}: a participant role belongs to a "
                               f"relationship, and the type's kind is {kind!r}")
    return owns


def _c3(type_name: str, owns: Mapping, cache: dict, path: tuple = ()) -> list:
    """The C3 linearization of ``type_name`` over the declared supertypes (itself first)."""
    found = cache.get(type_name)
    if found is not None:
        return found
    if type_name in path:
        raise _invalid(f"the supertypes of {type_name!r} form a cycle: {' -> '.join(path + (type_name,))}")
    own = owns.get(type_name)
    if own is None:
        raise _invalid(f"{path[-1]!r} declares the supertype {type_name!r}, which no module declares")
    supertypes = list(own.entity.get("supertypes", ()))
    sequences = [list(_c3(s, owns, cache, path + (type_name,))) for s in supertypes] + [supertypes]
    out = [type_name]
    while True:
        sequences = [s for s in sequences if s]
        if not sequences:
            break
        for sequence in sequences:
            head = sequence[0]
            if not any(head in other[1:] for other in sequences):
                break
        else:
            raise _invalid(f"the supertypes of {type_name!r} have no C3 linearization")
        out.append(head)
        for sequence in sequences:
            if sequence[0] == head:
                del sequence[0]
    cache[type_name] = out
    return out


def _create_inverses(owns: dict, lins: Mapping) -> None:
    """Add to each target the inverse ends its roles name and it does not declare (see "Inverses")."""
    for own in sorted(owns.values(), key=lambda o: o.first):
        for element in list(own.elements.values()):
            inverse = element.get("inverse")
            if _element_type(element) != "role" or inverse is None:
                continue
            target = owns.get(element["target"])
            if target is None:
                raise _invalid(f"type {own.type_name!r}, role {element['name']!r}: the target "
                               f"{element['target']!r} is not a declared type")
            if any(inverse in owns[node].elements for node in lins[target.type_name]):
                continue
            if "inverse_cardinality" not in element:
                raise _invalid(f"type {own.type_name!r}, role {element['name']!r}: it names the inverse {inverse!r} "
                               f"without its cardinality, and {target.type_name!r} does not declare it")
            created = {"name": inverse, "target": own.type_name, "inverse": element["name"],
                       "cardinality": element["inverse_cardinality"], "inverse_cardinality": element["cardinality"],
                       "provenance": [element["provenance"][0]]}
            if element.get("ordered"):
                created["ordered"] = True
            target.elements[inverse] = created


def _compose_type(type_name: str, owns: Mapping, lins: Mapping, rank: Mapping) -> dict:
    """The composed entity of ``type_name``: the fold of its linearization, last node first.  Every
    element's provenance lists its contributors in load order (``rank``: module -> position), whatever
    the fold order (format spec, "Composition and the canonical form")."""
    elements: dict = {}
    constraints: dict = {}
    key, key_from = None, None
    lin = lins[type_name]
    own = owns[type_name]
    kind = own.entity.get("kind", "entity")
    for node in reversed(lin):
        node_own = owns[node]
        node_kind = node_own.entity.get("kind", "entity")
        if node_kind != kind:
            raise _invalid(f"type {type_name!r}: its kind is {kind!r} and its supertype {node!r} is a {node_kind!r}")
        if "key" in node_own.entity:
            if key is not None and node_own.entity["key"] != key:
                raise _invalid(f"type {node!r}, field 'key': a subtype inherits the key {key!r} of {key_from!r} and "
                               f"may not redefine it (R282)")
            key, key_from = node_own.entity["key"], node
        for target, source in ((elements, node_own.elements), (constraints, node_own.constraints)):
            for name, item in source.items():
                found = target.get(name)
                if found is None:
                    added = copy.deepcopy(item)
                    if node != type_name:
                        added["inherited_from"] = node
                    target[name] = added
                else:
                    # ``found`` came from a node after this one: a subtype redefines an inherited element.
                    _merge(found, item, module=item["provenance"][-1], type_name=node, subtype=True,
                           contributors=item["provenance"])
    for item in list(elements.values()) + list(constraints.values()):
        item["provenance"].sort(key=lambda module: (rank[module], module))
    entity = {"type_name": type_name}
    if own.entity.get("supertypes"):
        entity["supertypes"] = list(own.entity["supertypes"])
    for field in ("meaning", "label", "help", "synonyms"):
        if own.entity.get(field):
            entity[field] = copy.deepcopy(own.entity[field])
    if key:
        entity["key"] = list(key)
    attributes, roles, intents = [], [], []
    for element in elements.values():
        kind_of = _element_type(element)
        if kind_of == "intent":
            intents.append(_clean(element))
        elif kind_of == "role" and element.get("participant"):
            roles.append(_clean(element))
        else:
            attributes.append(_clean(element))
    if attributes:
        entity["attributes"] = attributes
    if kind == "relationship":
        entity["roles"] = roles
    if intents:
        entity["intents"] = intents
    if constraints:
        entity["constraints"] = [_clean(e) for e in constraints.values()]
    if own.entity.get("certification"):
        entity["certification"] = own.entity["certification"]
    entity["provenance"] = list(own.provenance)
    return entity


def _clean(item: dict) -> dict:
    """``item`` without the fields at their default."""
    out = {}
    for field, value in item.items():
        if field in _DEFAULTS and type(value) is type(_DEFAULTS[field]) and value == _DEFAULTS[field]:
            continue
        if field != "default" and isinstance(value, (list, dict, str)) and not value:
            continue
        out[field] = value
    return out


def _check_agreement(entities: Mapping, owns: Mapping) -> None:
    """Every declared end with an inverse agrees with the end its target holds (see "Inverses")."""
    for own in owns.values():
        for element in own.elements.values():
            inverse = element.get("inverse")
            if _element_type(element) != "role" or inverse is None:
                continue
            where = f"type {own.type_name!r}, role {element['name']!r}"
            target = entities[element["target"]]
            other = next((e for e in target.get("attributes", []) + target.get("roles", []) if e["name"] == inverse),
                         None)
            if other is None or _element_type(other) != "role":
                raise _invalid(f"{where}: its inverse {inverse!r} on {element['target']!r} is not a role")
            pairs = (("target", own.type_name, other.get("target")), ("inverse", element["name"], other.get("inverse")),
                     ("cardinality", element.get("inverse_cardinality"), other.get("cardinality")),
                     ("inverse_cardinality", element.get("cardinality"), other.get("inverse_cardinality")),
                     ("ordered", element.get("ordered", False), other.get("ordered", False)))
            for field, expected, found in pairs:
                if expected is not None and found is not None and expected != found:
                    raise _invalid(f"{where}: the inverse end {element['target']!r}.{inverse} does not agree on "
                                   f"{field} ({found!r}, where this end implies {expected!r})")


def _check_aggregates(entities: Mapping) -> None:
    """Maintained aggregates read only neutral declarations of their members (spec section 10)."""
    for type_name, entity in entities.items():
        elements = {e["name"]: e for e in entity.get("attributes", []) + entity.get("roles", [])}
        for item in entity.get("attributes", []):
            if item.get("kind") != "computed_stored":
                continue
            tree = item["expression"]["ast"]
            where = f"type {type_name!r}, maintained aggregate {item['name']!r}"
            if tree[0] not in ("sum", "count") or not isinstance(tree[1], str):
                raise _invalid(f"{where}: a maintained aggregate is a sum or a count over one of its roles")
            over = elements.get(tree[1])
            if over is None or _element_type(over) != "role":
                raise _invalid(f"{where}: {tree[1]!r} is not a role of {type_name!r}")
            found = _python_only_read(entities, type_name, tree, set())
            if found is not None:
                raise _invalid(f"{where}: its terms are neutral all the way down (R276), and it reads, through "
                               f"{' -> '.join(found[:-1])}, {found[-1]}, a Python-only computed attribute")


def _declared(entity: Mapping, name: str):
    return next((e for e in entity.get("attributes", []) + entity.get("roles", []) if e["name"] == name), None)


def _python_only_read(entities: Mapping, type_name: str, tree, seen: set):
    """The path of reads from the tree ``tree`` of an element of ``type_name`` to a Python-only computed
    attribute, through computed attributes and aggregates of any depth and across roles, or ``None``
    (spec section 4: a maintained aggregate reads neutral terms all the way down).  Each element is
    followed once."""
    stack = [(tree, type_name)]
    while stack:
        node, owner = stack.pop()
        op = node[0]
        if op == "attr":
            found = _python_only_path(entities, owner, list(node[1:]), seen)
            if found is not None:
                return found
        elif op in ("sum", "count"):
            over = node[1]
            if isinstance(over, str):
                role = _declared(entities.get(owner, {}), over)
                member = role.get("target") if role is not None else None
                if member is not None:
                    stack.extend((operand, member) for operand in node[2:])
            # A via from a root or a cell reads stored values only.
        elif op not in ("const", "root", "via", "cell", "self"):
            stack.extend((child, owner) for child in node[1:] if isinstance(child, list))
    return None


def _python_only_path(entities: Mapping, owner: str, path: list, seen: set):
    for step in path:
        element = _declared(entities.get(owner, {}), step)
        if element is None:
            return None
        here = f"{owner!r}.{step}"
        if element.get("kind") in ("computed", "computed_stored"):
            if "expression" not in element:
                return [here]
            if (owner, step) in seen:
                return None
            seen.add((owner, step))
            below = _python_only_read(entities, owner, element["expression"]["ast"], seen)
            return None if below is None else [here, *below]
        if "target" not in element:
            return None
        owner = element["target"]           # a role: the next step reads its target
    return None


def compose_modules(modules: Mapping) -> dict:
    """The catalog model of the Module items ``modules`` (module name -> item): the module set, the
    items, and the entities composed in the load order of their ``depends``."""
    names = tuple(sorted(modules))
    for name in names:
        if modules[name].get("name") != name:
            raise _invalid(f"the Module item under {name!r} is named {modules[name].get('name')!r}")
        for dependency in modules[name].get("depends", ()):
            if dependency not in modules:
                raise _invalid(f"module {name!r} depends on {dependency!r}, which is not in the module set")
    order = _load_order(names, {name: tuple(modules[name].get("depends", ())) for name in names})
    owns = _own_folds(modules, order)
    cache: dict = {}
    lins = {name: _c3(name, owns, cache) for name in owns}
    _create_inverses(owns, lins)
    rank = {module: position for position, module in enumerate(order)}
    entities = {name: _compose_type(name, owns, lins, rank) for name in sorted(owns, key=lambda n: owns[n].first)}
    _check_agreement(entities, owns)
    _check_aggregates(entities)
    model = {"module_set": list(names), "modules": {name: copy.deepcopy(dict(modules[name])) for name in names},
             "entities": entities}
    check_model(model)
    return model


# -- reading a composed model ------------------------------------------------------------------------

def _entity(model: Mapping, type_name: str) -> dict:
    found = model["entities"].get(type_name)
    if found is None:
        raise ProtoValidationError(f"The semantic catalog describes no type {type_name!r}; it describes "
                                   f"{sorted(model['entities'])}")
    return found


def _neutral(value):
    if isinstance(value, Mapping):
        return {k: _neutral(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_neutral(v) for v in value]
    return value


def describe(model: Mapping, type_name: str) -> dict:
    """The composed entity ``type_name`` of ``model`` as neutral data (dicts, lists, strings and canonical
    values): its fields, attributes, roles, intents, constraints, provenance and certification, plus
    ``kind`` (``entity`` or ``relationship``)."""
    entity = _neutral(_entity(model, type_name))
    entity["kind"] = "relationship" if "roles" in entity else "entity"
    return entity


def linearization(model: Mapping, type_name: str) -> list:
    """``type_name`` and its supertypes in C3 order: the order resolution walks their chains, and the
    reverse of the order composition folds them."""
    _entity(model, type_name)
    owns = {}
    for name, entity in model["entities"].items():
        own = owns[name] = _Own(name, 0)
        if entity.get("supertypes"):
            own.entity["supertypes"] = list(entity["supertypes"])
    return list(_c3(type_name, owns, {}))


def subtypes(model: Mapping, type_name: str) -> frozenset:
    """``type_name`` and every type whose supertypes reach it (the reflexive, transitive closure)."""
    _entity(model, type_name)
    children: dict = {}
    for name, entity in model["entities"].items():
        for supertype in entity.get("supertypes", ()):
            children.setdefault(supertype, []).append(name)
    seen, stack = set(), [type_name]
    while stack:
        name = stack.pop()
        if name not in seen:
            seen.add(name)
            stack.extend(children.get(name, ()))
    return frozenset(seen)


def is_a(model: Mapping, type_name: str, supertype: str) -> bool:
    """Whether ``type_name`` is ``supertype`` or one of its subtypes (substitutability, R280)."""
    return type_name in subtypes(model, supertype)


def index_paths(model: Mapping, type_name: str, role: str) -> list:
    """The indexes of the role ``role`` of ``type_name`` (R262): for each ``index_by`` path,
    ``{"path", "index", "maintained_key"}``.  The index is ``by_<path>`` (dots as ``_``); a path whose
    first step is an identity role of the member is a **maintained key** (the end collection holds
    ``{member, key}`` entries indexed on ``key``), any other path an index of the member's own
    attributes."""
    entity = _entity(model, type_name)
    found = next((e for e in entity.get("attributes", []) + entity.get("roles", []) if e["name"] == role), None)
    if found is None or _element_type(found) != "role":
        raise ProtoValidationError(f"{type_name!r} has no role {role!r}")
    own = {e["name"]: e for e in entity.get("attributes", []) + entity.get("roles", [])}
    out = []
    for path in found.get("index_by", ()):
        first = own.get(path.split(".")[0])
        through_role = (first is not None and _element_type(first) == "role" and "." in path
                        and first.get("binding", "identity") == "identity")
        out.append({"path": path, "index": "by_" + path.replace(".", "_"), "maintained_key": through_role})
    return out
