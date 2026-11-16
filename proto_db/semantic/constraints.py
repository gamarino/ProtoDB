"""Constraints of the semantic catalog: cardinality and enforced invariants, with errors that name the rule
(spec section 10; rulings R272, R273, R279; Task 9; preflight F27, F28, F29).

What is checked, and when
    - **Cardinality** is checked at commit for every entity the commit touched (F28): a role of cardinality
      ``one`` holds exactly one target (an end absent, or nil after an unlink, violates it), and a ``1..*``
      end holds at least one member.  ``0..1`` and ``many`` hold by construction.  In direct mode the
      transaction's commit checks the cells its semantic operations wrote, before it leaves the active state
      and outside the root lock, reading them through the transaction (``Transaction.commit``); in intent
      mode the replay checks the cells it wrote, on the combined result at the serialized point.
    - **Enforced invariants** are checked on every entity whose operands changed: in direct mode when the
      operation (a primitive or an intent, the outermost one) is applied, on the transaction's view, every
      value the check reads entering the read set; in intent mode after the replay, on the combined result.
      An invariant declared ``enforced=False`` is published for agents only and never checked.
    - An intent's ``requires`` is checked on its target where the intent is applied (spec section 9).

    A violation raises :class:`~proto_db.errors.ProtoConstraintError`, a ``ProtoValidationError`` that
    ``Database.run`` never retries (R273), and nothing of the operation or the commit it refuses is
    published; in a group-commit round only that commit fails.  Raw writes (``set_mutable`` and its peers)
    keep today's rules: the semantic layer checks what it writes.

Which invariants this revision enforces
    An enforced invariant reads its entity's own attributes only: fields, maintained aggregates, roles (their
    stored value), the ``count`` of a role's members without ``where``, and computed attributes whose
    expressions read the same.  Its operands then change only when its entity's record changes, so checking
    the entities a change touched is complete.  An enforced invariant that reads another entity (a path
    through a role, a ``via``, a ``sum`` or a filtered ``count`` over the members, a root or a cell) or a
    Python-only computed attribute would not be re-checked when that other entity changes.  The refusal is
    the writer's: composing the modules a writer declares or installs refuses it (``ProtoNotSupportedError``,
    :func:`check_enforceable`), and ``enforced=False`` publishes it.  A stored catalog that holds one (written
    by another writer) still opens: the invariant is published but never checked, and ``describe`` says so
    (its ``not_enforced`` names the reason, :func:`unenforced_reason`).

Evaluation and the error
    The invariant's tree is evaluated on the entity's record (spec section 7: the stored value, then the
    catalogued default, else MISSING).  True holds; false and MISSING violate it (an absent operand never
    raises a ``TypeError``); any other result is ``ProtoValidationError`` (the tree is not a predicate).  The
    error carries the constraint's ``name``, its ``meaning``, the cell name of the entity, the ``values`` of
    the attribute paths the tree reads (``MISSING`` for an absent one, never ``None``), and the rendered
    ``message`` (the catalogued one, else a generic one naming the rule).

Message placeholders (R279)
    ``{name}`` renders the entity's attribute ``name``.  ``{entity}``, and the lower-case last component of the
    entity's type name or of any of its supertypes (``{customer}``, ``{order}``), render the entity's
    **key**: its ``key`` attributes' values joined by ``", "``, a key part that is a role rendering its
    target's key, and an entity whose type declares no key its cell name (F27).  For an invariant whose tree
    is an ordering comparison (``<``, ``<=``, ``>``, ``>=``), ``{left}`` and ``{right}`` render its operands and
    ``{excess}`` the left one minus the right one.  These reserved names win over an attribute of the same
    name.  A MISSING operand renders as ``<missing name>``, ``name`` being the attribute path it reads.  A
    decimal renders in plain notation, ``True`` and ``False`` as ``true`` and ``false``, nil as ``nil``, a date
    or a time in ISO 8601.  Any other placeholder is left as written.
"""
from __future__ import annotations

import datetime
import string
from collections.abc import Mapping
from decimal import Decimal

from ..collections.lists import List
from ..collections.sets import Set
from ..errors import ProtoConstraintError, ProtoDBError, ProtoNotSupportedError, ProtoValidationError
from ..indexing.keyspec import _step as _stored_step
from ..values import MISSING, MutableObject, exact_token, is_atom
from .expressions import _arithmetic, dependencies, describe_operand, evaluate

__all__ = ["check_constraints", "check_commit", "check_enforceable", "unenforced_reason", "requires_error"]

#: The ordering comparisons, whose operands render as ``{left}``, ``{right}`` and ``{excess}``.
_ORDERING = frozenset({"<", "<=", ">", ">="})
#: How deep a key part that is a role is followed to its target's key.
_KEY_DEPTH = 4
#: The operators an enforced invariant combines its operands with.
_COMBINING = frozenset({"+", "-", "*", "==", "!=", "<", "<=", ">", ">=", "and", "or", "not"})


# -- checking -------------------------------------------------------------------------------------------------

def check_constraints(model: Mapping, touched: Mapping, reader, *, before: Mapping | None = None,
                      cardinality: bool = True, invariants: bool = True) -> None:
    """Check the entities ``touched`` (identity handle -> its record in the result) against the catalog
    ``model``: the cardinality of every role of each (``cardinality``), and the enforced invariants whose
    operands changed (``invariants``).  ``before`` maps a cell name to its record before the change (``None``
    or no entry: created, every operand changed); without ``before`` every operand counts as changed.
    ``reader`` reads cells and defaults (:class:`~proto_db.semantic.expressions.Reader`), with the running
    resolution reader set by the caller.  A violation raises ``ProtoConstraintError``; anything that is not a
    record of an entity type of ``model`` is skipped."""
    entities = model["entities"]
    for handle, record in touched.items():
        type_name = getattr(type(record), "__type_name__", None)
        entity = entities.get(type_name) if isinstance(type_name, str) else None
        if entity is None or "roles" in entity or not is_atom(record):
            continue
        if cardinality:
            _check_cardinality(model, type_name, entity, handle, record, reader)
        if invariants:
            old = None if before is None else before.get(handle.name)
            _check_invariants(model, type_name, entity, handle, record, old, before is None, reader)


def check_commit(tr) -> None:
    """Direct mode, called by ``Transaction.commit`` under the transaction's lock while it is still active:
    the cardinality of every entity the transaction's semantic operations wrote, read through the
    transaction (the reads enter the read set).  Invariants were checked by each operation."""
    from .session import SemanticSession
    names = tr.semantic_touched()
    touched = {}
    for name in names:
        record = tr.get_mutable(name)
        if record is not None:
            touched[MutableObject(name)] = record
    if touched:
        SemanticSession(tr)._operations().check_constraints(touched, None, invariants=False)


def _check_cardinality(model, type_name: str, entity: Mapping, handle, record, reader) -> None:
    for element in entity.get("attributes", ()):
        if "target" not in element:
            continue
        cardinality, name = element["cardinality"], element["name"]
        value = _stored_step(record, name)
        if cardinality == "one":
            holds, what = value is not MISSING and value is not None, "exactly one"
        elif cardinality == "1..*":
            holds = isinstance(value, (List, Set)) and value.count > 0
            what = "at least one"
        else:
            continue
        if holds:
            continue
        key = _key_text(model, record, handle.name, reader, 0)
        raise ProtoConstraintError(
            f"The {_short(type_name)} {key} has no {name} (cardinality {cardinality})",
            constraint=f"{type_name}.{name}.cardinality", meaning=f"Every {type_name} has {what} {name}",
            entity=handle.name, values={name: MISSING})


def _check_invariants(model, type_name: str, entity: Mapping, handle, record, old, all_changed: bool,
                      reader) -> None:
    for constraint in entity.get("constraints", ()):
        if constraint.get("enforced", True) is False or unenforced_reason(entity, constraint) is not None:
            continue
        tree = constraint["expression"]["ast"]
        if not all_changed and old is not None and not _changed(_operands(entity, tree), old, record):
            continue
        result = evaluate(tree, record, reader=reader)
        if result is True:
            continue
        if result is not False and result is not MISSING:
            raise ProtoValidationError(f"The invariant {constraint['name']!r} of {type_name!r} gives "
                                       f"{describe_operand(result)}, not a boolean")
        raise _violation(model, type_name, constraint, handle.name, record, reader)


def requires_error(model: Mapping, type_name: str, intent: Mapping, cell: str, record,
                   reader) -> ProtoConstraintError:
    """The error of the intent ``intent``'s ``requires`` found false (or MISSING) on ``record``, the current
    version of the entity in ``cell`` (spec section 9; F26): named ``<type>.<intent>.requires``, with the
    intent's meaning and the values of the paths the requires reads."""
    constraint = f"{type_name}.{intent['name']}.requires"
    meaning = intent.get("meaning")
    values = _values(intent["requires"]["ast"], record, reader)
    return ProtoConstraintError(
        f"The intent {intent['name']!r} does not apply to {cell!r}: {constraint} is false on its current version"
        + (f" ({meaning})" if meaning else ""),
        constraint=constraint, meaning=meaning, entity=cell, values=values)


# -- what an invariant reads ----------------------------------------------------------------------------------

def _declared(entity: Mapping, name: str):
    return next((e for e in list(entity.get("attributes", ())) + list(entity.get("roles", ())) if e["name"] == name),
                None)


def _operands(entity: Mapping, tree) -> set:
    """The stored attribute names of ``entity`` that ``tree`` reads: its
    :func:`~proto_db.semantic.expressions.dependencies`, a computed attribute among them replaced by the
    dependencies of its expression, for a tree :func:`unenforced_reason` accepts."""
    names, seen, pending = set(), set(), list(dependencies(tree))
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        element = _declared(entity, name)
        if element is not None and element.get("kind") == "computed" and "expression" in element:
            pending.extend(dependencies(element["expression"]["ast"]))
        else:
            names.add(name)
    return names


def _same(one, other) -> bool:
    """Whether two stored values are the same, conservatively: the same object, or equal plain values (exact
    tokens); records and collections other than the same object count as changed."""
    if one is other:
        return True
    if is_atom(one) or is_atom(other) or isinstance(one, (List, Set)) or isinstance(other, (List, Set)):
        return False
    try:
        return exact_token(one, _no_atom) == exact_token(other, _no_atom)
    except (ProtoDBError, TypeError, ValueError):
        return False


def _no_atom(atom):
    raise ProtoValidationError("an atom is compared by identity")


def _changed(names, old, new) -> bool:
    return any(not _same(_stored_step(old, name), _stored_step(new, name)) for name in names)


def _unsupported(entity: Mapping, tree, seen: set):
    """What an enforced invariant's ``tree`` reads beyond its entity's own attributes (see "Which
    invariants this revision enforces"), described, or ``None``."""
    stack = [tree]
    while stack:
        node = stack.pop()
        op = node[0]
        if op == "const":
            continue
        if op == "attr":
            if len(node) > 2:
                return f"the path {'.'.join(node[1:])!r}, through a role, into another entity"
            element = _declared(entity, node[1])
            if element is not None and element.get("kind") == "computed":
                if "expression" not in element:
                    return f"the Python-only computed attribute {node[1]!r}"
                if node[1] not in seen:
                    seen.add(node[1])
                    stack.append(element["expression"]["ast"])
            continue
        if op == "count" and len(node) == 2 and isinstance(node[1], str):
            element = _declared(entity, node[1])
            if element is not None and "target" in element:
                continue
        if op in _COMBINING:
            stack.extend(node[1:])
            continue
        return f"a {op!r} over other entities or roots"
    return None


def unenforced_reason(entity: Mapping, constraint: Mapping):
    """Why the invariant ``constraint`` of ``entity``, declared enforced, is not enforced in this revision (it
    reads beyond its entity's own attributes: see "Which invariants this revision enforces"), or ``None``
    when it is enforced (or declared ``enforced=False``)."""
    if constraint.get("enforced", True) is False:
        return None
    found = _unsupported(entity, constraint["expression"]["ast"], set())
    if found is None:
        return None
    return (f"an enforced invariant reads only its entity's own attributes in this revision, and it reads "
            f"{found}, whose change would not check it again")


def check_enforceable(entities: Mapping) -> None:
    """Refuse an enforced invariant that reads beyond its entity's own attributes (see "Which invariants
    this revision enforces"): ``ProtoNotSupportedError`` naming the type, the constraint and what it
    reads.  The writer's refusal: called when a writer composes the modules it declares or installs, never
    when a stored catalog is verified for use."""
    for type_name, entity in entities.items():
        for constraint in entity.get("constraints", ()):
            reason = unenforced_reason(entity, constraint)
            if reason is not None:
                raise ProtoNotSupportedError(
                    f"type {type_name!r}, constraint {constraint['name']!r}: {reason}; declare it enforced=False "
                    f"to publish it for agents only")


# -- the error and its message --------------------------------------------------------------------------------

def _paths(tree) -> list:
    """The attribute paths ``tree`` reads from its element, in reading order, each once."""
    out, stack = [], [tree]
    while stack:
        node = stack.pop()
        if not isinstance(node, list) or not node:
            continue
        if node[0] == "attr":
            path = ".".join(node[1:])
            if path not in out:
                out.append(path)
        elif node[0] not in ("const", "root", "cell", "self", "via", "sum", "count"):
            stack.extend(reversed(node[1:]))
    return out


def _scalar(value) -> bool:
    return value is MISSING or not (isinstance(value, (MutableObject, List, Set, dict, list, tuple)) or is_atom(value))


def _values(tree, record, reader) -> dict:
    """The values of the attribute paths ``tree`` reads on ``record`` (scalars and MISSING; a role's handle or
    collection is left out)."""
    out = {}
    for path in _paths(tree):
        value = evaluate(["attr", *path.split(".")], record, reader=reader)
        if _scalar(value):
            out[path] = value
    return out


def _missing_name(tree, record, reader) -> str:
    """The name a MISSING operand ``tree`` renders with: the first attribute path it reads that is MISSING on
    ``record``, else the first path it reads (``value`` when it reads none)."""
    paths = _paths(tree)
    for path in paths:
        if evaluate(["attr", *path.split(".")], record, reader=reader) is MISSING:
            return path
    return paths[0] if paths else "value"


def _short(type_name: str) -> str:
    return type_name.rsplit(".", 1)[-1].lower()


def _short_names(model: Mapping, type_name: str) -> list:
    """The lower-case short names of ``type_name`` and of its supertypes, nearest first."""
    out, queue = [], [type_name]
    while queue:
        name = queue.pop(0)
        short = _short(name)
        if short not in out:
            out.append(short)
        queue.extend(model["entities"].get(name, {}).get("supertypes", ()))
    return out


def _key_text(model: Mapping, record, cell: str | None, reader, depth: int) -> str:
    """The entity's key, rendered (see "Message placeholders")."""
    type_name = getattr(type(record), "__type_name__", None)
    entity = model["entities"].get(type_name, {}) if isinstance(type_name, str) else {}
    key = entity.get("key")
    if not key or depth >= _KEY_DEPTH:
        return cell if cell is not None else str(type_name)
    return ", ".join(_text(model, evaluate(["attr", name], record, reader=reader), name, reader, depth + 1)
                     for name in key)


def _text(model: Mapping, value, name: str, reader, depth: int = 0) -> str:
    """A value as a message renders it (see "Message placeholders")."""
    if value is MISSING:
        return f"<missing {name}>"
    if value is None:
        return "nil"
    if value is True or value is False:
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value, "f") if value.is_finite() else str(value)
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, MutableObject) or (isinstance(value, dict) and value.keys() == {"entity", "version"}):
        cell = value.name if isinstance(value, MutableObject) else getattr(value["entity"], "name", None)
        try:
            target = reader.follow(value)
        except ProtoDBError:            # the message only: a target that cannot be read is named by its cell
            target = MISSING
        if target is MISSING:
            return cell if cell is not None else f"<missing {name}>"
        return _key_text(model, target, cell, reader, depth)
    if is_atom(value):
        return _key_text(model, value, None, reader, depth)
    if isinstance(value, (List, Set, list, tuple, dict)):
        return describe_operand(value)
    return str(value)


def _violation(model: Mapping, type_name: str, constraint: Mapping, cell: str, record,
               reader) -> ProtoConstraintError:
    tree = constraint["expression"]["ast"]
    values = _values(tree, record, reader)
    text = constraint.get("message") or (f"The invariant {constraint['name']} of {type_name} does not hold on "
                                         f"{{entity}}")
    rendered: dict = {}
    key = [None]

    def entity_key() -> str:
        if key[0] is None:
            key[0] = _key_text(model, record, cell, reader, 0)
        return key[0]
    for short in _short_names(model, type_name):
        rendered[short] = entity_key
    rendered["entity"] = entity_key
    if tree[0] in _ORDERING:
        left_tree, right_tree = tree[1], tree[2]
        left = evaluate(left_tree, record, reader=reader)
        right = evaluate(right_tree, record, reader=reader)
        rendered["left"] = lambda: _text(model, left, _missing_name(left_tree, record, reader), reader)
        rendered["right"] = lambda: _text(model, right, _missing_name(right_tree, record, reader), reader)

        def excess() -> str:
            if left is MISSING:
                return f"<missing {_missing_name(left_tree, record, reader)}>"
            if right is MISSING:
                return f"<missing {_missing_name(right_tree, record, reader)}>"
            try:
                return _text(model, _arithmetic("-", left, right), "excess", reader)
            except ProtoValidationError:
                return "<undefined excess>"
        rendered["excess"] = excess
    entity = model["entities"][type_name]
    names = {e["name"] for e in list(entity.get("attributes", ())) + list(entity.get("roles", ()))}

    def lookup(field: str):
        if field in rendered:
            return rendered[field]()
        if field in names:
            return _text(model, evaluate(["attr", field], record, reader=reader), field, reader)
        return None
    return ProtoConstraintError(_render(text, lookup), constraint=constraint["name"],
                                meaning=constraint.get("meaning"), entity=cell, values=values)


def _render(text: str, lookup) -> str:
    """``text`` with each ``{placeholder}`` that ``lookup`` knows rendered; any other is left as written, and
    a message that is not a valid format string is left whole."""
    try:
        parts = list(string.Formatter().parse(text))
    except ValueError:
        return text
    out = []
    for literal, field, spec, conversion in parts:
        out.append(literal)
        if field is None:
            continue
        found = lookup(field)
        if found is None:
            written = field + (f"!{conversion}" if conversion else "") + (f":{spec}" if spec else "")
            out.append("{" + written + "}")
        else:
            out.append(found)
    return "".join(out)
