"""The expression language of the semantic catalog, MVP subset (spec section 7; format spec, "The
expression language").

An expression is a nested list ``[operator, argument...]``: the neutral tree stored in an Expression
record's ``ast`` and evaluated the same way in every language.  This module gives:

* :func:`check_ast`, the static check of a tree (shape and arity of every node; ``root`` and
  ``cell`` origins only in queries);
* :func:`evaluate`, the normative evaluation on one element, which reads cells, roots and catalogued
  defaults through a :class:`Reader`;
* :func:`to_planner`, the translation of a simple predicate into the planner's
  :class:`~proto_db.query.terms.Term` / ``And`` / ``Or`` / ``Not`` so that indexes answer it;
* :func:`dependencies`, the attribute and role names an expression reads from its own element;
* :func:`parse`, the Python text form (R277), parsed by a hand-written tokenizer and
  recursive-descent parser whose grammar is the subset's, not Python's.

Comparisons call :func:`proto_db.query.terms.compare`, the code :meth:`Term.matches` runs, so an
indexed answer and a scanned one cannot diverge (D28, preflight F14).  An absent value is the
stored own attribute, then the catalogued default, else ``MISSING``; never a language's ``None``
(preflight F15).  A predicate on a field with a catalogued default is never given to the planner,
because an index sees ``MISSING`` where evaluation sees the default (preflight F16).

An evaluation error names an operand by its type only, never by its value (:func:`describe_operand`):
a scalar may be a personal value, and a record, a collection or a handle may hold one.  Such an error
also carries that description as its ``semantic_operand``, so a report that withholds the message (the
agent interface) can still name the operand's type.
"""
from __future__ import annotations

import contextlib
import contextvars
import keyword
from collections.abc import Collection
from decimal import Context, Decimal, DecimalException, Inexact, InvalidOperation, MAX_EMAX, MAX_PREC, MIN_EMIN, \
    Overflow, Rounded

from ..collections.base import IndexedCollection
from ..collections.lists import List
from ..collections.sets import Set
from ..core.dbobject import DBObject
from ..errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from ..indexing.keyspec import _step as _stored_step
from ..query.expressions import And, Expression, Not, Or
from ..query.terms import MIRRORED, Term, compare
from ..values import MISSING, MutableObject, canonical, is_atom

__all__ = ["Reader", "checking", "checking_reached", "reached", "check_ast", "evaluate", "to_planner", "dependencies", "parse", "unwrap_entry", "describe_operand",
           "ARITHMETIC", "COMPARISONS", "RESERVED", "MAX_EXPRESSION_DEPTH"]

#: The arithmetic operators (R264); division is not specified.
ARITHMETIC = frozenset({"+", "-", "*"})
#: The comparison operators (D28 semantics).
COMPARISONS = frozenset({"==", "!=", "<", "<=", ">", ">="})
#: Operators the format reserves and this revision does not specify: a reader that meets one raises
#: ``ProtoNotSupportedError``.
RESERVED = frozenset({"in", "is_missing", "min", "max", "exists", "all", "reach", "valid_at"})
#: The deepest nesting of operator lists an expression may have (the root node is at depth 1).
MAX_EXPRESSION_DEPTH = 128

#: The origins of a ``via``; ``root`` and ``cell`` are valid in queries only (R265).
_QUERY_ORIGINS = frozenset({"root", "cell"})

#: Decimal arithmetic without rounding: ``+``, ``-`` and ``*`` of finite decimals are exact at this
#: precision, and any signal that would lose a digit is trapped.
_EXACT = Context(prec=MAX_PREC, Emax=MAX_EMAX, Emin=MIN_EMIN, traps=[InvalidOperation, Overflow, Inexact, Rounded])
_DECIMAL_OPS = {"+": _EXACT.add, "-": _EXACT.subtract, "*": _EXACT.multiply}


# -- reading what the element does not hold ---------------------------------------------------------

class Reader:
    """What evaluation reads beyond the element itself: cells, roots and catalogued defaults.

    The base class has no database: :meth:`cell` and :meth:`root` raise ``ProtoUserError``, so an
    expression that reaches an identity handle without a reader fails loudly, and :meth:`default`
    knows no catalog.  The semantic layer subclasses it over a snapshot or a transaction.
    """

    def cell(self, name: str):
        """The value of the mutable cell ``name`` (a record), or ``None`` when there is no such cell."""
        raise ProtoUserError(f"Evaluating this expression reaches the identity handle {name!r}: pass a reader "
                             f"that reads cells")

    def root(self, name: str):
        """The value of the root ``name``, or ``None`` when there is no such root."""
        raise ProtoUserError(f"Evaluating this expression reads the root {name!r}: pass a reader that reads roots")

    def default(self, element, name: str):
        """The catalogued ``default`` of the field ``name`` of ``element``'s type, or ``MISSING``."""
        return MISSING

    def absent(self, element, name: str):
        """What a path step ``name`` that is not the last reads when the record ``element`` does not hold it:
        ``MISSING`` (the path is missing).  A reader that refuses absent attributes raises instead."""
        return MISSING

    def follow(self, value):
        """The element a role's stored value designates: an identity handle is dereferenced (``MISSING``
        when its cell is absent), a version-bound reference ``{"entity": handle, "version": record}``
        (spec section 6, R260) gives the version, and anything else is the element itself."""
        if isinstance(value, MutableObject):
            found = self.cell(value.name)
            return MISSING if found is None else found
        if isinstance(value, dict) and value.keys() == {"entity", "version"} \
                and isinstance(value["entity"], MutableObject):
            return value["version"]
        return value

    def members(self, value):
        """The members a role's stored value holds, not yet followed: the elements of a ``many`` end
        (a List or a Set, in its iteration order), the single target of a ``one`` end, none for an absent
        or nil value.  Each member is a record, an identity handle or a version-bound reference; anything
        else is refused when it is reached (:func:`_identity`)."""
        if value is MISSING or value is None:
            return ()
        if isinstance(value, (List, Set)):
            return value.elements()
        return (value,)


_NO_READER = Reader()

#: The check of every member a traversal reaches, while :func:`checking_reached` holds it (else ``None``).
_REACHED: contextvars.ContextVar = contextvars.ContextVar("proto_db_semantic_reached", default=None)


@contextlib.contextmanager
def checking_reached(check):
    """While the block runs in this context, call ``check(holder, step, member)`` for every member a path or a
    traversal reaches, before anything of the member is read: ``holder`` is the record whose attribute
    ``step`` held it (``None`` for a ``cell`` origin, ``step`` then being the cell's name) and ``member`` the
    record reached (a handle followed, a version-bound reference's version, or a record held inline).  A
    check that raises stops the evaluation.  The agent interface uses it to verify that a member has the type
    its role or its cell declares (:mod:`proto_db.agent.operations`, "Sensitivity")."""
    token = _REACHED.set(check)
    try:
        yield
    finally:
        _REACHED.reset(token)


def checking() -> bool:
    """Whether a check of reached members is held (:func:`checking_reached`) in this context."""
    return _REACHED.get() is not None


def reached(holder, step: str, member) -> None:
    """Report ``member``, reached through ``step`` of ``holder``, to the check :func:`checking_reached` holds."""
    check = _REACHED.get()
    if check is not None and member is not MISSING:
        check(holder, step, member)


# -- the static check -------------------------------------------------------------------------------

def _is_node(node) -> bool:
    return isinstance(node, list) and bool(node) and isinstance(node[0], str)


def _name(value, what: str, op: str) -> str:
    if not isinstance(value, str) or not value:
        raise ProtoValidationError(f"Operator {op!r} needs {what} as a non-empty str, not {value!r}")
    return value


def _arity(op: str, args: list, low: int, high: int | None) -> None:
    if len(args) < low or (high is not None and len(args) > high):
        wanted = (f"{low}" if high == low else f"{low} or {high}" if high == low + 1 else
                  f"at least {low}" if high is None else f"{low} to {high}")
        raise ProtoValidationError(f"Operator {op!r} takes {wanted} operand(s), not {len(args)}")


def _check_origin(origin, in_query: bool) -> None:
    if not _is_node(origin):
        raise ProtoValidationError(f"A 'via' origin is ['self'], ['root', name] or ['cell', name], not {origin!r}")
    op = origin[0]
    if op == "self":
        _arity(op, origin[1:], 0, 0)
    elif op in _QUERY_ORIGINS:
        _check_query_origin(origin, in_query)
    else:
        raise ProtoValidationError(f"A 'via' origin is ['self'], ['root', name] or ['cell', name], not {op!r}")


def _check_query_origin(node, in_query: bool) -> None:
    op = node[0]
    _arity(op, node[1:], 1, 1)
    _name(node[1], "a name", op)
    if not in_query:
        raise ProtoValidationError(f"The origin {op!r} is valid in queries only (R265)")


def _too_deep() -> ProtoValidationError:
    return ProtoValidationError(f"The expression nests deeper than MAX_EXPRESSION_DEPTH ({MAX_EXPRESSION_DEPTH}) "
                                f"operator lists")


def _children(node, in_query: bool) -> list:
    """Check one node's operator and arity (not its operands' own shape) and return the operand
    expressions to check next."""
    if not _is_node(node):
        raise ProtoValidationError(f"An expression is a non-empty list [operator, argument...] whose operator is "
                                   f"a str, not {node!r}")
    op, args = node[0], node[1:]
    if op == "const":
        _arity(op, args, 1, 1)
        canonical(args[0])
        return []
    if op == "attr":
        _arity(op, args, 1, None)
        for step in args:
            _name(step, "every path step", op)
        return []
    if op in ARITHMETIC or op in COMPARISONS:
        _arity(op, args, 2, 2)
        return args
    if op in ("and", "or"):
        _arity(op, args, 1, None)
        return args
    if op == "not":
        _arity(op, args, 1, 1)
        return args
    if op in ("sum", "count"):
        if op == "sum":
            _arity(op, args, 2, 3)
        else:
            _arity(op, args, 1, 2)
        over = args[0]
        if isinstance(over, str):
            _name(over, "a role name, a via expression or a root as 'over'", op)
            return list(args[1:])
        if _is_node(over) and over[0] in ("via", "root"):
            return list(args)
        raise ProtoValidationError(f"Operator {op!r} needs a role name, a via expression or a root as 'over', "
                                   f"not {over!r}")
    if op == "via":
        _arity(op, args, 2, None)
        _check_origin(args[0], in_query)
        for role in args[1:]:
            _name(role, "every role", op)
        return []
    if op == "root":
        _check_query_origin(node, in_query)
        return []
    if op in ("self", "cell"):
        raise ProtoValidationError(f"{op!r} is valid only as the origin of a 'via'")
    if op in RESERVED:
        raise ProtoNotSupportedError(f"Operator {op!r} is reserved by the format and not specified by this revision")
    raise ProtoValidationError(f"Operator {op!r} is not in the expression language (spec section 7)")


def check_ast(ast, *, in_query: bool = False) -> None:
    """Check that ``ast`` is a tree of the expression language (spec section 7).

    Every node is checked for its operator and arity; a ``const`` must hold a canonical value, names
    must be non-empty strings, and no node is deeper than :data:`MAX_EXPRESSION_DEPTH` (the root node
    is at depth 1, an operand one deeper than its operator; a ``via`` origin is part of its ``via``).
    ``root`` (an origin, or a bare root) and ``cell`` (an origin) are refused unless ``in_query``.
    ``ProtoValidationError`` names the offending operator; a reserved operator (``in``, ``min``,
    ...) raises ``ProtoNotSupportedError``.  Kinds of values (a float in arithmetic, a non-boolean
    in logic) are checked by :func:`evaluate`, where the values are known.
    """
    stack = [(ast, 1)]
    while stack:
        node, depth = stack.pop()
        if depth > MAX_EXPRESSION_DEPTH:
            raise _too_deep()
        stack.extend((child, depth + 1) for child in _children(node, in_query))


# -- evaluation -------------------------------------------------------------------------------------

def evaluate(ast, element, *, reader: Reader | None = None):
    """Evaluate ``ast`` with ``element`` as the current element.

    Returns a canonical value, ``MISSING``, or a tuple of members (for ``via``).  ``reader`` reads
    cells (dereferencing identity handles), roots and catalogued defaults; without one, reaching a
    handle or a root raises ``ProtoUserError`` and no field has a default.  An invalid tree raises
    as :func:`check_ast` does, at the node evaluation reaches: nodes it does not reach (after an
    ``or`` that is already true) are not checked, so a reader checks a whole tree with
    :func:`check_ast` before evaluating it (as the catalog verification and ``check_query`` do).
    ``root`` and ``cell`` origins are evaluated (whether a tree may use them is :func:`check_ast`'s
    decision).
    """
    reader = _NO_READER if reader is None else reader
    return _eval(ast, reader.follow(element), reader, 1)


def _eval(node, element, reader: Reader, depth: int):
    """Evaluate ``node`` at ``depth``.  The recursion takes at most two Python frames per level (this
    function, and the generator of ``and``/``or``), so a tree within :data:`MAX_EXPRESSION_DEPTH`
    needs at most ``2 * MAX_EXPRESSION_DEPTH`` frames, well within Python's default limit of 1000."""
    if depth > MAX_EXPRESSION_DEPTH:
        raise _too_deep()
    operands = _children(node, True)
    op, below = node[0], depth + 1
    if op == "const":
        return canonical(node[1])
    if op == "attr":
        return _read_path(element, node[1:], reader)
    if op in ARITHMETIC:
        return _arithmetic(op, _eval(operands[0], element, reader, below), _eval(operands[1], element, reader, below))
    if op in COMPARISONS:
        return compare(_eval(operands[0], element, reader, below), op, _eval(operands[1], element, reader, below))
    if op == "and":
        return all(_truth(op, _eval(operand, element, reader, below)) for operand in operands)
    if op == "or":
        return any(_truth(op, _eval(operand, element, reader, below)) for operand in operands)
    if op == "not":
        return not _truth(op, _eval(operands[0], element, reader, below))
    if op in ("via", "root"):
        return _members_of(node, element, reader)
    return _aggregate(op, node[1:], element, reader, depth)


def _read_path(element, steps, reader: Reader):
    """``["attr", step...]``: every step but the last follows a ``one`` role; the last reads the stored
    own attribute, then the catalogued default, else MISSING (preflight F15)."""
    value = element
    for step in steps[:-1]:
        stored = _stored_step(value, step)
        if stored is MISSING and is_atom(value):
            stored = reader.absent(value, step)
        holder, value = value, reader.follow(stored)
        if value is MISSING:
            return MISSING
        reached(holder, step, value)
    found = _stored_step(value, steps[-1])
    return reader.default(value, steps[-1]) if found is MISSING and is_atom(value) else found


def describe_operand(value) -> str:
    """How an error message names ``value``: by its type, never by its value or its contents (a scalar may
    be a personal value, and a record's ``repr`` shows every field): ``a missing value``, ``None``, ``an
    identity handle``, ``a version-bound reference``, ``a record of type T`` (its type name), ``a collection
    (T)`` (its Python type), else the article and the Python type (``a str``, ``an int``, ``a Decimal``)."""
    if value is MISSING:
        return "a missing value"
    if value is None:
        return "None"
    if isinstance(value, MutableObject):
        return "an identity handle"
    if isinstance(value, dict) and value.keys() == {"entity", "version"}:
        return "a version-bound reference"
    name = type(value).__name__
    if isinstance(value, IndexedCollection) or \
            (isinstance(value, Collection) and not is_atom(value) and not isinstance(value, (str, bytes))):
        return f"a collection ({name})"
    if is_atom(value):
        type_name = getattr(type(value), "__type_name__", None)
        return f"a record of type {type_name if isinstance(type_name, str) else name}"
    return f"{'an' if name[:1].lower() in 'aeiou' else 'a'} {name}"


def _operand_error(message: str, value) -> ProtoValidationError:
    """A ``ProtoValidationError`` with ``message`` (which names ``value`` by :func:`describe_operand`) that
    carries that description as ``semantic_operand``."""
    error = ProtoValidationError(message)
    error.semantic_operand = describe_operand(value)
    return error


def _number(op: str, value):
    if value is MISSING or (type(value) is not bool and isinstance(value, int)):
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise _operand_error(f"Operator {op!r} needs a finite decimal, not an infinite or NaN one", value)
        return value
    raise _operand_error(f"Operator {op!r} takes int and decimal operands only, not {describe_operand(value)}", value)


def _arithmetic(op: str, left, right):
    """``+``, ``-`` or ``*`` (R264): exact over int and decimal; MISSING in gives MISSING.  An operand of
    any other kind raises even when the other is MISSING, so a wrong kind is never masked."""
    left, right = _number(op, left), _number(op, right)
    if left is MISSING or right is MISSING:
        return MISSING
    if isinstance(left, int) and isinstance(right, int):
        return left + right if op == "+" else left - right if op == "-" else left * right
    try:
        return _DECIMAL_OPS[op](Decimal(left), Decimal(right))
    except DecimalException as e:
        raise ProtoValidationError(f"Operator {op!r} cannot give its result exactly: it needs more digits than "
                                   f"exact decimal arithmetic keeps") from e


def _truth(op: str, value, what: str = "an operand") -> bool:
    """A logic operand of ``op``: a boolean, or MISSING, which counts as false."""
    if value is True or value is False:
        return value
    if value is MISSING:
        return False
    raise _operand_error(f"Operator {op!r} needs a boolean as {what}, not {describe_operand(value)}", value)


def _identity(member):
    """What makes two reached members one member of a set (I2): an identity handle by its cell name, a
    version-bound reference by its entity's cell name and its version's oid, a record by its oid.  A
    role holds nothing else: any other value is refused."""
    if isinstance(member, MutableObject):
        return "handle", member.name
    if isinstance(member, dict) and member.keys() == {"entity", "version"} \
            and isinstance(member["entity"], MutableObject) and is_atom(member["version"]):
        return "version", member["entity"].name, member["version"].oid
    if is_atom(member):
        return "record", member.oid
    raise _operand_error(f"A role holds records, identity handles or version-bound references, "
                         f"not {describe_operand(member)}", member)


def _reach(raws, found: list, seen: dict, reader: Reader, holder=None, step=None) -> None:
    """Append to ``found`` each member of ``raws`` not yet ``seen``, followed; a handle whose cell is absent
    contributes nothing.  With a ``holder``, every member (also one already reached from another holder) is
    reported as reached through its ``step`` (:func:`reached`)."""
    for raw in raws:
        key = _identity(raw)
        if key in seen:
            followed = seen[key]
        else:
            followed = seen[key] = reader.follow(raw)
            if followed is not MISSING:
                found.append(followed)
        if holder is not None and followed is not MISSING:
            reached(holder, step, followed)


def _members_of(node, element, reader: Reader) -> tuple:
    """The members a ``via`` or a bare ``root`` gives: from the origin, through each role in turn, the set
    reached, in first-reached order (each member of the previous hop in its order, then its end's
    collection in that collection's order), each member once."""
    origin, roles = (node, ()) if node[0] == "root" else (node[1], node[2:])
    kind, current = origin[0], []
    if kind == "self":
        current = [element]
    elif kind == "cell":
        found = reader.cell(origin[1])
        current = [] if found is None else [found]
        if found is not None:
            reached(None, origin[1], found)
    else:
        _reach(reader.members(reader.root(origin[1])), current, {}, reader)
    for role in roles:
        hop, seen = [], {}
        for member in current:
            _reach(map(unwrap_entry, reader.members(_stored_step(member, role))), hop, seen, reader, member, role)
        current = hop
    return tuple(current)


def unwrap_entry(raw):
    """The member an element of a role's collection stands for (format spec, "Traversal"): an **entry
    record** -- a record of the plain record type ``proto_db.core.dbobject.DBObject``, without parents,
    whose fields are ``member`` and optionally ``key``, which a ``many`` end with a maintained key stores
    (spec section 6, R262) -- stands for its ``member``; any other element for itself."""
    if type(raw) is DBObject and not raw._persisted_parents():
        own = raw._own
        if "member" in own and own.keys() <= {"member", "key"}:
            return own["member"]
    return raw


def _aggregate(op: str, args, element, reader: Reader, depth: int):
    """``["sum", over, term, where?]`` and ``["count", over, where?]``: a sum is a left fold of ``+`` from
    the int 0 over the contributing terms (so the int 0 over none); a member whose term is MISSING
    contributes nothing; a member for which ``where`` is not true is not a member."""
    over, below = args[0], depth + 1
    if isinstance(over, str):
        members = _members_of(["via", ["self"], over], element, reader)
    else:
        if below > MAX_EXPRESSION_DEPTH:
            raise _too_deep()
        _children(over, True)
        members = _members_of(over, element, reader)
    term, where = (args[1], args[2] if len(args) > 2 else None) if op == "sum" else \
        (None, args[1] if len(args) > 1 else None)
    total, count = 0, 0
    for member in members:
        if where is not None and not _truth(op, _eval(where, member, reader, below), "its where"):
            continue
        count += 1
        if term is not None:
            value = _number(op, _eval(term, member, reader, below))
            if value is not MISSING:
                total = _arithmetic("+", total, value)
    return count if op == "count" else total


# -- translation to the planner ---------------------------------------------------------------------

def to_planner(ast, *, defaults: Collection[str] = ()) -> Expression | None:
    """The planner expression equivalent to ``ast``, or ``None`` when it must run as a residual.

    ``[cmp, ["attr", name], ["const", v]]`` (or mirrored) becomes ``Term(name, cmp, v)``, and
    ``and``/``or``/``not`` of such become ``And``/``Or``/``Not``; anything else (two attributes, a
    path, arithmetic, an operand no index orders) is ``None``.  ``defaults`` names the fields that
    declare a catalogued default: a predicate on one is ``None``, because an index sees MISSING where
    evaluation sees the default (preflight F16).  The tree is checked first (:func:`check_ast`, as in a
    query).
    """
    check_ast(ast, in_query=True)
    defaulted = frozenset(defaults)
    return _translate(ast, defaulted)


def _single_attr(node) -> str | None:
    if node[0] == "attr" and len(node) == 2 and "." not in node[1] and not node[1].startswith("@"):
        return node[1]
    return None


def _translate(node, defaulted: frozenset) -> Expression | None:
    op = node[0]
    if op in COMPARISONS:
        left, right = node[1], node[2]
        if right[0] == "const" and _single_attr(left):
            name, value = _single_attr(left), right[1]
        elif left[0] == "const" and _single_attr(right):
            name, value, op = _single_attr(right), left[1], MIRRORED[op]
        else:
            return None
        if name in defaulted:
            return None
        try:
            return Term(name, op, value)
        except ProtoValidationError:        # MISSING, or an operand no index orders: a residual
            return None
    if op in ("and", "or"):
        parts = [_translate(operand, defaulted) for operand in node[1:]]
        if any(part is None for part in parts):
            return None
        if len(parts) == 1:
            return parts[0]
        return And(*parts) if op == "and" else Or(*parts)
    if op == "not":
        inner = _translate(node[1], defaulted)
        return None if inner is None else Not(inner)
    return None


# -- dependencies -----------------------------------------------------------------------------------

def dependencies(ast) -> frozenset:
    """The attribute and role names ``ast`` reads from its own element: the first step of every
    ``attr``, the ``over`` role of an aggregate and the first role of a ``via`` from ``self``.  What
    an aggregate's ``term`` and ``where`` read belongs to its members, not to this element; a ``via``
    from a root or a cell reads nothing of it."""
    check_ast(ast, in_query=True)
    names: set = set()
    stack = [ast]
    while stack:
        node = stack.pop()
        op = node[0]
        if op == "attr":
            names.add(node[1])
        elif op in ("sum", "count"):
            over = node[1]
            if isinstance(over, str):
                names.add(over)
            else:
                stack.append(over)
        elif op == "via":
            if node[1][0] == "self":
                names.add(node[2])
        elif op not in ("const", "root"):
            stack.extend(node[1:])
    return frozenset(names)


# -- the text form (R277) ---------------------------------------------------------------------------

_COMPARISON_TOKENS = ("==", "!=", "<=", ">=", "<", ">")
_WORD_OPERATORS = frozenset({"and", "or", "not"})
_ESCAPES = {"\\": "\\", "'": "'", '"': '"'}


def _is_digit(ch: str) -> bool:
    """An ASCII digit: the text form's numbers are written with ``0`` to ``9`` only."""
    return "0" <= ch <= "9"


def _refuse(column: int, message: str):
    raise ProtoValidationError(f"Expression text, column {column}: {message}")


def _tokens(text: str):
    """Yield ``(kind, value, column)`` tokens lazily, so that the first construct outside the subset is
    the one reported.  Kinds: ``name``, ``int``, ``decimal``, ``str``, ``op`` and finally ``end``."""
    i, n = 0, len(text)
    while i < n:
        ch, column = text[i], i + 1
        if ch.isspace():
            i += 1
        elif ch.isalpha() or ch == "_":
            j = i + 1
            while j < n and (text[j].isalnum() or text[j] == "_"):
                j += 1
            word = text[i:j]
            if not word.isidentifier():
                _refuse(column, f"{word!r} is not a name")
            yield "name", word, column
            i = j
        elif _is_digit(ch):
            j = i
            while j < n and _is_digit(text[j]):
                j += 1
            kind = "int"
            if j + 1 < n and text[j] == "." and _is_digit(text[j + 1]):
                j += 1
                while j < n and _is_digit(text[j]):
                    j += 1
                kind = "decimal"
            if j < n and (text[j].isalnum() or text[j] in "_."):
                _refuse(column, "a number is an integer or a decimal (digits, a point, digits); no exponent, "
                                "underscore or suffix")
            literal = text[i:j]
            yield kind, int(literal) if kind == "int" else Decimal(literal), column
            i = j
        elif ch in "'\"":
            j, chars = i + 1, []
            while j < n and text[j] != ch:
                if text[j] == "\\":
                    escaped = text[j + 1] if j + 1 < n else ""
                    if escaped not in _ESCAPES:
                        _refuse(j + 1, "the only escapes are \\\\, \\' and \\\"")
                    chars.append(_ESCAPES[escaped])
                    j += 2
                else:
                    chars.append(text[j])
                    j += 1
            if j >= n:
                _refuse(column, "the string is not terminated")
            yield "str", "".join(chars), column
            i = j + 1
        elif text.startswith(("**", "//"), i):
            _refuse(column, f"{text[i:i + 2]!r} is not in the expression subset")
        elif text.startswith(_COMPARISON_TOKENS[:4], i):
            yield "op", text[i:i + 2], column
            i += 2
        elif ch in "<>+-*().":
            yield "op", ch, column
            i += 1
        elif ch == "/":
            _refuse(column, "division ('/') is not in the expression subset")
        else:
            _refuse(column, f"{ch!r} is not in the expression subset")
    yield "end", None, n + 1


#: Binding power of the operators, lowest first; the prefix ``not`` binds between ``and`` and the comparisons.
_PRECEDENCE = {"or": 1, "and": 2, "not": 3, **{op: 4 for op in _COMPARISON_TOKENS}, "+": 5, "-": 5, "*": 6}
_OPEN = "("


class _Group:
    """A parenthesized operand on the parser's stack: kept apart so that ``(a and b) and c`` is not
    flattened into one ``and``."""

    __slots__ = ("tree",)

    def __init__(self, tree):
        self.tree = tree


def _tree(operand):
    return operand.tree if isinstance(operand, _Group) else operand


class _Parser:
    """An operator-precedence (shunting-yard) parser of the subset's grammar, lowest precedence first::

        or_expr    := and_expr ("or" and_expr)*
        and_expr   := not_expr ("and" not_expr)*
        not_expr   := "not" not_expr | comparison
        comparison := sum [cmp sum]                  (no chained comparison)
        sum        := product (("+" | "-") product)*
        product    := operand ("*" operand)*
        operand    := "(" or_expr ")" | literal | "-" number | name ("." name)*

    It is iterative (an operand stack and an operator stack), so no text can exhaust Python's
    recursion; parentheses and ``not`` nest at most :data:`MAX_EXPRESSION_DEPTH` deep, and the tree is
    then checked like any other (:func:`check_ast`).
    """

    def __init__(self, text: str):
        self._tokens = _tokens(text)
        self._operands: list = []
        self._operators: list = []      # (operator, column); "(" for an open parenthesis
        self._nesting = 0               # open parentheses and pending 'not's on the operator stack
        self._advance()

    def _advance(self) -> None:
        self.kind, self.value, self.column = next(self._tokens)

    def _is(self, kind: str, value=None) -> bool:
        return self.kind == kind and (value is None or self.value == value)

    def _refuse_here(self, expected: str):
        if self.kind == "name" and keyword.iskeyword(self.value) and self.value not in _WORD_OPERATORS:
            _refuse(self.column, f"{self.value!r} is not in the expression subset")
        found = "the end of the text" if self.kind == "end" else repr(self.value)
        _refuse(self.column, f"expected {expected}, found {found}")

    def _binary(self):
        """The binary operator at the current token, or ``None``."""
        if self.kind == "name":
            if self.value in ("or", "and"):
                return self.value
            if self.value in ("in", "is"):
                self._refuse_here("an operator")
        elif self.kind == "op" and self.value in _PRECEDENCE:
            return self.value
        return None

    def _push(self, op: str) -> None:
        if op in (_OPEN, "not"):
            if self._nesting >= MAX_EXPRESSION_DEPTH:
                _refuse(self.column, f"the text nests deeper than MAX_EXPRESSION_DEPTH ({MAX_EXPRESSION_DEPTH}) "
                                     f"parentheses and 'not's")
            self._nesting += 1
        self._operators.append(op)

    def _reduce(self) -> str:
        """Apply the operator on top of the stack to its operands; return it."""
        op = self._operators.pop()
        if op == "not":
            self._nesting -= 1
            self._operands.append(["not", _tree(self._operands.pop())])
            return op
        right, left = _tree(self._operands.pop()), self._operands.pop()
        if op in ("and", "or") and not isinstance(left, _Group) and left[0] == op:
            left.append(right)              # a and b and c: one node (the chain built at this level)
        else:
            self._operands.append([op, _tree(left), right])
            return op
        self._operands.append(left)
        return op

    def parse(self) -> list:
        while True:
            self._operand()                 # also takes the prefix 'not's and '('s before it
            while self._is("op", ")") and _OPEN in self._operators:
                while self._operators[-1] != _OPEN:
                    self._reduce()
                self._operators.pop()
                self._nesting -= 1
                self._operands.append(_Group(_tree(self._operands.pop())))
                self._advance()
            op = self._binary()
            if op is None:
                break
            precedence, chained = _PRECEDENCE[op], False
            while self._operators and self._operators[-1] != _OPEN \
                    and _PRECEDENCE[self._operators[-1]] >= precedence:
                chained |= self._reduce() in _COMPARISON_TOKENS and op in _COMPARISON_TOKENS
            if chained:
                _refuse(self.column, "chained comparisons are not in the expression subset")
            self._push(op)
            self._advance()
        if not self._is("end"):
            self._refuse_here("')'" if _OPEN in self._operators else "an operator or the end of the text")
        if _OPEN in self._operators:
            self._refuse_here("')'")
        while self._operators:
            self._reduce()
        return _tree(self._operands.pop())

    def _operand(self) -> None:
        """Read the prefix 'not's and '('s, then one operand, onto the stacks."""
        while True:
            if self._is("op", _OPEN):
                self._push(_OPEN)
                self._advance()
            elif self._is("name", "not") and not (self._operators and self._operators[-1] != _OPEN
                                                  and _PRECEDENCE[self._operators[-1]] > _PRECEDENCE["not"]):
                self._push("not")
                self._advance()
            else:
                break
        kind, value, column = self.kind, self.value, self.column
        if kind in ("int", "decimal", "str"):
            self._advance()
            self._operands.append(["const", value])
        elif self._is("op", "-"):
            self._advance()
            if self.kind not in ("int", "decimal"):
                _refuse(column, "a unary '-' applies only to a number")
            self._operands.append(["const", -self.value])
            self._advance()
        elif kind == "name" and not keyword.iskeyword(value):
            steps = [value]
            self._advance()
            while self._is("op", "."):
                dot = self.column
                self._advance()
                if self.kind != "name" or keyword.iskeyword(self.value):
                    _refuse(dot, "a dotted path needs a name after '.'")
                steps.append(self.value)
                self._advance()
            if self._is("op", _OPEN):
                _refuse(self.column, "function calls are not in the expression subset")
            self._operands.append(["attr", *steps])
        else:
            self._refuse_here("an operand")


def parse(text: str) -> list:
    """Parse the Python text form of an expression (R277) into its tree.

    The subset: names and dotted paths (``attr``), literals (``'text'`` or ``"text"`` with the escapes
    ``\\\\``, ``\\'`` and ``\\"``; integers; decimals ``digits.digits``, exact; a ``-`` before a
    number), ``+ - *``, the six comparisons (not chained), ``and``/``or``/``not`` and parentheses.
    Anything else -- ``in``, ``is``, function calls, ``/``, other keywords such as ``None`` -- raises
    ``ProtoValidationError`` naming its column (1-based).  The text is a convenience: the tree is the
    normative form, and it is what is stored.
    """
    if not isinstance(text, str):
        raise ProtoValidationError(f"An expression text is a str, not {type(text).__name__}")
    tree = _Parser(text).parse()
    try:
        check_ast(tree)
    except ProtoValidationError as e:
        raise ProtoValidationError(f"Expression text: {e}") from e
    return tree
