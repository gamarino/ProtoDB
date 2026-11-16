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
"""
from __future__ import annotations

import keyword
from collections.abc import Collection
from decimal import Context, Decimal, DecimalException, Inexact, InvalidOperation, MAX_EMAX, MAX_PREC, MIN_EMIN, \
    Overflow, Rounded

from ..collections.lists import List
from ..collections.sets import Set
from ..errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from ..indexing.keyspec import _step as _stored_step
from ..query.expressions import And, Expression, Not, Or
from ..query.terms import MIRRORED, Term, compare
from ..values import MISSING, MutableObject, canonical, is_atom, is_orderable, order_key

__all__ = ["Reader", "check_ast", "evaluate", "to_planner", "dependencies", "parse",
           "ARITHMETIC", "COMPARISONS", "RESERVED"]

#: The arithmetic operators (R264); division is not specified.
ARITHMETIC = frozenset({"+", "-", "*"})
#: The comparison operators (D28 semantics).
COMPARISONS = frozenset({"==", "!=", "<", "<=", ">", ">="})
#: Operators the format reserves and this revision does not specify: a reader that meets one raises
#: ``ProtoNotSupportedError``.
RESERVED = frozenset({"in", "is_missing", "min", "max", "exists", "all", "reach", "valid_at"})
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
        (a List or a Set), the single target of a ``one`` end, none for an absent or nil value."""
        if value is MISSING or value is None:
            return ()
        if isinstance(value, (List, Set)):
            return value.elements()
        return (value,)


_NO_READER = Reader()


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
        _arity(op, origin[1:], 1, 1)
        _name(origin[1], "a name", op)
        if not in_query:
            raise ProtoValidationError(f"The origin {op!r} is valid in queries only (R265)")
    else:
        raise ProtoValidationError(f"A 'via' origin is ['self'], ['root', name] or ['cell', name], not {op!r}")


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
            _name(over, "a role name or a via expression as 'over'", op)
            return list(args[1:])
        if _is_node(over) and over[0] == "via":
            return list(args)
        raise ProtoValidationError(f"Operator {op!r} needs a role name or a via expression as 'over', not {over!r}")
    if op == "via":
        _arity(op, args, 2, None)
        _check_origin(args[0], in_query)
        for role in args[1:]:
            _name(role, "every role", op)
        return []
    if op == "self" or op in _QUERY_ORIGINS:
        raise ProtoValidationError(f"{op!r} is valid only as the origin of a 'via'")
    if op in RESERVED:
        raise ProtoNotSupportedError(f"Operator {op!r} is reserved by the format and not specified by this revision")
    raise ProtoValidationError(f"Operator {op!r} is not in the expression language (spec section 7)")


def check_ast(ast, *, in_query: bool = False) -> None:
    """Check that ``ast`` is a tree of the expression language (spec section 7).

    Every node is checked for its operator and arity; a ``const`` must hold a canonical value, names
    must be non-empty strings.  ``root`` and ``cell`` origins are refused unless ``in_query``.
    ``ProtoValidationError`` names the offending operator; a reserved operator (``in``, ``min``,
    ...) raises ``ProtoNotSupportedError``.  Kinds of values (a float in arithmetic, a non-boolean
    in logic) are checked by :func:`evaluate`, where the values are known.
    """
    stack = [ast]
    while stack:
        stack.extend(_children(stack.pop(), in_query))


# -- evaluation -------------------------------------------------------------------------------------

def evaluate(ast, element, *, reader: Reader | None = None):
    """Evaluate ``ast`` with ``element`` as the current element.

    Returns a canonical value, ``MISSING``, or a tuple of members (for ``via``).  ``reader`` reads
    cells (dereferencing identity handles), roots and catalogued defaults; without one, reaching a
    handle or a root raises ``ProtoUserError`` and no field has a default.  An invalid tree raises
    as :func:`check_ast` does, at the node evaluation reaches; ``root`` and ``cell`` origins are
    evaluated (whether a tree may use them is :func:`check_ast`'s decision).
    """
    reader = _NO_READER if reader is None else reader
    return _eval(ast, reader.follow(element), reader)


def _eval(node, element, reader: Reader):
    operands = _children(node, True)
    op = node[0]
    if op == "const":
        return canonical(node[1])
    if op == "attr":
        return _read_path(element, node[1:], reader)
    if op in ARITHMETIC:
        return _arithmetic(op, _eval(operands[0], element, reader), _eval(operands[1], element, reader))
    if op in COMPARISONS:
        return compare(_eval(operands[0], element, reader), op, _eval(operands[1], element, reader))
    if op == "and":
        return all(_truth(op, _eval(operand, element, reader)) for operand in operands)
    if op == "or":
        return any(_truth(op, _eval(operand, element, reader)) for operand in operands)
    if op == "not":
        return not _truth(op, _eval(operands[0], element, reader))
    if op == "via":
        return _via(node[1], node[2:], element, reader)
    return _aggregate(op, node[1:], element, reader)


def _read_path(element, steps, reader: Reader):
    """``["attr", step...]``: every step but the last follows a ``one`` role; the last reads the stored
    own attribute, then the catalogued default, else MISSING (preflight F15)."""
    value = element
    for step in steps[:-1]:
        value = reader.follow(_stored_step(value, step))
        if value is MISSING:
            return MISSING
    found = _stored_step(value, steps[-1])
    return reader.default(value, steps[-1]) if found is MISSING and is_atom(value) else found


def _number(op: str, value):
    if value is MISSING or (type(value) is not bool and isinstance(value, int)):
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ProtoValidationError(f"Operator {op!r} needs a finite decimal, not {value!r}")
        return value
    raise ProtoValidationError(f"Operator {op!r} takes int and decimal operands only, not {value!r} "
                               f"({type(value).__name__})")


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
        raise ProtoValidationError(f"Operator {op!r} cannot give {left!r} {op} {right!r} exactly") from e


def _truth(op: str, value, what: str = "an operand") -> bool:
    """A logic operand of ``op``: a boolean, or MISSING, which counts as false."""
    if value is True or value is False:
        return value
    if value is MISSING:
        return False
    raise ProtoValidationError(f"Operator {op!r} needs a boolean as {what}, not {value!r}")


def _identity(member):
    """What makes two reached members one member of a set: the value's order key (an atom's oid, a
    handle's cell name), or the object itself for a value that has none."""
    return order_key(member) if is_orderable(member) else ("object", id(member))


def _via(origin, roles, element, reader: Reader) -> tuple:
    """The set reached from ``origin`` through each role in turn, in first-reached order."""
    kind = origin[0]
    if kind == "self":
        current = [element]
    elif kind == "cell":
        found = reader.cell(origin[1])
        current = [] if found is None else [found]
    else:
        current = [m for m in map(reader.follow, reader.members(reader.root(origin[1]))) if m is not MISSING]
    for role in roles:
        reached, seen = [], set()
        for member in current:
            for raw in reader.members(_stored_step(member, role)):
                key = _identity(raw)
                if key in seen:
                    continue
                seen.add(key)
                followed = reader.follow(raw)
                if followed is not MISSING:
                    reached.append(followed)
        current = reached
    return tuple(current)


def _aggregate(op: str, args, element, reader: Reader):
    """``["sum", over, term, where?]`` and ``["count", over, where?]``: the int 0 over no members; a
    member whose term is MISSING contributes nothing to a sum; a member for which ``where`` is not
    true is not a member."""
    over = args[0]
    members = _via(["self"], [over], element, reader) if isinstance(over, str) else \
        _via(over[1], over[2:], element, reader)
    term, where = (args[1], args[2] if len(args) > 2 else None) if op == "sum" else \
        (None, args[1] if len(args) > 1 else None)
    total, count = None, 0
    for member in members:
        if where is not None and not _truth(op, _eval(where, member, reader), "its where"):
            continue
        count += 1
        if term is not None:
            value = _number(op, _eval(term, member, reader))
            if value is not MISSING:
                total = value if total is None else _arithmetic("+", total, value)
    if op == "count":
        return count
    return 0 if total is None else total


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
        elif op != "const":
            stack.extend(node[1:])
    return frozenset(names)


# -- the text form (R277) ---------------------------------------------------------------------------

_COMPARISON_TOKENS = ("==", "!=", "<=", ">=", "<", ">")
_WORD_OPERATORS = frozenset({"and", "or", "not"})
_ESCAPES = {"\\": "\\", "'": "'", '"': '"'}


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
        elif ch.isdigit():
            j = i
            while j < n and text[j].isdigit():
                j += 1
            kind = "int"
            if j + 1 < n and text[j] == "." and text[j + 1].isdigit():
                j += 1
                while j < n and text[j].isdigit():
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


class _Parser:
    """Recursive descent over the subset's grammar (lowest precedence first)::

        or_expr    := and_expr ("or" and_expr)*
        and_expr   := not_expr ("and" not_expr)*
        not_expr   := "not" not_expr | comparison
        comparison := sum [cmp sum]                  (no chained comparison)
        sum        := product (("+" | "-") product)*
        product    := operand ("*" operand)*
        operand    := "(" or_expr ")" | literal | "-" number | name ("." name)*
    """

    def __init__(self, text: str):
        self._tokens = _tokens(text)
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

    def parse(self) -> list:
        tree = self._or()
        if not self._is("end"):
            self._refuse_here("an operator or the end of the text")
        return tree

    def _chain(self, word: str, operand) -> list:
        items = [operand()]
        while self._is("name", word):
            self._advance()
            items.append(operand())
        return items[0] if len(items) == 1 else [word, *items]

    def _or(self) -> list:
        return self._chain("or", self._and)

    def _and(self) -> list:
        return self._chain("and", self._not)

    def _not(self) -> list:
        if self._is("name", "not"):
            self._advance()
            return ["not", self._not()]
        return self._comparison()

    def _comparison(self) -> list:
        left = self._sum()
        if not (self.kind == "op" and self.value in _COMPARISON_TOKENS):
            if self.kind == "name" and self.value in ("in", "is"):
                self._refuse_here("a comparison")
            return left
        op = self.value
        self._advance()
        right = self._sum()
        if self.kind == "op" and self.value in _COMPARISON_TOKENS:
            _refuse(self.column, "chained comparisons are not in the expression subset")
        return [op, left, right]

    def _sum(self) -> list:
        left = self._product()
        while self.kind == "op" and self.value in ("+", "-"):
            op = self.value
            self._advance()
            left = [op, left, self._product()]
        return left

    def _product(self) -> list:
        left = self._operand()
        while self._is("op", "*"):
            self._advance()
            left = ["*", left, self._operand()]
        return left

    def _operand(self) -> list:
        kind, value, column = self.kind, self.value, self.column
        if self._is("op", "("):
            self._advance()
            inner = self._or()
            if not self._is("op", ")"):
                self._refuse_here("')'")
            self._advance()
            return inner
        if kind in ("int", "decimal", "str"):
            self._advance()
            return ["const", value]
        if self._is("op", "-"):
            self._advance()
            if self.kind not in ("int", "decimal"):
                _refuse(column, "a unary '-' applies only to a number")
            literal = -self.value
            self._advance()
            return ["const", literal]
        if kind == "name" and not keyword.iskeyword(value):
            steps = [value]
            self._advance()
            while self._is("op", "."):
                dot = self.column
                self._advance()
                if self.kind != "name" or keyword.iskeyword(self.value):
                    _refuse(dot, "a dotted path needs a name after '.'")
                steps.append(self.value)
                self._advance()
            if self._is("op", "("):
                _refuse(self.column, "function calls are not in the expression subset")
            return ["attr", *steps]
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
    return _Parser(text).parse()
