"""Query expressions (spec section 10, decisions D28 and D36).

An expression is a :class:`~proto_db.query.terms.Term` or a boolean
combination of expressions: :class:`And`, :class:`Or`, :class:`Not`.  Every
node is immutable and hashable, compares by structure, has a readable
``repr``, a JSON-serialisable :meth:`describe` (used by ``explain()``) and a
:meth:`to_list` rendering in the 0.1 prefix syntax accepted by
:meth:`Expression.compile`.

Normalisation on construction: ``And``/``Or`` flatten nested nodes of their own
type (``And(And(a, b), c) == And(a, b, c)``) and keep operand order, which is
the evaluation order; ``Not(Not(e))`` is ``e`` itself.  ``And``/``Or`` need at
least two operands.

:func:`evaluate` decides an expression on one element: a Term by
:meth:`Term.matches` (the scan oracle, D28), ``And``/``Or`` short-circuit left
to right, ``Not`` is the logical negation of its operand.  Because a MISSING
key satisfies no comparison (D28), ``Not(Term("x", "<", 5))`` is True for an
element without ``x``; ask for presence explicitly (``?!N``) when that matters.
A ``near[]`` term cannot be evaluated per element (``ProtoNotSupportedError``);
it is answered by a ``VectorSearch`` plan.

:class:`PythonPredicate` is an opaque leaf wrapping an untranslatable Python
callable with its description.  The LINQ front end uses it to hand Python
predicates to a vector plan's residual (``near(k) AND lambda``, ruling R27); no
index answers it, a ``Filter`` evaluates it, and it has no 0.1 list form.

0.1 prefix syntax (:meth:`Expression.compile`): a list is a stream of
expressions joined by an implicit AND.  An expression in the stream is

* ``path, op`` for ``?N``, ``?!N``, ``?T``, ``?!T``;
* ``path, op, value`` for the comparisons, ``in`` and ``contains``;
* ``path, between-op, low, high`` for the four between variants;
* ``path, "near[]", vector, threshold`` (the 0.1 cosine threshold query) or
  ``path, "near[]", NearQuery-or-mapping``;
* ``"&" e1 e2`` / ``"|" e1 e2`` (binary, prefix), ``"!" e`` (unary);
* a nested list (compiled recursively), or an already built Expression.

``path`` is a str (dotted path or ``"@extractor"``) or None (the element itself).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator

from ..errors import ProtoNotSupportedError, ProtoValidationError
from .terms import BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_HIGH, BETWEEN_EXCL_LOW, NEAR, OPERATORS, \
    UNARY_OPERATORS, NearQuery, Term

__all__ = ["Expression", "Term", "And", "Or", "Not", "PythonPredicate", "evaluate", "iter_terms", "AND_TOKEN", "OR_TOKEN", "NOT_TOKEN"]

#: Prefix tokens of the 0.1 syntax.
AND_TOKEN = "&"
OR_TOKEN = "|"
NOT_TOKEN = "!"

_BETWEEN = (BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_LOW, BETWEEN_EXCL_HIGH)


class Expression(ABC):
    """Common base of every expression node (``Term`` is registered as a virtual subclass)."""

    __slots__ = ()

    @staticmethod
    def compile(expression) -> "Expression":
        """Compile the 0.1 prefix syntax (module docstring) into an expression.

        An Expression is returned unchanged.  Anything malformed raises
        ``ProtoValidationError`` naming the offending position.
        """
        if isinstance(expression, Expression):
            return expression
        if not isinstance(expression, list):
            raise ProtoValidationError(
                f"Expression.compile needs a list in the 0.1 prefix syntax, not {type(expression).__name__}")
        return _Compiler(expression).compile_all()

    @abstractmethod
    def describe(self) -> dict:
        """A JSON-serialisable description for ``explain()``."""

    @abstractmethod
    def to_list(self) -> list:
        """This expression in the 0.1 prefix syntax (``Expression.compile(e.to_list()) == e``)."""


Expression.register(Term)


def _check_operand(operand, owner: str) -> "Expression":
    if not isinstance(operand, Expression):
        raise ProtoValidationError(f"{owner} operands must be Expression instances, not {operand!r}; "
                                   f"use Expression.compile for the list syntax")
    return operand


class _Node(Expression):
    """Shared immutability, equality and hashing of the composite nodes."""

    __slots__ = ("_operands", "_hash")

    def _init(self, operands: tuple) -> None:
        object.__setattr__(self, "_operands", operands)
        object.__setattr__(self, "_hash", hash((type(self).__name__, operands)))

    def __setattr__(self, name, value):
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __delattr__(self, name):
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __eq__(self, other):
        if type(other) is not type(self):
            return NotImplemented
        return self._operands == other._operands

    def __hash__(self):
        return self._hash

    def __repr__(self):
        return f"{type(self).__name__}({', '.join(repr(o) for o in self._operands)})"

    def __reduce__(self):
        return (type(self), self._operands)


class _Junction(_Node):
    """``And``/``Or``: n-ary, flattened, operand order kept."""

    __slots__ = ()
    _key = ""
    _token = ""

    def __init__(self, *operands):
        flat = []
        for operand in operands:
            _check_operand(operand, type(self).__name__)
            if type(operand) is type(self):
                flat.extend(operand._operands)
            else:
                flat.append(operand)
        if len(flat) < 2:
            raise ProtoValidationError(f"{type(self).__name__} needs at least two operands, got {len(flat)}")
        self._init(tuple(flat))

    @property
    def operands(self) -> tuple:
        """The operands, in evaluation order."""
        return self._operands

    def describe(self) -> dict:
        """``{"and" | "or": [operand descriptions]}``."""
        return {self._key: [o.describe() for o in self._operands]}

    def to_list(self) -> list:
        """``[token, e1, e2]`` nested to the right for more than two operands (flattened back on compile)."""
        items = [o.to_list() for o in self._operands]
        result = items[-1]
        for item in reversed(items[:-1]):
            result = [self._token, item, result]
        return result


class And(_Junction):
    """Conjunction of two or more expressions (true iff every operand is true; short-circuits)."""

    __slots__ = ()
    _key = "and"
    _token = AND_TOKEN


class Or(_Junction):
    """Disjunction of two or more expressions (true iff some operand is true; short-circuits)."""

    __slots__ = ()
    _key = "or"
    _token = OR_TOKEN


class Not(_Node):
    """Logical negation of one expression; ``Not(Not(e))`` is ``e``.

    Note (D28/D36): a MISSING key satisfies no comparison, so the negation of a
    comparison is True for an element lacking the key.
    """

    __slots__ = ()

    def __new__(cls, operand):
        _check_operand(operand, "Not")
        if type(operand) is Not:
            return operand.operand
        return super().__new__(cls)

    def __init__(self, operand):
        self._init((operand,))

    @property
    def operand(self) -> "Expression":
        """The negated expression."""
        return self._operands[0]

    def describe(self) -> dict:
        """``{"not": operand description}``."""
        return {"not": self.operand.describe()}

    def to_list(self) -> list:
        """``["!", operand]``."""
        return [NOT_TOKEN, self.operand.to_list()]


class PythonPredicate(Expression):
    """An opaque predicate: ``bool(fn(element))``, described by ``description`` in ``explain()``.

    Immutable; equal when it wraps the same callable with the same description.
    Exceptions raised by ``fn`` propagate.  ``to_list`` raises
    ``ProtoNotSupportedError`` (the 0.1 syntax cannot express a callable).
    """

    __slots__ = ("_fn", "_description")

    def __init__(self, fn, description: str):
        if not callable(fn) or isinstance(fn, Expression):
            raise ProtoValidationError(f"PythonPredicate needs a callable, not {fn!r}")
        if not isinstance(description, str) or not description:
            raise ProtoValidationError(f"PythonPredicate needs a non-empty description, not {description!r}")
        object.__setattr__(self, "_fn", fn)
        object.__setattr__(self, "_description", description)

    @property
    def fn(self):
        """The wrapped callable."""
        return self._fn

    @property
    def description(self) -> str:
        """The description shown by ``explain()``."""
        return self._description

    def __setattr__(self, name, value):
        raise AttributeError("PythonPredicate is immutable")

    def __delattr__(self, name):
        raise AttributeError("PythonPredicate is immutable")

    def __eq__(self, other):
        if type(other) is not PythonPredicate:
            return NotImplemented
        return self._fn is other._fn and self._description == other._description

    def __hash__(self):
        return hash((id(self._fn), self._description))

    def __repr__(self):
        return f"PythonPredicate({self._description!r})"

    def describe(self) -> dict:
        """``{"python": description}``."""
        return {"python": self._description}

    def to_list(self) -> list:
        """Not expressible in the 0.1 syntax: ``ProtoNotSupportedError``."""
        raise ProtoNotSupportedError(f"{self!r} wraps a Python callable and has no 0.1 list form")


def evaluate(expr: Expression, element) -> bool:
    """Decide ``expr`` on ``element`` (module docstring); never catches an exception."""
    if isinstance(expr, Term):
        return expr.matches(element)
    if isinstance(expr, And):
        return all(evaluate(o, element) for o in expr.operands)
    if isinstance(expr, Or):
        return any(evaluate(o, element) for o in expr.operands)
    if isinstance(expr, Not):
        return not evaluate(expr.operand, element)
    if isinstance(expr, PythonPredicate):
        return bool(expr.fn(element))
    raise ProtoValidationError(f"evaluate needs an Expression, not {expr!r}")


def iter_terms(expr: Expression) -> Iterator[Term]:
    """Every Term of ``expr``, depth first, left to right."""
    if isinstance(expr, Term):
        yield expr
    elif isinstance(expr, _Node):
        for operand in expr._operands:
            yield from iter_terms(operand)
    elif not isinstance(expr, PythonPredicate):
        raise ProtoValidationError(f"iter_terms needs an Expression, not {expr!r}")


class _Compiler:
    """Recursive-descent parser over one token list of the 0.1 prefix syntax."""

    def __init__(self, tokens: list):
        self.tokens = tokens

    def compile_all(self) -> Expression:
        if not self.tokens:
            raise ProtoValidationError("Cannot compile an empty expression list")
        parsed, i = [], 0
        while i < len(self.tokens):
            expr, i = self.parse(i)
            parsed.append(expr)
        return parsed[0] if len(parsed) == 1 else And(*parsed)

    def parse(self, i: int) -> tuple:
        tokens = self.tokens
        token = tokens[i]
        if isinstance(token, Expression):
            return token, i + 1
        if isinstance(token, list):
            return Expression.compile(token), i + 1
        if token == NOT_TOKEN:
            if i + 1 >= len(tokens):
                raise ProtoValidationError(f"'!' needs an operand (position {i} of {tokens!r})")
            operand, j = self.parse(i + 1)
            return Not(operand), j
        if token in (AND_TOKEN, OR_TOKEN):
            if i + 1 >= len(tokens):
                raise ProtoValidationError(f"{token!r} needs two operands (position {i} of {tokens!r})")
            first, j = self.parse(i + 1)
            if j >= len(tokens):
                raise ProtoValidationError(f"{token!r} needs two operands (position {i} of {tokens!r})")
            second, k = self.parse(j)
            return (And if token == AND_TOKEN else Or)(first, second), k
        if token is not None and not isinstance(token, str):
            raise ProtoValidationError(f"Expected a path (str or None), '&', '|', '!' or a nested list at "
                                       f"position {i} of {tokens!r}, got {token!r}")
        return self.parse_term(i)

    def parse_term(self, i: int) -> tuple:
        tokens = self.tokens
        path = tokens[i]
        if i + 1 >= len(tokens):
            raise ProtoValidationError(f"Term at position {i} needs an operator after path {path!r}")
        op = tokens[i + 1]
        if not isinstance(op, str) or op not in OPERATORS:
            raise ProtoValidationError(f"Unknown operator {op!r} at position {i + 1}; expected one of "
                                       f"{sorted(OPERATORS)}")
        rest = len(tokens) - (i + 2)
        if op in UNARY_OPERATORS:
            return Term(path, op), i + 2
        if op in _BETWEEN:
            if rest < 2:
                raise ProtoValidationError(f"Operator {op!r} at position {i + 1} needs a lower and an upper bound")
            return Term(path, op, (tokens[i + 2], tokens[i + 3])), i + 4
        if rest < 1:
            raise ProtoValidationError(f"Operator {op!r} at position {i + 1} needs a value")
        value = tokens[i + 2]
        if op == NEAR and not isinstance(value, (NearQuery, dict)):
            if rest < 2:
                raise ProtoValidationError(f"'near[]' at position {i + 1} needs a vector and a threshold "
                                           f"(or a NearQuery / mapping of its fields)")
            return Term(path, op, NearQuery(value, threshold=tokens[i + 3], metric="cosine")), i + 4
        return Term(path, op, value), i + 3
