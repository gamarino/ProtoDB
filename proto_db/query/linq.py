"""LINQ front end of the query layer (spec section 10, decision D38).

:func:`from_collection` wraps a source in a :class:`Queryable`, an immutable,
lazy description of a query: every method returns a NEW Queryable and leaves
the receiver unchanged; nothing runs until the query is iterated, listed,
counted or asked for its first element.  The query is then turned into one
physical plan of :mod:`proto_db.query.plans` (see :meth:`Queryable.plan`), and
``explain()`` describes exactly the tree that runs.  No exception raised in
the query path is caught: an error in a predicate, a projection, a key
function or the planner reaches the caller (the 0.1 front end returned ``[]``
on any exception).

Sources
-------
An ``IndexedCollection`` (its indexes are consulted by the planner), a
``QueryPlan`` (a ``CollectionScanPlan`` keeps its collection's indexes; any
other plan is filtered on top), another Queryable (returned as is), or any
other Python iterable, copied once into a :class:`~proto_db.query.plans.ValuesScan`
(a scan of a tuple of values, never indexed).  A ``str``, ``bytes`` or a
mapping is rejected: iterate ``.items()``/``.values()`` explicitly.

Predicates and the ``F`` builder
--------------------------------
``F.a.b`` (or ``F["a"]["b"]``) names the key at the dotted path ``a.b``,
``F()`` the element itself and ``F["@name"]`` a registered extractor (the
key semantics of :class:`~proto_db.indexing.keyspec.KeySpec`: an absent
attribute is MISSING, not None).  Comparisons ``==``, ``!=``, ``<``, ``<=``,
``>``, ``>=`` with a value, ``in_``, the ``between`` family, ``range``,
``contains``, ``is_none``/``not_none`` (``?N``/``?!N``),
``is_true``/``not_true`` (``?T``/``?!T``) and ``near`` build a
:class:`Predicate` holding a pure :class:`~proto_db.query.terms.Term`; ``&``,
``|`` and ``~`` combine predicates into ``And``/``Or``/``Not``.  A predicate
built only from these is a pure Expression the planner answers with indexes
where it can.  A field whose name collides with a builder method (``range``,
``length``, ``path``, ...) or starts with ``_`` is named with ``F["name"]``.

The 0.1 extras are kept but are not translatable to terms: ``startswith``,
``endswith``, the computed values ``lower()``, ``upper()``, ``length()``,
``abs()`` and arithmetic (``+ - * /``), and comparisons between two fields.
Predicates over them run as an explicit
:class:`~proto_db.query.plans.PythonFilter` whose ``explain()`` description
reads like the source (``first_name.lower() == 'bob'``).  A computed value
compares with the term semantics (D28) by evaluating a ``Term`` on the
computed value; MISSING and None operands propagate unchanged (MISSING then
satisfies no comparison, like an absent key), and any other operand is handed
to the Python operation, whose errors propagate.

Python callables are accepted wherever a predicate or key is: as a predicate
they become a PythonFilter (described by qualified name, file and line).
Mixing: an AND at the top level is split -- its pure conjuncts go to the
planner (index or residual Filter) and each impure conjunct becomes its own
PythonFilter on top, applied in order, so a PythonFilter only sees elements
that satisfy every pure conjunct.  An OR or a NOT holding an impure part is
one PythonFilter for the whole predicate: never partially indexed, never
dropped.  A ``near[]`` term inside such a predicate raises
``ProtoNotSupportedError`` (it has no per-element semantics).  Using a
predicate as a bool (``F.a == 1 and F.b == 2``) raises ``ProtoUserError``,
because Python would silently keep only one side.  Note that ``F.a in values`` is not a
predicate either: Python compares each item with ``==`` and asks for its truth
value (``ProtoUserError``), except for an empty container, which answers False
without any comparison; write ``F.a.in_(values)``.  Builder methods passed
uncalled where a key or predicate belongs (``select(F.name.startswith)``) raise
``ProtoValidationError`` pointing to ``F["name"]``; :func:`path_of` reads a
field's path, so every other public name is a child field (``F.file.path``).

Placement
---------
The leading ``where``/``order_by``/``then_by`` calls (before any projection,
grouping, join, unnest, distinct or window) are merged -- the predicates ANDed,
the order keys composed -- and handed to :func:`~proto_db.query.planner.plan_query`
together with the ``skip``/``take`` calls that directly follow them, so
indexes answer predicates and may stream ordered results.  When that merged
predicate has impure parts, or an order key is a function, the planner gets
only the pure predicate and the PythonFilters, a ``Sort`` and a ``Limit``
are stacked on top in that order.  Every later operation is a plan node on top
of the previous ones, in call order (filtering commutes with a stable sort,
not with a window, so a ``where`` after ``take`` filters the window).

LINQ semantics
--------------
``order_by`` sorts stably by a total order (``Sort``: MISSING first, then None,
then the D28 type ranks; unorderable values last); a later ``order_by``
re-sorts the whole sequence, so earlier keys break its ties; ``then_by`` adds a
tie-breaker and must directly follow ``order_by``/``then_by``.  ``skip`` and
``take`` compose like slicing into one window.  ``select`` of one path yields
that value (:data:`MISSING` when absent -- ``MISSING`` is public API, exported
here and by :mod:`proto_db.query`; its repr is ``MISSING`` and it is falsy; None
would conflate an absent key with an explicit null), of several paths or a mapping a dict
(absent values are left out), of a function its result.  ``distinct`` keeps
the first occurrence by exact identity (R19).  ``group_by`` yields one dict
per group (:class:`Agg` aggregations; default ``{"key", "items"}``).
``join`` yields ``(left, right)`` pairs (``right`` is MISSING for an unmatched
left element of a left join).  ``select_many`` flattens a list, tuple or
collection per element.
"""
from __future__ import annotations

import os
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass

from ..collections.base import IndexedCollection
from ..errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from ..indexing.keyspec import KeySpec
from ..values import MISSING
from .expressions import And, Expression, Not, Or, evaluate, iter_terms
from .planner import plan_query
from .plans import (AGGREGATES, JOIN_KINDS, Count, Distinct, GroupBy, Join, Limit, Project, PythonFilter, QueryPlan,
                    Sort, Unnest, ValuesScan, Filter)
from .terms import (BETWEEN, BETWEEN_EXCL_BOTH, BETWEEN_EXCL_HIGH, BETWEEN_EXCL_LOW, CONTAINS, EQ, GE, GT, IN,
                    IS_NONE, IS_TRUE, LE, LT, NE, NEAR, NOT_NONE, NOT_TRUE, UNARY_OPERATORS, NearQuery, Term)

__all__ = ["from_collection", "Queryable", "F", "Field", "Computed", "Predicate", "Agg", "Aggregation", "path_of",
           "MISSING"]

_BOUNDS = {"[]": BETWEEN, "()": BETWEEN_EXCL_BOTH, "(]": BETWEEN_EXCL_LOW, "[)": BETWEEN_EXCL_HIGH}
_INCLUSIVE = {(True, True): BETWEEN, (False, False): BETWEEN_EXCL_BOTH, (False, True): BETWEEN_EXCL_LOW,
              (True, False): BETWEEN_EXCL_HIGH}


# -- descriptions ---------------------------------------------------------------------------------

def _describe_callable(fn) -> str:
    """A readable label for a user function: qualified name plus ``(file:line)`` when known."""
    name = getattr(fn, "__qualname__", None) or repr(fn)
    code = getattr(fn, "__code__", None)
    if code is not None:
        return f"{name} ({os.path.basename(code.co_filename)}:{code.co_firstlineno})"
    return name


def _render_path(path) -> str:
    return "element" if path is None else path


def _render(expr: Expression) -> str:
    """A readable one-line rendering of an expression, for PythonFilter descriptions."""
    if isinstance(expr, Term):
        if expr.op in UNARY_OPERATORS:
            return f"{_render_path(expr.path)} {expr.op}"
        return f"{_render_path(expr.path)} {expr.op} {expr.value!r}"
    if isinstance(expr, And):
        return " & ".join(f"({_render(o)})" for o in expr.operands)
    if isinstance(expr, Or):
        return " | ".join(f"({_render(o)})" for o in expr.operands)
    if isinstance(expr, Not):
        return f"~({_render(expr.operand)})"
    raise ProtoValidationError(f"Cannot render {expr!r}")


class _Described:
    """A callable labelled for ``explain()``: the plan nodes read the label from ``__qualname__``."""

    def __init__(self, fn: Callable, description: str):
        self.fn = fn
        self.__qualname__ = description

    def __call__(self, element):
        return self.fn(element)


# -- predicates -----------------------------------------------------------------------------------

class Predicate:
    """A LINQ predicate: a pure Expression (indexable) or a combination holding Python parts.

    ``expression`` is the pure Expression, or None when the predicate holds an
    untranslatable part.  Calling a predicate on an element decides it (pure
    parts by :func:`~proto_db.query.expressions.evaluate`).  ``&``, ``|`` and
    ``~`` combine predicates (a callable, an Expression or the 0.1 list syntax
    is accepted as the other operand).  Immutable.
    """

    __slots__ = ()

    @property
    def expression(self) -> Expression | None:
        """The pure Expression of this predicate, or None if it is not translatable."""
        return None

    def describe(self) -> str:
        """A readable one-line description (used by ``explain()``)."""
        raise NotImplementedError

    def __call__(self, element) -> bool:
        raise NotImplementedError

    def __and__(self, other) -> "Predicate":
        other = _as_predicate(other)
        if self.expression is not None and other.expression is not None:
            return _Pure(And(self.expression, other.expression))
        return _AllOf(_flatten(self, _AllOf) + _flatten(other, _AllOf))

    def __rand__(self, other) -> "Predicate":
        return _as_predicate(other) & self

    def __or__(self, other) -> "Predicate":
        other = _as_predicate(other)
        if self.expression is not None and other.expression is not None:
            return _Pure(Or(self.expression, other.expression))
        return _AnyOf(_flatten(self, _AnyOf) + _flatten(other, _AnyOf))

    def __ror__(self, other) -> "Predicate":
        return _as_predicate(other) | self

    def __invert__(self) -> "Predicate":
        if self.expression is not None:
            return _Pure(Not(self.expression))
        return _Negation(self)

    def __bool__(self):
        raise ProtoUserError("A predicate has no truth value; combine predicates with &, | and ~ "
                             "(Python's and/or/not would silently drop one side)")

    def __setattr__(self, name, value):
        raise ProtoUserError(f"{type(self).__name__} is immutable")

    def __repr__(self):
        return f"{type(self).__name__}({self.describe()})"


def _flatten(predicate: Predicate, kind: type) -> list:
    return list(predicate.parts) if isinstance(predicate, kind) else [predicate]


class _Pure(Predicate):
    """A predicate that is exactly one Expression."""

    __slots__ = ("_expr",)

    def __init__(self, expr: Expression):
        object.__setattr__(self, "_expr", expr)

    @property
    def expression(self) -> Expression:
        """The Expression."""
        return self._expr

    def describe(self) -> str:
        """The rendered expression."""
        return _render(self._expr)

    def __call__(self, element) -> bool:
        return evaluate(self._expr, element)


class _Opaque(Predicate):
    """An untranslatable predicate: ``bool(fn(element))``."""

    __slots__ = ("fn", "description")

    def __init__(self, fn: Callable, description: str):
        object.__setattr__(self, "fn", fn)
        object.__setattr__(self, "description", description)

    def describe(self) -> str:
        """The label given at construction."""
        return self.description

    def __call__(self, element) -> bool:
        return bool(self.fn(element))


class _Junction(Predicate):
    """AND / OR of two or more predicates, at least one of them impure; short-circuits left to right."""

    __slots__ = ("parts",)
    _symbol = ""

    def __init__(self, parts: list):
        object.__setattr__(self, "parts", tuple(parts))

    def describe(self) -> str:
        """Each part parenthesised, joined by the junction symbol."""
        return f" {self._symbol} ".join(f"({p.describe()})" for p in self.parts)


class _AllOf(_Junction):
    __slots__ = ()
    _symbol = "&"

    def __call__(self, element) -> bool:
        return all(p(element) for p in self.parts)


class _AnyOf(_Junction):
    __slots__ = ()
    _symbol = "|"

    def __call__(self, element) -> bool:
        return any(p(element) for p in self.parts)


class _Negation(Predicate):
    """Negation of an impure predicate."""

    __slots__ = ("part",)

    def __init__(self, part: Predicate):
        object.__setattr__(self, "part", part)

    def describe(self) -> str:
        """``~(part)``."""
        return f"~({self.part.describe()})"

    def __call__(self, element) -> bool:
        return not self.part(element)


def _as_predicate(value) -> Predicate:
    """A Predicate from a Predicate, an Expression, the 0.1 list syntax or a callable."""
    if isinstance(value, Predicate):
        return value
    if isinstance(value, Expression):
        return _Pure(value)
    if isinstance(value, list):
        return _Pure(Expression.compile(value))
    if isinstance(value, _Operand):
        raise ProtoValidationError(f"{value!r} is a value, not a predicate; compare it (e.g. == or is_true())")
    _reject_builder_method(value, "A predicate")
    if callable(value):
        return _Opaque(value, _describe_callable(value))
    raise ProtoValidationError(f"A predicate must be built with F, be an Expression, the 0.1 list syntax or a "
                               f"callable, not {value!r}")


def _split(predicate: Predicate) -> tuple:
    """``(pure expression or None, [impure predicates])``: a top-level AND is split, anything else is not."""
    if predicate.expression is not None:
        return predicate.expression, []
    if isinstance(predicate, _AllOf):
        pure = [p.expression for p in predicate.parts if p.expression is not None]
        impure = [p for p in predicate.parts if p.expression is None]
        expr = None if not pure else pure[0] if len(pure) == 1 else And(*pure)
        return expr, impure
    return None, [predicate]


def _check_no_near(predicate: Predicate) -> None:
    """Raise if an impure predicate (run per element) holds a ``near[]`` term."""
    stack = [predicate]
    while stack:
        p = stack.pop()
        if isinstance(p, _Pure):
            if any(t.op == NEAR for t in iter_terms(p.expression)):
                raise ProtoNotSupportedError(f"A 'near[]' term cannot run inside a Python predicate (it is combined "
                                             f"with one by OR/NOT in {predicate.describe()}): it has no "
                                             f"per-element semantics; AND it at the top level instead")
        elif isinstance(p, _Junction):
            stack.extend(p.parts)
        elif isinstance(p, _Negation):
            stack.append(p.part)


# -- field builder --------------------------------------------------------------------------------

_PREC_ATOM, _PREC_ADD, _PREC_MUL = 3, 1, 2


class _Operand:
    """Shared operators of :class:`Field` and :class:`Computed` (immutable, not hashable, not callable)."""

    __slots__ = ()
    __hash__ = None

    _prec = _PREC_ATOM

    def _compute(self, element):
        raise NotImplementedError

    def _label(self) -> str:
        raise NotImplementedError

    def _predicate(self, op: str, value=None) -> Predicate:
        raise NotImplementedError

    def __setattr__(self, name, value):
        raise ProtoUserError(f"{type(self).__name__} is immutable")

    def __bool__(self):
        raise ProtoUserError(f"{self!r} has no truth value; compare it (e.g. == or is_true())")

    def __repr__(self):
        return f"{type(self).__name__}({self._label()})"

    # comparisons
    def _compare(self, op: str, other) -> Predicate:
        if isinstance(other, _Operand):
            return _compare_operands(self, op, other)
        return self._predicate(op, other)

    def __eq__(self, other):
        """``key == other`` (D28): ``values_equal``; ``F.a == None`` matches only an explicit None, never a
        MISSING ``a`` (use ``is_none()`` for "MISSING or None")."""
        return self._compare(EQ, other)

    def __ne__(self, other):
        """``key != other`` (D28): a PRESENT key not equal to ``other``; ``F.a != 1`` does not match an
        element without ``a`` (absence is not a value)."""
        return self._compare(NE, other)

    def __lt__(self, other):
        return self._compare(LT, other)

    def __le__(self, other):
        return self._compare(LE, other)

    def __gt__(self, other):
        return self._compare(GT, other)

    def __ge__(self, other):
        return self._compare(GE, other)

    def in_(self, values) -> Predicate:
        """Membership in ``values`` (a list, tuple, set or any iterable; ``values_equal`` semantics).

        A mapping is rejected (iterate its keys or values explicitly).
        """
        if isinstance(values, Mapping):
            raise ProtoValidationError(f"in_ needs a collection of values, not a mapping {values!r}; pass "
                                       f"list(m) or list(m.values())")
        if not isinstance(values, (list, tuple, set, frozenset)):
            if isinstance(values, (str, bytes)) or not isinstance(values, Iterable):
                raise ProtoValidationError(f"in_ needs a collection of values, not {values!r}")
            values = tuple(values)
        return self._predicate(IN, tuple(values) if isinstance(values, list) else values)

    def between(self, low, high, inclusive: tuple = (True, True)) -> Predicate:
        """``low <= key <= high``; ``inclusive=(include low, include high)`` picks the variant."""
        if not (isinstance(inclusive, tuple) and len(inclusive) == 2
                and all(isinstance(flag, bool) for flag in inclusive)):
            raise ProtoValidationError(f"between inclusive must be a pair of bools, not {inclusive!r}")
        return self._predicate(_INCLUSIVE[inclusive], (low, high))

    def between_closed(self, low, high) -> Predicate:
        """``low <= key <= high``."""
        return self._predicate(BETWEEN, (low, high))

    def between_open(self, low, high) -> Predicate:
        """``low < key < high``."""
        return self._predicate(BETWEEN_EXCL_BOTH, (low, high))

    def between_left_open(self, low, high) -> Predicate:
        """``low < key <= high``."""
        return self._predicate(BETWEEN_EXCL_LOW, (low, high))

    def between_right_open(self, low, high) -> Predicate:
        """``low <= key < high``."""
        return self._predicate(BETWEEN_EXCL_HIGH, (low, high))

    def range(self, low, high, bounds: str = "[]") -> Predicate:
        """``between`` with ``bounds`` one of ``"[]"``, ``"()"``, ``"(]"``, ``"[)"``."""
        op = _BOUNDS.get(bounds.strip()) if isinstance(bounds, str) else None
        if op is None:
            raise ProtoValidationError(f"range bounds must be one of {sorted(_BOUNDS)}, not {bounds!r}")
        return self._predicate(op, (low, high))

    def contains(self, value) -> Predicate:
        """Substring of a str/bytes key, item of a list/tuple, key of a dict, member of a collection."""
        return self._predicate(CONTAINS, value)

    def is_none(self) -> Predicate:
        """The key is MISSING or None (``?N``)."""
        return self._predicate(IS_NONE)

    def not_none(self) -> Predicate:
        """The key is present and not None (``?!N``)."""
        return self._predicate(NOT_NONE)

    def is_true(self) -> Predicate:
        """The key is present and truthy (``?T``)."""
        return self._predicate(IS_TRUE)

    def not_true(self) -> Predicate:
        """The key is present and falsy (``?!T``)."""
        return self._predicate(NOT_TRUE)

    # untranslatable predicates
    def startswith(self, prefix) -> Predicate:
        """The (str or bytes) key starts with ``prefix``; MISSING and None never match (PythonFilter)."""
        return self._text_test("startswith", prefix)

    def endswith(self, suffix) -> Predicate:
        """The (str or bytes) key ends with ``suffix``; MISSING and None never match (PythonFilter)."""
        return self._text_test("endswith", suffix)

    def _text_test(self, method: str, operand) -> Predicate:
        compute = self._compute

        def test(element):
            value = compute(element)
            if value is MISSING or value is None:
                return False
            return getattr(value, method)(operand)

        return _Opaque(test, f"{self._wrapped(_PREC_ATOM)}.{method}({operand!r})")

    # computed values
    def _wrapped(self, prec: int) -> str:
        label = self._label()
        return f"({label})" if self._prec < prec else label

    def _unary(self, name: str, fn: Callable) -> "Computed":
        compute = self._compute

        def apply(element):
            value = compute(element)
            if value is MISSING or value is None:
                return value
            return fn(value)

        return Computed(apply, f"{self._wrapped(_PREC_ATOM)}.{name}()")

    def lower(self) -> "Computed":
        """The key lower-cased (computed, untranslatable)."""
        return self._unary("lower", lambda v: v.lower())

    def upper(self) -> "Computed":
        """The key upper-cased (computed, untranslatable)."""
        return self._unary("upper", lambda v: v.upper())

    def length(self) -> "Computed":
        """``len`` of the key (computed, untranslatable)."""
        return self._unary("length", len)

    def abs(self) -> "Computed":
        """``abs`` of the key (computed, untranslatable)."""
        return self._unary("abs", abs)

    def _binary(self, symbol: str, prec: int, fn: Callable, other, reflected: bool = False) -> "Computed":
        left, right = (other, self) if reflected else (self, other)
        lcomp, rcomp = _value_function(left), _value_function(right)

        def apply(element):
            a, b = lcomp(element), rcomp(element)
            if a is MISSING or b is MISSING:
                return MISSING
            if a is None or b is None:
                return None
            return fn(a, b)

        label = f"{_operand_label(left, prec)} {symbol} {_operand_label(right, prec + 1)}"
        return Computed(apply, label, prec)

    def __add__(self, other):
        return self._binary("+", _PREC_ADD, lambda a, b: a + b, other)

    def __radd__(self, other):
        return self._binary("+", _PREC_ADD, lambda a, b: a + b, other, reflected=True)

    def __sub__(self, other):
        return self._binary("-", _PREC_ADD, lambda a, b: a - b, other)

    def __rsub__(self, other):
        return self._binary("-", _PREC_ADD, lambda a, b: a - b, other, reflected=True)

    def __mul__(self, other):
        return self._binary("*", _PREC_MUL, lambda a, b: a * b, other)

    def __rmul__(self, other):
        return self._binary("*", _PREC_MUL, lambda a, b: a * b, other, reflected=True)

    def __truediv__(self, other):
        return self._binary("/", _PREC_MUL, lambda a, b: a / b, other)

    def __rtruediv__(self, other):
        return self._binary("/", _PREC_MUL, lambda a, b: a / b, other, reflected=True)


def _value_function(value) -> Callable:
    if isinstance(value, _Operand):
        return value._compute
    return lambda _element: value


def _operand_label(value, prec: int) -> str:
    return value._wrapped(prec) if isinstance(value, _Operand) else repr(value)


def _compare_operands(left: _Operand, op: str, right: _Operand) -> Predicate:
    """``left op right`` between two computed values: the term semantics on the computed key (PythonFilter)."""
    if op not in (EQ, NE, LT, LE, GT, GE):
        raise ProtoValidationError(f"Operator {op!r} cannot take a field as its operand")
    lcomp, rcomp = left._compute, right._compute

    def test(element):
        other = rcomp(element)
        if other is MISSING:
            return False
        return Term(None, op, other).matches(lcomp(element))

    return _Opaque(test, f"{left._label()} {op} {right._label()}")


class Field(_Operand):
    """The key at a path of the element (built with ``F``): comparisons build pure terms.

    Its term path (dotted path, None for the element itself, or ``"@name"``
    for a registered extractor) is read with :func:`path_of`; every public
    attribute name that is not a builder method is a child field, so
    ``F.file.path`` is the key ``file.path``.
    """

    __slots__ = ("_segments", "_spec", "_path")

    def __init__(self, segments: tuple):
        path = ".".join(segments) if segments else None
        object.__setattr__(self, "_segments", segments)
        object.__setattr__(self, "_path", path)
        object.__setattr__(self, "_spec", KeySpec.for_term_path(path))

    def _child(self, name) -> "Field":
        if not isinstance(name, str) or not name or "." in name:
            raise ProtoValidationError(f"A field name must be a non-empty str without '.', not {name!r}")
        if name.startswith("@") and self._segments:
            raise ProtoValidationError(f"An extractor ('@name') must be the whole path, not a step of "
                                       f"{self._path!r}")
        if self._segments and self._segments[0].startswith("@"):
            raise ProtoValidationError(f"An extractor key {self._path!r} has no sub-fields")
        return Field(self._segments + (name,))

    def __getattr__(self, name: str) -> "Field":
        if name.startswith("_"):
            raise AttributeError(name)
        return self._child(name)

    def __getitem__(self, name) -> "Field":
        return self._child(name)

    def _compute(self, element):
        return self._spec.extract(element)

    def _label(self) -> str:
        return _render_path(self._path)

    def _predicate(self, op: str, value=None) -> Predicate:
        return _Pure(Term(self._path, op) if op in UNARY_OPERATORS else Term(self._path, op, value))

    def near(self, vector, k: int | None = None, threshold: float | None = None,
             metric: str | None = None) -> Predicate:
        """The ``near[]`` term: the ``k`` nearest and/or those within ``threshold`` (needs a vector index)."""
        return _Pure(Term(self._path, NEAR, NearQuery(vector, k=k, threshold=threshold, metric=metric)))


def path_of(field: Field) -> str | None:
    """The term path of a :class:`Field`: the dotted path, None (the element) or ``"@name"``."""
    if not isinstance(field, Field):
        raise ProtoValidationError(f"path_of needs a Field (built with F), not {field!r}")
    return field._path


def _reject_builder_method(value, what: str) -> None:
    """A bound builder method (``F.name.startswith``) passed where a key or predicate belongs is a mistake."""
    owner = getattr(value, "__self__", None)
    if isinstance(owner, _Operand) and callable(value):
        name = getattr(value, "__name__", "?")
        base = path_of(owner) if isinstance(owner, Field) else owner._label()
        raise ProtoValidationError(
            f"{what} got the builder method {base}.{name} (not a field and not a predicate); for a field named "
            f"{name!r} write F[\"{name}\"] (e.g. F[\"{base}\"][\"{name}\"]), or call the method")


class Computed(_Operand):
    """A value computed from the element (``F.a.lower()``, ``F.a + F.b``): untranslatable.

    Comparisons build PythonFilter-backed predicates that apply the term
    semantics to the computed value; usable as a ``select`` value or a key.
    """

    __slots__ = ("_fn", "_description", "_prec")

    def __init__(self, fn: Callable, description: str, prec: int = _PREC_ATOM):
        object.__setattr__(self, "_fn", fn)
        object.__setattr__(self, "_description", description)
        object.__setattr__(self, "_prec", prec)

    def _compute(self, element):
        return self._fn(element)

    def _label(self) -> str:
        return self._description

    def _predicate(self, op: str, value=None) -> Predicate:
        term = Term(None, op) if op in UNARY_OPERATORS else Term(None, op, value)
        compute = self._compute
        if op in UNARY_OPERATORS:
            label = f"{self._description} {op}"
        else:
            label = f"{self._description} {op} {term.value!r}"
        return _Opaque(lambda element: term.matches(compute(element)), label)

    def near(self, *args, **kwargs):
        """Not supported: ``near[]`` needs a vector index on a stored key."""
        raise ProtoNotSupportedError(f"near() needs a stored field with a vector index, not the computed value "
                                     f"{self._description}")


class _FieldBuilder:
    """``F``: ``F.a.b`` / ``F["a"]`` name a key path, ``F()`` the element itself."""

    __slots__ = ()

    def __getattr__(self, name: str) -> Field:
        if name.startswith("_"):
            raise AttributeError(name)
        return Field(())._child(name)

    def __getitem__(self, name) -> Field:
        return Field(())._child(name)

    def __call__(self) -> Field:
        return Field(())

    def __repr__(self):
        return "F"


#: The field builder.
F = _FieldBuilder()


# -- keys -----------------------------------------------------------------------------------------

def _key(value, what: str):
    """A plan key: a path (str or None) or a labelled callable."""
    if value is None or isinstance(value, str):
        KeySpec.for_term_path(value)
        return value
    if isinstance(value, Field):
        return value._path
    _reject_builder_method(value, what)
    if isinstance(value, Computed):
        return _Described(value._compute, value._label())
    if isinstance(value, Predicate):
        return _Described(value, value.describe())
    if callable(value):
        return _Described(value, _describe_callable(value))
    raise ProtoValidationError(f"{what} key must be a field (F.x), a path, a computed value or a callable, "
                               f"not {value!r}")


# -- aggregations ---------------------------------------------------------------------------------

@dataclass(frozen=True)
class Aggregation:
    """One named aggregation of :meth:`Queryable.group_by`: ``op`` over the values at ``path``.

    ``op`` is a GroupBy built-in (count, sum, avg, min, max, collect) or
    ``"key"`` (the group key itself, renamed to the aggregation's name).
    """

    op: str
    path: object = None


class Agg:
    """Aggregation helpers for :meth:`Queryable.group_by` (the 0.1 ``F.count()``/``F.sum()`` family)."""

    @staticmethod
    def key() -> Aggregation:
        """The group key, under the aggregation's name."""
        return Aggregation("key")

    @staticmethod
    def count(path=None) -> Aggregation:
        """Number of elements (path None), or of present, non-None values at ``path``."""
        return Aggregation("count", path)

    @staticmethod
    def sum(path=None) -> Aggregation:
        """Sum of the numbers at ``path`` (MISSING/None skipped; 0 for none)."""
        return Aggregation("sum", path)

    @staticmethod
    def avg(path=None) -> Aggregation:
        """Mean of the numbers at ``path`` (None when there is none)."""
        return Aggregation("avg", path)

    average = avg

    @staticmethod
    def min(path=None) -> Aggregation:
        """Smallest value at ``path`` by the total order (None when there is none)."""
        return Aggregation("min", path)

    @staticmethod
    def max(path=None) -> Aggregation:
        """Largest value at ``path`` by the total order (None when there is none)."""
        return Aggregation("max", path)

    @staticmethod
    def collect(path=None) -> Aggregation:
        """Tuple of the present, non-None values at ``path`` (the elements for path None)."""
        return Aggregation("collect", path)


def _aggregations(aggregations) -> tuple:
    """``(GroupBy mapping, key names, output order)`` from the user mapping (validated eagerly)."""
    if aggregations is None:
        return {"items": ("collect", None)}, (), None
    if not isinstance(aggregations, Mapping):
        raise ProtoValidationError(f"group_by aggregations must be a mapping, not {aggregations!r}")
    mapping, key_names = {}, []
    for name, spec in aggregations.items():
        if not isinstance(name, str) or not name:
            raise ProtoValidationError(f"Aggregation names must be non-empty str, not {name!r}")
        if isinstance(spec, Aggregation):
            if spec.op == "key":
                key_names.append(name)
                continue
            if spec.op not in AGGREGATES:
                raise ProtoValidationError(f"Unknown aggregation {spec.op!r}")
            mapping[name] = (spec.op, _key(spec.path, "Aggregation"))
        elif isinstance(spec, tuple) and spec:
            mapping[name] = spec  # GroupBy validates the raw (op, path[, description]) form
        else:
            raise ProtoValidationError(f"Aggregation {name!r} must be an Agg helper or an (op, path) tuple, "
                                       f"not {spec!r}")
        if name == "key":
            raise ProtoValidationError("The aggregation name 'key' is reserved for the group key (use Agg.key())")
    return mapping, tuple(key_names), tuple(aggregations)


# -- projections ----------------------------------------------------------------------------------

def _projection(spec):
    """``(fields or function, description or None)`` for a Project node, validated eagerly."""
    if isinstance(spec, (str, Field)):
        path = spec if isinstance(spec, str) else spec._path
        extract = KeySpec.for_term_path(path).extract
        return extract, _render_path(path)
    if isinstance(spec, Computed):
        return spec._compute, spec._label()
    if isinstance(spec, Predicate):
        return spec, spec.describe()
    if isinstance(spec, Mapping):
        if not spec or not all(isinstance(k, str) for k in spec):
            raise ProtoValidationError(f"select needs a non-empty mapping with str names, not {spec!r}")
        if all(isinstance(v, (str, Field)) and (isinstance(v, str) or v._path is not None) for v in spec.values()):
            return {k: (v if isinstance(v, str) else v._path) for k, v in spec.items()}, None
        parts = []
        for name, value in spec.items():
            fn, label = _projection(value)
            if isinstance(fn, dict):
                raise ProtoValidationError(f"select mapping values must be single values, not {value!r}")
            parts.append((name, fn, label))

        def project(element):
            out = {}
            for name, fn, _ in parts:
                value = fn(element)
                if value is not MISSING:
                    out[name] = value
            return out

        return project, "{" + ", ".join(f"{name!r}: {label}" for name, _, label in parts) + "}"
    if isinstance(spec, (list, tuple)):
        paths = []
        for item in spec:
            if isinstance(item, str):
                paths.append(item)
            elif isinstance(item, Field) and item._path is not None:
                paths.append(item._path)
            else:
                raise ProtoValidationError(f"select lists hold paths or fields only (use a mapping for computed "
                                           f"values), not {item!r}")
        if not paths:
            raise ProtoValidationError("select needs at least one field")
        return paths, None
    _reject_builder_method(spec, "select")
    if callable(spec):
        return spec, _describe_callable(spec)
    raise ProtoValidationError(f"select needs a field, a path, a list or mapping of them, a computed value or a "
                               f"callable, not {spec!r}")


# -- the queryable --------------------------------------------------------------------------------

def _count(value, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProtoValidationError(f"{what} needs an int >= 0, not {value!r}")
    return value


def _source(source):
    if isinstance(source, (IndexedCollection, QueryPlan)):
        return source
    return ValuesScan(source)


class Queryable:
    """An immutable, lazy LINQ query over a source (module docstring).

    Every operator returns a new Queryable; the plan is built on execution by
    :meth:`plan`, and :meth:`explain` describes that very plan.
    """

    __slots__ = ("_source", "_ops")

    def __init__(self, source, ops: tuple = ()):
        """Prefer :func:`from_collection`; a Queryable source keeps its source and operators (no copy)."""
        if isinstance(source, Queryable):
            source, ops = source._source, source._ops + tuple(ops)
        object.__setattr__(self, "_source", _source(source))
        object.__setattr__(self, "_ops", ops)

    def __setattr__(self, name, value):
        raise ProtoUserError("Queryable is immutable; every operator returns a new Queryable")

    def __delattr__(self, name):
        raise ProtoUserError("Queryable is immutable; every operator returns a new Queryable")

    def __repr__(self):
        return f"Queryable({type(self._source).__name__}, {[op[0] for op in self._ops]})"

    def _with(self, *op) -> "Queryable":
        new = object.__new__(Queryable)
        object.__setattr__(new, "_source", self._source)
        object.__setattr__(new, "_ops", self._ops + (op,))
        return new

    # -- operators ----------------------------------------------------------------------------

    def where(self, predicate) -> "Queryable":
        """Keep the elements satisfying ``predicate`` (an F predicate, an Expression, list syntax or callable)."""
        predicate = _as_predicate(predicate)
        for part in _split(predicate)[1]:  # pure conjuncts of a top-level AND go to the planner
            _check_no_near(part)
        return self._with("where", predicate)

    def select(self, selector) -> "Queryable":
        """Project each element (module docstring: a path's value, a dict of fields, or a function's result)."""
        return self._with("select", _projection(selector))

    def order_by(self, key, descending: bool = False) -> "Queryable":
        """Sort stably by ``key``; a previous order only breaks the ties of this one."""
        return self._with("order", _key(key, "order_by"), _flag(descending), True)

    def then_by(self, key, descending: bool = False) -> "Queryable":
        """Break the ties of the directly preceding ``order_by``/``then_by`` by ``key``."""
        if not self._ops or self._ops[-1][0] != "order":
            raise ProtoUserError("then_by must directly follow order_by or then_by")
        return self._with("order", _key(key, "then_by"), _flag(descending), False)

    def skip(self, n: int) -> "Queryable":
        """Drop the first ``n`` elements."""
        return self._with("skip", _count(n, "skip"))

    def take(self, n: int) -> "Queryable":
        """Keep at most the first ``n`` elements."""
        return self._with("take", _count(n, "take"))

    def distinct(self) -> "Queryable":
        """Drop repeated elements by exact identity (first occurrence kept)."""
        return self._with("distinct")

    def group_by(self, key, aggregations: Mapping | None = None) -> "Queryable":
        """One dict per group (first-seen order): ``{"key": k, "items": (...)}`` by default, else
        ``{name: aggregate}`` for ``aggregations`` (name -> :class:`Agg` helper or a GroupBy ``(op, path)``),
        with the key under the name of an ``Agg.key()`` entry, or under ``"key"`` when there is none."""
        mapping, key_names, order = _aggregations(aggregations)
        return self._with("group", _key(key, "group_by"), mapping, key_names, order)

    def join(self, other, left_key, right_key, kind: str = "inner") -> "Queryable":
        """Equi-join with ``other`` (any :func:`from_collection` source): ``(left, right)`` pairs."""
        if kind not in JOIN_KINDS:
            raise ProtoValidationError(f"join kind must be one of {JOIN_KINDS}, not {kind!r}")
        return self._with("join", from_collection(other), _key(left_key, "join"), _key(right_key, "join"), kind)

    def select_many(self, path, with_parent: bool = False) -> "Queryable":
        """Flatten the list/tuple/collection at ``path`` (or returned by a callable) of each element."""
        return self._with("unnest", _key(path, "select_many"), _flag(with_parent))

    # -- plan ---------------------------------------------------------------------------------

    def plan(self) -> QueryPlan:
        """Build the physical plan this query runs (module docstring, "Placement")."""
        ops = self._ops
        n, i = len(ops), 0
        predicates, keys = [], []
        while i < n and ops[i][0] in ("where", "order"):
            if ops[i][0] == "where":
                predicates.append(ops[i][1])
            else:
                keys = _add_key(keys, ops[i])
            i += 1
        offset, limit, i = _window(ops, i)
        expr, impure = (None, []) if not predicates else _split(_conjunction(predicates))
        path_keys = all(not callable(k) for k, _ in keys)
        if not impure and path_keys:
            plan = plan_query(self._source, expr, keys or None, limit, offset)
        else:
            # Filtering commutes with a stable sort, so the planner may still order by path keys (an ordered
            # index scan) below the PythonFilters; the window must stay above them.
            plan = plan_query(self._source, expr, (keys or None) if path_keys else None)
            for part in impure:
                plan = PythonFilter(plan, part, part.describe())
            if keys and not path_keys:
                plan = Sort(plan, keys)
            if limit is not None or offset:
                plan = Limit(plan, limit, offset)
        while i < n:
            kind = ops[i][0]
            if kind == "order":
                keys = []
                while i < n and ops[i][0] == "order":
                    keys = _add_key(keys, ops[i])
                    i += 1
                plan = Sort(plan, keys)
                continue
            if kind in ("skip", "take"):
                offset, limit, i = _window(ops, i)
                plan = Limit(plan, limit, offset)
                continue
            plan = _apply(plan, ops[i])
            i += 1
        return plan

    def explain(self) -> dict:
        """The ``explain()`` of :meth:`plan`: exactly the tree that iteration executes."""
        return self.plan().explain()

    # -- terminals ----------------------------------------------------------------------------

    def __iter__(self) -> Iterator:
        return self.plan().execute()

    def to_list(self) -> list:
        """Execute and return the results as a list."""
        return list(self.plan().execute())

    def count(self, predicate=None) -> int:
        """The number of results (of those satisfying ``predicate`` when given), by a ``Count`` plan."""
        query = self if predicate is None else self.where(predicate)
        return next(Count(query.plan()).execute())

    def explain_count(self) -> dict:
        """The ``explain()`` of the plan :meth:`count` runs."""
        return Count(self.plan()).explain()

    def first(self, predicate=None):
        """The first result (satisfying ``predicate`` when given); ``ProtoUserError`` when there is none."""
        query = self if predicate is None else self.where(predicate)
        for element in query.plan().execute():
            return element
        raise ProtoUserError("first() of an empty query; use first_or_none() when no result is expected")

    def first_or_none(self, predicate=None):
        """The first result (satisfying ``predicate`` when given), or None when there is none."""
        query = self if predicate is None else self.where(predicate)
        for element in query.plan().execute():
            return element
        return None


def _flag(value) -> bool:
    if not isinstance(value, bool):
        raise ProtoValidationError(f"Expected a bool, not {value!r}")
    return value


def _conjunction(predicates: list) -> Predicate:
    result = predicates[0]
    for p in predicates[1:]:
        result = result & p
    return result


def _add_key(keys: list, op: tuple) -> list:
    """Compose an order op: ``order_by`` puts its key first (earlier keys break ties), ``then_by`` last."""
    _, key, descending, restart = op
    return [(key, descending)] + keys if restart else keys + [(key, descending)]


def _window(ops: tuple, i: int) -> tuple:
    """``(offset, limit, next index)`` composing the run of skip/take ops starting at ``i`` like slicing."""
    offset, limit = 0, None
    while i < len(ops) and ops[i][0] in ("skip", "take"):
        kind, m = ops[i]
        if kind == "skip":
            offset += m
            if limit is not None:
                limit = max(limit - m, 0)
        else:
            limit = m if limit is None else min(limit, m)
        i += 1
    return offset, limit, i


def _apply(plan: QueryPlan, op: tuple) -> QueryPlan:
    """The plan node(s) of one operator above ``plan``."""
    kind = op[0]
    if kind == "where":
        expr, impure = _split(op[1])
        if expr is not None:
            plan = Filter(plan, expr)
        for part in impure:
            plan = PythonFilter(plan, part, part.describe())
        return plan
    if kind == "select":
        fields, description = op[1]
        if callable(fields):
            return Project(plan, fields, description=description)
        return Project(plan, fields)
    if kind == "distinct":
        return Distinct(plan)
    if kind == "group":
        _, key, mapping, key_names, order = op
        plan = GroupBy(plan, key, mapping)
        if key_names:
            plan = _rename_project(plan, key_names, order)
        return plan
    if kind == "join":
        _, other, left_key, right_key, join_kind = op
        return Join(plan, other.plan(), left_key, right_key, join_kind)
    if kind == "unnest":
        _, path, with_parent = op
        return Unnest(plan, path, with_parent=with_parent)
    raise ProtoValidationError(f"Unknown query operator {kind!r}")


def _rename_project(plan: QueryPlan, key_names: tuple, order: tuple) -> QueryPlan:
    """A function Project copying each GroupBy output by name (no path parsing), the key under ``key_names``."""
    sources = tuple((name, "key" if name in key_names else name) for name in order)

    def rename(group: dict) -> dict:
        return {name: group[source] for name, source in sources if source in group}

    description = "{" + ", ".join(f"{name!r}: {'key' if source == 'key' else repr(source)}"
                                  for name, source in sources) + "}"
    return Project(plan, rename, description=f"rename {description}")


def from_collection(source) -> Queryable:
    """A :class:`Queryable` over ``source`` (module docstring, "Sources"); a Queryable is returned as is."""
    if isinstance(source, Queryable):
        return source
    if isinstance(source, (IndexedCollection, QueryPlan)):
        return Queryable(source)
    if isinstance(source, (str, bytes, bytearray, Mapping)) or not isinstance(source, Iterable):
        raise ProtoValidationError(f"from_collection needs an IndexedCollection, a QueryPlan or an iterable "
                                   f"(not a str, bytes or mapping), not {type(source).__name__}")
    return Queryable(source)
