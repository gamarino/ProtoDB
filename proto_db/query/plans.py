"""Physical query plans (spec section 10, rulings R5 and R19, decisions D29, D30 and D36).

Every plan is a :class:`QueryPlan` with ``execute()`` (a new iterator per call),
``explain()`` and ``estimate()``.  Plans are runtime objects, never persisted
(they are not atoms).  A plan is immutable once built and holds only immutable
inputs (persistent collections and indexes, frozen expressions, the caller's
functions), so it may be executed many times and from several threads at once;
each ``execute()`` call starts from scratch and shares no state with another.
No node catches an exception: an error raised by a source, a predicate or a
user function propagates to the caller (the 0.1 layer returned ``[]``).

``explain()`` returns a JSON-serialisable nested dict describing exactly the
plan ``execute()`` runs::

    {"plan": NodeName, ...node parameters..., "estimate": {"rows": r, "cost": c},
     "children": [child explain, ...]}

Nodes: :class:`CollectionScanPlan` ("CollectionScan"), :class:`ValuesScan` (a
scan of a materialised tuple of values: the LINQ source for a plain Python
iterable, D38), :class:`IndexPlan` (the
single index access node; its ``kind`` names the access path: "IndexLookup",
"IndexRange", "IndexComplement", "IndexOrderedScan", see
:func:`ordered_index_scan`), :class:`VectorSearch`, :class:`Filter`,
:class:`PythonFilter`, :class:`Intersect`, :class:`Union`, :class:`Project`,
:class:`Sort`, :class:`Limit`, :class:`GroupBy`, :class:`Join`,
:class:`Unnest`, :class:`Count` and :class:`Distinct` (D38).

Cost model (D30 units: one per element produced or touched).  A scan costs its
row count; index access paths cost what ``Index.estimate`` says; a filter
touches every source row (cost + source rows) and keeps a fraction of them
estimated per term at :data:`FILTER_SELECTIVITY` (And multiplies, Or is
``1 - prod(1 - s)``, Not is ``1 - s``; a PythonFilter counts as one term);
Union/Intersect touch every row of every source; Sort adds ``n log2 n``;
Limit caps rows; Project, GroupBy, Join and Unnest add one unit per row they
touch; Unnest estimates :data:`UNNEST_FANOUT` items per element; Count
produces one row; a ValuesScan costs its length; Distinct adds one unit per
row and keeps the source row count as an upper bound.

Element identity (Union, Intersect, D36): ``(element_identity(e),
exact_token(e))``, so an atom is identified by oid and a plain value by value
and exact type and payload (``1``, ``1.0`` and ``Decimal("1.00")`` are three
distinct elements, ruling R19).  Results are multisets: occurrences within one
source are kept; Union yields each identity with its maximum multiplicity
over the sources, Intersect with its minimum.
"""
from __future__ import annotations

import math
import threading
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from decimal import Decimal
from itertools import islice

from ..errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from ..values import MISSING, exact_token, is_atom, is_orderable, order_key
from .expressions import And, Expression, Not, Or, evaluate, iter_terms
from .terms import NEAR, Term

__all__ = ["Estimate", "QueryPlan", "CollectionScanPlan", "ValuesScan", "Distinct", "IndexPlan",
           "ordered_index_scan", "VectorSearch", "Filter", "PythonFilter", "Intersect", "Union", "Project",
           "Sort", "Limit", "GroupBy", "Join", "Unnest", "Count", "JOIN_KINDS", "FILTER_SELECTIVITY",
           "UNNEST_FANOUT", "AGGREGATES", "element_key"]

#: Estimated fraction of rows one term (or one PythonFilter) keeps (D36).
FILTER_SELECTIVITY = 0.5
#: Estimated items per element produced by :class:`Unnest` (D36).
UNNEST_FANOUT = 4
#: Built-in aggregation operators of :class:`GroupBy`.
AGGREGATES = frozenset({"count", "sum", "min", "max", "avg", "collect"})


@dataclass(frozen=True)
class Estimate:
    """Planner estimate of a plan: expected ``rows`` produced and abstract ``cost`` (both finite, >= 0)."""

    rows: float
    cost: float

    def __post_init__(self):
        for name in ("rows", "cost"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ProtoValidationError(f"Estimate.{name} must be a number, not {type(value).__name__}")
            if not math.isfinite(value) or value < 0:
                raise ProtoValidationError(f"Estimate.{name} must be finite and non-negative, not {value!r}")
            object.__setattr__(self, name, float(value))

    def describe(self) -> dict:
        """``{"rows", "cost"}`` for ``explain()``."""
        return {"rows": self.rows, "cost": self.cost}


def element_key(element) -> tuple:
    """The identity of a result element for Union/Intersect (module docstring, D36)."""
    from ..indexing.base import element_identity  # deferred: indexing.base imports this module
    return element_identity(element), exact_token(element)


class QueryPlan(ABC):
    """An executable physical plan.

    ``explain()`` describes exactly the plan that ``execute()`` runs.  Concrete
    plans are immutable after construction: assigning an attribute raises
    ``ProtoUserError``.
    """

    __frozen = False

    def _freeze(self) -> None:
        """End construction: later attribute assignments raise ``ProtoUserError``."""
        object.__setattr__(self, "_QueryPlan__frozen", True)

    def __setattr__(self, name, value):
        if self.__frozen:
            raise ProtoUserError(f"{type(self).__name__} plans are immutable; build a new plan instead")
        object.__setattr__(self, name, value)

    def __delattr__(self, name):
        raise ProtoUserError(f"{type(self).__name__} plans are immutable; build a new plan instead")

    @abstractmethod
    def execute(self) -> Iterator:
        """Return a new iterator over the plan's result elements."""

    @abstractmethod
    def explain(self) -> dict:
        """Return a JSON-friendly description of this plan."""

    @abstractmethod
    def estimate(self) -> Estimate:
        """Return the planner's estimate for this plan."""

    # -- rank access (D56); optional, the default is "not offered" ----------------------------

    def window(self, start: int, stop: int | None = None) -> Iterator | None:
        """Rows ``start .. stop-1`` of ``execute()`` (``stop=None``: to the end) without producing
        the rows before ``start``, or ``None`` when this plan cannot seek (the default).

        A plan that returns an iterator guarantees it equals ``islice(execute(), start, stop)``;
        index and positional plans seek in O(log n) (D56)."""
        return None

    def exact_rows(self) -> int | None:
        """The exact number of rows ``execute()`` yields, when known without producing them;
        ``None`` (the default) otherwise."""
        return None

    def _explain(self, name: str, params: dict | None = None, children: tuple = ()) -> dict:
        """The common explain shape: ``{"plan", **params, "estimate", "children"}``."""
        top = getattr(_explain_memo, "cache", None) is None
        if top:
            _explain_memo.cache = {}
        try:
            return {"plan": name, **(params or {}), "estimate": _est(self).describe(),
                    "children": [child.explain() for child in children]}
        finally:
            if top:
                _explain_memo.cache = None


#: Per-thread estimate memo, active during one top-level ``explain()`` call, so every node's
#: estimate is computed once per explain instead of once per ancestor.
_explain_memo = threading.local()


def _est(plan: QueryPlan) -> Estimate:
    """``plan.estimate()``, memoised for the duration of the current top-level ``explain()``."""
    cache = getattr(_explain_memo, "cache", None)
    if cache is None:
        return plan.estimate()
    hit = cache.get(id(plan))
    if hit is None:
        hit = cache[id(plan)] = (plan, plan.estimate())  # the plan is kept so its id stays unique
    return hit[1]


def _check_window(start, stop) -> tuple:
    if type(start) is not int or start < 0:
        raise ProtoValidationError(f"A window start must be an int >= 0, not {start!r}")
    if stop is not None and (type(stop) is not int or stop < 0):
        raise ProtoValidationError(f"A window stop must be an int >= 0 or None, not {stop!r}")
    return start, stop


def _check_plan(plan, owner: str) -> QueryPlan:
    if not isinstance(plan, QueryPlan):
        raise ProtoValidationError(f"{owner} needs a QueryPlan source, not {type(plan).__name__}")
    return plan


def _describe_function(fn, description, owner: str) -> str:
    if not callable(fn):
        raise ProtoValidationError(f"{owner} needs a callable, not {fn!r}")
    if description is None:
        return getattr(fn, "__qualname__", None) or repr(fn)
    if not isinstance(description, str) or not description:
        raise ProtoValidationError(f"{owner} description must be a non-empty str, not {description!r}")
    return description


def _key_function(key, description, owner: str) -> tuple:
    """``(extract, described)`` for a key given as a path (str or None), a KeySpec or a callable."""
    from ..indexing.keyspec import KeySpec
    if isinstance(key, KeySpec):
        return key.extract, key.describe()
    if key is None or isinstance(key, str):
        spec = KeySpec.for_term_path(key)
        return spec.extract, spec.describe()
    return key, _describe_function(key, description, owner)


class CollectionScanPlan(QueryPlan):
    """Full scan of a collection; carries the collection so the planner can consult its indexes."""

    def __init__(self, collection):
        self.collection = collection
        self._freeze()

    def execute(self) -> Iterator:
        """Yield every element of the collection (``collection.elements()``)."""
        return self.collection.elements()

    def explain(self) -> dict:
        """``{"plan": "CollectionScan", "collection", "rows", "indexes", "estimate", "children": []}``."""
        c = self.collection
        return self._explain("CollectionScan", {"collection": type(c).__name__, "rows": c.count,
                                                "indexes": c.index_names()})

    def estimate(self) -> Estimate:
        """A scan produces and touches every element: rows = cost = count."""
        n = self.collection.count
        return Estimate(rows=n, cost=n)

    def window(self, start: int, stop: int | None = None) -> Iterator | None:
        """Rows ``start .. stop-1`` when the collection can seek (List, Dictionary), else ``None``."""
        start, stop = _check_window(start, stop)
        return self.collection._scan_window(start, stop)

    def exact_rows(self) -> int | None:
        """``count`` when the collection can seek (its count is then its number of elements)."""
        return self.collection.count if self.collection._scan_window(0, 0) is not None else None


class ValuesScan(QueryPlan):
    """Scan of a materialised tuple of plain values (D38): no collection, so no index.

    ``values`` is any iterable; it is copied into a tuple once, at
    construction, so every ``execute()`` yields the same elements in the same
    order.  The tuple holds references: the values must not be mutated
    afterwards (they are not copied).
    """

    def __init__(self, values):
        if isinstance(values, (str, bytes, bytearray, Mapping)) or not isinstance(values, Iterable):
            raise ProtoValidationError(f"ValuesScan needs an iterable of values (not a str, bytes or mapping), "
                                       f"not {type(values).__name__}")
        self.values = tuple(values)
        self._freeze()

    def execute(self) -> Iterator:
        """Yield every value, in order."""
        return iter(self.values)

    def explain(self) -> dict:
        """``{"plan": "ValuesScan", "rows", "estimate", "children": []}``."""
        return self._explain("ValuesScan", {"rows": len(self.values)})

    def estimate(self) -> Estimate:
        """rows = cost = number of values."""
        n = len(self.values)
        return Estimate(rows=n, cost=n)


class IndexPlan(QueryPlan):
    """Elements produced by ``index``: those matching ``term`` (``Index.plan``) or, with ``term=None``,
    every element in key order (``Index.ordered_plan``).

    The single index access node of the plan vocabulary (D36): ``kind`` names
    the access path in ``explain()`` (``"IndexLookup"``, ``"IndexRange"``,
    ``"IndexComplement"``, ``"IndexOrderedScan"``, or a kind of a custom
    index).  ``producer()`` must return a fresh iterator over the elements on
    every call (each occurrence once) and capture only immutable state, so the
    plan can be executed several times and from several threads.
    ``estimate`` overrides ``index.estimate(term)`` and is required when
    ``term`` is None; ``details`` adds entries to ``explain()``.  ``rank``, a pair
    ``(window(start, stop), rows())``, offers rank access (D56): ``window`` must yield
    exactly ``islice(execute(), start, stop)`` and ``rows`` the exact row count.
    """

    def __init__(self, index, term, producer: Callable[[], Iterator], *, kind: str = "IndexLookup",
                 estimate: Estimate | None = None, details: dict | None = None,
                 rank: tuple | None = None):
        if not callable(producer):
            raise ProtoValidationError(f"IndexPlan needs a callable producer, not {type(producer).__name__}")
        if not isinstance(kind, str) or not kind:
            raise ProtoValidationError(f"IndexPlan kind must be a non-empty str, not {kind!r}")
        if estimate is not None and not isinstance(estimate, Estimate):
            raise ProtoValidationError(f"IndexPlan estimate must be an Estimate, not {type(estimate).__name__}")
        if term is None and estimate is None:
            raise ProtoValidationError("An IndexPlan without a term needs an explicit estimate")
        details = dict(details or {})
        clash = sorted({"plan", "index", "term", "estimate", "children"} & set(details))
        if clash:
            raise ProtoValidationError(f"IndexPlan details may not redefine {clash}")
        self.index = index
        self.term = term
        self.kind = kind
        self._producer = producer
        self._estimate = estimate
        self._details = details
        if rank is not None and (not isinstance(rank, tuple) or len(rank) != 2
                                 or not all(callable(f) for f in rank)):
            raise ProtoValidationError("IndexPlan rank must be a (window(start, stop), rows()) pair of callables")
        self._rank = rank
        self._freeze()

    def execute(self) -> Iterator:
        """Return a new iterator over the plan's elements."""
        return iter(self._producer())

    def window(self, start: int, stop: int | None = None) -> Iterator | None:
        """Rows ``start .. stop-1`` by rank (D56) when the index offers rank access, else ``None``."""
        if self._rank is None:
            return None
        start, stop = _check_window(start, stop)
        return self._rank[0](start, stop)

    def exact_rows(self) -> int | None:
        """The exact row count when the index offers rank access, else ``None``."""
        return None if self._rank is None else self._rank[1]()

    def explain(self) -> dict:
        """``{"plan": kind, "index": index.explain(), "term": term.describe() or None, **details,
        "estimate", "children": []}``."""
        term = None if self.term is None else self.term.describe()
        return self._explain(self.kind, {"index": self.index.explain(), "term": term, **self._details})

    def estimate(self) -> Estimate:
        """The explicit estimate, else the index's estimate for the term."""
        return self._estimate if self._estimate is not None else self.index.estimate(self.term)


def ordered_index_scan(index, key, descending: bool = False) -> QueryPlan:
    """Every element of ``index`` ordered by ``key`` (a path or KeySpec): ``index.ordered_plan(descending)``.

    Raises ``ProtoNotSupportedError`` unless ``index.supports_order(key)``.
    The order is the index's (ascending: MISSING and None first, unorderable
    keys last, R18; descending: the exact reverse).
    """
    from ..indexing.keyspec import KeySpec
    spec = key if isinstance(key, KeySpec) else KeySpec.for_term_path(key)
    if not isinstance(descending, bool):
        raise ProtoValidationError(f"descending must be a bool, not {descending!r}")
    if not index.supports_order(spec):
        raise ProtoNotSupportedError(f"Index {index.explain().get('index')!r} cannot iterate in the order of "
                                     f"{spec.describe()}")
    return _check_plan(index.ordered_plan(descending), "ordered_index_scan")


class VectorSearch(QueryPlan):
    """The elements a vector-capable index returns for a ``near[]`` term, optionally among those
    satisfying a residual predicate ``where`` (ruling R27).

    Works with any index whose ``supports(term)`` accepts the term: the node
    runs ``index.plan(term)``, or ``index.filtered_plan(term, where)`` when
    ``where`` (an Expression or the 0.1 list syntax, without ``near[]``) is
    given, which then requires ``index.supports_filtered(term)``
    (``ProtoNotSupportedError`` otherwise).  ``near(k) AND where`` is the k
    nearest among the elements satisfying ``where``, not a post-filtered kNN.
    The index plan is its only child in ``explain()``; ``where`` is shown.
    """

    def __init__(self, index, term: Term, where=None):
        if not isinstance(term, Term) or term.op != NEAR:
            raise ProtoValidationError(f"VectorSearch needs a 'near[]' term, not {term!r}")
        if not index.supports(term):
            raise ProtoNotSupportedError(f"Index {index.explain().get('index')!r} does not support {term!r}")
        if where is not None:
            where = Expression.compile(where)
            near = [t for t in iter_terms(where) if t.op == NEAR]
            if near:
                raise ProtoValidationError(f"A VectorSearch residual predicate cannot hold 'near[]' terms "
                                           f"({near[0]!r})")
            if not index.supports_filtered(term):
                raise ProtoNotSupportedError(f"Index {index.explain().get('index')!r} cannot answer {term!r} "
                                             f"restricted to a predicate")
        self.index = index
        self.term = term
        self.where = where
        inner = index.plan(term) if where is None else index.filtered_plan(term, where)
        self.inner = _check_plan(inner, "VectorSearch")
        self._freeze()

    def execute(self) -> Iterator:
        """The inner index plan's elements."""
        return self.inner.execute()

    def explain(self) -> dict:
        """``{"plan": "VectorSearch", "index", "term", ["where",] "estimate", "children": [index plan]}``."""
        params = {"index": self.index.explain(), "term": self.term.describe()}
        if self.where is not None:
            params["where"] = self.where.describe()
        return self._explain("VectorSearch", params, (self.inner,))

    def estimate(self) -> Estimate:
        """The inner plan's estimate."""
        return _est(self.inner)


def _selectivity(expr: Expression) -> float:
    if isinstance(expr, And):
        return math.prod(_selectivity(o) for o in expr.operands)
    if isinstance(expr, Or):
        return 1 - math.prod(1 - _selectivity(o) for o in expr.operands)
    if isinstance(expr, Not):
        return 1 - _selectivity(expr.operand)
    return FILTER_SELECTIVITY


class Filter(QueryPlan):
    """The source elements for which ``evaluate(expr, element)`` is True.

    ``expr`` is an Expression or the 0.1 list syntax (compiled).  A ``near[]``
    term has no per-element semantics, so an expression holding one is
    rejected with ``ProtoValidationError`` (use :class:`VectorSearch`).
    """

    def __init__(self, source: QueryPlan, expr):
        expr = Expression.compile(expr)
        near = [t for t in iter_terms(expr) if t.op == NEAR]
        if near:
            raise ProtoValidationError(f"A Filter cannot evaluate 'near[]' terms ({near[0]!r}); "
                                       f"use a VectorSearch plan")
        self.source = _check_plan(source, "Filter")
        self.expr = expr
        self._freeze()

    def execute(self) -> Iterator:
        """Stream the matching source elements."""
        expr = self.expr
        return (e for e in self.source.execute() if evaluate(expr, e))

    def explain(self) -> dict:
        """``{"plan": "Filter", "predicate": expr.describe(), ...}``."""
        return self._explain("Filter", {"predicate": self.expr.describe()}, (self.source,))

    def estimate(self) -> Estimate:
        """rows = source rows * selectivity(expr); cost = source cost + source rows."""
        src = _est(self.source)
        return Estimate(rows=src.rows * _selectivity(self.expr), cost=src.cost + src.rows)


class PythonFilter(QueryPlan):
    """The source elements for which ``fn(element)`` is truthy: an explicit, untranslatable predicate.

    ``description`` (a non-empty str) is shown in ``explain()``.  Exceptions
    raised by ``fn`` propagate.
    """

    def __init__(self, source: QueryPlan, fn: Callable, description: str):
        if not callable(fn):
            raise ProtoValidationError(f"PythonFilter needs a callable, not {fn!r}")
        if not isinstance(description, str) or not description:
            raise ProtoValidationError(f"PythonFilter needs a non-empty description, not {description!r}")
        self.source = _check_plan(source, "PythonFilter")
        self.fn = fn
        self.description = description
        self._freeze()

    def execute(self) -> Iterator:
        """Stream the source elements ``fn`` accepts."""
        fn = self.fn
        return (e for e in self.source.execute() if fn(e))

    def explain(self) -> dict:
        """``{"plan": "PythonFilter", "description", ...}``."""
        return self._explain("PythonFilter", {"description": self.description}, (self.source,))

    def estimate(self) -> Estimate:
        """Like a one-term Filter."""
        src = _est(self.source)
        return Estimate(rows=src.rows * FILTER_SELECTIVITY, cost=src.cost + src.rows)


class _SetPlan(QueryPlan):
    """Shared construction of Union and Intersect (two or more source plans)."""

    def __init__(self, *sources: QueryPlan):
        name = type(self).__name__
        if len(sources) < 2:
            raise ProtoValidationError(f"{name} needs at least two source plans, got {len(sources)}")
        self.sources = tuple(_check_plan(s, name) for s in sources)
        self._freeze()

    def explain(self) -> dict:
        """``{"plan": name, "estimate", "children": [sources]}``."""
        return self._explain(type(self).__name__, {}, self.sources)


class Union(_SetPlan):
    """Multiset union by element identity: each identity with its maximum multiplicity over the sources.

    Streams: every occurrence of the first source, then from each later
    source the occurrences beyond those already produced for that identity.
    """

    def execute(self) -> Iterator:
        """Stream the union (first-seen order)."""
        sources = self.sources

        def produce():
            produced: Counter = Counter()
            for source in sources:
                seen: Counter = Counter()
                for element in source.execute():
                    key = element_key(element)
                    seen[key] += 1
                    if seen[key] > produced[key]:
                        produced[key] = seen[key]
                        yield element

        return produce()

    def estimate(self) -> Estimate:
        """rows = sum of source rows (upper bound); cost = sum of costs + sum of rows."""
        ests = [_est(s) for s in self.sources]
        rows = sum(e.rows for e in ests)
        return Estimate(rows=rows, cost=sum(e.cost for e in ests) + rows)


class Intersect(_SetPlan):
    """Multiset intersection by element identity: each identity with its minimum multiplicity.

    The later sources are counted first; the first source is then streamed,
    so results follow its order.
    """

    def execute(self) -> Iterator:
        """Stream the intersection (order of the first source)."""
        sources = self.sources

        def produce():
            counts = [Counter(element_key(e) for e in s.execute()) for s in sources[1:]]
            seen: Counter = Counter()
            for element in sources[0].execute():
                key = element_key(element)
                seen[key] += 1
                if all(seen[key] <= c[key] for c in counts):
                    yield element

        return produce()

    def estimate(self) -> Estimate:
        """rows = min of source rows; cost = sum of costs + sum of rows."""
        ests = [_est(s) for s in self.sources]
        return Estimate(rows=min(e.rows for e in ests), cost=sum(e.cost + e.rows for e in ests))


class Project(QueryPlan):
    """Map each source element to a dict of fields or through a function.

    ``fields``: a sequence of paths (output names = the paths) or a mapping
    output name -> path; a MISSING value leaves its name out of that dict
    (absence is not a value).  Or a callable ``fn(element)``, shown in
    ``explain()`` by ``description`` (default: its qualified name).
    """

    def __init__(self, source: QueryPlan, fields, *, description: str | None = None):
        self.source = _check_plan(source, "Project")
        if callable(fields) and not isinstance(fields, (str, Mapping)):
            self.fn = fields
            self.function = _describe_function(fields, description, "Project")
            self.fields = None
            self._extractors = None
        else:
            if isinstance(fields, Mapping):
                mapping = dict(fields)
            elif isinstance(fields, (list, tuple)):
                mapping = {f: f for f in fields}
            else:
                raise ProtoValidationError(f"Project needs a sequence of paths, a mapping or a callable, "
                                           f"not {fields!r}")
            if not mapping or not all(isinstance(k, str) and isinstance(v, str) for k, v in mapping.items()):
                raise ProtoValidationError(f"Project needs at least one field, names and paths as str, "
                                           f"not {fields!r}")
            self.fn = None
            self.function = None
            self.fields = mapping
            self._extractors = tuple((name, _key_function(path, None, "Project")[0])
                                     for name, path in mapping.items())
        self._freeze()

    def execute(self) -> Iterator:
        """Stream the projected values."""
        if self.fn is not None:
            return map(self.fn, self.source.execute())
        extractors = self._extractors

        def project(element):
            out = {}
            for name, extract in extractors:
                value = extract(element)
                if value is not MISSING:
                    out[name] = value
            return out

        return map(project, self.source.execute())

    def explain(self) -> dict:
        """``{"plan": "Project", "fields": mapping}`` or ``{"plan": "Project", "function": description}``."""
        params = {"fields": dict(self.fields)} if self.fn is None else {"function": self.function}
        return self._explain("Project", params, (self.source,))

    def estimate(self) -> Estimate:
        """rows = source rows; cost = source cost + rows."""
        src = _est(self.source)
        return Estimate(rows=src.rows, cost=src.cost + src.rows)


def _sort_slot(value) -> tuple:
    """Ascending sort slot: orderable values by ``order_key``, then every unorderable value (R18)."""
    return (0, order_key(value)) if is_orderable(value) else (1,)


class Sort(QueryPlan):
    """The source elements sorted by ``keys``: a list of ``key``, ``(key, descending)`` or
    ``(key, descending, description)``.

    A key is a path (str or None), a KeySpec or a callable.  Each callable is
    named in ``explain()`` by its own ``description`` (third tuple item), else
    by the ``description`` keyword when it is the only callable key (several
    callables cannot share one label: ``ProtoValidationError``), else by its
    qualified name.  The order is
    total: :func:`order_key` across type ranks (MISSING, then None, bool,
    numbers, ...), unorderable keys (lists, dicts, NaN) last and equal to one
    another (R18).  The sort is stable; a descending key reverses the order of
    distinct keys (unorderable first) while ties keep their input order.
    """

    def __init__(self, source: QueryPlan, keys, *, description: str | None = None):
        self.source = _check_plan(source, "Sort")
        if not isinstance(keys, (list, tuple)) or not keys:
            raise ProtoValidationError(f"Sort needs a non-empty list of keys, not {keys!r}")
        items = []
        for item in keys:
            if isinstance(item, tuple) and len(item) not in (2, 3):
                raise ProtoValidationError(f"Sort keys are key, (key, descending) or (key, descending, "
                                           f"description), not {item!r}")
            items.append(tuple(item) + (None,) * (3 - len(item)) if isinstance(item, tuple) else (item, False, None))
        callables = [key for key, _, own in items if callable(key) and own is None]
        if description is not None and len(callables) != 1:
            raise ProtoValidationError(f"Sort description labels exactly one callable key without its own "
                                       f"description; found {len(callables)}")
        if any(own is not None and not callable(key) for key, _, own in items):
            raise ProtoValidationError("Sort key descriptions label callable keys only")
        parsed = []
        for key, descending, own in items:
            if not isinstance(descending, bool):
                raise ProtoValidationError(f"Sort descending flag must be a bool, not {descending!r}")
            extract, described = _key_function(key, own if own is not None else description, "Sort")
            parsed.append((extract, described, descending))
        self._keys = tuple(parsed)
        self.keys = tuple((described, descending) for _, described, descending in parsed)
        self._freeze()

    def execute(self) -> Iterator:
        """Materialise the source, sort it (one stable pass per key, last key first) and stream it."""
        keys = self._keys

        def produce():
            rows = [(e, [_sort_slot(extract(e)) for extract, _, _ in keys]) for e in self.source.execute()]
            for i in range(len(keys) - 1, -1, -1):
                rows.sort(key=lambda row: row[1][i], reverse=keys[i][2])
            for element, _ in rows:
                yield element

        return produce()

    def explain(self) -> dict:
        """``{"plan": "Sort", "keys": [{"key", "descending"}]}``."""
        return self._explain("Sort", {"keys": [{"key": k, "descending": d} for k, d in self.keys]},
                             (self.source,))

    def estimate(self) -> Estimate:
        """rows = n; cost = source cost + n log2 n."""
        src = _est(self.source)
        n = src.rows
        return Estimate(rows=n, cost=src.cost + (n * math.log2(n) if n > 1 else 0))


def _check_count(value, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProtoValidationError(f"Limit {what} must be an int >= 0, not {value!r}")
    return value


class Limit(QueryPlan):
    """At most ``n`` source elements after skipping ``offset`` (``itertools.islice``).

    ``n=None`` means no upper bound (an offset-only window, D37).
    """

    def __init__(self, source: QueryPlan, n: int | None, offset: int = 0):
        self.source = _check_plan(source, "Limit")
        self.n = None if n is None else _check_count(n, "n")
        self.offset = _check_count(offset, "offset")
        self._freeze()

    def execute(self) -> Iterator:
        """Stream the window: by rank when the source can seek (D56), else by skipping rows."""
        stop = None if self.n is None else self.offset + self.n
        seek = self.source.window(self.offset, stop)
        return seek if seek is not None else islice(self.source.execute(), self.offset, stop)

    def window(self, start: int, stop: int | None = None) -> Iterator | None:
        """Rows ``start .. stop-1`` of this window, by the source's rank access (``None`` without it)."""
        start, stop = _check_window(start, stop)
        end = self.n if stop is None else (stop if self.n is None else min(stop, self.n))
        if end is not None and end <= start:
            return iter(()) if self.source.window(0, 0) is not None else None
        return self.source.window(self.offset + start, None if end is None else self.offset + end)

    def exact_rows(self) -> int | None:
        """``min(n, max(source rows - offset, 0))`` when the source's row count is exact."""
        rows = self.source.exact_rows()
        if rows is None:
            return None
        rows = max(rows - self.offset, 0)
        return rows if self.n is None else min(rows, self.n)

    def explain(self) -> dict:
        """``{"plan": "Limit", "limit" (None when unbounded), "offset"}``."""
        return self._explain("Limit", {"limit": self.n, "offset": self.offset}, (self.source,))

    def estimate(self) -> Estimate:
        """rows = min(n, max(source rows - offset, 0)); cost = source cost (an upper bound)."""
        src = _est(self.source)
        rows = max(src.rows - self.offset, 0)
        return Estimate(rows=rows if self.n is None else min(self.n, rows), cost=src.cost)


class Count(QueryPlan):
    """A single int: the number of source elements."""

    def __init__(self, source: QueryPlan):
        self.source = _check_plan(source, "Count")
        self._freeze()

    def execute(self) -> Iterator:
        """An iterator over one int."""
        source = self.source

        def produce():
            rows = source.exact_rows()
            yield sum(1 for _ in source.execute()) if rows is None else rows

        return produce()

    def explain(self) -> dict:
        """``{"plan": "Count"}``."""
        return self._explain("Count", {}, (self.source,))

    def estimate(self) -> Estimate:
        """rows = 1; cost = source cost."""
        return Estimate(rows=1, cost=_est(self.source).cost)


class Distinct(QueryPlan):
    """The source elements without repetitions, first occurrence kept, by :func:`element_key` (D38).

    Identity is ``(element_identity, exact_token)`` as for Union/Intersect
    (ruling R19): ``1``, ``1.0``, ``Decimal("1")`` and ``True`` stay four
    elements; atoms are identified by oid; lists, tuples and dicts by content
    (a dict's token follows its insertion order).  A value
    :func:`element_identity` cannot identify raises ``ProtoValidationError``.
    """

    def __init__(self, source: QueryPlan):
        self.source = _check_plan(source, "Distinct")
        self._freeze()

    def execute(self) -> Iterator:
        """Stream the first occurrence of each element."""
        source = self.source

        def produce():
            seen = set()
            for element in source.execute():
                key = element_key(element)
                if key not in seen:
                    seen.add(key)
                    yield element

        return produce()

    def explain(self) -> dict:
        """``{"plan": "Distinct"}``."""
        return self._explain("Distinct", {}, (self.source,))

    def estimate(self) -> Estimate:
        """rows = source rows (upper bound); cost = source cost + source rows."""
        src = _est(self.source)
        return Estimate(rows=src.rows, cost=src.cost + src.rows)


def _present(values) -> list:
    return [v for v in values if v is not MISSING and v is not None]


def _numbers(values, op: str) -> list:
    values = _present(values)
    for v in values:
        if isinstance(v, bool) or not isinstance(v, (int, float, Decimal)):
            raise ProtoValidationError(f"Aggregation {op!r} needs number values, got {v!r}")
    if any(isinstance(v, float) for v in values) and any(isinstance(v, Decimal) for v in values):
        raise ProtoValidationError(f"Aggregation {op!r} cannot mix float and Decimal values")
    return values


def _aggregate(op, values: list):
    if callable(op):
        return op([v for v in values if v is not MISSING])
    if op == "count":
        return len(_present(values))
    if op == "collect":
        return tuple(_present(values))
    if op in ("min", "max"):
        present = _present(values)
        if not present:
            return None
        return (min if op == "min" else max)(present, key=order_key)
    numbers = _numbers(values, op)
    if op == "sum":
        return sum(numbers)
    return sum(numbers) / len(numbers) if numbers else None  # avg


class GroupBy(QueryPlan):
    """One dict per group: ``{"key": group key, name: aggregate, ...}``, groups in first-seen order.

    ``key``: path (str or None = the element), KeySpec or callable.  Keys are
    grouped by ``element_identity`` (``1`` and ``1.0`` share a group whose key
    is the first value seen; lists and dicts group by content); MISSING keys
    form their own group (key ``MISSING``), apart from None.
    ``aggregations``: mapping name -> ``(op, path)`` where ``op`` is one of
    :data:`AGGREGATES` or a callable receiving the group's list of values
    (MISSING removed) and ``path`` is a path or None (the element); a callable
    may be labelled for ``explain()`` as ``(callable, path, description)``
    (default: its qualified name).  A callable ``key`` is labelled by
    ``description`` (default: its qualified name).
    ``count`` with path None counts elements; the built-ins skip MISSING and
    None values; ``min``/``max`` use :func:`order_key` (an unorderable value
    raises), ``sum``/``avg`` need numbers (bool rejected, float and Decimal
    not mixed); ``min``/``max``/``avg`` of no value are None, ``sum`` 0.
    """

    def __init__(self, source: QueryPlan, key, aggregations: Mapping, *, description: str | None = None):
        self.source = _check_plan(source, "GroupBy")
        self._extract, self.key = _key_function(key, description, "GroupBy")
        if not isinstance(aggregations, Mapping):
            raise ProtoValidationError(f"GroupBy aggregations must be a mapping, not {aggregations!r}")
        parsed, described = [], {}
        for name, spec in aggregations.items():
            if not isinstance(name, str) or not name or name == "key":
                raise ProtoValidationError(f"GroupBy aggregation names must be non-empty str other than "
                                           f"'key', not {name!r}")
            valid = isinstance(spec, tuple) and (len(spec) == 2 or (len(spec) == 3 and callable(spec[0])))
            if not valid:
                raise ProtoValidationError(f"GroupBy aggregation {name!r} must be (op, path) or "
                                           f"(callable, path, description), not {spec!r}")
            op, path = spec[0], spec[1]
            if callable(op):
                op_name = _describe_function(op, spec[2] if len(spec) == 3 else None, "GroupBy")
            elif op in AGGREGATES:
                op_name = op
            else:
                raise ProtoValidationError(f"Unknown aggregation {op!r}; expected one of {sorted(AGGREGATES)} "
                                           f"or a callable")
            extract, path_name = _key_function(path, None, "GroupBy")
            parsed.append((name, op, path is None and op == "count", extract))
            described[name] = {"op": op_name, "path": path if isinstance(path, str) else path_name}
        self._aggregations = tuple(parsed)
        self.aggregations = described
        self._freeze()

    def execute(self) -> Iterator:
        """Materialise the groups, then stream one dict per group."""
        extract, aggregations = self._extract, self._aggregations

        def produce():
            from ..indexing.base import element_identity
            groups: dict = {}
            for element in self.source.execute():
                k = extract(element)
                ident = ("missing",) if k is MISSING else element_identity(k)
                entry = groups.get(ident)
                if entry is None:
                    groups[ident] = entry = (k, [])
                entry[1].append(element)
            for k, members in groups.values():
                out = {"key": k}
                for name, op, count_rows, value_of in aggregations:
                    out[name] = len(members) if count_rows else _aggregate(op, [value_of(m) for m in members])
                yield out

        return produce()

    def explain(self) -> dict:
        """``{"plan": "GroupBy", "key", "aggregations": {name: {"op", "path"}}}``."""
        return self._explain("GroupBy", {"key": self.key, "aggregations": {k: dict(v) for k, v in
                                                                           self.aggregations.items()}},
                             (self.source,))

    def estimate(self) -> Estimate:
        """rows = source rows (upper bound: the number of distinct keys is unknown); cost = source cost + rows."""
        src = _est(self.source)
        return Estimate(rows=src.rows, cost=src.cost + src.rows)


JOIN_KINDS = ("inner", "left")


class Join(QueryPlan):
    """Hash equi-join of ``left`` and ``right``: pairs ``(l, r)`` whose keys are equal.

    Keys (path, KeySpec or callable) compare like ``==`` terms (D28): by
    ``values_equal``, None equals None, while MISSING and unorderable keys
    never match.  Output follows the left order, matches in right order.
    ``kind="left"`` also yields ``(l, MISSING)`` for a left element without
    match (MISSING, not None, so it cannot be confused with a None element).
    The right source is materialised once per ``execute()``.
    """

    def __init__(self, left: QueryPlan, right: QueryPlan, left_key, right_key, kind: str = "inner"):
        if kind not in JOIN_KINDS:
            raise ProtoValidationError(f"Join kind must be one of {JOIN_KINDS}, not {kind!r}")
        self.left = _check_plan(left, "Join")
        self.right = _check_plan(right, "Join")
        self._left_key, self.left_key = _key_function(left_key, None, "Join")
        self._right_key, self.right_key = _key_function(right_key, None, "Join")
        self.kind = kind
        self._freeze()

    def execute(self) -> Iterator:
        """Stream the joined pairs."""
        lkey, rkey, outer = self._left_key, self._right_key, self.kind == "left"

        def joinable(k) -> bool:
            return k is not MISSING and is_orderable(k)

        def produce():
            table: dict = {}
            for r in self.right.execute():
                k = rkey(r)
                if joinable(k):
                    table.setdefault(order_key(k), []).append(r)
            for l in self.left.execute():
                k = lkey(l)
                matches = table.get(order_key(k), ()) if joinable(k) else ()
                for r in matches:
                    yield (l, r)
                if outer and not matches:
                    yield (l, MISSING)

        return produce()

    def explain(self) -> dict:
        """``{"plan": "Join", "kind", "left_key", "right_key", "children": [left, right]}``."""
        return self._explain("Join", {"kind": self.kind, "left_key": self.left_key, "right_key": self.right_key},
                             (self.left, self.right))

    def estimate(self) -> Estimate:
        """rows = left rows (one match per left element assumed); cost = both costs + both row counts."""
        left, right = _est(self.left), _est(self.right)
        return Estimate(rows=left.rows, cost=left.cost + right.cost + left.rows + right.rows)


class Unnest(QueryPlan):
    """The items of the list or tuple at ``path`` of each source element (or ``(element, item)`` pairs).

    A MISSING or None value yields nothing; a collection atom yields its
    ``elements()``; any other value raises ``ProtoValidationError``.
    """

    def __init__(self, source: QueryPlan, path, *, with_parent: bool = False):
        self.source = _check_plan(source, "Unnest")
        self._extract, self.path = _key_function(path, None, "Unnest")
        if not isinstance(with_parent, bool):
            raise ProtoValidationError(f"Unnest with_parent must be a bool, not {with_parent!r}")
        self.with_parent = with_parent
        self._freeze()

    def execute(self) -> Iterator:
        """Stream the items."""
        extract, path, with_parent = self._extract, self.path, self.with_parent

        def produce():
            for element in self.source.execute():
                value = extract(element)
                if value is MISSING or value is None:
                    continue
                if isinstance(value, (list, tuple)):
                    items = value
                elif is_atom(value) and callable(getattr(value, "elements", None)):
                    items = value.elements()
                else:
                    raise ProtoValidationError(f"Unnest of {path!r} needs a list or tuple (or a collection), "
                                               f"got {type(value).__name__}")
                for item in items:
                    yield (element, item) if with_parent else item

        return produce()

    def explain(self) -> dict:
        """``{"plan": "Unnest", "path", "with_parent"}``."""
        return self._explain("Unnest", {"path": self.path, "with_parent": self.with_parent}, (self.source,))

    def estimate(self) -> Estimate:
        """rows = source rows * UNNEST_FANOUT; cost = source cost + rows."""
        src = _est(self.source)
        rows = src.rows * UNNEST_FANOUT
        return Estimate(rows=rows, cost=src.cost + rows)
