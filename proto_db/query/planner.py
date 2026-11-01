"""The query planner (spec section 10, decision D37).

:func:`plan_query` turns a source (a collection, or a plan), an optional
predicate, an optional order and an optional window into a physical plan built
from the nodes of :mod:`proto_db.query.plans`.  The returned plan is exactly
what runs: ``plan.explain()`` describes the very node tree ``plan.execute()``
executes, and nothing in the query path catches an exception to fall back.

Invariants
----------
* Every predicate is honoured: each conjunct is answered either by an index
  access path or by a residual :class:`~proto_db.query.plans.Filter`; none is
  ever dropped, so ``execute()`` returns the same multiset as a full scan
  filtered by :func:`~proto_db.query.expressions.evaluate`.
* ``near[]`` terms have no per-element semantics; they are answered only by a
  ``VectorSearch`` over an index that supports them, never by a filter.  A
  ``near[]`` term no index supports, or one under a negation, raises
  ``ProtoNotSupportedError`` (no silent fallback).
* Planning is a pure function of its arguments: collections are immutable
  versions and nothing is cached between calls.

Normalisation (:func:`normalize`)
---------------------------------
And/Or are flattened; a negation is pushed through And/Or by De Morgan, which
is exact because :func:`evaluate` is two-valued (every term answers True or
False; MISSING simply fails comparisons).  A negated term is never rewritten
into a complementary operator (D36 amendment: ``Not(x == 5)`` is not
``x != 5`` because MISSING satisfies only the former), with the single exact
exception ``Not(?N)`` -> ``?!N`` and ``Not(?!N)`` -> ``?N``.

Access paths and cost
---------------------
Costs are the D30/D36 estimates of the candidate plan trees themselves
(``plan.estimate().cost``), so the residual filter is charged by the D36
``Filter`` model: one unit per source row it evaluates.  For a conjunction the
candidates are: a full scan with every conjunct as residual; and, for each
conjunct with an access path, the cheapest such path with the other conjuncts
as residual.  A conjunct's access path is the cheapest plan among the indexes
whose ``supports(term)`` accepts it (a ``VectorSearch`` for ``near[]``), or,
for a disjunction, the ``Union`` of index-only plans of every branch.
Conjuncts that cannot be residual (they hold a ``near[]`` term) are always
answered by their access paths, combined by ``Intersect`` when there are
several.  An ``Intersect`` of an extra index plan is never a candidate:
under the D36 model ``Intersect(a, b)`` costs ``cost(a) + rows(a) + cost(b) +
rows(b)``, never less than ``Filter(a, b)`` at ``cost(a) + rows(a)``.

A disjunction is planned as a ``Union`` only if every branch has an index-only
plan (each branch is a conjunction planned recursively on the base
collection) and the Union is cheaper than a scan filtered by the whole
disjunction; or when a branch holds a ``near[]`` term, which forces the Union
(branches without an index path are then a filtered scan).  The Union output
is in first-seen order, not in collection order; ``order_by`` makes the order
deterministic.  Union (max multiplicity) and Intersect (min multiplicity) by
element identity (D36) equal OR and AND over the base collection's multiset
because every source plan yields, for each identity, either all of its
occurrences in the collection or none of them.

Ties between candidates of equal cost prefer index-based plans over scans,
then the lexicographically smaller tuple of index names, so the choice is
deterministic.

Order and window
----------------
``order_by`` with a single key may use an index whose ``supports_order``
accepts the key (``IndexOrderedScan``, ascending or descending) with the whole
predicate as a residual filter on top, when that is cheaper than sorting the
planned result; otherwise a ``Sort`` node orders it.  The two orders are
equal up to the order of ties (D36).  With several keys a ``Sort`` is always
used.  ``limit``/``offset`` add a ``Limit`` node on top, after ordering.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from ..collections.base import IndexedCollection
from ..errors import ProtoNotSupportedError, ProtoValidationError
from ..indexing.keyspec import KeySpec
from .expressions import And, Expression, Not, Or, iter_terms
from .plans import (CollectionScanPlan, Filter, Intersect, Limit, QueryPlan, Sort, Union, VectorSearch,
                    ordered_index_scan)
from .terms import IS_NONE, NEAR, NOT_NONE, Term

__all__ = ["plan_query", "normalize"]

_PRESENCE_COMPLEMENT = {IS_NONE: NOT_NONE, NOT_NONE: IS_NONE}


def normalize(expr: Expression) -> Expression:
    """Push negations through And/Or (De Morgan) and flatten; only ``Not(?N)``/``Not(?!N)`` become terms.

    A negated ``near[]`` term raises ``ProtoNotSupportedError``.
    """
    if not isinstance(expr, Expression):
        raise ProtoValidationError(f"normalize needs an Expression, not {expr!r}")
    return _normalize(expr, False)


def _normalize(expr: Expression, negate: bool) -> Expression:
    if isinstance(expr, Term):
        if not negate:
            return expr
        if expr.op in _PRESENCE_COMPLEMENT:
            return Term(expr.path, _PRESENCE_COMPLEMENT[expr.op])
        if expr.op == NEAR:
            raise ProtoNotSupportedError(f"A negated 'near[]' term cannot be planned ({expr!r}): there is no "
                                         f"per-element semantics to complement a vector search")
        return Not(expr)
    if isinstance(expr, Not):
        return _normalize(expr.operand, not negate)
    if isinstance(expr, (And, Or)):
        operands = [_normalize(o, negate) for o in expr.operands]
        junction = type(expr) if not negate else (Or if isinstance(expr, And) else And)
        return junction(*operands)
    raise ProtoValidationError(f"Cannot normalise {expr!r}")


def _has_near(expr: Expression) -> bool:
    return any(t.op == NEAR for t in iter_terms(expr))


def _conjuncts(expr: Expression) -> list:
    """The conjuncts of ``expr`` without duplicates (And is idempotent), in order."""
    operands = expr.operands if isinstance(expr, And) else (expr,)
    return list(dict.fromkeys(operands))


def _conjunction(conjuncts: list) -> Expression:
    return conjuncts[0] if len(conjuncts) == 1 else And(*conjuncts)


@dataclass(frozen=True)
class _Candidate:
    """A candidate plan with what ranks it: its cost, whether it scans, and the indexes it uses."""

    plan: QueryPlan
    indexes: tuple = ()
    scans: bool = False
    cost: float = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "cost", self.plan.estimate().cost)

    @property
    def rank(self) -> tuple:
        """Cheapest first; on equal cost index-only plans first, then by index names."""
        return self.cost, self.scans, self.indexes

    def wrap(self, plan: QueryPlan) -> "_Candidate":
        """The same access path under a new top node."""
        return _Candidate(plan, self.indexes, self.scans)


def _best(candidates: list):
    return min(candidates, key=lambda c: c.rank) if candidates else None


def _merge_names(candidates) -> tuple:
    return tuple(sorted({name for c in candidates for name in c.indexes}))


class _Planner:
    """Plans expressions over one base plan and the indexes (name, index) of its collection."""

    def __init__(self, base: QueryPlan, indexes: list):
        self.base = base
        self.indexes = indexes

    def with_residual(self, candidate: _Candidate, residual: list) -> _Candidate:
        if not residual:
            return candidate
        return candidate.wrap(Filter(candidate.plan, _conjunction(residual)))

    def plan(self, expr: Expression) -> _Candidate:
        """The cheapest plan of a normalised expression."""
        return self.conjunction(_conjuncts(expr), require_index=False)

    def conjunction(self, conjuncts: list, *, require_index: bool):
        """The cheapest plan of the conjunction (with ``require_index``, only index-only plans; may be None)."""
        conjuncts = list(dict.fromkeys(conjuncts))
        access = [self.access(c) for c in conjuncts]
        forced = [i for i, c in enumerate(conjuncts) if _has_near(c)]
        for i in forced:
            if access[i] is None:
                raise ProtoNotSupportedError(
                    f"No index supports the 'near[]' term in {conjuncts[i]!r}, and a 'near[]' term cannot be "
                    f"evaluated by a filter; add a vector index on its path")
        if forced:
            parts = [access[i] for i in forced]
            base = parts[0] if len(parts) == 1 else _Candidate(
                Intersect(*(p.plan for p in parts)), _merge_names(parts), any(p.scans for p in parts))
            rest = [c for i, c in enumerate(conjuncts) if i not in forced]
            return self.with_residual(base, rest)
        candidates = []
        if not require_index:
            candidates.append(self.with_residual(_Candidate(self.base, (), True), conjuncts))
        for i, option in enumerate(access):
            if option is not None:
                candidates.append(self.with_residual(option, conjuncts[:i] + conjuncts[i + 1:]))
        return _best(candidates)

    def access(self, conjunct: Expression):
        """The cheapest access path answering ``conjunct`` exactly without a full scan, else None."""
        if isinstance(conjunct, Term):
            if conjunct.op == NEAR:
                options = [_Candidate(VectorSearch(index, conjunct), (name,))
                           for name, index in self.indexes if index.supports(conjunct)]
            else:
                options = [_Candidate(index.plan(conjunct), (name,))
                           for name, index in self.indexes if index.supports(conjunct)]
            return _best(options)
        if isinstance(conjunct, Or):
            return self.union(conjunct)
        return None  # a negated term: residual only

    def union(self, disjunction: Or):
        """A Union of per-branch plans if every branch has an index-only plan (or ``near[]`` forces it)."""
        forced = _has_near(disjunction)
        branches = []
        for branch in disjunction.operands:
            planned = self.conjunction(_conjuncts(branch), require_index=True)
            if planned is None:
                if not forced:
                    return None
                planned = self.conjunction(_conjuncts(branch), require_index=False)
            branches.append(planned)
        return _Candidate(Union(*(b.plan for b in branches)), _merge_names(branches), any(b.scans for b in branches))

    def order(self, candidate: _Candidate, expr, keys: list) -> QueryPlan:
        """``candidate`` ordered by ``keys``: a Sort, or an ordered index scan plus the residual predicate."""
        if not keys:
            return candidate.plan
        options = [candidate.wrap(Sort(candidate.plan, keys))]
        if len(keys) == 1 and (expr is None or not _has_near(expr)):
            path, descending = keys[0]
            spec = KeySpec.for_term_path(path)
            for name, index in self.indexes:
                if index.supports_order(spec):
                    plan = ordered_index_scan(index, spec, descending)
                    options.append(_Candidate(plan if expr is None else Filter(plan, expr), (name,)))
        return _best(options).plan


def _source(source) -> tuple:
    """``(base plan, indexes)`` of a planner source."""
    if isinstance(source, IndexedCollection):
        collection, base = source, source.as_query_plan()
    elif isinstance(source, CollectionScanPlan):
        collection, base = source.collection, source
    elif isinstance(source, QueryPlan):
        return source, []
    else:
        raise ProtoValidationError(f"plan_query needs an IndexedCollection or a QueryPlan source, "
                                   f"not {type(source).__name__}")
    return base, [(name, collection.get_index(name)) for name in sorted(collection.index_names())]


def _order_keys(order_by) -> list:
    """``[(path, descending)]`` from None, a path str, a ``(path, descending)`` pair, or a list of either."""
    if order_by is None:
        return []
    if isinstance(order_by, str):
        order_by = [(order_by, False)]
    elif isinstance(order_by, tuple) and len(order_by) == 2 and isinstance(order_by[1], bool):
        order_by = [order_by]
    if not isinstance(order_by, (list, tuple)) or not order_by:
        raise ProtoValidationError(f"order_by must be a path, a (path, descending) pair or a non-empty list of "
                                   f"them, not {order_by!r}")
    keys = []
    for item in order_by:
        if isinstance(item, str):
            item = (item, False)
        if not isinstance(item, tuple) or len(item) != 2:
            raise ProtoValidationError(f"order_by keys are a path or a (path, descending) pair, not {item!r}")
        path, descending = item
        if path is not None and not isinstance(path, str):
            raise ProtoValidationError(f"An order_by path must be a str or None, not {path!r}")
        if not isinstance(descending, bool):
            raise ProtoValidationError(f"An order_by descending flag must be a bool, not {descending!r}")
        keys.append((path, descending))
    return keys


def plan_query(source, where=None, order_by=None, limit=None, offset=0) -> QueryPlan:
    """Plan a query (module docstring): the cheapest plan returning the multiset a scan would return.

    ``source`` is an ``IndexedCollection`` (its indexes are consulted), a
    ``CollectionScanPlan`` (its collection's indexes are consulted) or any
    other ``QueryPlan`` (no index; the predicate becomes a Filter on top).
    ``where`` is an Expression, the 0.1 prefix list syntax, or None.
    ``order_by`` is a path, a ``(path, descending)`` pair, or a list of
    either; ``limit`` an int >= 0 or None; ``offset`` an int >= 0.
    Raises ``ProtoValidationError`` for malformed arguments and
    ``ProtoNotSupportedError`` for a ``near[]`` term no index can answer.
    """
    base, indexes = _source(source)
    expr = None if where is None else normalize(Expression.compile(where))
    keys = _order_keys(order_by)
    for name, value in (("limit", limit), ("offset", offset)):
        if value is None and name == "limit":
            continue
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ProtoValidationError(f"plan_query {name} must be an int >= 0, not {value!r}")
    planner = _Planner(base, indexes)
    candidate = _Candidate(base, (), True) if expr is None else planner.plan(expr)
    plan = planner.order(candidate, expr, keys)
    if limit is not None or offset:
        plan = Limit(plan, limit, offset)
    return plan
