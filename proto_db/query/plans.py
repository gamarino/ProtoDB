"""Physical query plans (spec section 10, ruling R5).

This module starts minimal: the :class:`QueryPlan` base, the planner's cost
record :class:`Estimate`, :class:`CollectionScanPlan` (the access path every
collection exposes through ``IndexedCollection.as_query_plan()``) and
:class:`IndexPlan` (what ``Index.plan(term)`` returns, Task 4.1).  Later tasks add
the filter and composition plans.

Plans are runtime objects, never persisted (they are not atoms).  A plan holds
only immutable inputs, so it may be executed several times and from several
threads; each ``execute()`` call returns an independent iterator.
"""
from __future__ import annotations

import math
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from dataclasses import dataclass

from ..errors import ProtoValidationError

__all__ = ["Estimate", "QueryPlan", "CollectionScanPlan", "IndexPlan"]


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


class QueryPlan(ABC):
    """An executable physical plan.

    ``explain()`` describes exactly the plan that ``execute()`` runs.
    """

    @abstractmethod
    def execute(self) -> Iterator:
        """Return a new iterator over the plan's result elements."""

    @abstractmethod
    def explain(self) -> dict:
        """Return a JSON-friendly description of this plan."""

    @abstractmethod
    def estimate(self) -> Estimate:
        """Return the planner's estimate for this plan."""


class CollectionScanPlan(QueryPlan):
    """Full scan of a collection; carries the collection so the planner can consult its indexes."""

    def __init__(self, collection):
        self.collection = collection

    def execute(self) -> Iterator:
        """Yield every element of the collection (``collection.elements()``)."""
        return self.collection.elements()

    def explain(self) -> dict:
        """``{"plan": "CollectionScan", "collection", "rows", "indexes"}``."""
        c = self.collection
        return {"plan": "CollectionScan", "collection": type(c).__name__, "rows": c.count,
                "indexes": c.index_names()}

    def estimate(self) -> Estimate:
        """A scan produces and touches every element: rows = cost = count."""
        n = self.collection.count
        return Estimate(rows=n, cost=n)


class IndexPlan(QueryPlan):
    """Elements matching ``term``, produced by ``index`` (returned by ``Index.plan``).

    ``producer()`` must return a fresh iterator over the matching elements on
    every call (each occurrence once) and capture only immutable state, so the
    plan can be executed several times and from several threads.  ``kind`` names
    the access path in ``explain()`` (e.g. ``"IndexLookup"``, ``"IndexRange"``).
    """

    def __init__(self, index, term, producer: Callable[[], Iterator], *, kind: str = "IndexLookup"):
        if not callable(producer):
            raise ProtoValidationError(f"IndexPlan needs a callable producer, not {type(producer).__name__}")
        if not isinstance(kind, str) or not kind:
            raise ProtoValidationError(f"IndexPlan kind must be a non-empty str, not {kind!r}")
        self.index = index
        self.term = term
        self.kind = kind
        self._producer = producer

    def execute(self) -> Iterator:
        """Return a new iterator over the matching elements."""
        return iter(self._producer())

    def explain(self) -> dict:
        """``{"plan": kind, "index": index.explain(), "term": term.describe()}``."""
        return {"plan": self.kind, "index": self.index.explain(), "term": self.term.describe()}

    def estimate(self) -> Estimate:
        """The index's estimate for the term."""
        return self.index.estimate(self.term)
