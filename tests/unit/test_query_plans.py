"""Unit tests for proto_db.query.plans (ruling R5 minimal stub)."""
import dataclasses
import math

import pytest

from proto_db.errors import ProtoValidationError
from proto_db.query.plans import CollectionScanPlan, Estimate, QueryPlan
from tests.support.recording_index import RecordingIndex
from tests.support.toy_collection import ToySet


def test_estimate_is_frozen_and_validated():
    e = Estimate(rows=3, cost=4.5)
    assert e.rows == 3.0 and e.cost == 4.5
    with pytest.raises(dataclasses.FrozenInstanceError):
        e.rows = 1
    for rows, cost in ((-1, 0), (0, -1), (math.nan, 0), (0, math.inf), ("1", 0)):
        with pytest.raises(ProtoValidationError):
            Estimate(rows=rows, cost=cost)


def test_query_plan_is_abstract():
    with pytest.raises(TypeError):
        QueryPlan()


def test_collection_scan_plan():
    s = ToySet().add(2).add(1).add_index(RecordingIndex("i"))
    plan = s.as_query_plan()
    assert isinstance(plan, CollectionScanPlan) and isinstance(plan, QueryPlan)
    assert plan.collection is s
    assert list(plan.execute()) == [1, 2]
    assert plan.explain() == {"plan": "CollectionScan", "collection": "ToySet", "rows": 2, "indexes": ["i"]}
    assert plan.estimate() == Estimate(rows=2, cost=2)


def test_collection_scan_plan_on_empty_collection():
    plan = CollectionScanPlan(ToySet())
    assert list(plan.execute()) == [] and plan.estimate() == Estimate(0, 0)
    assert plan.explain()["indexes"] == []
