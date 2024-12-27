from __future__ import annotations
from typing import cast

from .exceptions import ProtoUnexpectedException, ProtoValidationException, ProtoCorruptionException
from .common import Atom, QueryPlan


class ListPlan(QueryPlan):
    """
    Create a QueryPlan from a python list
    """
    # TODO


class FromPlan(QueryPlan):
    """

    """
    # TODO


class WherePlan(QueryPlan):
    """

    """
    # TODO


class GroupByPlan(QueryPlan):
    """

    """
    # TODO


class SelectPlan(QueryPlan):
    """

    """
    # TODO


class OrderByPlan(QueryPlan):
    """

    """
    # TODO


class HavingPlan(QueryPlan):
    """

    """
    # TODO


class LimitPlan(QueryPlan):
    """

    """
    # TODO


class OffsetPlan(QueryPlan):
    """

    """
    # TODO


class ExternalJoinPlan(QueryPlan):
    """

    """
    # TODO


class JoinPlan(QueryPlan):
    """

    """
    # TODO


class LeftPlan(QueryPlan):
    """

    """
    # TODO


class RightPlan(QueryPlan):
    """

    """
    # TODO


class UnionPlan(QueryPlan):
    """

    """
    # TODO

