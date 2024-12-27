from __future__ import annotations

import uuid
from typing import cast

from . import DBCollections
from .exceptions import ProtoUnexpectedException, ProtoValidationException, ProtoCorruptionException
from .common import Atom, QueryPlan


class ListPlan(QueryPlan):
    """
    Create a QueryPlan from a python list
    """
    base_list: list

    def __init__(self,
                 base_list: list,
                 transaction_id: uuid.UUID = None,
                 offset: int = 0,
                 **kwargs):
        super().__init__(transaction_id=transaction_id, offset=offset)
        self.base_list = base_list

    def execute(self) -> list:
        for item in self.base_list:
            yield item

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return self


class FromPlan(QueryPlan):
    """

    """
    alias: str

    def __init__(self,
                 based_on: QueryPlan,
                 alias: str,
                 transaction_id: uuid.UUID = None,
                 offset: int = 0,
                 **kwargs):
        super().__init__(transaction_id=transaction_id, offset=offset)
        self.based_on = based_on
        self.alias = alias

    def execute(self) -> list:
        for item in self.based_on.execute():
            yield item

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        return self.based_on.optimize(full_plan)


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

