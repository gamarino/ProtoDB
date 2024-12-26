from __future__ import annotations
from typing import cast

from .exceptions import ProtoUnexpectedException, ProtoValidationException, ProtoCorruptionException
from .common import Atom, QueryPlan


class List(Atom):
    """

    """
    def get_at(self, index: int) -> Atom:
        """

        :param index:
        :return:
        """
        # TODO

    def insert_at(self, index: int, item: Atom) -> List:
        """

        :param index:
        :param item:
        :return:
        """
        # TODO

    def remove_at(self, index: int) -> List:
        """

        :param index:
        :return:
        """
        # TODO

    def remove_first(self) -> List:
        """

        :return:
        """
        # TODO

    def remove_last(self) -> List:
        """

        :return:
        """
        # TODO

    def extend(self, items: List) -> List:
        """

        :param items:
        :return:
        """
        # TODO

    def as_query_plan(self) -> QueryPlan:
        """

        :return:
        """
        # TODO

    def as_iterable(self) -> list[Atom]:
        """

        :return:
        """
        # TODO
