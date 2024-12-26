from __future__ import annotations
from typing import cast

from . import QueryPlan
from .exceptions import ProtoUnexpectedException, ProtoValidationException, ProtoCorruptionException
from .common import Atom


class HashSet(Atom):
    """

    """
    def add(self, item: Atom) -> HashSet:
        """

        :param item:
        :return:
        """
        # TODO

    def has(self, item: Atom) -> bool:
        """

        :param item:
        :return:
        """
        # TODO

    def remove(self, item: Atom) -> HashSet:
        """

        :param item:
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
