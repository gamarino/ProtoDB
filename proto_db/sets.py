from __future__ import annotations

import uuid

from .common import Atom, QueryPlan
from .dictionaries import HashDictionary


class Set(Atom):
    """
    A set of Atoms
    """
    content: HashDictionary

    def __init__(self,
                 content: HashDictionary = None,
                 transaction_id: uuid.UUID = None,
                 offset:int = 0,
                 **kwargs):
        super().__init__(transaction_id=transaction_id, offset=offset)
        self.content = content

    def as_iterable(self) -> list[Atom]:
        for hash_value, item in self.content.as_iterable():
            yield item

    def as_query_plan(self) -> QueryPlan:
        return self.content.as_query_plan()

    def get_at(self, key: Atom) -> Atom:
        """

        :param key:
        :return:
        """
        item_hash = key.hash()
        return self.content.get_at(item_hash)

    def add(self, key: Atom) -> Set:
        """
        Returns a new Set with the key added

        :param key: Atom
        :return: a new Set with the key added
        """
        item_hash = key.hash()
        return Set(
            content=self.content.set_at(item_hash, key),
        )

    def remove_key(self, key: Atom) -> Set:
        """
        Returns a new HashDirectory with the key removed if exists

        :param key: int
        :return: a new HashDirectory with the key removed
        """
        item_hash = key.hash()
        return Set(
            content=self.content.remove_key(item_hash),
        )

    def has(self, key: Atom) -> bool:
        """
        Test for key

        :param key:
        :return: True if key is in the dictionary, False otherwise
        """
        item_hash = key.hash()
        return self.content.has(item_hash)

