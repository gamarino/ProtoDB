from . import common

from .common import ProtoUnexpectedException, ProtoValidationException, ProtoNotSupportedException
from .common import Atom, DBObject, DBCollections

import uuid


class HashDictionary(DBCollections):
    key: int
    value: Atom
    height: int
    next: DBCollections
    previous: DBCollections

    def as_iterable(self) -> list[Atom]:
        # TODO
        return []

    def get_at(self, key: int) -> Atom:
        """

        :param key:
        :return:
        """

    def set_at(self, key: int, value: Atom) -> DBCollections:
        """
        Returns a new HashDirectory with the value set at key

        :param key: int
        :param value: Atom
        :return: a new HashDirectory with the value set at key
        """

    def remove_key(self, key: int) -> DBCollections:
        """
        Returns a new HashDirectory with the key removed if exists

        :param key: int
        :return: a new HashDirectory with the key removed
        """

    def has(self, key:int) -> bool:
        """
        Test for key

        :param key:
        :return: True if key is in the dictionary, False otherwise
        """


class Dictionary(DBCollections):
    """
    A mapping between strings and values
    """
    content: HashDictionary

    def __init__(self, transaction_id: uuid.UUID = None, offset:int = 0, content: HashDictionary = None,
                 **kwargs):
        super().__init__(transaction_id=transaction_id, offset=offset)
        self.content = content

    def as_iterable(self) -> list[Atom]:
        # TODO
        return []

    def get_at(self, key: str) -> Atom:
        """

        :param key:
        :return:
        """
        hash = self._transaction._get_string_hash(key)
        return self.content.get_at(hash)

    def set_at(self, key: str, value: Atom) -> DBCollections:
        """
        Returns a new HashDirectory with the value set at key

        :param key: int
        :param value: Atom
        :return: a new HashDirectory with the value set at key
        """
        hash = self._transaction._get_string_hash(key)
        return Dictionary(
            content=self.content.set_at(hash, value),
        )

    def remove_key(self, key: str) -> DBCollections:
        """
        Returns a new HashDirectory with the key removed if exists

        :param key: int
        :return: a new HashDirectory with the key removed
        """
        hash = self._transaction._get_string_hash(key)
        return Dictionary(
            content=self.content.remove_key(hash),
        )

    def has(self, key: str) -> bool:
        """
        Test for key

        :param key:
        :return: True if key is in the dictionary, False otherwise
        """
        hash = self._transaction.get_string_hash(key)
        return self.content.has(hash)

