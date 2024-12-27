from __future__ import annotations
from typing import cast

from .common import Atom, DBCollections, QueryPlan, Literal

import uuid


class HashDictionaryQueryPlan(QueryPlan):
    base: HashDictionary

    def __init__(self,
                 transaction_id: uuid.UUID = None,
                 offset: int = 0,
                 base: HashDictionary = None):
        super().__init__(transaction_id=transaction_id, offset=offset)
        self.base = base

    def execute(self) -> DBCollections:
        """

        :return:
        """
        # TODO

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        """

        :param full_plan:
        :return:
        """
        return self


class HashDictionary(DBCollections):
    key: int | None = None  # The key associated with this node; can be None for an empty structure.
    value: Atom | None  # The value associated with the key; None indicates no value.
    height: int  # The height of the current subtree rooted at this node.
    next: HashDictionary  # Reference to the next node in the structure (right child in a tree context).
    previous: HashDictionary  # Reference to the previous node in the structure (left child in a tree context).

    def __init__(self,
                 transaction_id: uuid.UUID = None,
                 offset: int = 0,
                 key: int = 0,
                 value: Atom = None,
                 next: HashDictionary = None,
                 previous: HashDictionary = None):
        super().__init__(transaction_id=transaction_id, offset=offset)

        # Initialize the current node's key, value, and child references.
        self.key = key
        self.value = value
        self.next = next
        self.previous = previous

        # Calculate the total count of nodes in the current subtree.
        if key is not None:
            count = 1
            if self.previous:
                count += self.previous.count
            if self.next:
                count += self.next.count
            self.count = count
        else:
            self.count = 0

        # Calculate the height of the current subtree.
        if key is not None:
            height = 1
            previous_height = previous.height if previous else 0
            next_height = next.height if next else 0
            height += max(previous_height, next_height)
            self.height = height
        else:
            self.height = 0

    def as_iterable(self) -> list[tuple[int, Atom]]:
        """
        Get an iterable list of the HashDictionary items

        :return:
        """
        # Get an iterable of the HashDictionary items
        def scan(node: HashDictionary) -> list:
            result = scan(node.previous) if node.previous else []
            if node.key is not None:
                result.append((node.key, node.value))
            result += scan(node.next) if node.next else []
            return result

        return scan(self)

    def as_query_plan(self) -> QueryPlan:
        """
        Get a QueryPlan out of the HashDictionary
        :return:
        """
        return HashDictionaryQueryPlan(base=self)

    def get_at(self, key: int) -> Atom | None:
        """
        Searches for the value associated with the given key in the HashDictionary.

        :param key: The integer key to look for in the structure.
        :return: The value (Atom) associated with the key, or None if not found.
        """
        node = self
        while node is not None:
            if node.key == key:
                return node.value
            if key > node.key:
                node = node.next  # Traverse to the right subtree.
            else:
                node = node.previous  # Traverse to the left subtree.

        return None  # Return None if the key is not found.

    def _balance(self) -> int:
        """
        Calculates the balance factor of the current node.

        Balance factor: height(right subtree) - height(left subtree).
        :return: The balance factor of the node.
        """
        if not self:
            return 0
        if self.next and self.previous:
            return self.next.height - self.previous.height
        elif self.previous:
            return -self.previous.height
        elif self.next:
            return self.next.height
        else:
            return 0

    def _right_rotation(self) -> HashDictionary:
        """
        Performs a right rotation on the current node.

        Rotates the current node to the right, promoting its left child as the root of the subtree.
        :return: The new root node of the rotated subtree.
        """
        if not self.previous:
            return self  # Cannot perform a right rotation without a left child.

        # Create a new right subtree with the current node.
        new_right = HashDictionary(
            key=self.key,
            value=self.value,
            previous=self.previous.next,
            next=self.next
        )

        # Promote the left child as the new root.
        return HashDictionary(
            key=self.previous.key,
            value=self.previous.value,
            previous=self.previous.previous,
            next=new_right
        )

    def _left_rotation(self) -> HashDictionary:
        """
        Performs a left rotation on the current node.

        Rotates the current node to the left, promoting its right child as the root of the subtree.
        :return: The new root node of the rotated subtree.
        """
        if not self.next:
            return self  # Cannot perform a left rotation without a right child.

        # Create a new left subtree with the current node.
        new_left = HashDictionary(
            key=self.key,
            value=self.value,
            previous=self.previous,
            next=self.next.previous
        )

        # Promote the right child as the new root.
        return HashDictionary(
            key=self.next.key,
            value=self.next.value,
            previous=new_left,
            next=self.next.next
        )

    def _rebalance(self) -> HashDictionary:
        """
        Rebalances the current subtree if necessary.

        If the balance factor is outside the range [-1, 1], performs rotations to adjust the structure.
        :return: The root of the rebalanced subtree.
        """
        balance = self._balance()  # Check the balance factor of the current node.
        new_node = self

        if -1 <= balance <= 1:
            # If the node is balanced, return it as-is.
            return new_node

        # Perform a right rotation if the left subtree is too heavy.
        if balance < -1 and new_node.previous and new_node.previous._balance() < 0:
            new_node = new_node._right_rotation()
        else:
            # Handle other balancing scenarios by performing appropriate rotations.
            while balance > 0 and new_node.previous and new_node.previous._balance() > 0:
                new_node = HashDictionary(
                    key=new_node.key,
                    value=new_node.value,
                    previous=new_node.previous._left_rotation(),
                    next=new_node.next
                )
                if not new_node.previous:
                    return new_node
                new_node = new_node._right_rotation()

            while balance < 0 and new_node.next and new_node.next._balance() < 0:
                new_node = HashDictionary(
                    key=new_node.key,
                    value=new_node.value,
                    previous=new_node.previous,
                    next=new_node.next._right_rotation()
                )
                if not new_node.next:
                    return new_node
                new_node = new_node._left_rotation()

        return new_node  # Return the new root of the rebalanced subtree.

    def set_at(self, key: int, value: Atom) -> HashDictionary:
        """
        Adds or updates a key-value pair in the HashDictionary.

        Creates a new node for the key or updates the value if the key already exists.
        Rebalances the structure after insertion if necessary.

        :param key: The key to set in the HashDictionary.
        :param value: The value (Atom) associated with the key.
        :return: A new HashDictionary reflecting the updated state.
        """
        # Case: Inserting into an empty HashDictionary.
        if self.key is None:
            return HashDictionary(
                key=key,
                value=value,
                previous=None,
                next=None
            )

        new_node = None  # Placeholder for the updated subtree.
        cmp = key - self.key
        if cmp > 0:
            # Insert into the right subtree.
            if self.next:
                new_node = HashDictionary(
                    key=self.key,
                    value=self.value,
                    previous=self.previous,
                    next=self.next.set_at(key, value)
                )
            else:
                new_node = HashDictionary(
                    key=self.key,
                    value=self.value,
                    previous=self.previous,
                    next=HashDictionary(
                        key=key,
                        value=value,
                        previous=None,
                        next=None
                    )
                )
        elif cmp < 0:
            # Insert into the left subtree.
            if self.previous:
                new_node = HashDictionary(
                    key=self.key,
                    value=self.value,
                    previous=self.previous.set_at(key, value),
                    next=self.next
                )
            else:
                new_node = HashDictionary(
                    key=self.key,
                    value=value,
                    previous=HashDictionary(
                        key=key,
                        value=value,
                        previous=None,
                        next=None
                    ),
                    next=self.next
                )
        else:
            # Replace the value of the current node.
            new_node = HashDictionary(
                key=self.key,
                value=value,
                previous=self.previous,
                next=self.next
            )

        return new_node._rebalance()

class DictionaryItem(Atom):
    key: Literal
    value: Atom

    def __init__(self,
                 key: str = None,
                 value: Atom = None,
                 transaction_id: uuid.UUID = None,
                 offset:int = 0,
                 **kwargs):
        super().__init__(transaction_id=transaction_id, offset=offset)
        self.key = Literal(literal=key)
        self.value = value


class Dictionary(DBCollections):
    """
    A mapping between strings and values
    """
    content: HashDictionary

    def __init__(self,
                 content: HashDictionary = None,
                 transaction_id: uuid.UUID = None,
                 offset:int = 0,
                 **kwargs):
        super().__init__(transaction_id=transaction_id, offset=offset)
        self.content = content

    def as_iterable(self) -> list[tuple[str, Atom]]:
        return [(cast(DictionaryItem, item).key.literal,
                cast(DictionaryItem, item).value)
                for item in self.content.as_iterable()]

    def as_query_plan(self) -> QueryPlan:
        return self.content.as_query_plan()

    def get_at(self, key: str) -> Atom:
        """

        :param key:
        :return:
        """
        item_hash = self._transaction.get_literal(key).hash()
        return self.content.get_at(item_hash)

    def set_at(self, key: str, value: Atom) -> Dictionary:
        """
        Returns a new HashDirectory with the value set at key

        :param key: int
        :param value: Atom
        :return: a new HashDirectory with the value set at key
        """
        item = DictionaryItem(key=key, value=value)
        item_hash = self._transaction.get_literal(key)
        return Dictionary(
            content=self.content.set_at(item_hash, item),
        )

    def remove_key(self, key: str) -> Dictionary:
        """
        Returns a new HashDirectory with the key removed if exists

        :param key: int
        :return: a new HashDirectory with the key removed
        """
        item_hash = self._transaction.get_literal(key)
        return Dictionary(
            content=self.content.remove_key(item_hash),
        )

    def has(self, key: str) -> bool:
        """
        Test for key

        :param key:
        :return: True if key is in the dictionary, False otherwise
        """
        item_hash = self._transaction.get_literal(key)
        return self.content.has(item_hash)

