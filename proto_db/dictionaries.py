from __future__ import annotations
from typing import cast

from .exceptions import ProtoCorruptionException
from .common import Atom, DBCollections, QueryPlan, Literal, AbstractTransaction, AtomPointer
from .lists import List

import uuid
import logging


_logger = logging.getLogger(__name__)


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
        for item in self.base.as_iterable():
            yield from item

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

    def __init__(
            self,
            key: int | None = None,
            value: object = None,
            next: HashDictionary = None,
            previous: HashDictionary = None,

            transaction: AbstractTransaction = None,
            atom_pointer: AtomPointer = None,
            **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)

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

    def _save(self):
        if not self._saved:
            if self.previous:
                self.previous.transaction = self.transaction
                self.previous._save()
            if self.next:
                self.next.transaction = self.transaction
                self.next._save()
            if isinstance(self.value, Atom):
                self.value.transaction = self.transaction
                self.value._save()
            super()._save()

    def as_iterable(self):
        """
            Get an iterable generator of the HashDictionary items.

            :return: A generator that yields tuples (key, value) representing the nodes of the tree.
            """

        def scan(node: HashDictionary):
            node._load()
            if node.previous:
                yield from scan(node.previous)  # Subárbol izquierdo (recursión/yield)
            if node.key is not None:
                yield (node.key, node.value)  # Nodo actual
            if node.next:
                yield from scan(node.next)  # Subárbol derecho (recursión/yield)

        # Iniciar el generador desde el nodo actual
        return scan(self)

    def as_query_plan(self) -> QueryPlan:
        """
        Get a QueryPlan out of the HashDictionary
        :return:
        """
        return HashDictionaryQueryPlan(base=self)

    def get_at(self, key: int) -> object | None:
        """
        Searches for the value associated with the given key in the HashDictionary.

        :param key: The integer key to look for in the structure.
        :return: The value (Atom) associated with the key, or None if not found.
        """
        self._load()

        if self.key is None:
            return None

        node = self
        while node is not None:
            node._load()

            if node.key == key:
                if isinstance(node.value, Atom):
                    node.value._load()
                return node.value
            if key > node.key:
                node = node.next  # Traverse to the right subtree.
            else:
                node = node.previous  # Traverse to the left subtree.

        return None  # Return None if the key is not found.

    def has(self, key: int) -> bool:
        """
        Determines whether the specified key exists within the HashDictionary.

        This method iterates through the dictionary structure to locate a node
        with a key that matches the input key. If the key is found, it confirms
        its existence. If the key is not found after traversing the relevant
        nodes of the structure, the method returns False.

        :param key: The integer key to locate within the structure.
        :return: True if the key exists, otherwise False.
        """
        if self.key is None:
            return False

        node = self
        while node is not None:
            node._load()

            if node.key == key:
                return True
            if key > node.key:
                node = node.next  # Traverse to the right subtree.
            else:
                node = node.previous  # Traverse to the left subtree.

        return False  # Return None if the key is not found.

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
            next=self.next,
            transaction = self.transaction
        )

        # Promote the left child as the new root.
        return HashDictionary(
            key=self.previous.key,
            value=self.previous.value,
            previous=self.previous.previous,
            next=new_right,
            transaction = self.transaction
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
            next=self.next.previous,
            transaction = self.transaction
        )

        # Promote the right child as the new root.
        return HashDictionary(
            key=self.next.key,
            value=self.next.value,
            previous=new_left,
            next=self.next.next,
            transaction = self.transaction
        )

    def _rebalance(self) -> HashDictionary:
        """
        Fully rebalance the entire subtree rooted at this node.

        This method performs a recursive rebalance on the entire subtree, ensuring AVL properties
        are maintained at all levels of the tree, not just local adjustments.

        :return: The root of the balanced subtree.
        """
        # Rebalance child subtrees first (post-order traversal)
        node = self

        while node.previous and not -1 <= node.previous._balance() <= 1:
            node = HashDictionary(
                key=node.key,
                value=node.value,
                previous=node.previous._rebalance(),
                next=node.next,
                transaction = self.transaction
            )

        while node.next and not -1 <= node.next._balance() <= 1:
            node = HashDictionary(
                key=node.key,
                value=node.value,
                previous=node.previous,
                next=node.next._rebalance(),
                transaction = self.transaction
            )

        # Calculate balance factor for the current node
        balance = node._balance()

        # Perform rotations where necessary
        if balance < -1:  # Left-heavy
            if node.previous and node.previous._balance() > 0:  # Right-Left Case
                node = HashDictionary(
                    key=node.key,
                    value=node.value,
                    previous=node.previous._left_rotation(),
                    next=node.next,
                    transaction = self.transaction
                )
            return node._right_rotation()

        if balance > 1:  # Right-heavy
            if self.next and self.next._balance() < 0:  # Left-Right Case
                node = HashDictionary(
                    key=node.key,
                    value=node.value,
                    previous=node.previous,
                    next=node.next._right_rotation(),
                    transaction = self.transaction
                )
            return node._left_rotation()

        # If balance is satisfactory, return this node
        return node

    def set_at(self, key: int, value: object) -> HashDictionary:
        """
        Adds or updates a key-value pair in the HashDictionary.

        Creates a new node for the key or updates the value if the key already exists.
        Rebalances the structure after insertion if necessary.

        :param key: The key to set in the HashDictionary.
        :param value: The value (Atom) associated with the key.
        :return: A new HashDictionary reflecting the updated state.
        """
        self._load()

        # Case: Inserting into an empty HashDictionary.
        if self.key is None:
            return HashDictionary(
                key=key,
                value=value,
                previous=None,
                next=None,
                transaction = self.transaction
            )

        cmp = key - self.key
        if cmp > 0:
            # Insert into the right subtree.
            if self.next:
                new_node = HashDictionary(
                    key=self.key,
                    value=self.value,
                    previous=self.previous,
                    next=self.next.set_at(key, value),
                    transaction = self.transaction
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
                        next=None,
                        transaction = self.transaction
                    ),
                    transaction = self.transaction
                )
        elif cmp < 0:
            # Insert into the left subtree.
            if self.previous:
                new_node = HashDictionary(
                    key=self.key,
                    value=self.value,
                    previous=self.previous.set_at(key, value),
                    next=self.next,
                    transaction = self.transaction
                )
            else:
                new_node = HashDictionary(
                    key=self.key,
                    value=self.value,
                    previous=HashDictionary(
                        key=key,
                        value=value,
                        previous=None,
                        next=None,
                        transaction=self.transaction
                    ),
                    next=self.next,
                    transaction = self.transaction
                )
        else:
            # Replace the value of the current node.
            new_node = HashDictionary(
                key=self.key,
                value=value,
                previous=self.previous,
                next=self.next,
                transaction = self.transaction
            )

        return new_node._rebalance()


    def remove_at(self, key: int) -> HashDictionary:
        self._load()

        # Case: Removing from an empty HashDictionary.
        if self.key is None:
            return self

        cmp = key - self.key
        if cmp > 0:
            # Remove from the right subtree.
            if self.next:
                new_next = self.next.remove_at(key)
                new_node = HashDictionary(
                    key=self.key,
                    value=self.value,
                    previous=self.previous,
                    next=new_next if new_next.key is not None else None,
                    transaction = self.transaction
                )
            else:
                return HashDictionary(transaction=self.transaction)
        elif cmp < 0:
            # Remove from the left subtree.
            if self.previous:
                new_previous = self.previous.remove_at(key)
                new_node = HashDictionary(
                    key=self.key,
                    value=self.value,
                    previous=new_previous if new_previous.key is not None else None,
                    next=self.next,
                    transaction = self.transaction
                )
            else:
                return HashDictionary(transaction=self.transaction)
        else:
            # Remove the value of the current node.
            if self.next:
                next_first = self.next._get_first()
                new_next = self.next.remove_at(next_first.key)
                new_node = HashDictionary(
                    key=next_first.key,
                    value=next_first.value,
                    previous=self.previous,
                    next=new_next if new_next.key is not None else None,
                    transaction = self.transaction
                )
            elif self.previous:
                previous_last = self.previous._get_last()
                new_previous = self.previous.remove_at(previous_last.key)
                new_node = HashDictionary(
                    key=previous_last.key,
                    value=previous_last.value,
                    previous=new_previous if new_previous.key is not None else None,
                    next=self.next,
                    transaction = self.transaction
                )
            else:
                # It was the only node, return an empty HashDictionary.
                return HashDictionary(transaction=self.transaction)

        return new_node._rebalance()

    def _get_first(self) -> HashDictionary | None:
        """
        Fetches the first value in a series of linked nodes. This function traverses
        linked nodes starting from the current node (`self`) and moves to the leftmost
        node. The value of this leftmost node is returned unless the key attribute of
        the current node is None, in which case None is immediately returned. If the
        chain of nodes has no leftmost node with a valid value, None is returned as
        well.

        :return: The value of the first node in the linked chain or None if no such
                 value is found.
        :rtype: Atom or None
        """
        self._load()

        if self.key is None:
            return self

        node = self
        while node:
            self._load()

            if not node.previous:
                return node
            node = node.previous  # Traverse to the left subtree.

        raise ProtoCorruptionException(
            message=f'get_first traversal has found an inconsistency!'
        )

    def _get_last(self) -> HashDictionary| None:
        """
        Retrieve the last node's value in the sequence of the linked list-like structure. The method traverses
        the linked nodes until it finds the last node by checking the absence of a `next` reference.

        :return: The value of the last node if present, otherwise None.
        :rtype: Atom | None
        """
        self._load()

        if self.key is None:
            return self

        node = self
        while node:
            node._load()

            if not node.next:
                return node
            node = node.next  # Traverse to the right subtree.

        raise ProtoCorruptionException(
            message=f'get_last traversal has found an inconsistency!'
        )


class DictionaryItem(Atom):
    # Represents a key-value pair in a Dictionary, both durable and transaction-safe.
    key: Literal
    value: object

    def __init__(
            self,
            key: str = None,  # The key for the dictionary item.
            value: object = None,  # The value associated with the key.
            transaction: AbstractTransaction = None,  # The associated transaction.
            atom_pointer: AtomPointer = None,  # Pointer to the atom for durability/consistency.
            **kwargs):  # Additional keyword arguments.
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.key = Literal(literal=key)  # Wrap the key as a Literal for durability.
        self.value = value  # Assign the value to the dictionary item.


class Dictionary(DBCollections):
    """
    Represents a durable, transaction-safe dictionary-like mapping between strings and values.

    Only Atoms are recommended as keys and values to ensure consistency and durability.
    Mixing other objects is not supported, and no warnings will be issued for doing so.
    """

    content: List  # Internal storage for dictionary items as a list.

    def __init__(
            self,
            content: List = None,  # List to store the content.
            transaction: AbstractTransaction = None,  # Transaction context for operations.
            atom_pointer: AtomPointer = None,  # Pointer to ensure atomicity and durability.
            **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.content = content if content else List(transaction=transaction)  # Initialize content or create a new List.
        self.count = self.content.count  # Number of items in the dictionary.

    def _save(self):
        if not self._saved:
            self.content._save()
            super()._save()

    def as_iterable(self) -> list[tuple[str, object]]:
        """
        Provides an iterable generator for the dictionary's key-value pairs.

        :return: A generator yielding tuples of (key, value).
        """
        for item in self.content.as_iterable():  # Iterate through the content.
            item = (cast(DictionaryItem, item))  # Cast item to a DictionaryItem type.
            item._load()  # Ensure the item is loaded into memory.
            yield item.key.string, item.value  # Yield the key-value pair.

    def as_query_plan(self) -> QueryPlan:
        """
        Converts the dictionary into a QueryPlan, a representation for query execution.
        :return: The dictionary's query plan.
        """
        self._load()
        return self.content.as_query_plan()

    def get_at(self, key: str) -> object | None:
        """
        Gets the element at a given key exists in the dictionary.

        Uses binary search to find the key efficiently.

        :param key: The string key to be searched.
        :return: The value storea at key or None if not found
        """
        self._load()

        left = 0
        right = self.content.count - 1

        while left <= right:
            center = (left + right) // 2

            item = self.content.get_at(center)
            if item and str(item.key) == key:
                if isinstance(item.value, Atom):
                    item.value._load()
                return item.value

            if str(item.key) > key:
                right = center - 1
            else:
                left = center + 1

        return None

    def set_at(self, key: str, value: object) -> Dictionary:
        """
        Inserts or updates a key-value pair in the dictionary.

        If the key exists, updates its value and rebalances the underlying structure.
        If the key does not exist, inserts the new key-value pair at the appropriate position.

        :param key: The string key for the item being added or updated.
        :param value: The value associated with the key.
        :return: A new instance of Dictionary with the updated content.
        """
        """
        Returns a new HashDirectory with the value set at key

        :param key: int
        :param value: Atom
        :return: a new HashDirectory with the value set at key
        """
        self._load()

        left = 0
        right = self.content.count - 1
        center = 0

        while left <= right:
            center = (left + right) // 2

            item = cast(DictionaryItem, self.content.get_at(center))
            if item and str(item.key) == key:  # Check if the key already exists.
                # It's a repeated key, it's ok
                return Dictionary(
                    content=self.content.set_at(
                        center,
                        DictionaryItem(
                            key=key,
                            value=value,
                            transaction=self.transaction
                        )
                    ),
                    transaction=self.transaction
                )

            if str(item.key) > key:
                right = center - 1
            else:
                left = center + 1

        return Dictionary(
            content=self.content.insert_at(
                left,
                DictionaryItem(
                    key=key,
                    value=value,
                    transaction=self.transaction
                )
            ),
            transaction=self.transaction
        )

    def remove_at(self, key: str) -> Dictionary:
        """
        Removes a key-value pair from the dictionary if the key exists.

        If the key is found, it removes the corresponding entry and rebalances the structure.
        If the key does not exist, the method returns the original dictionary.

        :param key: The string key of the item to be removed.
        :return: A new instance of Dictionary reflecting the removal.
        """
        """
        Returns a new HashDirectory with the key removed if exists

        :param key: int
        :return: a new HashDirectory with the key removed
        """
        self._load()

        left = 0
        right = self.content.count - 1

        while left <= right:
            center = (left + right) // 2

            item = self.content.get_at(center)
            if item and str(item.key) == key:
                # It's a replacement of an existing value
                return Dictionary(
                    content=self.content.remove_at(center),
                    transaction=self.transaction
                )

            if str(item.key) > key:
                right = center - 1
            else:
                left = center + 1

        # Not found, nothing is changed
        return self

    def has(self, key: str) -> bool:
        """
        Checks whether a given key exists in the dictionary.

        Uses binary search to find the key efficiently.

        :param key: The string key to be searched.
        :return: True if the key is found; otherwise, False.
        """
        """
        Test for key

        :param key:
        :return: True if key is in the dictionary, False otherwise
        """
        self._load()

        left = 0
        right = self.content.count - 1

        while left <= right:
            center = (left + right) // 2

            item = self.content.get_at(center)
            if item and str(item.key) == key:
                return True

            if str(item.key) > key:
                right = center - 1
            else:
                left = center + 1

        return False

