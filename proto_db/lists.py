from __future__ import annotations
from typing import cast, TYPE_CHECKING
from .common import Atom, QueryPlan, DBCollections, AbstractTransaction, AtomPointer
from .indexes import IndexRegistry, IndexDefinition
from .queries import IndexedQueryPlan

if TYPE_CHECKING:
    # For type checking only to avoid circular imports at runtime
    from .dictionaries import RepeatedKeysDictionary


class ListQueryPlan(QueryPlan):
    base: List

    def __init__(
            self,
            base: List = None,
            transaction: AbstractTransaction = None,
            atom_pointer: AtomPointer = None,
            **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.base = base

    def execute(self) -> DBCollections:
        """
        Executes the query plan and returns the results as a collection.

        :return: An iterable collection of items from the list.
        """
        for item in self.base.as_iterable():
            yield item

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        """
        Optimizes the query plan for better performance.

        :param full_plan: The complete query plan to be optimized.
        :return: The optimized query plan.
        """
        return self


class List(DBCollections):
    empty: bool  # Indicator to represent empty lists
    value: object | None  # The value associated with this position; None is a valid value.
    height: int  # The height of the current subtree rooted at this node.
    next: List  # Reference to the next node in the structure (right child in a tree context).
    previous: List  # Reference to the previous node in the structure (left child in a tree context).

    def __init__(
            self,
            value: object = None,
            empty: bool = True,
            next: List = None,
            previous: List = None,
            indexes: DBCollections = None,
            transaction: AbstractTransaction = None,
            atom_pointer: AtomPointer = None,
            **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)

        # Initialize the current node's key, value, and child references.
        self.value = value
        self.next = next
        self.previous = previous
        self.empty = not value and empty
        self.indexes = indexes or {}

        # Calculate the total count of nodes in the current subtree.
        if not self.empty:
            count = 1
            if self.previous:
                self.previous._load()
                count += self.previous.count
            if self.next:
                self.next._load()
                count += self.next.count
            self.count = count
        else:
            self.count = 0

        # Calculate the height of the current subtree.
        if not self.empty:
            height = 1
            previous_height = previous.height if previous else 0
            next_height = next.height if next else 0
            height += max(previous_height, next_height)
            self.height = height
        else:
            self.height = 0

    def add_index(self, field_name: str):
        # Local import to avoid circular dependency at module import time
        from .dictionaries import RepeatedKeysDictionary
        new_index = RepeatedKeysDictionary(self.transaction)
        # Regenerate index on creation
        if not self.empty:
            for v in self.as_iterable():
                new_index = new_index.common_add(v)

        new_indexes = self.indexes.set_at(field_name, new_index)

        return List(
            value = self.value,
            empty = self.empty,
            next = self.next,
            previous = self.previous,
            indexes = new_indexes,
            transaction = self.transaction
        )

    def remove_index(self, field_name: str):
        if self.indexes and self.indexes.has(field_name):
            new_indexes = self.indexes.remove_at(field_name)

            return List(
                value=self.value,
                empty=self.empty,
                next=self.next,
                previous=self.previous,
                indexes=new_indexes,
                transaction=self.transaction
            )
        else:
            return self

    def _load(self):
        if not self._loaded:
            super()._load()
            self._loaded = True

    def _save(self):
        if not self._saved:
            if self.previous:
                self.previous.transaction = self.transaction
                self.previous._save()
            if self.next:
                self.next.transaction = self.transaction
                self.next._save()

            if self.value and isinstance(self.value, Atom):
                self.value.transaction = self.transaction
                self.value._save()

            super()._save()

    def as_iterable(self) -> list[tuple[int, object]]:
        """
        Returns an iterable representation of the list items.

        This method traverses the list structure in-order and yields each item,
        allowing for iteration over the list's contents.

        :return: An iterable of the list items.
        """

        # Get an iterable of the List items
        def scan(node: List) -> list:
            node._load()
            if node.previous:
                yield from scan(node.previous)  # Left subtree (recursion/yield)
            if not node.empty:
                yield node.value  # Current node
            if node.next:
                yield from scan(node.next)  # Right subtree (recursion/yield)

        return scan(self)

    def as_query_plan(self) -> QueryPlan:
        """
        Creates a query plan based on this list.

        This method generates a ListQueryPlan object that can be used to execute
        queries against the list's data.

        :return: A QueryPlan object for this list.
        """
        if self.indexes:
            return IndexedQueryPlan(base=self, indexes=self.indexes)
        else:
            return ListQueryPlan(base=self)

    def get_at(self, offset: int) -> Atom | None:
        """
        Searches for the value associated with the offset in the List.

        :param offset: The integer offset to look up for in the structure.
        :return: The value (Atom) associated with the key, or None if not found.
        """
        self._load()

        if self.empty:
            return None

        if offset < 0:
            offset = self.count + offset

        if offset < 0 or offset >= self.count:
            return None

        node = self
        while node is not None:
            node._load()

            if node.previous:
                node.previous._load()
                node_offset = node.previous.count
            else:
                node_offset = 0

            if offset == node_offset:
                if isinstance(node.value, Atom):
                    node.value._load()
                return node.value
            if offset > node_offset:
                node = node.next  # Traverse to the right subtree.
                offset -= node_offset + 1
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
            self.next._load()
            self.previous._load()
            return self.next.height - self.previous.height
        elif self.previous:
            self.previous._load()
            return -self.previous.height
        elif self.next:
            self.next._load()
            return self.next.height
        else:
            return 0

    def _right_rotation(self) -> List:
        """
        Performs a right rotation on the current node.

        Rotates the current node to the right, promoting its left child as the root of the subtree.
        :return: The new root node of the rotated subtree.
        """
        if not self.previous:
            return self  # Cannot perform a right rotation without a left child.

        # Create a new right subtree with the current node.
        self.previous._load()
        new_right = List(
            value=self.value,
            empty=False,
            previous=self.previous.next,
            next=self.next,
            transaction=self.transaction
        )

        # Promote the left child as the new root.
        return List(
            value=self.previous.value,
            empty=False,
            previous=self.previous.previous,
            next=new_right,
            transaction=self.transaction
        )

    def _left_rotation(self) -> List:
        """
        Performs a left rotation on the current node.

        Rotates the current node to the left, promoting its right child as the root of the subtree.
        :return: The new root node of the rotated subtree.
        """
        if not self.next:
            return self  # Cannot perform a left rotation without a right child.

        # Create a new left subtree with the current node.
        self.next._load()
        new_left = List(
            value=self.value,
            empty=False,
            previous=self.previous,
            next=self.next.previous,
            transaction=self.transaction
        )

        # Promote the right child as the new root.
        return List(
            value=self.next.value,
            empty=False,
            previous=new_left,
            next=self.next.next,
            transaction=self.transaction
        )

    def _rebalance(self) -> List:
        """
        Fully rebalance the entire subtree rooted at this node.

        This method performs a recursive rebalance on the entire subtree, ensuring AVL properties
        are maintained at all levels of the tree, not just local adjustments.

        :return: The root of the balanced subtree.
        """
        # Rebalance child subtrees first (post-order traversal)
        node = self

        while node.previous:
            node.previous._load()
            if not -1 <= node.previous._balance() <= 1:
                node = List(
                    value=node.value,
                    empty=False,
                    previous=node.previous._rebalance(),
                    next=node.next,
                    transaction=self.transaction
                )
            else:
                break

        while node.next:
            node.next._load()
            if not -1 <= node.next._balance() <= 1:
                node = List(
                    value=node.value,
                    empty=False,
                    previous=node.previous,
                    next=node.next._rebalance(),
                    transaction=self.transaction
                )
            else:
                break

        # Calculate balance factor for the current node
        balance = node._balance()

        # Perform rotations where necessary
        if balance < -1:  # Left-heavy
            if node.previous and node.previous._balance() > 0:  # Right-Left Case
                node = List(
                    value=node.value,
                    empty=False,
                    previous=node.previous._left_rotation(),
                    next=node.next,
                    transaction=self.transaction
                )
            return node._right_rotation()

        if balance > 1:  # Right-heavy
            if self.next and self.next._balance() < 0:  # Left-Right Case
                node = List(
                    value=node.value,
                    empty=False,
                    previous=node.previous,
                    next=node.next._right_rotation(),
                    transaction=self.transaction
                )
            return node._left_rotation()

        # If balance is satisfactory, return this node
        return node

    def set_at(self, offset: int, value: object) -> List | None:
        """
        Updates the value at the given offset in the list.

        Negative offsets are supported (from the end). If the normalized offset is
        outside the range [0, count-1], an IndexError is raised.
        """
        self._load()

        if offset < 0:
            offset = self.count + offset

        # Out-of-range for set_at: must reference an existing element
        # Special-case empty list: only offset 0 is valid (creates first element)
        if self.empty:
            if offset == 0:
                return List(
                    value=value,
                    empty=False,
                    previous=None,
                    next=None,
                    transaction=self.transaction
                )
            raise IndexError('Offset out of range')

        # Out-of-range for set_at: allow appending at the end (offset == count)
        if offset < 0 or offset > self.count:
            raise IndexError('Offset out of range')

        node_offset = self.previous.count if self.previous else 0

        cmp = offset - node_offset
        if cmp > 0:
            # Insert into the right subtree.
            if self.next:
                self.next._load()
                new_node = List(
                    value=self.value,
                    empty=False,
                    previous=self.previous,
                    next=self.next.set_at(offset - node_offset - 1, value),
                    transaction=self.transaction
                )
            else:
                new_node = List(
                    value=self.value,
                    empty=False,
                    previous=self.previous,
                    next=List(
                        value=value,
                        empty=False,
                        transaction=self.transaction
                    ),
                    transaction=self.transaction
                )
        elif cmp < 0:
            # Insert into the left subtree.
            if self.previous:
                self.previous._load()
                new_node = List(
                    value=self.value,
                    empty=False,
                    previous=self.previous.set_at(offset, value),
                    next=self.next,
                    transaction=self.transaction
                )
            else:
                new_node = List(
                    value=self.value,
                    empty=False,
                    previous=List(
                        value=value,
                        empty=False,
                        transaction=self.transaction
                    ),
                    next=self.next,
                    transaction=self.transaction
                )
        else:
            # Replace the value of the current node.
            new_node = List(
                value=value,
                empty=False,
                previous=self.previous,
                next=self.next,
                transaction=self.transaction
            )

        result = new_node._rebalance()

        if self.indexes:
            new_indexes = self.add2indexes(value)
        else:
            new_indexes = self.indexes

        return List(
            value=result.value,
            empty=result.empty,
            previous=result.previous,
            next=result.next,
            indexes=new_indexes,
            transaction=self.transaction
        )

    def insert_at(self, offset: int, value: object) -> List:
        """
        Insert value at the specified position. All followers will shift its position by one.

        Rebalances the structure after insertion if necessary.

        :param offset: The position to insert the new value
        :param value: The value (Atom) associated with the key.
        :return: A new List reflecting the updated state.
        """
        self._load()

        if offset < 0:
            offset = self.count + offset

        if offset < 0:
            offset = 0

        if offset >= self.count:
            offset = self.count

        node_offset = self.previous.count if self.previous else 0

        # Case: Inserting into an empty List.
        if self.empty:
            return List(
                value=value,
                empty=False,
                previous=None,
                next=None,
                transaction=self.transaction
            )

        cmp = offset - node_offset
        if cmp > 0:
            # Insert into the right subtree.
            if self.next:
                self.next._load()
                new_node = List(
                    value=self.value,
                    empty=False,
                    previous=self.previous,
                    next=self.next.insert_at(cmp - 1, value),
                    transaction=self.transaction
                )
            else:
                new_node = List(
                    value=self.value,
                    empty=False,
                    previous=self.previous,
                    next=List(
                        value=value,
                        empty=False,
                        previous=None,
                        next=None,
                        transaction=self.transaction
                    ),
                    transaction=self.transaction
                )
        elif cmp < 0:
            # Insert into the left subtree.
            if self.previous:
                self.previous._load()
                new_node = List(
                    value=self.value,
                    empty=False,
                    previous=self.previous.insert_at(cmp, value),
                    next=self.next,
                    transaction=self.transaction
                )
            else:
                new_node = List(
                    value=self.value,
                    empty=False,
                    previous=List(
                        value=value,
                        empty=False,
                        previous=None,
                        next=None,
                        transaction=self.transaction
                    ),
                    next=self.next,
                    transaction=self.transaction
                )
        else:
            # Insert the new value of the current node.
            new_node = List(
                value=value,
                empty=False,
                previous=self.previous,
                next=List(
                    value=self.value,
                    empty=False,
                    previous=None,
                    next=self.next,
                    transaction=self.transaction
                ),
                transaction=self.transaction
            )

        result = new_node._rebalance()

        if self.indexes:
            new_indexes = self.add2indexes(value)
        else:
            new_indexes = self.indexes

        return List(
            value=result.value,
            empty=result.empty,
            previous=result.previous,
            next=result.next,
            indexes=new_indexes,
            transaction=self.transaction
        )

    def remove_at(self, offset: int) -> List:
        """
        Removes the element at the specified index from the list.

        This method removes the element at the given offset and rebalances the list
        structure if necessary. If the offset is negative, it is treated as counting
        from the end of the list. If the offset is out of bounds, the list remains unchanged.

        :param offset: The index of the element to remove. Negative indices count from the end.
        :return: A new List instance with the element removed, or the original list if no changes were made.
        """
        self._load()

        if offset < 0:
            offset = self.count + offset

        if offset < 0:
            return self

        if offset >= self.count:
            return self

        node_offset = self.previous.count if self.previous else 0

        # Case: Remove from an empty List.
        if self.empty:
            return self

        current_value = self.get_at(offset)

        cmp = offset - node_offset
        if cmp > 0:
            # Remove from the right subtree.
            if self.next:
                self.next._load()
                new_node = List(
                    value=self.value,
                    empty=False,
                    previous=self.previous,
                    next=self.next.remove_at(offset - node_offset - 1),
                    transaction=self.transaction
                )
            else:
                if self.previous:
                    self.previous._load()
                return self.previous
        elif cmp < 0:
            # Remove from the left subtree.
            if self.previous:
                self.previous._load()
                new_node = List(
                    value=self.value,
                    empty=False,
                    previous=self.previous.remove_at(offset),
                    next=self.next,
                    transaction=self.transaction
                )
            else:
                if self.next:
                    self.next._load()
                return self.next
        else:
            # Remove this node
            if self.next:
                self.next._load()
                first_value = self.next.get_at(0)
                new_next = self.next.remove_first()
                new_node = List(
                    value=first_value,
                    empty=False,
                    previous=self.previous if self.previous and not self.previous.empty else None,
                    next=new_next if not new_next.empty else None,
                    transaction=self.transaction
                )
            elif self.previous:
                self.previous._load()
                last_value = self.previous.get_at(-1)
                new_previous = self.previous.remove_last()
                new_node = List(
                    value=last_value,
                    empty=False,
                    previous=new_previous if not new_previous.empty else None,
                    next=self.next,
                    transaction=self.transaction
                )
            else:
                return None

        result = new_node._rebalance()

        if self.indexes:
            new_indexes = self.remove_from_indexes(current_value)
        else:
            new_indexes = self.indexes

        return List(
            value=result.value,
            empty=result.empty,
            previous=result.previous,
            next=result.next,
            indexes=new_indexes,
            transaction=self.transaction
        )

    def remove_first(self) -> List:
        """
        Removes the first element from the list. If the list is empty, the operation
        returns the current list instance. Otherwise, it modifies the list by removing
        the first element, re-balancing the structure if required.

        :return: A new list instance where the first element has been removed, or the
            original list instance if the list was empty.
        :rtype: List
        """
        self._load()

        node_offset = self.previous.count if self.previous else 0

        # Case: Removing from an empty List.
        if self.empty:
            return self

        current_value = self.get_at(0)

        if node_offset > 0:
            # Remove from the left subtree.
            if self.previous:
                self.previous._load()
                previous_removed = self.previous.remove_first()
                new_node = List(
                    value=self.value,
                    previous=previous_removed if not previous_removed.empty else None,
                    next=self.next,
                    transaction=self.transaction
                )
            else:
                new_node = self
        else:
            # Remove this node.
            new_node = List(
                value=None,
                empty=True,
                previous=None,
                next=None,
                transaction=self.transaction
            )

        result = new_node._rebalance()

        if self.indexes:
            new_indexes = self.remove_from_indexes(current_value)
        else:
            new_indexes = self.indexes

        return List(
            value=result.value,
            empty=result.empty,
            previous=result.previous,
            next=result.next,
            indexes=new_indexes,
            transaction=self.transaction
        )

    def remove_last(self) -> List:
        """
        Removes the last element from the list.

        If the list is empty, the operation returns the current list instance. 
        Otherwise, it modifies the list by removing the last element, re-balancing 
        the structure if required.

        :return: A new list instance with the last element removed, or the original
                list if it was empty.
        """
        self._load()

        # Case: Removing from an empty List.
        if self.empty:
            return self

        current_value = self.get_at(-1)

        if self.next:
            self.next._load()
            # Remove from the right subtree
            next_removed = self.next.remove_last()
            new_node = List(
                value=self.value,
                previous=self.previous,
                next=next_removed if not next_removed.empty else None,
                transaction=self.transaction
            )
        else:
            # Remove this node.
            new_node = List(
                value=None,
                empty=True,
                previous=None,
                next=None,
                transaction=self.transaction
            )

        result = new_node._rebalance()

        if self.indexes:
            new_indexes = self.remove_from_indexes(current_value)
        else:
            new_indexes = self.indexes

        return List(
            value=result.value,
            empty=result.empty,
            previous=result.previous,
            next=result.next,
            indexes=new_indexes,
            transaction=self.transaction
        )

    def extend(self, items: List) -> List:
        """
        Return a new list with items from 'items' appended after the current list.
        This simple implementation builds the result by appending each element of
        'items' to the end, preserving ordering and counts.
        """
        self._load()
        result = self
        if items is None or (hasattr(items, 'empty') and items.empty):
            return result

        for it in items.as_iterable():
            result = result.insert_at(result.count, it)
        return result

    def append_first(self, item: object):
        """
        Inserts the given item at the first position of a collection by delegating to the `insert_at`
        method. This method serves as a utility to prepend an atom to the existing collection.

        :param item: The Atom instance to be appended as the first element.
        :type item: Atom
        :return: The result returned by the `insert_at` method when the item is inserted at index 0.
        """
        return self.insert_at(0, item)

    def append_last(self, item: object):
        """
        Appends the specified item at the end by delegating to insert_at(count, item).
        """
        self._load()
        return self.insert_at(self.count, item)

    def head(self, upper_limit: int):
        """
        Returns a portion of the list up to the specified upper limit. The behavior
        differs based on the `upper_limit` parameter, adjusting for cases where
        the limit exceeds the bounds of the list or is composed negatively, as well
        as scenarios requiring rebalancing of the nodes.

        :param upper_limit:
            The maximum number of items to include in the resulting list. If negative,
            it represents a count from the end of the list. Values shorter or greater
            than the bounds of the list are normalized accordingly.

        :return:
            A new list instance containing the items from the start to the specified
            `upper_limit`, rebalanced as necessary.
        """
        self._load()

        if upper_limit < 0:
            upper_limit = self.count + upper_limit

        if upper_limit < 0:
            upper_limit = 0

        if upper_limit >= self.count:
            upper_limit = self.count

        if upper_limit == 0:
            # Case: Returning an empty List.
            return List(transaction=self.transaction)

        if upper_limit == self.count:
            # Case: the full list
            return self

        node = self
        offset = node.previous.count if node.previous else 0
        cmp = upper_limit - offset

        if cmp == 0:
            # Exactly the left subtree count; do not include the current node
            if node.previous:
                return node.previous.head(upper_limit)
            else:
                return List(transaction=self.transaction)
        elif cmp > 0 and node.next:
            next_node = node.next.head(cmp - 1)
            node = List(
                value=node.value,
                empty=False,
                previous=self.previous,
                next=next_node if not next_node.empty else None,
                transaction=self.transaction
            )
        elif cmp < 0 and node.previous:
            node = node.previous.head(upper_limit)
        else:
            return List(transaction=self.transaction)

        return node._rebalance()

    def tail(self, lower_limit: int):
        """
        Provides functionality for handling and creating a new sublist starting from the given
        lower limit index, based on the current doubly linked list structure. The method adjusts
        and returns either the list starting from the given position, or an empty list if the
        provided lower limit is out of range.

        :param lower_limit: The starting index from which the sublist should be created.
            This index can be positive or negative. If negative, it starts counting from
            the end of the list.
        :type lower_limit: int
        :return: The modified sublist starting from the given lower limit, or an empty list
            if lower_limit is outside the range of the current list's indices.
        :rtype: List
        """
        self._load()

        if lower_limit < 0:
            lower_limit = self.count + lower_limit

        if lower_limit < 0:
            lower_limit = 0

        if lower_limit >= self.count:
            lower_limit = self.count

        if lower_limit == self.count:
            # Case: Returning an empty List.
            return List(transaction=self.transaction)

        if lower_limit == 0:
            # Case: the full list
            return self

        node = self
        offset = node.previous.count if node.previous else 0
        cmp = lower_limit - offset

        if cmp == 0:
            node = List(
                value=node.value,
                empty=False,
                previous=None,
                next=self.next,
                transaction=self.transaction
            )
        elif cmp > 0 and node.next:
            # Move to the right subtree reducing the remaining lower_limit
            node = node.next.tail(lower_limit - offset - 1)
        elif cmp < 0 and node.previous:
            # Move to the left subtree keeping the same lower_limit
            previous_node = node.previous.tail(lower_limit)
            node = List(
                value=node.value,
                empty=False,
                previous=previous_node if not previous_node.empty else None,
                next=self.next,
                transaction=self.transaction
            )
        else:
            return List(transaction=self.transaction)

        return node._rebalance()

    def slice(self, from_offset: int, to_offset: int):
        """
        Slices a portion of a sequence based on the specified start and end offsets.

        This method returns a subset of the sequence from the specified starting
        offset (inclusive) to the ending offset (exclusive). The offsets should
        be within the bounds of the sequence.

        :param from_offset: The starting index of the slice (inclusive).
        :param to_offset: The ending index of the slice (exclusive).
        :return: A portion of the sequence between the specified offsets.
        :rtype: Sequence
        """
        self._load()

        if from_offset < 0:
            from_offset = self.count + from_offset

        if from_offset < 0:
            from_offset = 0

        if from_offset >= self.count:
            from_offset = self.count

        if to_offset < 0:
            to_offset = self.count + to_offset

        if to_offset < 0:
            to_offset = 0

        if to_offset >= self.count:
            to_offset = self.count

        if from_offset > to_offset:
            # It's an empty list
            return List(
                value=None,
                empty=True,
                previous=None,
                next=None,
                transaction=self.transaction
            )

        # Here, it will be a non-empty list

        return self.tail(from_offset).head(to_offset - from_offset)
