from __future__ import annotations

import uuid

from .common import Atom, QueryPlan, DBCollections, AbstractTransaction, AtomPointer


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

        :return:
        """
        for item in self.base.as_iterable():
            yield item

    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        """

        :param full_plan:
        :return:
        """
        return self


class List(Atom):
    empty: bool # Indicator to represent empty lists
    value: object | None  # The value associated with this position; None is a valid value.
    height: int  # The height of the current subtree rooted at this node.
    next: List  # Reference to the next node in the structure (right child in a tree context).
    previous: List  # Reference to the previous node in the structure (left child in a tree context).

    def __init__(
            self,
            value: object = None,
            empty: bool = False,
            next: List = None,
            previous: List = None,
            transaction: AbstractTransaction = None,
            atom_pointer: AtomPointer = None,
            **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)

        # Initialize the current node's key, value, and child references.
        self.value = value
        self.next = next
        self.previous = previous
        self.empty = empty

        # Calculate the total count of nodes in the current subtree.
        if not empty:
            count = 1
            if self.previous:
                count += self.previous.count
            if self.next:
                count += self.next.count
            self.count = count
        else:
            self.count = 0

        # Calculate the height of the current subtree.
        if not empty:
            height = 1
            previous_height = previous.height if previous else 0
            next_height = next.height if next else 0
            height += max(previous_height, next_height)
            self.height = height
        else:
            self.height = 0

    def as_iterable(self) -> list[tuple[int, object]]:
        """
        Get an iterable list of the list items

        :return:
        """

        # Get an iterable of the List items
        def scan(node: List) -> list:
            if node.previous:
                yield from scan(node.previous)  # Subárbol izquierdo (recursión/yield)
            if not node.empty:
                yield node.value  # Nodo actual
            if node.next:
                yield from scan(node.next)  # Subárbol derecho (recursión/yield)

        return scan(self)

    def as_query_plan(self) -> QueryPlan:
        """
        Get a QueryPlan out of the HashDictionary
        :return:
        """
        return ListQueryPlan(base=self)

    def get_at(self, offset: int) -> Atom | None:
        """
        Searches for the value associated with the offset in the List.

        :param offset: The integer offset to look up for in the structure.
        :return: The value (Atom) associated with the key, or None if not found.
        """
        if self.empty:
            return None

        if offset < 0:
            offset = self.count + offset

        if offset < 0 or offset >= self.count:
            return None

        node = self
        while node is not None:
            node_offset = node.previous.count if node.previous else 0

            if offset == node_offset:
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
            return self.next.height - self.previous.height
        elif self.previous:
            return -self.previous.height
        elif self.next:
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
        new_right = List(
            value=self.value,
            previous=self.previous.next,
            next=self.next
        )

        # Promote the left child as the new root.
        return List(
            value=self.previous.value,
            previous=self.previous.previous,
            next=new_right
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
        new_left = List(
            value=self.value,
            previous=self.previous,
            next=self.next.previous
        )

        # Promote the right child as the new root.
        return List(
            value=self.next.value,
            previous=new_left,
            next=self.next.next
        )

    def _rebalance(self) -> List:
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
                new_node = List(
                    value=new_node.value,
                    previous=new_node.previous._left_rotation(),
                    next=new_node.next
                )
                if not new_node.previous:
                    return new_node
                new_node = new_node._right_rotation()

            while balance < 0 and new_node.next and new_node.next._balance() < 0:
                new_node = List(
                    value=new_node.value,
                    previous=new_node.previous,
                    next=new_node.next._right_rotation()
                )
                if not new_node.next:
                    return new_node
                new_node = new_node._left_rotation()

        return new_node  # Return the new root of the rebalanced subtree.

    def set_at(self, offset: int, value: Atom) -> List | None:
        """
        Updates or inserts a value in a self-balancing linked list structure at the given offset.

        If the offset is negative, it is treated as counting backwards from the end
        of the list. If the offset is out of bounds, the value is inserted at the
        start or end of the list. The method ensures the appropriate rebalancing
        after performing the insertion or update.

        :param offset: Specifies the position in the list where the value is to be
            inserted or updated. Negative values indicate a position relative
            to the end of the list.
        :param value: The new value to be set at the specified offset. An instance
            of the `Atom` class.
        :return: A new instance of the `List` reflecting the updated state of the
            self-balancing linked list, or None if no changes occurred.
        """

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
                previous=None,
                next=None
            )

        cmp = offset - node_offset
        if cmp > 0:
            # Insert into the right subtree.
            if self.next:
                new_node = List(
                    value=self.value,
                    previous=self.previous,
                    next=self.next.set_at(cmp, value)
                )
            else:
                new_node = List(
                    value=self.value,
                    previous=self.previous,
                    next=List(
                        value=value,
                    )
                )
        elif cmp < 0:
            # Insert into the left subtree.
            if self.previous:
                new_node = List(
                    value=self.value,
                    previous=self.previous.set_at(cmp, value),
                    next=self.next
                )
            else:
                new_node = List(
                    value=self.value,
                    previous=List(
                        value=value,
                    ),
                    next=self.next
                )
        else:
            # Replace the value of the current node.
            new_node = List(
                value=value,
                previous=self.previous,
                next=self.next
            )

        return new_node._rebalance()

    def insert_at(self, offset: int, value: Atom) -> List:
        """
        Insert value at the specified position. All followers will shift its position by one.

        Rebalances the structure after insertion if necessary.

        :param offset: The position to insert the new value
        :param value: The value (Atom) associated with the key.
        :return: A new HashDictionary reflecting the updated state.
        """
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
                previous=None,
                next=None
            )

        cmp = offset - node_offset
        if cmp > 0:
            # Insert into the right subtree.
            if self.next:
                new_node = List(
                    value=self.value,
                    previous=self.previous,
                    next=self.next.insert_at(cmp, value)
                )
            else:
                new_node = List(
                    value=self.value,
                    previous=self.previous,
                    next=List(
                        value=value,
                        previous=None,
                        next=None
                    )
                )
        elif cmp < 0:
            # Insert into the left subtree.
            if self.previous:
                new_node = List(
                    value=self.value,
                    previous=self.previous.insert_at(cmp, value),
                    next=self.next
                )
            else:
                new_node = List(
                    value=self.value,
                    previous=List(
                        value=value,
                        previous=None,
                        next=None
                    ),
                    next=self.next
                )
        else:
            # Insert the new value of the current node.
            new_node = List(
                value=value,
                previous=self.previous,
                next=List(
                    value=self.value,
                    previous=None,
                    next=self.next
                )
            )

        return new_node._rebalance()

    def remove_at(self, offset: int) -> List:
        """
        Removes the element at the specified index.
        :param offset:
        :return:
        """
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

        cmp = offset - node_offset
        if cmp > 0:
            # Remove from the right subtree.
            if self.next:
                new_node = List(
                    value=self.value,
                    previous=self.previous,
                    next=self.next.remove_at(cmp)
                )
            else:
                new_node = List()
        elif cmp < 0:
            # Remove from the left subtree.
            if self.previous:
                new_node = List(
                    value=self.value,
                    previous=self.previous.remove_at(cmp),
                    next=self.next
                )
            else:
                new_node = List()
        else:
            # Remove this node
            if self.previous:
                last_value = self.previous.get_at(-1)
                new_previous = self.previous.remove_last()
                new_node = List(
                    value=last_value,
                    previous=new_previous if not new_previous.empty else None,
                    next=self.next
                )
            elif self.next:
                first_value = self.next.get_at(0)
                new_next = self.next.remove_first()
                new_node = List(
                    value=first_value,
                    next=new_next if not new_next.empty else None
                )
            else:
                return List(
                    empty=True
                )

        return new_node._rebalance()

    def remove_first(self) -> List:
        """
        Removes the first element from the list. If the list is empty, the operation
        returns the current list instance. Otherwise, it modifies the list by removing
        the first element, re-balancing the structure if required.

        :return: A new list instance where the first element has been removed, or the
            original list instance if the list was empty.
        :rtype: List
        """

        node_offset = self.previous.count if self.previous else 0

        # Case: Removing from an empty List.
        if self.empty:
            return self

        new_node: List | None = None  # Placeholder for the updated subtree.
        if node_offset > 0:
            # Remove from the left subtree.
            if self.previous:
                previous_removed = self.previous.remove_first()
                new_node = List(
                    value=self.value,
                    previous=previous_removed if not previous_removed.empty else None,
                    next=self.next
                )
            else:
                new_node = self
        else:
            # Remove this node.
            new_node = List(
                value=None,
                empty=True,
                previous=None,
                next=None
            )

        return new_node._rebalance()

    def remove_last(self) -> List:
        """
        Removes the last element from the list. If the list is empty, the operation
        returns the current list instance. Otherwise, it modifies the list by removing
        the last element, re-balancing the structure if required.

        :return:
        """
        # Case: Removing from an empty List.
        if self.empty:
            return self

        if self.next:
            # Remove from the right subtree
            next_removed = self.next.remove_last()
            new_node = List(
                value=self.value,
                previous=self.previous,
                next=next_removed if not next_removed.empty else None
            )
        else:
            # Remove this node.
            new_node = List(
                value=None,
                empty=True,
                previous=None,
                next=None
            )

        return new_node._rebalance()

    def extend(self, items: List) -> List:
        """
        Extend the current list with the given items, maintaining the structure of the list.
        If the current list is empty, the returned list is a new list with the provided items.
        Otherwise, appends or extends the list while ensuring the correct structure.

        :param items: The items to extend the current list with.
        :type items: List
        :return: A new rebalanced list with the items appended.
        :rtype: List
        """
        # Case: extending an empty List.
        if self.empty:
            return items

        if self.next:
            # Extend the right subtree.
            new_node = self.next.extend(items)
        else:
            # Extend this node
            new_node = List(
                value=None,
                empty=True,
                previous=None,
                next=items
            )

        return new_node._rebalance()

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
        Appends the specified item to the end of the collection by inserting it at the
        last position.

        :param item: The item to be appended to the collection.
        :type item: Atom
        :return: The result of the insertion operation.
        """
        node = self
        while node:
            if node.next:
                node = node.next
            else:
                break

        node = List(
            value=node.value,
            empty=False,
            previous=None,
            next=List(
                value=item
            )
        )

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
                next=None
            )

        # Here, it will be a non-empty list

        # TODO Find a more efficient way to do this
        new_list = List()
        for offset in range(from_offset, to_offset):
            new_list = new_list.set_at(offset-from_offset, self.get_at(offset))

        return new_list


