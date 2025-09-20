from __future__ import annotations

import logging
from typing import cast

from .common import Atom, DBCollections, QueryPlan, Literal, AbstractTransaction, AtomPointer, ConcurrentOptimized
from .exceptions import ProtoNotSupportedException
from .lists import List
from .queries import IndexedQueryPlan, QueryableIndex, QueryContext, Term, Equal, Between, Greater, GreaterOrEqual, Lower, LowerOrEqual, IndexedSearchPlan, IndexedRangeSearchPlan
from .sets import Set, CountedSet

_logger = logging.getLogger(__name__)


class DictionaryItem(Atom):
    # Represents a key-value pair in a Dictionary, both durable and transaction-safe.
    key: object
    value: object

    def __init__(
            self,
            key: object = None,  # The key for the dictionary item (native type; no string coercion).
            value: object = None,  # The value associated with the key.
            transaction: AbstractTransaction = None,  # The associated transaction.
            atom_pointer: AtomPointer = None,  # Pointer to the atom for durability/consistency.
            **kwargs):  # Additional keyword arguments.
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.key = key
        self.value = value

    def _load(self):
        if not self._loaded:
            super()._load()
            if isinstance(self.key, Literal):
                self.key = self.key.string
            self._loaded = True

    # Provide deterministic ordering across mixed key types for AVL ordering
    @staticmethod
    def _order_key(val: object):
        t = type(val)
        try:
            if t is bool:
                return ("bool", 1 if val else 0)
            if t in (int, float):
                return ("number", float(val))
            if t is str:
                return ("str", val)
            if t is bytes:
                return ("bytes", val)
            # Fallback: type name + string representation
            return (t.__name__, str(val))
        except Exception:
            return (t.__name__, str(val))

    def __lt__(self, other: "DictionaryItem") -> bool:
        return self._order_key(self.key) < self._order_key(other.key)

    def __gt__(self, other: "DictionaryItem") -> bool:
        return self._order_key(self.key) > self._order_key(other.key)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DictionaryItem):
            return False
        return self.key == other.key


class Dictionary(DBCollections, ConcurrentOptimized):
    """
    Represents a durable, transaction-safe dictionary-like mapping between strings and values,
    with support for concurrent modifications.

    Only Atoms are recommended as keys and values to ensure consistency and durability.
    Mixing other objects is not supported, and no warnings will be issued for doing so.
    """

    content: List  # Internal storage for dictionary items as a list.
    _op_log: list

    def __init__(
            self,
            content: List = None,  # List to store the content.
            transaction: AbstractTransaction = None,  # Transaction context for operations.
            atom_pointer: AtomPointer = None,  # Pointer to ensure atomicity and durability.
            op_log: list = None,
            indexes: DBCollections | None = None,
            **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.content = content if content else List(transaction=transaction)  # Initialize content or create a new List.
        self.count = self.content.count  # Number of items in the dictionary.
        self._op_log = op_log if op_log is not None else []
        self.indexes = indexes

    def _load(self):
        if not self._loaded:
            super()._load()
            self.content._load()
            self._loaded = True

    def _save(self):
        self._load()
        if not self._saved:
            self.content.transaction = self.transaction
            self.content._save()
            super()._save()
            self._saved = True

    def as_iterable(self) -> list[tuple[str, object]]:
        """
        Provides an iterable generator for the dictionary's key-value pairs.

        :return: A generator yielding tuples of (key, value).
        """
        for item in self.content.as_iterable():  # Iterate through the content.
            item = (cast(DictionaryItem, item))  # Cast item to a DictionaryItem type.
            item._load()  # Ensure the item is loaded into memory.
            yield item.key, item.value  # Yield the key-value pair.

    def as_query_plan(self) -> QueryPlan:
        """
        Converts the dictionary into a QueryPlan, a representation for query execution.
        :return: The dictionary's query plan.
        """
        self._load()

        if self.indexes:
            return IndexedQueryPlan(base=self, indexes=cast(RepeatedKeysDictionary, self.indexes))
        else:
            return self.content.as_query_plan()

    def get_at(self, key: object) -> object | None:
        """
        Gets the element associated with the given key in the dictionary.

        Uses binary search to find the key efficiently, using native-type key comparisons
        with a deterministic ordering for mixed types (see DictionaryItem._order_key).

        :param key: The key to be searched.
        :return: The value stored at key or None if not found
        """
        self._load()
        self.content._load()

        def _ok(v):
            return DictionaryItem._order_key(v)

        left = 0
        right = self.content.count - 1
        target_ok = _ok(key)

        while left <= right:
            center = (left + right) // 2

            item = cast(DictionaryItem, self.content.get_at(center))
            if item is None:
                break
            item_ok = _ok(item.key)
            if item_ok == target_ok and item.key == key:
                if isinstance(item.value, Atom):
                    item.value._load()
                return item.value

            if item_ok >= target_ok:
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
        self._load()
        # Ensure Atom values have a stable pointer before insertion (immutability-friendly)
        try:
            from .common import Atom as _Atom
            if isinstance(value, _Atom):
                if not getattr(value, 'atom_pointer', None):
                    # Bind the child's transaction to this dictionary's transaction if missing
                    try:
                        if not getattr(value, 'transaction', None):
                            object.__setattr__(value, 'transaction', self.transaction)
                            # Ensure a fresh save cycle
                            object.__setattr__(value, '_saved', False)
                    except Exception:
                        pass
                    value._save()
        except Exception:
            pass

        def _ok(v):
            return DictionaryItem._order_key(v)

        left = 0
        right = self.content.count - 1
        new_content = self.content
        old_value = None
        target_ok = _ok(key)

        while left <= right:
            center = (left + right) // 2

            item = cast(DictionaryItem, self.content.get_at(center))
            if item is None:
                break
            item_ok = _ok(item.key)
            if item_ok == target_ok and item.key == key:  # Check if the key already exists.
                old_value = item.value
                new_content = new_content.set_at(
                    center,
                    DictionaryItem(
                        key=key,
                        value=value,
                        transaction=self.transaction
                    )
                )
                break
            if item_ok >= target_ok:
                right = center - 1
            else:
                left = center + 1
        else:
            new_content = new_content.insert_at(
                left,
                DictionaryItem(
                    key=key,
                    value=value,
                    transaction=self.transaction
                )
            )

        new_op_log = self._op_log + [('set', key, value)]

        new_indexes = self.indexes
        if self.indexes:
            if old_value:
                new_indexes = self.remove_from_indexes(old_value)
            new_indexes = self.add2indexes(value)

        return Dictionary(
            content=new_content,
            transaction=self.transaction,
            op_log=new_op_log,
            indexes=new_indexes
        )

    def remove_at(self, key: str) -> Dictionary:
        """
        Removes a key-value pair from the dictionary if the key exists.

        If the key is found, it removes the corresponding entry and rebalances the structure.
        If the key does not exist, the method returns the original dictionary.

        :param key: The string key of the item to be removed.
        :return: A new instance of Dictionary reflecting the removal.
        """
        self._load()

        def _ok(v):
            return DictionaryItem._order_key(v)

        left = 0
        right = self.content.count - 1
        target_ok = _ok(key)

        while left <= right:
            center = (left + right) // 2

            item = cast(DictionaryItem, self.content.get_at(center))
            if item is None:
                break
            item_ok = _ok(item.key)
            if item_ok == target_ok and item.key == key:
                # Found existing value to remove
                new_content = self.content.remove_at(center)
                if new_content is None:
                    # If the content is None, create an empty dictionary
                    new_content = List(transaction=self.transaction)
                new_op_log = self._op_log + [('remove', key, None)]
                new_indexes = self.indexes
                if self.indexes:
                    new_indexes = self.remove_from_indexes(item.value)
                return Dictionary(
                    content=new_content,
                    transaction=self.transaction,
                    op_log=new_op_log,
                    indexes=new_indexes
                )

            if item_ok >= target_ok:
                right = center - 1
            else:
                left = center + 1

        # Not found, nothing is changed
        return self

    def has(self, key: str) -> bool:
        """
        Checks whether a given key exists in the dictionary.

        Uses binary search to find the key efficiently with native-type comparisons.

        :param key: The key to be searched.
        :return: True if the key is found; otherwise, False.
        """
        self._load()

        def _ok(v):
            return DictionaryItem._order_key(v)

        left = 0
        right = self.content.count - 1
        target_ok = _ok(key)

        while left <= right:
            center = (left + right) // 2

            item = cast(DictionaryItem, self.content.get_at(center))
            if item is None:
                break
            item_ok = _ok(item.key)
            if item_ok == target_ok and item.key == key:
                return True

            if item_ok >= target_ok:
                right = center - 1
            else:
                left = center + 1

        return False

    def _rebase_on_concurrent_update(self, current_db_object: Atom) -> Atom:
        if not isinstance(current_db_object, Dictionary):
            raise ProtoNotSupportedException(
                "Cannot rebase onto a different object type."
            )

        rebased_dict = cast(Dictionary, current_db_object)
        rebased_dict._op_log = []

        # Data-driven merge: compare our staged values with the current DB values and reconcile
        try:
            # Build a dict of our staged key->value pairs
            staged_items = []
            for k, v in self.as_iterable():
                staged_items.append((k, v))
            for key, value in staged_items:
                current_val = rebased_dict.get_at(key)
                # 1) Integer counters: if both ints and value differs, treat as +1 increment
                if isinstance(value, int) and isinstance(current_val, int):
                    # Treat integer updates as idempotent increments: always apply +1 on the current value
                    rebased_dict = rebased_dict.set_at(key, current_val + 1)
                    continue
                # 2) Set/CountedSet buckets: union staged bucket into current bucket
                from .sets import Set as _Set, CountedSet as _CSet
                if hasattr(value, 'as_iterable') and isinstance(current_val, (_Set, _CSet)):
                    bucket = current_val
                    try:
                        for e in value.as_iterable():
                            # Guard against nested Set/CountedSet being added as elements
                            if isinstance(e, (_Set, _CSet)):
                                continue
                            bucket = bucket.add(e)
                        rebased_dict = rebased_dict.set_at(key, bucket)
                        continue
                    except Exception:
                        pass
                # 3) For other types: if value differs or key absent, last-writer-wins
                try:
                    equal = (value == current_val)
                except Exception:
                    equal = False
                if not equal:
                    rebased_dict = rebased_dict.set_at(key, value)
        except Exception:
            # On any failure, fallback to replaying op_log if available; otherwise last-writer-wins
            for op_type, key, value in self._op_log:
                if op_type == 'set':
                    rebased_dict = rebased_dict.set_at(key, value)
                elif op_type == 'remove':
                    rebased_dict = rebased_dict.remove_at(key)
                else:
                    raise ProtoNotSupportedException(
                        f"Unknown operation '{op_type}' during rebase."
                    )
        return rebased_dict


class RepeatedKeysDictionary(Dictionary, QueryableIndex):
    """
    Represents a dictionary-like data structure allowing multiple records
    associated with a single key, with support for concurrent modifications.

    This class extends the base Dictionary class and provides additional
    functionality for handling repeated keys, updating, and removing
    associated records. Duplicate values in the list associated with a key
    are allowed.
    """

    def __init__(
            self,
            content: List = None,
            indexes: DBCollections = None,
            transaction: AbstractTransaction = None,
            atom_pointer: AtomPointer = None,
            op_log: list = None,
            **kwargs):
        super().__init__(content=content, transaction=transaction, atom_pointer=atom_pointer, op_log=op_log, **kwargs)
        # Indexes are optional; keep None when not provided to avoid instantiating abstract base classes.
        self.indexes = indexes

    def get_at(self, key: str) -> Set | None:
        """
        Gets the elements at a given key, as a Set, if exists in the dictionary.

        :param key: The string key to be searched.
        :return: A Set of records for the key; returns an empty Set if not found.
        """
        value = super().get_at(key)
        if value is None:
            return Set(transaction=self.transaction)
        return value

    def set_at(self, key: str, value: Atom) -> RepeatedKeysDictionary:
        """
        Registers an intent to insert or update a key-value pair in the dictionary.

        This method checks if the specified key already exists in the dictionary. If the key exists,
        its associated record set (bucket) is updated with the new value. Buckets are CountedSet to
        preserve reference counts when the same record is added multiple times.

        :param key: The string key for the item being added or updated.
        :param value: The value associated with the key.
        :return: A new instance of Dictionary with the updated content.
        """
        # Load current bucket, upgrading to CountedSet when necessary
        if super().has(key):
            bucket = cast(Set, super().get_at(key))
            if not isinstance(bucket, CountedSet):
                # Convert Set -> CountedSet with count=1 per unique element
                cs = CountedSet(transaction=self.transaction)
                for v in bucket.as_iterable():
                    cs = cs.add(v)
                bucket = cs
        else:
            bucket = CountedSet(transaction=self.transaction)

        # Detect first insertion of this specific value for index update semantics
        previously_present = bucket.has(value)
        bucket = bucket.add(value)

        new_content = super(RepeatedKeysDictionary, self).set_at(key, bucket).content
        new_op_log = self._op_log + [('set', key, value)]

        new_indexes = self.indexes
        if self.indexes and not previously_present:
            # Update indexes only on the 0 -> 1 transition for this value within the bucket
            new_indexes = self.add2indexes(value)
        return RepeatedKeysDictionary(
            content=new_content,
            transaction=self.transaction,
            op_log=new_op_log,
            indexes=new_indexes
        )

    def remove_at(self, key: str) -> RepeatedKeysDictionary:
        """
        Registers an intent to remove a key and its associated values from the dictionary.

        :param key: The string key of the item to be removed.
        :return: A new instance of Dictionary reflecting the removal.
        """
        if super().has(key):
            value_set = cast(Set, super().get_at(key))
            new_indexes = self.indexes
            for value in value_set.as_iterable():
                new_indexes = self.remove_from_indexes(value)

            result = super(RepeatedKeysDictionary, self).remove_at(key)
            if result is None:
                # If the result is None, create an empty dictionary
                new_content = List(transaction=self.transaction)
            else:
                new_content = result.content
            new_op_log = self._op_log + [('remove', key, None)]

            return RepeatedKeysDictionary(
                content=new_content,
                transaction=self.transaction,
                op_log=new_op_log,
                indexes=new_indexes
            )
        else:
            return self

    def remove_record_at(self, key: str, record: Atom) -> RepeatedKeysDictionary:
        """
        Registers an intent to remove a specific record from the set associated with a given key.

        :param key: The key in the dictionary whose associated set must be updated.
        :param record: The specific record to be removed from the set.
        :return: Returns an updated dictionary object with the intended removal logged.
        """
        if super().has(key):
            record_set = cast(Set, super().get_at(key))
            new_content = self.content
            if record_set.has(record):
                updated_set = record_set.remove_at(record)
                # Remove the entire key if the bucket becomes empty
                if updated_set.count == 0:
                    new_content = new_content.remove_at(key)
                else:
                    # Update the key with the reduced set
                    new_content = super(RepeatedKeysDictionary, self).set_at(key, updated_set).content

                new_op_log = self._op_log + [('remove_record', key, record)]

                # Update indexes only when the record is no longer present in the bucket (last removal)
                new_indexes = self.indexes
                if self.indexes and not updated_set.has(record):
                    new_indexes = self.remove_from_indexes(record)
                return RepeatedKeysDictionary(
                    content=new_content,
                    transaction=self.transaction,
                    op_log=new_op_log,
                    indexes=new_indexes
                )

            return self

    def build_query_plan(self, term: Term, context: QueryContext) -> QueryPlan | None:
        """
        Implement QueryableIndex: produce an index-backed plan for supported operations.
        This dictionary represents an ordered index over a single field (the caller passes the Term).
        """
        try:
            # Ensure we have a valid Term
            if not isinstance(term, Term):
                return None
            field = term.target_attribute
            op = term.operation

            # Build a minimal index mapping for the planner: {field: this_index}
            idxs = Dictionary(transaction=context.transaction or self.transaction)
            idxs = idxs.set_at(field, self)
            tx = context.transaction or self.transaction

            if isinstance(op, Equal):
                return IndexedSearchPlan(
                    field_to_scan=field,
                    operator=op,
                    value=term.value,
                    indexes=idxs,
                    based_on=None,
                    transaction=tx,
                )
            if isinstance(op, Between):
                lo, hi = term.value if isinstance(term.value, tuple) else (None, None)
                if lo is None or hi is None:
                    return None
                return IndexedRangeSearchPlan(
                    field_to_scan=field,
                    lo=lo,
                    hi=hi,
                    include_lower=op.include_lower,
                    include_upper=op.include_upper,
                    indexes=idxs,
                    based_on=None,
                    transaction=tx,
                )
            if isinstance(op, Greater):
                return IndexedRangeSearchPlan(
                    field_to_scan=field,
                    lo=term.value,
                    hi=float('inf'),
                    include_lower=False,
                    include_upper=True,
                    indexes=idxs,
                    based_on=None,
                    transaction=tx,
                )
            if isinstance(op, GreaterOrEqual):
                return IndexedRangeSearchPlan(
                    field_to_scan=field,
                    lo=term.value,
                    hi=float('inf'),
                    include_lower=True,
                    include_upper=True,
                    indexes=idxs,
                    based_on=None,
                    transaction=tx,
                )
            if isinstance(op, Lower):
                return IndexedRangeSearchPlan(
                    field_to_scan=field,
                    lo=float('-inf'),
                    hi=term.value,
                    include_lower=True,
                    include_upper=False,
                    indexes=idxs,
                    based_on=None,
                    transaction=tx,
                )
            if isinstance(op, LowerOrEqual):
                return IndexedRangeSearchPlan(
                    field_to_scan=field,
                    lo=float('-inf'),
                    hi=term.value,
                    include_lower=True,
                    include_upper=True,
                    indexes=idxs,
                    based_on=None,
                    transaction=tx,
                )
            return None
        except Exception:
            return None

    def _rebase_on_concurrent_update(self, current_db_object: Atom) -> Atom:
        """
        Re-applies the operations from this transaction on top of the current
        database state for this object, allowing for conflict-free merges.
        """
        if not isinstance(current_db_object, RepeatedKeysDictionary):
            raise ProtoNotSupportedException(
                "Cannot rebase onto a different object type."
            )

        rebased_dict = cast(RepeatedKeysDictionary, current_db_object)
        rebased_dict._op_log = []

        for op_type, key, value in self._op_log:
            if op_type == 'set':
                rebased_dict = rebased_dict.set_at(key, value)
            elif op_type == 'remove':
                rebased_dict = rebased_dict.remove_at(key)
            elif op_type == 'remove_record':
                rebased_dict = rebased_dict.remove_record_at(key, value)
            else:
                raise ProtoNotSupportedException(
                    f"Unknown operation '{op_type}' during rebase."
                )

        return rebased_dict
