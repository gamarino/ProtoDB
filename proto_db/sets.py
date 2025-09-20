from __future__ import annotations
from typing import cast, TYPE_CHECKING, Iterable
from .common import Atom, QueryPlan, AbstractTransaction, AtomPointer, DBCollections, canonical_hash
from .lists import List
from .hash_dictionaries import HashDictionary
from .queries import IndexedQueryPlan

if TYPE_CHECKING:
    # Only for type annotations
    from .dictionaries import Dictionary, RepeatedKeysDictionary


class Set(Atom):
    """
    A mathematical set of unique elements with dual ephemeral/persistent behavior.

    Backed by a ``HashDictionary`` for efficient membership and iteration, ``Set`` can
    hold any Python object; however, only ``Atom`` instances participate in durable
    persistence.

    .. note::
       ``Set`` uses a dual-state model to handle temporary objects safely:

       - ``_new_objects`` is a staging area for elements added during the transaction that
         have not yet been persisted. Objects here live only in memory and are NOT written
         to storage unless the Set itself becomes part of the committed object graph.
       - ``content`` contains the persisted elements. Only when a Set is committed are the
         staged objects promoted from ``_new_objects`` into ``content``.

       This design avoids unintended persistence caused by hashing new Atoms. You can freely
       use Sets for intermediate computations (union, intersection, etc.) without incurring
       writes, as long as the Set is not stored into a persistent structure or root.
    """
    content: HashDictionary  # The underlying container storing the set elements.

    """
    Initializes a `Set` instance.

    :param content: The `HashDictionary` instance that represents the underlying storage of the set.
    :param transaction_id: (Optional) A unique transaction identifier for audit or rollback use.
    :param offset: An optional offset for identifying the set's relative position in an operation.
    :param kwargs: Any additional data passed for extended configurations.
    """

    # _new_objects is a temporary storage for objects that have not been saved yet.
    # This is used to ensure that the set will not be persisted in storage until commit.
    # This logic enables the creation of temporary sets inside the transaction, that will
    # be persisted only when the transaction is committed. If the set is not part of
    # the finally committed objects, no storage will be consumed.
    _new_objects: HashDictionary = None

    def __init__(
            self,
            content: HashDictionary = None,
            new_objects: HashDictionary = None,
            transaction: AbstractTransaction = None,
            atom_pointer: AtomPointer = None,
            indexes: DBCollections | None = None,
            **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.content = content if content else HashDictionary(transaction=transaction)
        self._new_objects = new_objects if new_objects else HashDictionary(transaction=transaction)
        self.count = self.content.count + self._new_objects.count

        if indexes is None:
            # Local import to avoid circular dependency
            from .dictionaries import Dictionary as _Dictionary
            self.indexes = _Dictionary(transaction=transaction)
        else:
            self.indexes = indexes

    # Unified hashing that avoids cross-type collisions and unstable process salts
    def _hash_of(self, key: object) -> int:
        """
        Compute a stable identity/code for membership and indexing that minimizes collisions:
        - Atoms: if persisted, use AtomPointer.hash(); else fall back to id(obj) to avoid forcing persistence.
        - str: use transaction._get_string_hash (sha256-based) for stability and low collision.
        - Numbers/bool: compute a SHA-256 over a typed string "<type>:<value>" for deterministic hashing.
        - Other objects: compute SHA-256 over a typed repr to reduce collisions; if repr fails, use built-in hash.
        """
        try:
            from .common import Atom as _Atom, AtomPointer as _AtomPointer
            if isinstance(key, _Atom):
                ap = getattr(key, 'atom_pointer', None)
                if ap and getattr(ap, 'transaction_id', None):
                    try:
                        return ap.hash()
                    except Exception:
                        pass
                # Ephemeral atom: avoid persisting just for hashing
                return id(key)
            if isinstance(key, str):
                tr = getattr(self, 'transaction', None)
                if tr and hasattr(tr, '_get_string_hash'):
                    return tr._get_string_hash(key)
                # Fallback: stable sha256
                import hashlib
                return int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16)
            if isinstance(key, (int, float, bool)):
                import hashlib
                s = f"{type(key).__name__}:{key}".encode('utf-8')
                return int(hashlib.sha256(s).hexdigest(), 16)
            # Generic object: try a typed repr
            import hashlib
            try:
                s = f"{type(key).__name__}:{repr(key)}".encode('utf-8')
                return int(hashlib.sha256(s).hexdigest(), 16)
            except Exception:
                return hash(key)
        except Exception:
            return hash(key)

    def _save(self):
        if not self._saved:
            for h, element in self._new_objects.as_iterable():
                if isinstance(element, Atom):
                    element._save()
                self.content = self.content.set_at(self._hash_of(element), element)

            self.content.transaction = self.transaction
            self.content._save()

            # Ensure indexes are persisted along with the Set
            try:
                if self.indexes is not None:
                    self.indexes.transaction = self.transaction
                    self.indexes._save()
            except Exception:
                pass

            super()._save()

    def as_iterable(self) -> Iterable:
        """
        Converts the `Set` to an iterable structure, essentially a collection of its unique
        elements, and yields each element stored in the set.

        :return: A generator containing all the elements (`Atom`) in the set.
        """
        # Iterate over the stored hash dictionary's iterable and yield its items (the stored Atoms).
        self._load()

        for h, element in self._new_objects.as_iterable():
            yield element

        for hash_value, item in self.content.as_iterable():
            yield item

    def __iter__(self):
        # Allow Python iteration protocols (e.g., list(set_obj))
        return iter(self.as_iterable())

    def as_query_plan(self) -> QueryPlan:
        """
        Converts the `Set` into a `QueryPlan` object for integration with larger query
        execution frameworks. Delegates the conversion to the underlying `HashDictionary`.

        :return: A `QueryPlan` representation of the current Set.
        """
        self._load()

        if self.indexes:
            return IndexedQueryPlan(base=self, indexes=self.indexes)
        else:
            return QueryPlan(base=self)

    def add_index(self, index_def):
        """
        Add an index over the elements of the Set.
        Backward compatible: if given a string, builds a RepeatedKeysDictionary whose key is the element itself.
        If given an IndexDefinition, uses its index_class and extractor.
        For vector indexes, uses bulk build with an id→object map.
        """
        from .dictionaries import RepeatedKeysDictionary
        from .indexes import IndexDefinition as _IndexDef
        from .common import canonical_hash as _canonical_hash

        if isinstance(index_def, str):
            field_name = index_def
            new_index = RepeatedKeysDictionary(transaction=self.transaction)
            if self.count > 0:
                for rec in self.as_iterable():
                    try:
                        key = getattr(rec, field_name)
                    except Exception:
                        key = None
                    if key is not None:
                        # Unwrap Literal-like objects to raw values
                        try:
                            if hasattr(key, 'string'):
                                key = getattr(key, 'string')
                        except Exception:
                            pass
                        new_index = new_index.set_at(key, rec)
            index_name = field_name
        else:
            if not isinstance(index_def, _IndexDef):
                raise TypeError("add_index expects a field name (str) or IndexDefinition")
            index_name = index_def.name
            params = index_def.index_params or {}
            new_index = index_def.index_class(transaction=self.transaction, **params) if 'transaction' in getattr(index_def.index_class.__init__, '__code__', ()).co_varnames else index_def.index_class(**params)

            if hasattr(new_index, 'set_at'):
                if not self.empty:
                    for rec in self.as_iterable():
                        keys = index_def.extractor(rec)
                        try:
                            it = iter(keys)
                            for k in it:
                                if isinstance(k, tuple) and len(k) == 2:
                                    k = k[1]
                                if k is not None:
                                    new_index = new_index.set_at(k, rec)
                        except TypeError:
                            k = keys
                            if k is not None:
                                new_index = new_index.set_at(k, rec)
            elif hasattr(new_index, 'build'):
                vecs = []
                ids = []
                id_to_obj = {}
                if not self.empty:
                    for rec in self.as_iterable():
                        v = index_def.extractor(rec)
                        if v is None:
                            continue
                        vid = _canonical_hash(rec)
                        ids.append(vid)
                        vecs.append(v)
                        id_to_obj[vid] = rec
                try:
                    new_index.build(vectors=vecs, ids=ids)
                except TypeError:
                    new_index.build(vecs, ids)
                try:
                    setattr(new_index, '_id_to_obj', id_to_obj)
                except Exception:
                    pass
            else:
                raise TypeError("Unsupported index type: missing set_at/build")

        new_indexes = self.indexes.set_at(index_name, new_index) if self.indexes else None
        if self.indexes is None:
            from .dictionaries import Dictionary as _Dictionary
            new_indexes = _Dictionary(transaction=self.transaction).set_at(index_name, new_index)

        return Set(
            content=self.content,
            new_objects=self._new_objects,
            indexes=new_indexes,
            transaction=self.transaction
        )

    def remove_index(self, field_name: str):
        if self.indexes and self.indexes.has(field_name):
            new_indexes = self.indexes.remove_at(field_name)

            return Set(
                content=self.content,
                indexes=new_indexes,
                transaction=self.transaction
            )
        else:
            return self

    def has(self, key: object) -> bool:
        """
        Checks whether the specified `key` exists in the `Set`.

        :param key: The object to search for in the set. This can be an instance of `Atom`.
        :return: `True` if the key exists in the set, otherwise `False`.
        """
        # Calculate the canonical hash of the key
        item_hash = self._hash_of(key)

        # Check if the computed hash exists in the `HashDictionary`.
        self._load()

        if self._new_objects.has(item_hash):
            return True
        else:
            return self.content.has(item_hash)

    def add(self, key: object) -> Set:
        """
        Adds the specified `key` to the `Set`, creating and returning a new `Set`
        that includes the newly added key.

        The current `Set` instance remains immutable, and instead, a new instance is returned.

        :param key: The object to add to the set. This can be an instance of `Atom`.
        :return: A new `Set` object that contains the additional key.
        """

        # Create and return a new `Set` with the updated `HashDictionary`.
        self._load()

        # Defensive: avoid adding Set/CountedSet objects as elements (would be unhashable and semantically invalid)
        try:
            from .sets import CountedSet as _CSet  # local import to avoid circulars in type checkers
            if isinstance(key, (Set, _CSet)):
                return self
        except Exception:
            try:
                if isinstance(key, Set):
                    return self
            except Exception:
                pass

        if self.has(key):
            return self

        self._new_objects = self._new_objects.set_at(self._hash_of(key), key)

        new_indexes = self.indexes
        if self.indexes:
            new_indexes = self.indexes.add2indexes(key)

        return Set(
            content=self.content,
            new_objects=self._new_objects,
            transaction=self.transaction,
            indexes=new_indexes
        )

    def remove_at(self, key: object) -> Set:
        """
        Removes the specified `key` from the `Set`, creating and returning a new `Set`
        that excludes the specified key. Returns the same set if the key does not exist.

        :param key: The object to remove from the set. This can be an instance of `Atom`.
        :return: A new `Set` object with the key removed, or unchanged if the key is absent.
        """
        self._load()

        # Calculate the canonical hash of the key for removal
        item_hash = self._hash_of(key)

        if not self.has(key):
            return self

        new_objects = self._new_objects
        new_content = self.content
        if self._new_objects.has(item_hash):
            new_objects = self._new_objects.remove_at(item_hash)
        else:
            new_content = self.content.remove_at(item_hash)

        # Create and return a new `Set` with the updated `HashDictionary`.
        new_indexes = self.indexes
        if new_indexes:
            new_indexes = new_indexes.remove_from_indexes(key)

        return Set(
            content=new_content,
            new_objects=new_objects,
            transaction=self.transaction,
            indexes=new_indexes
        )

    def union(self, other: Set) -> Set:
        """
        Creates a new set containing all elements from both this set and the other set.

        :param other: Another Set to union with this one.
        :return: A new Set containing all elements from both sets.
        """
        self._load()
        other._load()

        result = self
        for item in other.as_iterable():
            result = result.add(item)

        return result

    def intersection(self, other: Set) -> Set:
        """
        Creates a new set containing only elements that are present in both this set and the other set.

        :param other: Another Set to intersect with this one.
        :return: A new Set containing only elements present in both sets.
        """
        self._load()
        other._load()

        result = Set(transaction=self.transaction)
        for item in self.as_iterable():
            item_hash = self._hash_of(item)

            if other.has(item):
                result = result.add(item)

        return result

    def difference(self, other: Set) -> Set:
        """
        Creates a new set containing elements that are in this set but not in the other set.

        :param other: Another Set to subtract from this one.
        :return: A new Set containing elements in this set that are not in the other set.
        """
        self._load()
        other._load()

        result = Set(transaction=self.transaction)
        for item in self.as_iterable():
            item_hash = self._hash_of(item)

            if not other.has(item):
                result = result.add(item)

        return result



class CountedSet(Set):
    """
    A multiset variant of Set that counts occurrences internally while preserving Set's external semantics:
    - Iteration yields unique items (no duplicates)
    - count reflects the number of unique items
    - total_count reflects the total number of occurrences (sum of per-item counters)

    Index update semantics:
    - On first insertion (0 -> 1) of a key, add2indexes(key) is invoked
    - On last removal (1 -> 0), remove_from_indexes(key) is invoked
    - Intermediate increments/decrements do not touch indexes
    """

    # _new_objects is a temporary storage for objects that have not been saved yet.
    # This is used to ensure that the set will not be persisted in storage until commit.
    # This logic enables the creation of temporary sets inside the transaction, that will
    # be persisted only when the transaction is committed. If the set is not part of
    # the finally committed objects, no storage will be consumed.
    _new_objects: HashDictionary = None
    _new_counts: HashDictionary = None

    def __init__(self,
                 items: HashDictionary | None = None,
                 counts: HashDictionary | None = None,
                 new_objects: HashDictionary | None = None,
                 new_counts: HashDictionary | None = None,
                 transaction: AbstractTransaction = None,
                 atom_pointer: AtomPointer = None,
                 indexes: DBCollections | None = None,
                 **kwargs):
        super().__init__(content=items if items is not None else HashDictionary(transaction=transaction),
                         transaction=transaction, atom_pointer=atom_pointer, indexes=indexes, **kwargs)
        # Internals
        self.items: HashDictionary = self.content  # alias for clarity
        self.counts: HashDictionary = counts if counts is not None else HashDictionary(transaction=transaction)
        self._new_objects = new_objects if new_objects is not None else HashDictionary(transaction=transaction)
        # Unique count mirrors items.count
        self._new_counts = new_counts if new_counts is not None else HashDictionary(transaction=transaction)
        # Unique item count equals items.count (items holds unique view; _new_objects are mirrored in items)
        self.count = self.items.count

    # Hashing strategy identical to Set
    def _hash_of(self, key: object) -> int:
        """
        Compute a stable integer identity for membership that minimizes collisions:
        - Atoms: if persisted, use AtomPointer.hash(); else use id(obj) to avoid forcing persistence.
        - str: use transaction._get_string_hash (sha256-based) or fallback to sha256 of the string.
        - Numbers/bool: sha256 over a typed string "<type>:<value>" for deterministic hashing.
        - Other objects: sha256 over a typed repr; fallback to built-in hash on failure.
        """
        try:
            from .common import Atom as _Atom, AtomPointer as _AtomPointer
            if isinstance(key, _Atom):
                ap = getattr(key, 'atom_pointer', None)
                if ap and getattr(ap, 'transaction_id', None):
                    try:
                        return ap.hash()
                    except Exception:
                        pass
                return id(key)
            if isinstance(key, str):
                tr = getattr(self, 'transaction', None)
                if tr and hasattr(tr, '_get_string_hash'):
                    return tr._get_string_hash(key)
                import hashlib
                return int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16)
            if isinstance(key, (int, float, bool)):
                import hashlib
                s = f"{type(key).__name__}:{key}".encode('utf-8')
                return int(hashlib.sha256(s).hexdigest(), 16)
            import hashlib
            try:
                s = f"{type(key).__name__}:{repr(key)}".encode('utf-8')
                return int(hashlib.sha256(s).hexdigest(), 16)
            except Exception:
                return hash(key)
        except Exception:
            return hash(key)

    def _save(self):
        if not self._saved:
            # Persist base Set first (moves _new_objects into items/content)
            super()._save()
            # Consolidate counts for pending new objects using their pending counts
            for h, element in self._new_objects.as_iterable():
                if isinstance(element, Atom):
                    element._save()
                hash_index = h
                inc = cast(int, self._new_counts.get_at(hash_index)) or 0
                base = cast(int, self.counts.get_at(hash_index)) if self.counts.has(hash_index) else 0
                # Ensure the item exists and set the correct total count
                self.items = self.items.set_at(hash_index, element)
                self.counts = self.counts.set_at(hash_index, base + inc)

            # Save both dictionaries
            self.items.transaction = self.transaction
            self.items._save()
            self.counts.transaction = self.transaction
            self.counts._save()

    # External API
    def as_iterable(self):
        self._load()
        # Items dictionary holds the unique elements view (persisted + staged)
        for h, item in self.items.as_iterable():
            yield item

    def __iter__(self):
        return iter(self.as_iterable())

    def as_query_plan(self) -> QueryPlan:
        self._load()
        if self.indexes:
            return IndexedQueryPlan(base=self, indexes=self.indexes)
        return QueryPlan(base=self)

    def has(self, key: object) -> bool:
        h = self._hash_of(key)

        self._load()
        if self._new_counts.has(h):
            return True
        return self.counts.has(h)

    def get_count(self, key: object) -> int:
        h = self._hash_of(key)
        self._load()
        if self.counts.has(h):
            return cast(int, self.counts.get_at(h))
        elif self._new_counts.has(h):
            return cast(int, self._new_counts.get_at(h))
        else:
            return 0

    @property
    def total_count(self) -> int:
        """
        Total number of occurrences across all unique items (persisted + pending).
        """
        self._load()
        total = 0
        # Sum persisted counts
        for h, cnt in self.counts.as_iterable():
            try:
                total += int(cnt)
            except Exception:
                pass
        return total

    def add(self, key: object) -> 'CountedSet':
        # Defensive: avoid adding Set/CountedSet objects as elements
        try:
            if isinstance(key, (Set, CountedSet)):
                return self
        except Exception:
            pass
        h = self._hash_of(key)
        self._load()
        if self.counts.has(h):
            # Increment existing persisted count; no index updates for intermediate increments
            new_counts = self.counts.set_at(h, cast(int, self.counts.get_at(h)) + 1)
            return CountedSet(
                items=self.items,
                counts=new_counts,
                indexes=self.indexes,
                new_objects=self._new_objects,
                new_counts=self._new_counts,
                transaction=self.transaction,
            )
        elif self._new_counts.has(h):
            # Increment pending count for an element staged in this transaction
            new_new_counts = self._new_counts.set_at(h, cast(int, self._new_counts.get_at(h)) + 1)
            # Mirror increment to persisted counts view
            new_counts_persisted = self.counts.set_at(h, (cast(int, self.counts.get_at(h)) if self.counts.has(h) else 0) + 1)
            return CountedSet(
                items=self.items,
                counts=new_counts_persisted,
                indexes=self.indexes,
                new_objects=self._new_objects,
                new_counts=new_new_counts,
                transaction=self.transaction,
            )
        else:
            # First insertion: materialize element in items and set count=1; update indexes on 0 -> 1 transition
            new_items = self.items.set_at(h, key)
            new_objects = self._new_objects.set_at(h, key)
            new_new_counts = self._new_counts.set_at(h, 1)
            new_counts_persisted = self.counts.set_at(h, 1)
            if self.indexes:
                new_indexes = self.indexes.add2indexes(key)
            else:
                new_indexes = self.indexes
            return CountedSet(
                items=new_items,
                counts=new_counts_persisted,
                indexes=new_indexes,
                new_objects=new_objects,
                new_counts=new_new_counts,
                transaction=self.transaction,
            )

    def remove_at(self, key: object) -> 'CountedSet':
        h = self._hash_of(key)
        self._load()
        if self.counts.has(h):
            repetition = cast(int, self.counts.get_at(h)) - 1
            new_counts = self.counts
            new_items = self.items
            new_indexes = self.indexes
            new_new_counts = self._new_counts
            new_objects = self._new_objects
            if repetition > 0:
                new_counts = new_counts.set_at(h, repetition)
            else:
                new_counts = new_counts.remove_at(h)
                new_items = new_items.remove_at(h)
                new_indexes = new_indexes.remove_from_indexes(key)
                # Also clear staged views for this key (0 -> removal)
                if new_new_counts.has(h):
                    new_new_counts = new_new_counts.remove_at(h)
                if new_objects.has(h):
                    new_objects = new_objects.remove_at(h)

            return CountedSet(
                items=new_items,
                counts=new_counts,
                new_objects=new_objects,
                new_counts=new_new_counts,
                transaction=self.transaction,
                indexes=new_indexes
            )
        elif self._new_counts.has(h):
            repetition = cast(int, self._new_counts.get_at(h)) - 1
            new_new_counts = self._new_counts
            new_objects = self._new_objects
            new_indexes = self.indexes
            if repetition > 0:
                new_new_counts = new_new_counts.set_at(h, repetition)
            else:
                new_new_counts = new_new_counts.remove_at(h)
                new_objects = new_objects.remove_at(h)
                new_indexes = new_indexes.remove_from_indexes(key)

            return CountedSet(
                items=self.items,
                counts=self.counts,
                indexes=new_indexes,
                new_objects=new_objects,
                new_counts=new_new_counts,
                transaction=self.transaction,
            )
        else:
            return self
