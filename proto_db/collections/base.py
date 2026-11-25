"""Indexed collections: the base every ProtoDB collection derives from (spec section 8.1, rulings R5, R6).

Every collection supports associated indexes (none by default) and exposes them to
the planner through :meth:`IndexedCollection.as_query_plan`.  Collections are
immutable: a mutator computes the new structure *and* the new :class:`IndexSet`
in the same step and returns a new version through
:meth:`IndexedCollection._with`::

    return self._with(root=new_root, count=self.count + 1, indexes=self._after_add(e))

There is no code path that returns a collection with stale indexes; the
collection contract (``tests/contracts/collection_contract.py``) enforces it for
every mutator of every registered collection.

Index objects are duck-typed here (the ``Index`` base arrives with the index
framework): an index exposes a non-empty ``str`` ``name`` and the hooks
``empty()``, ``build(elements)``, ``with_added(e)``, ``with_removed(e)`` and
``with_replaced(old, new)``, each returning a new index with the same name.
Collections always build an index as ``index.empty().build(elements)``, so an
index passed with content of its own never leaks that content into a collection.

:class:`ConcurrentOptimized` adds a runtime-only record of intentions (``_changes``) to a
collection, so that a transaction can reapply its net effect onto a concurrently committed
version (:meth:`ConcurrentOptimized._reapplied_on`, spec R34).

Linked collections (:mod:`proto_db.collections.linking`, spec section 9): a version a transaction
obtained from a root carries a link (``_link``), its intentions (``_changes``) and a whole mark
(``_whole``, set by an operation the precondition model does not list: ``add_index``,
``remove_index``, ``rebuilt``, ...), all runtime fields carried by :meth:`IndexedCollection._with`.
A linked instance holds its persisted fields apart, so that a read of one by user code records
the whole collection (B3); the engine's own reads run inside ``engine_reads()``.

Thread safety: collections and index sets are immutable once constructed (fields
are set only by constructors and ``_with``), so they may be shared freely between
threads.
"""
from __future__ import annotations

import threading
from collections.abc import Callable, Iterable, Iterator
from types import MappingProxyType

from ..core.atom import BUILTIN_TYPE_ALIASES, Atom, PersistenceContext
from ..errors import ProtoMisfiledError, ProtoUserError, ProtoValidationError
from ..preconditions import Unit, UnitKind, same_token, token
from ..query.plans import CollectionScanPlan
from ..values import MISSING, canonical, is_atom, order_key, values_equal
from . import avl
from .linking import engine_reading, engine_reads, link_of, lookup_key, record_read, record_whole

__all__ = ["IndexSet", "IndexedCollection", "ConcurrentOptimized", "net_effect", "same_value",
           "whole_unless_logged", "IntentionConflict", "prepared_parts"]

#: The kind of the ``_changes`` entry holding the precomputed index keys of a prepared record of
#: intentions (:meth:`IndexedCollection.prepare_changes`, spec R24); collection ops never start
#: with "@".
_INDEX_KEYS = "@index_keys"


class IntentionConflict(Exception):
    """Internal: reapplying intentions on the current collection met what the preparation did not
    cover (an element whose index keys were not precomputed: ``UnitKind.WHOLE``) or a unique key
    the reapplied state holds twice (``UnitKind.INDEX_GROUP``, with ``index`` and ``key``).  The
    transaction turns it into ``ProtoConflictError`` naming the unit on its root (R29, R36)."""

    def __init__(self, kind: UnitKind, detail: str, *, index: str | None = None, key=None):
        super().__init__(detail)
        self.kind = kind
        self.detail = detail
        self.index = index
        self.key = key


# -- reapplication from precomputed keys (spec R24, R49) ----------------------------------------

_reapply = threading.local()


def _key_table():
    """The precomputed keys of the reapplication running in this thread, or None."""
    return getattr(_reapply, "table", None)


class _Reapplying:
    """Context manager: while it is active, this thread's IndexSet hooks take every key from
    ``table`` and never run an extractor (R49).  Nested uses restore the outer table."""

    __slots__ = ("table", "_outer")

    def __init__(self, table):
        self.table = table

    def __enter__(self):
        self._outer = getattr(_reapply, "table", None)
        _reapply.table = self.table
        return self

    def __exit__(self, *exc):
        _reapply.table = self._outer
        return False


_member_key = None


def element_identity(element):
    """How a prepared table identifies ``element``: the order key of its index member key
    (:func:`proto_db.indexing.keyed.member_key`: atoms by oid, values by equality part and exact
    token), so two elements an index stores as one member share their keys."""
    global _member_key
    if _member_key is None:
        from ..indexing.keyed import member_key     # deferred: the indexing package imports this one
        _member_key = member_key
    return order_key(_member_key(element))


def _prepared_key(table, index, name: str, element):
    key = table.get((name, element_identity(element)), _NO_KEY)
    if key is _NO_KEY:
        raise IntentionConflict(
            UnitKind.WHOLE, f"the element {element!r} of the current collection has no precomputed key for "
                            f"the index {name!r} (a concurrent commit stored an element this transaction did "
                            f"not prepare)")
    return key


_NO_KEY = object()


def prepared_parts(changes: tuple) -> tuple:
    """``(operations, table)`` of a record of intentions: the operations without the precomputed
    keys entry, and its table (None when the record was not prepared)."""
    if changes and changes[-1][0] == _INDEX_KEYS:
        return changes[:-1], changes[-1][1]
    return changes, None


def check_unique_after(collection, table) -> None:
    """After a reapplication: ``IntentionConflict`` (``INDEX_GROUP``) when a unique index of
    ``collection`` holds two elements under a key the prepared ``table`` names (R29).  Only
    engine data is read: the keys are the precomputed ones."""
    if not table:
        return
    with engine_reads():
        indexes = collection.indexes
    seen = set()
    for (name, _), key in table.items():
        index = indexes.get(name)
        unique = getattr(index, "_is_unique", None)
        if unique is None or not unique() or key is MISSING or key is None:
            continue
        okey = order_key(key)
        if (name, okey) in seen:
            continue
        seen.add((name, okey))
        if index.group_count(key) > 1:
            raise IntentionConflict(UnitKind.INDEX_GROUP,
                                    f"a concurrent commit added an element with the key {key!r} to the unique "
                                    f"index {name!r}", index=name, key=key)


def _current(index) -> bool:
    check = getattr(index, "has_current_tokens", None)
    return check is None or check()


def _index_name(index) -> str:
    """Return the validated name of ``index``; ``ProtoValidationError`` if it has no usable name."""
    name = getattr(index, "name", None)
    if not isinstance(name, str) or not name:
        raise ProtoValidationError(f"An index needs a non-empty str name, got {name!r} from {index!r}")
    return name


class _Immutable:
    """Mixin forbidding assignment and deletion of public attributes after construction."""

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        raise ProtoUserError(f"{type(self).__name__} is immutable; cannot set {name!r}")

    def __delattr__(self, name):
        if name.startswith("_"):
            object.__delattr__(self, name)
            return
        raise ProtoUserError(f"{type(self).__name__} is immutable; cannot delete {name!r}")


def repair_pairs(pairs: Iterable[tuple]) -> list[tuple]:
    """``(key, value)`` pairs of a collection being repaired (:meth:`IndexedCollection.rebuilt`),
    one per key, sorted by ``order_key(key)``.  Pairs of one key and one value (:func:`same_value`)
    are merged; one key with two different values is ``ProtoValidationError`` naming the key."""
    by_key: dict = {}
    for key, value in pairs:
        key = canonical(key)
        okey = order_key(key)
        previous = by_key.get(okey)
        if previous is not None and not same_value(previous[1], value):
            raise ProtoValidationError(
                f"Cannot repair: the key {key!r} holds two different values ({previous[1]!r} and {value!r}); "
                f"rebuild the collection by hand, choosing the value to keep")
        if previous is None:
            by_key[okey] = (key, value)
    return [by_key[okey] for okey in sorted(by_key)]


def check_sorted_keys(collection, root) -> None:
    """``ProtoMisfiledError`` (through ``collection._misfiled``) unless the keys of the keyed tree
    ``root`` are in strictly increasing ``order_key``; then the tree's own invariants."""
    previous = None
    for key, _ in avl.key_iter(root):
        okey = order_key(key)
        if previous is not None and not previous[0] < okey:
            collection._misfiled(f"the key {key!r} is stored after {previous[1]!r}, which its key order "
                                 f"places after it")
        previous = (okey, key)
    avl.check_invariants(root)


def same_value(a, b) -> bool:
    """Content equality of two stored values (D27).

    :func:`values_equal` for orderable values; lists, dicts and tuples compare
    item by item at any depth (an explicit stack; a list never equals a tuple); NaN equals NaN,
    since a stored NaN is the same content.  Atoms compare by identity (oid).
    """
    pending = [(a, b)]
    while pending:
        a, b = pending.pop()
        if isinstance(a, (list, dict, tuple)) or isinstance(b, (list, dict, tuple)):
            if isinstance(a, list) and isinstance(b, list) or isinstance(a, tuple) and isinstance(b, tuple):
                if len(a) != len(b):
                    return False
                pending.extend(reversed(list(zip(a, b))))
                continue
            if isinstance(a, dict) and isinstance(b, dict):
                if len(a) != len(b):
                    return False
                pairs = []
                for k, v in a.items():
                    if k not in b:
                        return False
                    pairs.append((v, b[k]))
                pending.extend(reversed(pairs))
                continue
            return False
        a_nan = not is_atom(a) and a != a
        b_nan = not is_atom(b) and b != b
        if a_nan or b_nan:
            if not (a_nan and b_nan):
                return False
        elif not values_equal(a, b):
            return False
    return True


# -- IndexSet ------------------------------------------------------------------------------

class IndexSet(_Immutable, Atom):
    """Immutable persistent map ``name -> index``, stored in a keyed AVL tree (``root``).

    Persisted like any atom; a loaded index set and its indexes are lazy stubs.
    Every operation returns a new IndexSet; the element hooks of an empty set
    return the set itself.
    """

    __type_name__ = "proto_db.collections.IndexSet"
    _required_fields = ("root",)

    def __init__(self, *, root: avl.KeyNode | None = None, context: PersistenceContext | None = None):
        """Create an index set; ``root`` (internal) is the keyed tree, empty by default."""
        super().__init__(context=context)
        object.__setattr__(self, "root", root)

    def get(self, name: str):
        """Return the index called ``name`` or ``None``."""
        return avl.key_get(self.root, name)

    def names(self) -> list[str]:
        """Index names in sorted order."""
        return [name for name, _ in avl.key_iter(self.root)]

    def __len__(self) -> int:
        return 0 if self.root is None else self.root.count

    def __contains__(self, name) -> bool:
        return avl.key_get(self.root, name) is not None

    def _new(self, root) -> "IndexSet":
        return IndexSet(root=root, context=self.context)

    def with_index(self, index) -> "IndexSet":
        """Return a set that also holds ``index``; ``ProtoValidationError`` if its name is taken."""
        name = _index_name(index)
        if name in self:
            raise ProtoValidationError(f"An index named {name!r} already exists")
        root, _, _ = avl.key_put(self.root, name, index)
        return self._new(root)

    def without(self, name: str) -> "IndexSet":
        """Return a set without the index ``name``; ``ProtoValidationError`` if absent."""
        root, removed, _ = avl.key_remove(self.root, name)
        if not removed:
            raise ProtoValidationError(f"No index named {name!r}")
        return self._new(root)

    def _map(self, hook: Callable) -> "IndexSet":
        """Apply ``hook(index) -> index`` to every index; the empty set returns itself."""
        if self.root is None:
            return self
        pairs = []
        for name, index in avl.key_iter(self.root):
            updated = hook(index)
            if updated is None or _index_name(updated) != name:
                raise ProtoValidationError(f"Index {name!r} returned {updated!r} from a hook; "
                                           f"hooks must return an index with the same name")
            pairs.append((name, updated))
        return self._new(avl.key_from_sorted(pairs))

    def rebuilt_stale(self, elements: Callable) -> "IndexSet":
        """This set with every index whose ``has_current_tokens()`` is False rebuilt as
        ``index.empty().build(elements())`` (D58); the set itself when there is none.  During a
        reapplication from precomputed keys a stale index is never rebuilt (that runs
        extractors): ``IntentionConflict`` instead (the preparation takes such a collection whole,
        B8)."""
        if self.root is None:
            return self
        pairs = list(avl.key_iter(self.root))
        stale = [i for i, (_, index) in enumerate(pairs) if not _current(index)]
        if not stale:
            return self
        if _key_table() is not None:
            raise IntentionConflict(UnitKind.WHOLE, f"the index {pairs[stale[0]][0]!r} must be rebuilt")
        for i in stale:
            name, index = pairs[i]
            pairs[i] = (name, index.empty().build(elements()))
        return self._new(avl.key_from_sorted(pairs))

    def rebuilt_all(self, elements: Callable) -> "IndexSet":
        """This set with every index rebuilt as ``index.empty().build(elements())`` (a repair, D58)."""
        if self.root is None:
            return self
        return self._new(avl.key_from_sorted(
            [(name, index.empty().build(elements())) for name, index in avl.key_iter(self.root)]))

    def on_add(self, element) -> "IndexSet":
        """Return the set after ``with_added(element)`` on every index; during a reapplication
        ``with_added_key`` with the precomputed key, uniqueness checked on the final state (R49)."""
        table = _key_table()
        if table is None:
            return self._map(lambda index: index.with_added(element))
        return self._map(lambda index: index.with_added_key(
            element, _prepared_key(table, index, index.name, element), check_unique=False))

    def on_remove(self, element) -> "IndexSet":
        """Return the set after ``with_removed(element)`` on every index (``with_removed_key``
        during a reapplication)."""
        table = _key_table()
        if table is None:
            return self._map(lambda index: index.with_removed(element))
        return self._map(lambda index: index.with_removed_key(
            element, _prepared_key(table, index, index.name, element)))

    def on_replace(self, old, new) -> "IndexSet":
        """Return the set after ``with_replaced(old, new)`` on every index (``with_replaced_keys``
        during a reapplication)."""
        table = _key_table()
        if table is None:
            return self._map(lambda index: index.with_replaced(old, new))
        return self._map(lambda index: index.with_replaced_keys(
            old, _prepared_key(table, index, index.name, old), new, _prepared_key(table, index, index.name, new),
            check_unique=False))

    def definitions_token(self):
        """The token of this set's index definitions (``Index.definition_data`` of each, by
        name): what ``Unit(INDEX_DEFS)`` compares (R25)."""
        defs = []
        for name, index in avl.key_iter(self.root):
            data = getattr(index, "definition_data", None)
            defs.append((name, data() if data is not None
                         else (getattr(type(index), "__type_name__", type(index).__qualname__),)))
        return token(tuple(defs))

    def keyed(self) -> bool:
        """Whether every index is maintained from precomputed keys and has current member tokens:
        what a reapplication under the space root needs (R30, B7, B8)."""
        for _, index in avl.key_iter(self.root):
            keyed = getattr(index, "reapplies_from_keys", None)
            if keyed is None or not keyed() or not _current(index):
                return False
        return True


# -- IndexedCollection ---------------------------------------------------------------------

class IndexedCollection(_Immutable, Atom):
    """Abstract base of every collection: content plus an :class:`IndexSet` kept in step with it.

    Persisted fields: ``indexes`` (an IndexSet, empty by default), ``count`` (the
    number of elements) and whatever the subclass adds.  A subclass lists all its
    persisted fields in the class attribute ``_fields`` (which must contain
    ``"count"`` and ``"indexes"``), implements :meth:`elements`, sets its own
    fields in ``__init__`` with ``object.__setattr__``, and builds every new
    version with :meth:`_with`.  Runtime-only attributes that a new version must
    carry over are named in ``_runtime_fields`` (gathered across the MRO).
    """

    __type_name__ = "proto_db.collections.IndexedCollection"
    _fields: tuple = ("count", "indexes")
    #: Runtime state of the precondition model (spec section 9): the link of a linked version
    #: (R40), its record of intentions (R43) and the whole mark of a version an unlisted
    #: operation produced (P-B).  Never persisted.
    _link = None
    _changes: tuple = ()
    _whole: bool = False
    #: The units whose value a linked version's own writes determine (a Set element it added or
    #: removed, a RepeatedKeysDictionary key it cleared): a read of one records nothing (R9).  A
    #: runtime keyed tree (``linking.written_with``), ``None`` when empty.
    _written = None
    #: Whether this whole version differs from the linked version it derives from by index
    #: definition changes only (``add_index``/``remove_index`` on a linked version with no other
    #: operation): a candidate for the catch-up of R69 (:meth:`_definition_changes` decides).
    _definitions_only: bool = False
    _runtime_fields: tuple = ("_link", "_changes", "_whole", "_written", "_definitions_only")
    _all_runtime_fields: tuple = ()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.__dict__.get("__type_name__") in BUILTIN_TYPE_ALIASES:
            # Required fields bind built-in classes only: an application subclass inherits its
            # built-in base's, so a field it adds in a later release may be absent from its
            # older records (re-review Minor 4).
            cls._required_fields = tuple(cls._fields)
        missing = {"count", "indexes"} - set(cls._fields)
        if missing:
            raise ProtoUserError(f"{cls.__name__}._fields must include {sorted(missing)}")
        mro = cls.__mro__
        if ConcurrentOptimized in mro and mro.index(ConcurrentOptimized) > mro.index(IndexedCollection):
            raise ProtoUserError(f"{cls.__name__}: ConcurrentOptimized must precede IndexedCollection "
                                 f"in the bases")
        names = []
        for klass in reversed(mro):
            for name in klass.__dict__.get("_runtime_fields", ()):
                if not name.startswith("_"):
                    raise ProtoUserError(f"Runtime field {name!r} of {cls.__name__} must start with '_'")
                if name not in names:
                    names.append(name)
        cls._all_runtime_fields = tuple(names)

    def __init__(self, *, context: PersistenceContext | None = None):
        """Initialise an empty collection: no elements, no indexes."""
        super().__init__(context=context)
        object.__setattr__(self, "indexes", IndexSet(context=context))
        object.__setattr__(self, "count", 0)

    # -- linked instances (spec section 9, B3) -------------------------------------------------

    def __getattr__(self, name: str):
        # A linked instance holds its persisted fields apart (linking.linked_copy): a read of one
        # by user code reveals the whole collection (R18, B3); an engine read records nothing.
        held = object.__getattribute__(self, "__dict__").get("_linked_state")
        if held is not None and name in held:
            if not engine_reading():
                record_whole(self)
            return held[name]
        return Atom.__getattr__(self, name)

    def _snapshot(self) -> dict:
        """See :meth:`Atom._snapshot`; a linked instance's persisted fields are given back by name."""
        state = super()._snapshot()
        held = state.pop("_linked_state", None)
        if held:
            state.update(held)
        return state

    def _probe(self, name: str):
        """See :meth:`Atom._probe`; a linked instance's persisted fields are read without recording."""
        held = object.__getattribute__(self, "__dict__").get("_linked_state")
        if held is not None and name in held:
            return held[name]
        return super()._probe(name)

    # -- content ------------------------------------------------------------------------

    def elements(self) -> Iterator:
        """Iterate over what the collection indexes and what queries return (abstract)."""
        raise NotImplementedError(f"{type(self).__name__} must implement elements()")

    def __iter__(self) -> Iterator:
        return self.elements()

    def __len__(self) -> int:
        return self.count

    def __contains__(self, element) -> bool:
        """True iff some element equals ``element`` by ``values_equal``.

        The default is an O(n) scan, which needs orderable elements; subclasses
        override it with their structure's lookup (or their own notion of
        membership, e.g. keys for dictionaries).
        """
        return any(e is element or values_equal(e, element) for e in self.elements())

    def content_equals(self, other) -> bool:
        """True iff ``other`` is a collection of the same type holding the same content (ruling R12).

        ``==`` stays identity by oid (D10); this compares content.  Indexes and
        op logs are not content.  Values are compared with :func:`same_value`.
        """
        if type(other) is not type(self):
            return False
        if other is self:
            return True
        if self.count != other.count:
            return False
        return self._same_content(other)

    def _same_content(self, other) -> bool:
        """Content comparison with a collection of the same type and count (hook).

        The default compares :meth:`elements` pairwise, in order; collections
        whose iteration order is not determined by content override it.
        """
        sentinel = object()
        mine, theirs = self.elements(), other.elements()
        while True:
            a, b = next(mine, sentinel), next(theirs, sentinel)
            if a is sentinel or b is sentinel:
                return a is b
            if not same_value(a, b):
                return False

    # -- indexes ------------------------------------------------------------------------

    def add_index(self, index) -> "IndexedCollection":
        """Return a version with ``index`` built over the current elements.

        The index is built as ``index.empty().build(self.elements())``: only its
        definition is used, never content it may already hold.
        ``ProtoValidationError`` if an index with the same name exists (checked
        before building) or if the built index has another name.  It is built here, in the
        transaction's own thread, never under the space root; the version takes the whole
        collection (R26, P-B) and is published as is.

        On a linked version it records the index definitions (R25) and nothing else: building
        the index reveals no content to the caller.  Unless the transaction also reads or writes
        the collection, the commit then catches the index up with concurrent commits instead of
        conflicting (R69, :meth:`_definition_changes`).
        """
        self._record_index_definitions()
        name = _index_name(index)
        with engine_reads():
            if name in self.indexes:
                raise ProtoValidationError(f"An index named {name!r} already exists")
            built = index.empty().build(self.elements())
            if built is None or _index_name(built) != name:
                raise ProtoValidationError(f"Index {name!r} build() returned {built!r}; expected the same name")
            indexes = self.indexes.with_index(built)
        return self._with(indexes=indexes, _whole=True, _definitions_only=self._only_definitions_so_far())

    def remove_index(self, name: str) -> "IndexedCollection":
        """Return a version without the index ``name``; ``ProtoValidationError`` if absent.
        The version takes the whole collection (R26, P-B); on a linked version it records the
        index definitions only, as :meth:`add_index` does (R25, R69)."""
        self._record_index_definitions()
        with engine_reads():
            indexes = self.indexes.without(name)
        return self._with(indexes=indexes, _whole=True, _definitions_only=self._only_definitions_so_far())

    def _only_definitions_so_far(self) -> bool:
        """Whether this version is a linked version with no operation but index definition
        changes: the first version a transaction derives from a root, or one that only
        ``add_index``/``remove_index`` produced since (R69)."""
        if self._link is None or self._changes:
            return False
        return self._definitions_only or not self._whole

    # -- index catch-up (spec R69): outside the space root --------------------------------------

    def _definition_changes(self, base) -> tuple | None:
        """``(added, removed)`` when this version is ``base`` (the root's base collection) with
        index definition changes only: the names of the indexes it built (absent from ``base``,
        or present there with another index) and of the indexes of ``base`` it dropped.  ``None``
        when it is not: another operation ran (``_changes``), or the content differs from
        ``base`` (an operation outside the listed ones), or the class differs.  Engine reads
        only; a new index is unsaved, an index of ``base`` is saved, so they never compare equal."""
        if not self._definitions_only or self._changes or type(base) is not type(self):
            return None
        with engine_reads():
            for field in type(self)._fields:
                if field != "indexes" and not _identical(self._probe(field), getattr(base, field)):
                    return None
            before = dict(avl.key_iter(base.indexes.root))
            after = dict(avl.key_iter(self.indexes.root))
        added = tuple(name for name, index in after.items()
                      if name not in before or not _identical(index, before[name]))
        removed = tuple(name for name in before if name not in after)
        return added, removed

    def _caught_up(self, previous, current, added: tuple, removed: tuple, root: str) -> "IndexedCollection":
        """The catch-up of R69, in the pre-pass (outside the space root; extractors run here): this
        version holds ``previous``'s content with the indexes ``added`` built over it; return
        ``current``'s content (a newer committed version of root ``root``, same class and index
        definitions as ``previous``) with ``current``'s indexes, those named in ``added`` and
        ``removed`` taken out, and each index of ``added`` brought from ``previous``'s elements to
        ``current``'s: the elements of every unit the diff ``previous -> current`` reports changed
        are removed (as ``previous`` holds them) and added (as ``current`` does).  When the diff
        cannot name the units (``Unit(WHOLE)``) or a class cannot list a unit's elements, the
        index is rebuilt from ``current``'s elements."""
        with engine_reads():
            built = {name: self.indexes.get(name) for name in added}
            if built:
                moves = []
                for unit in previous._changed_element_units(current, root):
                    if unit.kind is UnitKind.WHOLE:
                        moves = None
                        break
                    key = lookup_key(unit.key)
                    old, new = previous._unit_elements(unit.kind, key), current._unit_elements(unit.kind, key)
                    if old is None or new is None:
                        moves = None
                        break
                    moves.append((old, new))
                for name, index in built.items():
                    if moves is None:
                        index = index.empty().build(current.elements())
                    else:
                        for old, new in moves:
                            for element in old:
                                index = index.with_removed(element)
                            for element in new:
                                index = index.with_added(element)
                    built[name] = index
            indexes = current.indexes
            for name in set(added) | set(removed):
                if name in indexes:
                    indexes = indexes.without(name)
            for name in added:
                indexes = indexes.with_index(built[name])
            return current._with(indexes=indexes, _whole=True, _definitions_only=True)

    def _changed_element_units(self, other, root: str) -> Iterator[Unit]:
        """The units of root ``root`` whose elements may differ between this version and ``other``
        (R69): :meth:`changed_units` by default, which covers every element of a keyed class."""
        return self.changed_units(other, root)

    def _unit_elements(self, kind: UnitKind, key) -> list | None:
        """The elements (as :meth:`elements` yields them) that unit ``kind`` of the collection key
        ``key`` holds in this version, for the catch-up of R69; ``None`` when the class cannot
        tell (the default: the index is rebuilt instead).  Engine reads only."""
        return None

    def get_index(self, name: str):
        """Return the index ``name`` or ``None``.  On a linked version it records the index
        definitions (R25) and the whole collection: the index is read directly, bypassing the
        collection's recorded reads (B5)."""
        if self._link is not None:
            record_whole(self)
            self._record_index_definitions()
            with engine_reads():
                return self.indexes.get(name)
        return self.indexes.get(name)

    def index_names(self) -> list[str]:
        """Names of the associated indexes, sorted.  On a linked version it records the index
        definitions (spec section 6, R25)."""
        if self._link is not None:
            self._record_index_definitions()
            with engine_reads():
                return self.indexes.names()
        return self.indexes.names()

    # -- the index units of the precondition model (spec section 7) ------------------------------

    def _indexed(self) -> bool:
        """Whether this version has an index (an engine read: records nothing).  On an indexed
        collection the removal intentions of the D26 types and of a Set take a declared fine
        precondition: their reapplication takes out a stored element, whose index keys the
        preparation computed only for the form the base stores (spec section 7, R49)."""
        with engine_reads():
            return len(self.indexes) > 0

    def _record_index_definitions(self) -> None:
        """On a linked version: record ``Unit(INDEX_DEFS, root)`` expecting the base's index
        definitions (R25, R8); nothing on an unlinked one or inside ``engine_reads``."""
        link = link_of(self)
        if link is None or engine_reading():
            return
        unit = Unit(UnitKind.INDEX_DEFS, link.root)
        log = link.transaction._read_log
        if unit not in log:
            record_read(self, unit, link.transaction._base_root_value(link.root)._index_definitions_token())

    def _index_definitions_token(self):
        """The token of this version's index definitions (an engine read: records nothing)."""
        with engine_reads():
            return self.indexes.definitions_token()

    def _index_group_token(self, name: str, key):
        """The token of the group of ``key`` in the key-group index ``name`` (R28): its
        ``(members, total)`` tuple, the members tree by pointer; ``NOT_A_UNIT`` when the index is
        absent or keeps no groups (an engine read: records nothing)."""
        from .linking import NOT_A_UNIT
        with engine_reads():
            index = self.indexes.get(name)
        group = getattr(index, "group_token_data", None)
        if group is None:
            return NOT_A_UNIT
        return token(group(key))

    def _range_tree(self, index):
        """The keyed tree a range precondition on source ``index`` compares (R67, R68): the
        ``groups`` tree of the key-group index named ``index`` (keys ordered by ``order_key``,
        each value a group ``(members, total)``); ``NOT_A_UNIT`` when the collection has no
        such index or no own key tree (``index`` None here).  An engine read: records nothing."""
        from .linking import NOT_A_UNIT
        if index is None:
            return NOT_A_UNIT
        with engine_reads():
            found = self.indexes.get(index)
        if getattr(found, "group_token_data", None) is None:
            return NOT_A_UNIT
        return found.groups

    # -- delta validation (spec R65) --------------------------------------------------------------

    def changed_units(self, other, root: str) -> Iterator[Unit]:
        """The conflict units of root ``root`` whose value may differ between this version and
        ``other`` (a saved version of the same class): a superset of the units whose token
        differs, computed by a structural diff that skips shared subtrees unloaded (R65).
        ``Unit(WHOLE, root)`` means that every unit of the root is to be taken as changed; that
        is the default, for a class without a structural diff.  Engine reads only (P-C)."""
        yield Unit(UnitKind.WHOLE, root)

    def changed_index_groups(self, other, index: str) -> Iterator:
        """The keys whose group in the key-group index ``index`` differs between this version and
        ``other`` (the ``groups`` trees diffed, shared subtrees skipped unloaded: exactly the
        keys a group or range precondition on the index compares), or
        :data:`~proto_db.collections.diff.EVERYTHING` when either version lacks that index or
        it keeps no groups.  Engine reads only (P-C)."""
        from .diff import EVERYTHING, changed_keys
        with engine_reads():
            old, new = self.indexes.get(index), other.indexes.get(index)
        if getattr(old, "group_token_data", None) is None or getattr(new, "group_token_data", None) is None:
            yield EVERYTHING
            return
        yield from changed_keys(old.groups, new.groups)

    def _intention_elements(self, base) -> Iterable | None:
        """Every element reapplying this version's intentions may add, remove or replace on a
        current collection that the preconditions keep equal to ``base`` where they constrain it:
        the elements the operations name and the stored elements of ``base`` they touch.  ``None``
        when the class cannot tell (the default): such a collection with indexes takes the whole
        collection at commit.  Engine reads only (the caller holds ``engine_reads``)."""
        return None

    def prepare_changes(self, base) -> tuple:
        """The preparation phase of a commit (spec section 8, R24), outside the space root: this
        version's record of intentions (``_changes``) with the key of every element it may add,
        remove or replace (:meth:`_intention_elements`) in every index of ``base`` (the root's
        base collection), computed now with the extractors.  The keys travel at the end of the
        record, as ``("@index_keys", {(index name, element identity): key})``; under the space
        root the reapplication takes every key from there and runs no extractor (R49).

        ``ProtoValidationError`` when the class cannot enumerate those elements while ``base`` has
        indexes (the caller takes the collection whole instead, see :meth:`_reapplies_from_keys`)."""
        with engine_reads():
            indexes = base.indexes
            pairs = list(avl.key_iter(indexes.root))
            elements = [] if not pairs else self._intention_elements(base)
            if elements is None:
                raise ProtoValidationError(f"A {type(self).__name__} cannot precompute its index keys")
            elements = list(elements)
            table = {}
            for name, index in pairs:
                for element in elements:
                    ident = (name, element_identity(element))
                    if ident not in table:
                        table[ident] = index.key_of(element)
        operations, _ = prepared_parts(self._changes)
        return operations + ((_INDEX_KEYS, MappingProxyType(table)),)

    def _reapplies_from_keys(self, base) -> bool:
        """Whether this version's intentions can be reapplied under the space root from keys
        :meth:`prepare_changes` computes against ``base``: every index of ``base`` is maintained
        from precomputed keys with current member tokens (R30, B7, B8), and the class enumerates
        the elements its intentions touch.  Otherwise the commit takes the whole collection."""
        with engine_reads():
            indexes = base.indexes
            if not len(indexes):
                return True
            return indexes.keyed() and self._intention_elements(base) is not None

    def _scan_window(self, start: int, stop: int | None):
        """Elements ``start .. stop-1`` of :meth:`elements` without producing the earlier ones, or
        ``None`` when the structure cannot seek (the default).  A collection that returns an
        iterator must also make ``count`` the number of elements yielded (D56)."""
        return None

    def as_query_plan(self) -> CollectionScanPlan:
        """A full-scan plan over this collection; the planner reads ``collection.indexes`` from it.

        Every query plan over a collection starts here, so a linked collection records the whole
        collection (spec section 6, R22), also for a result the query cache serves."""
        record_whole(self)
        return CollectionScanPlan(self)

    # -- protected helpers for mutators ---------------------------------------------------

    def _with(self, **changes) -> "IndexedCollection":
        """Return a new instance of ``type(self)`` with this version's state updated by ``changes``.

        ``changes`` may name persisted fields (``_fields``) and runtime fields
        (``_runtime_fields``); fields not named are carried over unchanged.  The
        receiver is never modified.
        """
        cls = type(self)
        runtime = cls._all_runtime_fields
        unknown = [k for k in changes if k not in cls._fields and k not in runtime]
        if unknown:
            raise ProtoValidationError(f"{cls.__name__}._with got unknown fields {unknown}")
        if "indexes" in changes and not isinstance(changes["indexes"], IndexSet):
            raise ProtoValidationError(f"{cls.__name__}._with: indexes must be an IndexSet, "
                                       f"not {type(changes['indexes']).__name__}")
        new = object.__new__(cls)
        Atom.__init__(new, context=self.context)
        osa = object.__setattr__
        held = object.__getattribute__(self, "__dict__").get("_linked_state")
        if held is None:
            for name in cls._fields:
                osa(new, name, changes[name] if name in changes else getattr(self, name))
        else:
            # A version of a linked instance is linked (R42): its fields are held apart too.
            fields = {}
            for name in cls._fields:
                if name in changes:
                    fields[name] = changes[name]
                elif name in held:
                    fields[name] = held[name]
            osa(new, "_linked_state", fields)
        for name in runtime:
            osa(new, name, changes[name] if name in changes else getattr(self, name))
        return new

    # -- integrity check and repair (D58, re-review 2 I-A) ------------------------------------

    def validate(self) -> None:
        """Check this version's structure, then every index against :meth:`elements`.

        Raises :class:`~proto_db.errors.ProtoMisfiledError` when an entry is filed where its own
        key does not place it: a sorted tree whose keys are out of order, a hash bucket holding
        an element of another hash, an index group holding an element of another key.  That is
        the damage format 3 left when it wrote an instance of a built-in subclass through the
        subclass's own conversion (a ``(str, Enum)`` member stored as ``"Color.RED"`` but filed
        as ``"red"``): lookups of that entry fail, and in a sorted dictionary lookups of other
        keys too.  :meth:`rebuilt` repairs it.  Raises ``ProtoCorruptionError`` for any other
        damage.  Loads every node: O(n log n).
        """
        self._check_placement()
        elements = list(self.elements())
        for name in self.index_names():
            self.get_index(name).validate(elements)

    def _check_placement(self) -> None:
        """``ProtoMisfiledError`` when an entry of the content is out of place (none for positional content)."""

    def _misfiled(self, detail: str):
        raise ProtoMisfiledError(
            f"{type(self).__name__} holds a misfiled entry: {detail}. Format 3 wrote built-in "
            f"subclasses (a (str, Enum) member, a subclass overriding __str__, __float__ or __bytes__) "
            f"through their own conversion but filed them by their base value (D58); the collection "
            f"must be rebuilt: replace it with collection.rebuilt()")

    def rebuilt(self) -> "IndexedCollection":
        """A repaired version: the content refiled from its entries as they read now, and every
        index rebuilt from the elements (D58).  Entries that become one key are merged when
        they are one value (a set member, a counted member's counts add up) and refused with
        ``ProtoValidationError`` naming the key when they map one key to two different values:
        a repair never drops a value.  It is not one of the operations the precondition model
        lists: the version is marked whole, so a transaction that assigns it publishes it with a
        whole-collection precondition (P-B).
        """
        new = self._refiled()
        return new._with(indexes=new.indexes.rebuilt_all(new.elements), _whole=True)

    def _refiled(self) -> "IndexedCollection":
        """The content refiled (the default, positional content, needs nothing)."""
        return self

    def _current_indexes(self) -> IndexSet:
        """``indexes`` ready to be changed: every index whose member keys use an older token
        scheme (``has_current_tokens()`` False: written by format 3) rebuilt from this version's
        elements first (D58).  Usually the IndexSet itself."""
        return self.indexes.rebuilt_stale(self.elements)

    def _after_add(self, element) -> IndexSet:
        """The IndexSet after adding ``element`` to the content."""
        return self._current_indexes().on_add(element)

    def _after_remove(self, element) -> IndexSet:
        """The IndexSet after removing ``element`` from the content."""
        return self._current_indexes().on_remove(element)

    def _after_removes(self, elements: Iterable) -> IndexSet:
        """The IndexSet after removing each of ``elements`` from the content, in order."""
        indexes = self._current_indexes()
        for element in elements:
            indexes = indexes.on_remove(element)
        return indexes

    def _after_replace(self, old, new) -> IndexSet:
        """The IndexSet after replacing element ``old`` by ``new``."""
        return self._current_indexes().on_replace(old, new)


def _identical(a, b) -> bool:
    """Whether two persisted field values are the same value: the same object, the same saved
    atom (by storage pointer: an unsaved atom equals only itself), or equal plain values by token."""
    if a is b:
        return True
    if isinstance(a, Atom) or isinstance(b, Atom):
        return (isinstance(a, Atom) and isinstance(b, Atom) and a.atom_pointer is not None
                and a.atom_pointer == b.atom_pointer)
    return same_token(token(a), token(b))


# -- ConcurrentOptimized -------------------------------------------------------------------

def whole_unless_logged(collection, changes: dict) -> dict:
    """``changes`` for ``collection._with``, marked whole (P-B) when they change persisted content
    and extend no ``_changes`` (an operation outside the listed ones, e.g. a subclass method
    calling ``_with`` directly): such a change cannot be reapplied at commit, so the version is
    published as is under a whole-collection precondition instead of being silently dropped.
    An explicit ``_whole`` is kept."""
    content = any(name in type(collection)._fields for name in changes)
    logged = len(changes.get("_changes", ())) > len(collection._changes)
    if content and not logged and "_whole" not in changes:
        changes["_whole"] = True
    return changes


def net_effect(ops: Iterable[tuple], key: Callable | None = None) -> list[tuple]:
    """Return the last op per key, ordered by the position of that last occurrence.

    ``key(op)`` identifies the element an op touches; the default is
    ``order_key(op[1])``, so equal keys of different types (``1``, ``1.0``) are
    one key.  Runs in O(n).
    """
    if key is None:
        key = _default_op_key
    last: dict = {}
    for op in ops:
        k = key(op)
        last.pop(k, None)  # re-inserting moves the key to the end: order of last occurrence
        last[k] = op
    return list(last.values())


def _default_op_key(op: tuple):
    return order_key(op[1])


class ConcurrentOptimized:
    """Mixin for collections whose intentions can be reapplied on a concurrently committed version
    (spec R34, R43; ruling R6).

    Each version carries ``_changes`` (:class:`IndexedCollection`), a runtime-only tuple of the
    operations applied since the version was loaded or created (never persisted: a loaded
    version starts with none).  Mutators extend it immutably with
    ``_with(..., _changes=self._logged(op))``; an op is a tuple whose first item
    is a ``str`` kind not starting with ``"@"`` (reserved for index-definition
    changes, which :class:`IndexedCollection` logs itself).

    The log records INTENT, not observed change: every mutator call logs its op
    (add means "present", remove means "absent", set means "mapped to this
    value"), including calls that change nothing in this version (a blind
    write).  Such a call returns a NEW version that carries the extended log
    and reuses the receiver's content and indexes (no index work).  Mutators
    must therefore be idempotent with respect to their intent, so that
    replaying an op onto any version forces its key to the intended state.

    :meth:`_reapplied_on` applies the net intent of a linked version's changes on the current
    collection (R34); an index-definition change marks the version whole (P-B), so it is
    published as is, never reapplied.  Subclasses implement :meth:`_replay_op` and, when ``op[1]`` is not the key an
    op touches, override :meth:`_op_key`.  It must precede
    :class:`IndexedCollection` in the class bases.

    The record grows with every mutation of a version chain; it lives as long as the
    versions holding it (typically one transaction).
    """

    def _with(self, **changes):
        """See :meth:`IndexedCollection._with`; also marks the version whole (P-B) when a change
        of persisted content extends no ``_changes`` (an operation outside the listed ones, e.g.
        a subclass method calling ``_with`` directly): such a change cannot be reapplied at
        commit, so the version is published as is under a whole-collection precondition instead
        of being silently dropped.
        """
        return super()._with(**whole_unless_logged(self, changes))

    def _logged(self, *ops: tuple) -> tuple:
        """Return this version's changes extended with ``ops`` (the receiver's are unchanged)."""
        for op in ops:
            if not isinstance(op, tuple) or not op or not isinstance(op[0], str) or op[0].startswith("@"):
                raise ProtoValidationError(f"An op must be a tuple starting with a str kind not starting "
                                           f"with '@', not {op!r}")
        return self._changes + ops

    def _op_key(self, op: tuple):
        """Key identifying what ``op`` touches, for :func:`net_effect` (default ``order_key(op[1])``)."""
        return order_key(op[1])

    def _net_ops(self, element_ops: list) -> list:
        """Reduce this log's element ops to the ops :meth:`_reapplied_on` replays, in replay order.

        The default keeps the last op per key (:func:`net_effect` with
        :meth:`_op_key`): the right reduction for ops that force a key to a
        state.  Collections whose ops compose differently (counters, D26)
        override it and return their own net ops, which :meth:`_replay_op`
        must understand.
        """
        return net_effect(element_ops, self._op_key)

    def _replay_op(self, target, op: tuple):
        """Apply one net element op to ``target`` through mutators that maintain its indexes and log; return it."""
        raise NotImplementedError(f"{type(self).__name__} must implement _replay_op()")

    def _reapplied_on(self, current, changes: tuple | None = None):
        """``current`` with the net intent of this version's changes applied (R34): the net ops of
        :meth:`_net_ops` replayed through :meth:`_replay_op`, the same tree operations the
        transaction used.  ``changes`` is the record :meth:`prepare_changes` gave (default: the
        version's own ``_changes``); a prepared record takes every index key from its table, so
        no extractor runs (R49), and a unique index is checked on the final state (R29;
        ``IntentionConflict``).  Only a version that is not whole is reapplied: an index change
        or an unlisted operation marks the version whole (P-B), and it is published as is."""
        if type(current) is not type(self):
            raise ProtoValidationError(f"Cannot reapply a {type(self).__name__} on a {type(current).__name__}")
        if self._whole:
            raise ProtoValidationError("A whole version (an index change or an unlisted operation) is published "
                                       "as is; it is not reapplied")
        operations, table = prepared_parts(self._changes if changes is None else changes)
        target = current
        with _Reapplying(table):
            for op in self._net_ops(list(operations)):
                target = self._replay_op(target, op)
        check_unique_after(target, table)
        return target
