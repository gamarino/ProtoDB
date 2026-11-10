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

:class:`ConcurrentOptimized` adds a runtime-only operation log to a collection so
that a transaction can replay its net effect onto a concurrently committed
version (:meth:`ConcurrentOptimized.rebase_onto`, ruling R6).

Thread safety: collections and index sets are immutable once constructed (fields
are set only by constructors and ``_with``), so they may be shared freely between
threads.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator

from ..core.atom import BUILTIN_TYPE_ALIASES, Atom, PersistenceContext
from ..errors import ProtoConflictError, ProtoUserError, ProtoValidationError
from ..query.plans import CollectionScanPlan
from ..values import is_atom, order_key, values_equal
from . import avl

__all__ = ["IndexSet", "IndexedCollection", "ConcurrentOptimized", "net_effect", "same_value"]

#: Op-log kinds reserved for index-definition changes; collection ops must not start with "@".
_INDEX_ADD = "@index_add"
_INDEX_REMOVE = "@index_remove"


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


def same_value(a, b) -> bool:
    """Content equality of two stored values (D27).

    :func:`values_equal` for orderable values; lists, dicts and tuples compare
    recursively (a list never equals a tuple); NaN equals NaN, since a stored
    NaN is the same content.  Atoms compare by identity (oid).
    """
    if isinstance(a, (list, dict, tuple)) or isinstance(b, (list, dict, tuple)):
        if isinstance(a, list) and isinstance(b, list) or isinstance(a, tuple) and isinstance(b, tuple):
            return len(a) == len(b) and all(same_value(x, y) for x, y in zip(a, b))
        if isinstance(a, dict) and isinstance(b, dict):
            return len(a) == len(b) and all(k in b and same_value(v, b[k]) for k, v in a.items())
        return False
    a_nan = not is_atom(a) and a != a
    b_nan = not is_atom(b) and b != b
    if a_nan or b_nan:
        return a_nan and b_nan
    return values_equal(a, b)


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
        ``index.empty().build(elements())`` (D58); the set itself when there is none."""
        if self.root is None:
            return self
        pairs = list(avl.key_iter(self.root))
        stale = [i for i, (_, index) in enumerate(pairs) if not _current(index)]
        if not stale:
            return self
        for i in stale:
            name, index = pairs[i]
            pairs[i] = (name, index.empty().build(elements()))
        return self._new(avl.key_from_sorted(pairs))

    def on_add(self, element) -> "IndexSet":
        """Return the set after ``with_added(element)`` on every index."""
        return self._map(lambda index: index.with_added(element))

    def on_remove(self, element) -> "IndexSet":
        """Return the set after ``with_removed(element)`` on every index."""
        return self._map(lambda index: index.with_removed(element))

    def on_replace(self, old, new) -> "IndexSet":
        """Return the set after ``with_replaced(old, new)`` on every index."""
        return self._map(lambda index: index.with_replaced(old, new))


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
    _runtime_fields: tuple = ()
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
        before building) or if the built index has another name.
        """
        name = _index_name(index)
        if name in self.indexes:
            raise ProtoValidationError(f"An index named {name!r} already exists")
        built = index.empty().build(self.elements())
        if built is None or _index_name(built) != name:
            raise ProtoValidationError(f"Index {name!r} build() returned {built!r}; expected the same name")
        return self._with(indexes=self.indexes.with_index(built),
                          **self._index_log((_INDEX_ADD, name, index)))

    def remove_index(self, name: str) -> "IndexedCollection":
        """Return a version without the index ``name``; ``ProtoValidationError`` if absent."""
        return self._with(indexes=self.indexes.without(name), **self._index_log((_INDEX_REMOVE, name)))

    def get_index(self, name: str):
        """Return the index ``name`` or ``None``."""
        return self.indexes.get(name)

    def index_names(self) -> list[str]:
        """Names of the associated indexes, sorted."""
        return self.indexes.names()

    def _scan_window(self, start: int, stop: int | None):
        """Elements ``start .. stop-1`` of :meth:`elements` without producing the earlier ones, or
        ``None`` when the structure cannot seek (the default).  A collection that returns an
        iterator must also make ``count`` the number of elements yielded (D56)."""
        return None

    def as_query_plan(self) -> CollectionScanPlan:
        """A full-scan plan over this collection; the planner reads ``collection.indexes`` from it."""
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
        for name in cls._fields:
            osa(new, name, changes[name] if name in changes else getattr(self, name))
        for name in runtime:
            osa(new, name, changes[name] if name in changes else getattr(self, name))
        return new

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

    def _index_log(self, op: tuple) -> dict:
        """Extra ``_with`` changes recording an index-definition change (none unless ConcurrentOptimized)."""
        return {}


# -- ConcurrentOptimized -------------------------------------------------------------------

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
    """Mixin for collections whose concurrent writers can be merged by net-effect replay (ruling R6).

    Each version carries ``_op_log``, a runtime-only tuple of the operations
    applied since the version was loaded or created (never persisted: a loaded
    version starts with an empty log).  Mutators extend it immutably with
    ``_with(..., _op_log=self._logged(op))``; an op is a tuple whose first item
    is a ``str`` kind not starting with ``"@"`` (reserved for index-definition
    changes, which :class:`IndexedCollection` logs itself).

    The log records INTENT, not observed change: every mutator call logs its op
    (add means "present", remove means "absent", set means "mapped to this
    value"), including calls that change nothing in this version (a blind
    write).  Such a call returns a NEW version that carries the extended log
    and reuses the receiver's content and indexes (no index work).  Mutators
    must therefore be idempotent with respect to their intent, so that
    replaying an op onto any version forces its key to the intended state.

    :meth:`rebase_onto` replays the net intent of the log onto another version.
    Subclasses implement :meth:`_replay_op` and, when ``op[1]`` is not the key an
    op touches, override :meth:`_op_key`.  It must precede
    :class:`IndexedCollection` in the class bases.

    The log grows with every mutation of a version chain; it lives as long as the
    versions holding it (typically one transaction).

    :attr:`log_is_complete` tells whether the log describes the whole content of
    the version (D33): true for a collection constructed empty in this process and
    for versions derived from it only through logged mutators; false for loaded
    versions, bulk-built ones (``from_items``/``from_iterable``) and rebased ones,
    and for every version derived from those.  A transaction merges a written root
    by replaying its log only when the log is complete; otherwise replaying would
    drop content the log does not describe.
    """

    _op_log: tuple = ()
    _log_complete: bool = False
    _runtime_fields: tuple = ("_op_log", "_log_complete")

    def __init__(self, *args, **kwargs):
        """Initialise the collection (cooperatively) and mark its empty log as complete."""
        super().__init__(*args, **kwargs)
        object.__setattr__(self, "_log_complete", True)

    @property
    def log_is_complete(self) -> bool:
        """True iff replaying this version's op log onto an empty collection rebuilds its content (D33)."""
        return self._log_complete

    def _with(self, **changes):
        """See :meth:`IndexedCollection._with`; also maintains :attr:`log_is_complete`.

        A change of persisted content keeps the log complete only when it also
        extends the log; a version built from an incomplete one stays incomplete.
        """
        if "_log_complete" not in changes:
            complete = self._log_complete
            if complete and any(name in type(self)._fields for name in changes):
                complete = len(changes.get("_op_log", ())) > len(self._op_log)
            changes["_log_complete"] = complete
        return super()._with(**changes)

    def _logged(self, *ops: tuple) -> tuple:
        """Return this version's op log extended with ``ops`` (the receiver's log is unchanged)."""
        for op in ops:
            if not isinstance(op, tuple) or not op or not isinstance(op[0], str):
                raise ProtoValidationError(f"An op must be a tuple starting with a str kind, not {op!r}")
        return self._op_log + ops

    def _index_log(self, op: tuple) -> dict:
        return {"_op_log": self._op_log + (op,)}

    def _op_key(self, op: tuple):
        """Key identifying what ``op`` touches, for :func:`net_effect` (default ``order_key(op[1])``)."""
        return order_key(op[1])

    def _net_ops(self, element_ops: list) -> list:
        """Reduce this log's element ops to the ops :meth:`rebase_onto` replays, in replay order.

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

    def rebase_onto(self, current):
        """Return ``current`` with the net effect of this version's op log applied.

        Element ops: the net ops of :meth:`_net_ops` (by default the last op per
        key, in order of last occurrence) are replayed unconditionally through
        :meth:`_replay_op` (public, idempotent mutators, so ``current``'s indexes
        are maintained and every touched key ends in its last intended state).
        Index-definition changes are then replayed per index name: an index this
        log created is added (``ProtoConflictError`` if ``current`` already has
        one with that name), an index this log removed is removed if still
        present, a removed-then-added index replaces ``current``'s, and an index
        created and removed within the log leaves ``current`` untouched.  Added
        indexes are built (from ``empty()``) over the merged elements.
        """
        if type(current) is not type(self):
            raise ProtoValidationError(f"Cannot rebase a {type(self).__name__} onto a {type(current).__name__}")
        element_ops, index_ops = [], []
        for op in self._op_log:
            (index_ops if op[0] in (_INDEX_ADD, _INDEX_REMOVE) else element_ops).append(op)
        target = current
        for op in self._net_ops(element_ops):
            target = self._replay_op(target, op)
        first_kind: dict = {}
        for op in index_ops:
            first_kind.setdefault(op[1], op[0])
        for op in net_effect(index_ops, key=lambda o: o[1]):
            kind, name = op[0], op[1]
            preexisting = first_kind[name] == _INDEX_REMOVE
            if kind == _INDEX_REMOVE:
                if preexisting and name in target.indexes:
                    target = target.remove_index(name)
                continue
            if name in target.indexes:
                if not preexisting:
                    raise ProtoConflictError(f"Index {name!r} was created concurrently by another transaction")
                target = target.remove_index(name)
            target = target.add_index(op[2])
        return target
