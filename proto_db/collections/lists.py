"""List: an immutable, persistent, indexable sequence over a positional AVL tree (spec section 8.2, D23).

Elements are addressed by position.  Every mutator returns a new ``List`` and
keeps the associated indexes in step (one index event per element added,
removed or replaced); the receiver is never modified.  ``elements()`` yields the
items in order.

Positions follow Python conventions: a negative position counts from the end.
Single-element operations (``get_at``, ``set_at``, ``remove_at``, ``insert_at``)
raise ``ProtoValidationError`` for an out-of-range position, ``bool`` or
non-``int`` positions; ``slice``, ``head`` and ``tail`` clamp like Python slices.

Under the precondition model (spec sections 5, 6 and 9, R14-R16, R19, R48) a linked version --
one a transaction obtained from a root -- carries a record of intentions (``_changes``):

* ``append_last(v)`` and ``extend(vs)`` have no precondition; at commit their values are
  appended at the end of the current list, after the concurrent appends (commit order);
* ``set_at(i, v)`` with ``0 <= i <`` the base length records ``Unit(POSITION, root, key=i)``
  with the base's item at i (base indices, R15): position i must exist in the current list
  and hold the same value; at commit position i takes v;
* ``get_at(i)`` with ``0 <= i <`` the base length records ``Unit(POSITION, root, key=i)``;
* a ``get_at``/``set_at`` whose position depends on the length records ``Unit(WHOLE, root)``:
  a negative position (normalized against the transaction's own view, review correction of
  R15), a position at or beyond the base length -- one this version appended, which is "its
  own append" only while no concurrent append moved it (controller ruling: serializability,
  P-A, over folding by R9) -- and a position outside the view (the error reveals the length);
  the transaction's own list is then published as is, or the commit conflicts;
* ``insert_at``, ``remove_at``, ``append_first``, ``slice``/``head``/``tail`` and every
  other structural operation take the whole list for the transaction (R14, R16: insertions
  are not relocated); the version is published as is, and later reads add nothing (R19);
* every other read (``count``, ``len``, iteration, ``in``, ``content_equals``, queries)
  records the whole list (P-B).

A ``List`` is not ``ConcurrentOptimized``: that contract reapplies every intention on any
concurrent version without a precondition, which positional writes do not allow.  It keeps its
own record and :meth:`List._reapplied_on` (R34, R48).  A subclass that overrides an operation
the model relies on (:data:`LIST_OPERATIONS`) is an object root (N5).

Thread safety: a ``List`` version is immutable once built and may be shared
between threads; lazily loaded nodes are published atomically by the atom layer.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator
from itertools import islice

from ..core.atom import PersistenceContext
from ..errors import ProtoValidationError
from ..preconditions import Unit, UnitKind, token
from ..values import canonical_for
from . import avl, diff
from .base import (CATCH_UP_OPERATIONS, IndexedCollection, _Reapplying, check_unique_after, prepared_parts,
                   whole_unless_logged)
from .linking import NOT_A_UNIT, engine_reads, record_key, record_whole, register_fine_grained

__all__ = ["List", "LIST_OPERATIONS"]

_APPEND = "append"      # ("append", values): values appended at the end, in order
_SET = "set"            # ("set", position, value): position (of the version's own view) holds value


class List(IndexedCollection):
    """Persistent sequence; persisted fields ``root`` (a ``PosNode`` tree or ``None``), ``count``, ``indexes``."""

    __type_name__ = "proto_db.collections.List"
    _fields = ("root", "count", "indexes")
    #: How many items the version's own appends added since it was linked (or created): the base
    #: length of a linked version that is not whole is ``count - _appended``.  Runtime only.
    _appended: int = 0
    _runtime_fields = ("_appended",)

    def __init__(self, *, context: PersistenceContext | None = None):
        """Create an empty list."""
        super().__init__(context=context)
        object.__setattr__(self, "root", None)

    @classmethod
    def from_iterable(cls, items: Iterable, context: PersistenceContext | None = None) -> "List":
        """Return a list of ``items`` (canonicalised) built as a balanced tree in O(n)."""
        holder = f"{cls.__name__} element"
        values = [canonical_for(v, holder) for v in items]
        empty = cls(context=context)
        if not values:
            return empty
        return empty._with(root=avl.pos_from_iterable(values), count=len(values))

    def _with(self, **changes) -> "List":
        """See :meth:`IndexedCollection._with`; a change of persisted content that extends no
        ``_changes`` (an operation outside the listed ones) marks the version whole (P-B)."""
        return super()._with(**whole_unless_logged(self, changes))

    # -- reading ------------------------------------------------------------------------

    def elements(self) -> Iterator:
        """Items in order."""
        return avl.pos_iter(self.root)

    def _scan_window(self, start: int, stop: int | None):
        """Items ``start .. stop-1`` by position, O(log n + k) (D56)."""
        if type(self).elements is not List.elements:
            return None  # a subclass that redefines its elements cannot be windowed by position
        stop = self.count if stop is None else min(stop, self.count)
        if start >= stop:
            return iter(())
        return islice(avl.pos_iter_from(self.root, start), stop - start)

    def _position(self, index, upper: int) -> int:
        """Translate a Python-style position to ``0..upper``; ``ProtoValidationError`` if outside."""
        if type(index) is not int:
            raise ProtoValidationError(f"A list position must be an int, not {type(index).__name__}")
        position = index + self.count if index < 0 else index
        if not 0 <= position <= upper:
            raise ProtoValidationError(f"List position {index} out of range for a list of {self.count}")
        return position

    def _recorded_position(self, index) -> int:
        """The position of an existing item ``index`` (Python-style), its read recorded on a linked
        version (spec sections 5 and 6): ``Unit(POSITION)`` for a base position; ``Unit(WHOLE)``
        for a negative index, a position this version appended or a position outside the view
        (each depends on the length, R15, P-A).  The type is checked before anything is
        recorded."""
        if type(index) is not int:
            raise ProtoValidationError(f"A list position must be an int, not {type(index).__name__}")
        if self._link is None:
            return self._position(index, self.count - 1)
        with engine_reads():
            count = self.count
        # Recorded outside engine_reads(): inside it nothing is recorded.
        if self._whole or index < 0 or index >= count - self._appended:
            record_whole(self)
            with engine_reads():
                return self._position(index, count - 1)
        record_key(self, index, UnitKind.POSITION)
        return index

    def get_at(self, index: int):
        """Return the item at ``index`` (negative counts from the end)."""
        position = self._recorded_position(index)
        with engine_reads():
            return avl.pos_get(self.root, position)

    def _unit_token(self, kind, key):
        """The token of unit ``kind`` for position ``key``: for ``POSITION`` the item's token, or
        ``NOT_A_UNIT`` when the position does not exist; ``NOT_A_UNIT`` for any other kind."""
        if kind is not UnitKind.POSITION or not 0 <= key < self.count:
            return NOT_A_UNIT
        return token(avl.pos_get(self.root, key))

    def changed_units(self, other, root: str) -> Iterator[Unit]:
        """See :meth:`IndexedCollection.changed_units`: ``Unit(POSITION)`` of every position of
        this version whose item differs in ``other`` when ``other`` is at least as long (appends,
        replacements: the shared prefix is skipped unloaded); ``Unit(WHOLE)`` when ``other`` is
        shorter (an item was removed: positions moved, R14)."""
        if type(other) is not type(self):
            yield Unit(UnitKind.WHOLE, root)
            return
        with engine_reads():
            old, new, old_count, new_count = self.root, other.root, self.count, other.count
        if new_count < old_count:
            yield Unit(UnitKind.WHOLE, root)
            return
        for position in diff.changed_positions(old, new, old_count):
            yield Unit(UnitKind.POSITION, root, key=position)

    def _changed_element_units(self, other, root: str) -> Iterator[Unit]:
        """See :meth:`IndexedCollection._changed_element_units`: :meth:`changed_units`, and the
        positions ``other`` appended past this version's end (no precondition names them, but
        their items are new elements, R69)."""
        units = list(self.changed_units(other, root))
        yield from units
        if any(unit.kind is UnitKind.WHOLE for unit in units):
            return
        with engine_reads():
            old_count, new_count = self.count, other.count
        for position in range(old_count, new_count):
            yield Unit(UnitKind.POSITION, root, key=position)

    def _unit_elements(self, kind, key):
        """See :meth:`IndexedCollection._unit_elements`: the item at position ``key`` (R69)."""
        if kind is not UnitKind.POSITION:
            return None
        return [avl.pos_get(self.root, key)] if 0 <= key < self.count else []

    # -- mutators -----------------------------------------------------------------------

    def set_at(self, index: int, value) -> "List":
        """Return a list whose item at ``index`` is ``value`` (fires one replace event).

        On a linked version: a base position records ``Unit(POSITION)`` and the intention is
        reapplied on that position; any other position records the whole list (see
        :meth:`_recorded_position`), so the version is published as is."""
        value = canonical_for(value, f"{type(self).__name__} element")
        position = self._recorded_position(index)
        with engine_reads():
            old = avl.pos_get(self.root, position)
            return self._with(root=avl.pos_set(self.root, position, value),
                              indexes=self._after_replace(old, value),
                              _changes=self._changes + ((_SET, position, value),))

    def _structural(self, index, upper_extra: int) -> int:
        """Record the whole list (R14) and return the position of a structural operation."""
        if type(index) is not int:
            raise ProtoValidationError(f"A list position must be an int, not {type(index).__name__}")
        record_whole(self)
        with engine_reads():
            return self._position(index, self.count - 1 + upper_extra)

    def insert_at(self, index: int, value) -> "List":
        """Return a list with ``value`` inserted before position ``index`` (``count`` appends).

        A structural operation: the whole list (R14)."""
        value = canonical_for(value, f"{type(self).__name__} element")
        position = self._structural(index, 1)
        with engine_reads():
            return self._with(root=avl.pos_insert(self.root, position, value), count=self.count + 1,
                              indexes=self._after_add(value), _whole=True)

    def remove_at(self, index: int) -> "List":
        """Return a list without the item at ``index``.  A structural operation: the whole list (R14)."""
        position = self._structural(index, 0)
        with engine_reads():
            old = avl.pos_get(self.root, position)
            return self._with(root=avl.pos_remove(self.root, position), count=self.count - 1,
                              indexes=self._after_remove(old), _whole=True)

    def append_first(self, value) -> "List":
        """Return a list with ``value`` before the first item.  A structural operation (R14)."""
        return self.insert_at(0, value)

    def append_last(self, value) -> "List":
        """Return a list with ``value`` after the last item.  No precondition: at commit it is
        appended after the concurrent appends (section 5)."""
        return self._appended_with((canonical_for(value, f"{type(self).__name__} element"),))

    def extend(self, items: Iterable) -> "List":
        """Return a list with ``items`` appended in order (one add event per item).

        ``items`` is any iterable, a ``List`` included; it is fully read before
        anything is built.  An empty ``items`` returns the receiver.  No precondition, like
        :meth:`append_last`.
        """
        holder = f"{type(self).__name__} element"
        values = tuple(canonical_for(v, holder) for v in items)
        if not values:
            return self
        return self._appended_with(values)

    def _appended_with(self, values: tuple) -> "List":
        """The version with ``values`` appended, logged as one append intention.  One value is
        inserted at the end and several are concatenated as a balanced tree: the tree operations
        (and so the persisted node shapes) ``append_last`` and ``extend`` always used."""
        with engine_reads():
            indexes = self._current_indexes()
            for v in values:
                indexes = indexes.on_add(v)
            if len(values) == 1:
                root = avl.pos_insert(self.root, self.count, values[0])
            else:
                root = avl.pos_concat(self.root, avl.pos_from_iterable(values))
            return self._with(root=root,
                              count=self.count + len(values), indexes=indexes,
                              _changes=self._changes + ((_APPEND, values),),
                              _appended=self._appended + len(values))

    # -- reapplication (R34) ----------------------------------------------------------------

    def _net_ops(self, operations: tuple | None = None) -> tuple[dict, list]:
        """This version's intentions (``operations``, default its ``_changes``) reduced to what is
        reapplied: ``{base position: value}`` (the last value per position) and the appended
        values in order, each changed by the ``set_at`` calls on it.  (In a transaction a
        ``set_at`` on an appended position records the whole list, so such a version is
        published as is, not reapplied.)"""
        with engine_reads():
            base_count = self.count - self._appended
        sets: dict = {}
        appended: list = []
        for op in (prepared_parts(self._changes)[0] if operations is None else operations):
            if op[0] == _APPEND:
                appended.extend(op[1])
            elif op[0] == _SET:
                position = op[1]
                if position < base_count:
                    sets[position] = op[2]
                else:
                    appended[position - base_count] = op[2]
            else:
                raise ProtoValidationError(f"A List version with the op {op!r} is whole; it is not reapplied")
        return sets, appended

    def _intention_elements(self, base):
        """See :meth:`IndexedCollection._intention_elements`: each value set or appended, and the
        item ``base`` holds at each base position set (the item a ``POSITION`` precondition keeps)."""
        with engine_reads():
            count = base.count
        for op in prepared_parts(self._changes)[0]:
            if op[0] == _APPEND:
                yield from op[1]
            elif op[0] == _SET:
                yield op[2]
                if 0 <= op[1] < count:
                    yield avl.pos_get(base.root, op[1])

    def _reapplied_on(self, current: "List", changes: tuple | None = None) -> "List":
        """``current`` with this version's intentions applied (R34): each base position set by
        ``set_at`` (its precondition proved it present and unchanged), then the appended values
        at the end, through the same mutators the transaction used.  ``changes`` is the record
        :meth:`prepare_changes` gave (default: the version's own): a prepared record takes every
        index key from its table, so no extractor runs (R49).  Only a version that is not whole
        is reapplied."""
        if type(current) is not type(self):
            raise ProtoValidationError(f"Cannot reapply a {type(self).__name__} on a {type(current).__name__}")
        if self._whole:
            raise ProtoValidationError("A whole List version is published as is; it is not reapplied")
        operations, table = prepared_parts(self._changes if changes is None else changes)
        sets, appended = self._net_ops(operations)
        target = current
        with _Reapplying(table):
            for position, value in sets.items():
                target = target.set_at(position, value)
            if appended:
                target = target.extend(appended)
        check_unique_after(target, table)
        return target

    # -- derived lists --------------------------------------------------------------------

    def slice(self, start: int | None = None, stop: int | None = None) -> "List":
        """Return the list of items ``start .. stop-1`` with Python slice semantics (step 1).

        The result is a new collection, not an edit of this one: its indexes are
        the same index definitions rebuilt (``empty().build``) over the slice.
        Runs in O(log n) plus the index rebuilds.  On a linked version it reads the whole
        list (section 6).
        """
        record_whole(self)
        with engine_reads():
            lo, hi, _ = slice(start, stop).indices(self.count)
            hi = max(lo, hi)
            root = avl.pos_slice(self.root, lo, hi)
            indexes = self.indexes
            if len(indexes):
                items = list(avl.pos_iter(root))
                indexes = indexes._map(lambda index: index.empty().build(items))
            # Not an operation the precondition model lists: a version assigned to a root takes the
            # whole collection (P-B).
            return self._with(root=root, count=hi - lo, indexes=indexes, _whole=True)

    def head(self, n: int) -> "List":
        """Return the first ``n`` items (``slice(None, n)``; a negative ``n`` drops items from the end)."""
        return self.slice(None, n)

    def tail(self, n: int) -> "List":
        """Return the items from position ``n`` on (``slice(n, None)``; a negative ``n`` keeps the last items)."""
        return self.slice(n, None)

    def __repr__(self):
        if not self._loaded_now():
            return super().__repr__()  # never load a stub to describe it
        with engine_reads():           # describing it is not a read of the transaction
            return f"<List count={self.count}>"


#: The operations of a List the precondition model lists (spec sections 5 and 6) or relies on to
#: record, validate and reapply them (N5): a subclass overriding one is an object root.
LIST_OPERATIONS = ("get_at", "set_at", "append_last", "extend", "insert_at", "remove_at", "append_first",
                   "slice", "head", "tail", "_position", "_recorded_position", "_structural", "_appended_with",
                   "_unit_token", "elements", "_scan_window", "__iter__", "__len__", "__contains__",
                   "content_equals", "_same_content", "_net_ops", "_reapplied_on", "_with", "_current_indexes",
                   "_after_add", "_after_replace", "_after_remove", "_intention_elements", "prepare_changes",
                   "changed_units", "changed_index_groups") + CATCH_UP_OPERATIONS

register_fine_grained(List, LIST_OPERATIONS)
