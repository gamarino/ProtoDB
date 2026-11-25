"""Structural diffs of persistent AVL trees (spec R65, delta validation).

Two versions of a collection share every subtree a commit did not touch: path copying rebuilds
only the nodes along the paths it changed (:mod:`proto_db.collections.avl`).  The functions here
walk two trees together and skip every pair of subtrees that is the same subtree -- the same
object, or the same storage pointer -- without loading it.  The cost is therefore bounded by the
nodes that differ (O(changes x log n) node visits), not by the size of the trees.

* :func:`changed_keys` -- keyed trees (``KeyNode``): the keys present in one tree only, or whose
  stored value has another token (:func:`proto_db.preconditions.token`; a ``ValueBox`` or a nested
  atom by its pointer, never loaded).  Exactly the keys a range precondition compares
  (:func:`proto_db.preconditions.range_holds`), so a range is violated between the two trees iff
  one of these keys lies inside it.
* :func:`changed_entries` -- the same walk, yielding ``(key, old stored, new stored)``
  (:data:`~proto_db.preconditions.ABSENT` for the missing side): what a hash collection needs to
  compare the buckets that changed.
* :func:`changed_positions` -- positional trees (``PosNode``): the positions below a bound
  whose stored item has another token.

Engine code only: comparisons by ``order_key`` and token, never a user callback (P-C).
"""
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from ..core.atom import Atom
from ..preconditions import ABSENT, same_token, token

__all__ = ["changed_keys", "changed_entries", "changed_bucket_members", "changed_positions", "same_subtree", "EVERYTHING"]


class _Everything:
    """Type of :data:`EVERYTHING`."""

    __slots__ = ()

    def __repr__(self):
        return "EVERYTHING"


EVERYTHING = _Everything()
"""What :meth:`~proto_db.collections.base.IndexedCollection.changed_index_groups` yields when the
two versions cannot be compared group by group (the index is missing on one side or keeps no
groups): every group of that index is to be taken as changed."""


def same_subtree(a, b) -> bool:
    """Whether ``a`` and ``b`` are the same subtree: the same object, or two atoms with the same
    storage pointer (a stub is compared by its pointer, never loaded)."""
    if a is b:
        return True
    if isinstance(a, Atom) and isinstance(b, Atom):
        pointer = a.atom_pointer
        return pointer is not None and pointer == b.atom_pointer
    return False


class _Entry:
    """A node visited for its own key (as opposed to a subtree still to be expanded)."""

    __slots__ = ("node",)

    def __init__(self, node):
        self.node = node


def _expand(stack: list) -> None:
    """Replace the subtree on top of ``stack`` by its right subtree, its own entry and its left
    subtree (the left one on top: in-order)."""
    node = stack.pop()
    if node.right is not None:
        stack.append(node.right)
    stack.append(_Entry(node))
    if node.left is not None:
        stack.append(node.left)


class _FirstKeys:
    """The order key of the first key of a subtree (its leftmost node), memoized per subtree
    for one walk (the subtree is kept alive with its key)."""

    __slots__ = ("_memo",)

    def __init__(self):
        self._memo: dict = {}

    def __call__(self, node):
        found = self._memo.get(id(node))
        if found is not None and found[0] is node:
            return found[1]
        leftmost = node
        while leftmost.left is not None:
            leftmost = leftmost.left
        okey = leftmost._order_key()
        self._memo[id(node)] = (node, okey)
        return okey


def _keyed_walk(old_root, new_root) -> Iterator[tuple]:
    """Yield ``(key, old stored, new stored)`` for every key whose entry differs between the two
    keyed trees, in key order (see the module docstring)."""
    old = [old_root] if old_root is not None else []
    new = [new_root] if new_root is not None else []
    first = _FirstKeys()
    while old or new:
        a = old[-1] if old else None
        b = new[-1] if new else None
        a_tree = a is not None and not isinstance(a, _Entry)
        b_tree = b is not None and not isinstance(b, _Entry)
        if a_tree and b_tree:
            if same_subtree(a, b):
                old.pop()
                new.pop()
                continue
            ka, kb = first(a), first(b)
            if ka < kb:
                _expand(old)        # a holds a key b's remainder cannot hold: not shared whole
            elif kb < ka:
                _expand(new)
            elif a.height >= b.height:
                _expand(old)        # same first key: the taller may contain the shorter
            else:
                _expand(new)
            continue
        if a_tree:
            if b is not None and b.node._order_key() < first(a):
                new.pop()           # a key only the new tree holds (the old remainder starts later)
                yield b.node.key, ABSENT, b.node.value
            else:
                _expand(old)
            continue
        if b_tree:
            if a is not None and a.node._order_key() < first(b):
                old.pop()
                yield a.node.key, a.node.value, ABSENT
            else:
                _expand(new)
            continue
        if b is None:
            old.pop()
            yield a.node.key, a.node.value, ABSENT
            continue
        if a is None:
            new.pop()
            yield b.node.key, ABSENT, b.node.value
            continue
        ka, kb = a.node._order_key(), b.node._order_key()
        if ka < kb:
            old.pop()
            yield a.node.key, a.node.value, ABSENT
        elif kb < ka:
            new.pop()
            yield b.node.key, ABSENT, b.node.value
        else:
            old.pop()
            new.pop()
            if not same_token(token(a.node.value), token(b.node.value)):
                yield b.node.key, a.node.value, b.node.value


def changed_keys(old_root, new_root) -> Iterator[Any]:
    """The keys present in one of the keyed trees ``old_root``, ``new_root`` only, or whose stored
    value differs by token, in key order; pairs of identical subtrees are skipped unloaded."""
    for key, _old, _new in _keyed_walk(old_root, new_root):
        yield key


def changed_entries(old_root, new_root) -> Iterator[tuple]:
    """Like :func:`changed_keys`, yielding ``(key, old stored value, new stored value)`` with
    :data:`~proto_db.preconditions.ABSENT` for the side that lacks the key."""
    return _keyed_walk(old_root, new_root)


def changed_bucket_members(old_root, new_root, element_of) -> Iterator[Any]:
    """The members of a hash collection (a tree ``bucket hash -> bucket tuple``) whose entry
    differs between the two trees: present in one only, or stored with another token (the
    exact member, its count or its value).  Only the buckets that changed are compared, member
    by member (``element_of(entry)`` gives the member an entry holds; ``order_key`` equality
    identifies a member, as the collection's own lookup does)."""
    from ..values import order_key
    for _hash, old_bucket, new_bucket in _keyed_walk(old_root, new_root):
        old_entries = {} if old_bucket is ABSENT else {order_key(element_of(e)): e for e in old_bucket}
        new_entries = {} if new_bucket is ABSENT else {order_key(element_of(e)): e for e in new_bucket}
        for okey, entry in old_entries.items():
            other = new_entries.get(okey, ABSENT)
            if other is ABSENT or not same_token(token(entry), token(other)):
                yield element_of(entry)
        for okey, entry in new_entries.items():
            if okey not in old_entries:
                yield element_of(entry)


def changed_positions(old_root, new_root, stop: int) -> Iterator[int]:
    """The positions ``0 <= p < stop`` whose stored item differs by token between the positional
    trees ``old_root`` and ``new_root`` (both hold at least ``stop`` items), in order.  Both walks
    advance position by position, so two subtrees are skipped when they are the same subtree at
    the same position (reading the skipped subtree's root for its count: one node per skip)."""
    if same_subtree(old_root, new_root):
        return
    old = [old_root] if old_root is not None else []
    new = [new_root] if new_root is not None else []
    position = 0
    while position < stop and old and new:
        a, b = old[-1], new[-1]
        a_tree, b_tree = not isinstance(a, _Entry), not isinstance(b, _Entry)
        if a_tree and b_tree:
            if same_subtree(a, b):
                old.pop()
                new.pop()
                position += a.count
                continue
            if a.count >= b.count:
                _expand(old)
            else:
                _expand(new)
            continue
        if a_tree:
            _expand(old)
            continue
        if b_tree:
            _expand(new)
            continue
        old.pop()
        new.pop()
        if not same_token(token(a.node.value), token(b.node.value)):
            yield position
        position += 1
