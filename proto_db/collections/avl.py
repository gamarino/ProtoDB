"""Persistent AVL trees: the structure under every ProtoDB collection (spec section 8.2, D8, D20).

Two node kinds share one balancing core:

* :class:`PosNode` -- positional trees (``List``): elements are addressed by their
  in-order index, found through the subtree ``count`` stored in every node.
* :class:`KeyNode` -- keyed trees (``Dictionary``, ``HashDictionary``, ``Set``,
  indexes): nodes are ordered by :func:`proto_db.values.order_key` of their key.
  Keys are equal when their order keys are equal (``1`` and ``1.0`` are one key).

The empty tree is ``None``.  Nodes are :class:`~proto_db.core.atom.Atom` objects:
they are persisted like any atom and children read back from storage are lazy
stubs, loaded on first access.  A node's fields are set only by its constructor
(``__setattr__`` raises ``ProtoUserError``), so a node is safe to share between
threads once built.

Every function is pure: it returns new nodes along the touched path (path
copying) and shares all other subtrees with its input, which is never modified.

Complexity (n elements, k elements produced)::

    pos_get, pos_insert, pos_set, pos_remove        O(log n)
    pos_slice, pos_concat                           O(log n)  (split / join)
    key_get, key_put, key_remove                    O(log n)
    pos_iter, key_iter                              O(n), explicit stack
    key_iter_range                                  O(log n + k), explicit stack
    pos_from_iterable, key_from_sorted              O(n)
    check_invariants                                O(n), explicit stack

Recursive helpers recurse only along one root-to-leaf path, so their depth is
bounded by the tree height (at most about ``1.44 * log2(n)``); traversals never
recurse.  Index arguments must be ``int`` (``bool`` is rejected) within range;
violations raise ``ProtoValidationError``.  Negative indexes are not accepted
here; collections translate Python-style indexes before calling in.

Values are stored as given; validating them (``values.canonical``) is the
caller's job.  Keys must be orderable (``order_key`` raises
``ProtoValidationError`` otherwise).
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator

from ..core.atom import Atom, PersistenceContext
from ..errors import ProtoCorruptionError, ProtoUserError, ProtoValidationError
from ..values import order_key

__all__ = [
    "PosNode", "KeyNode",
    "pos_get", "pos_insert", "pos_set", "pos_remove", "pos_slice", "pos_concat",
    "pos_iter", "pos_count", "pos_from_iterable",
    "key_get", "key_put", "key_remove", "key_iter", "key_iter_range", "key_from_sorted",
    "check_invariants",
]


# -- nodes --------------------------------------------------------------------------------

class _Node(Atom):
    """Common behaviour of AVL nodes: immutability and child-derived height/count."""

    __type_name__ = "proto_db.avl._Node"

    def _init_shape(self, left, right) -> None:
        osa = object.__setattr__
        osa(self, "left", left)
        osa(self, "right", right)
        osa(self, "height", 1 + max(_height(left), _height(right)))
        osa(self, "count", 1 + _count(left) + _count(right))

    def _with_children(self, left, right) -> "_Node":
        """Return a new node with this node's payload and the given children."""
        raise NotImplementedError

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        raise ProtoUserError(f"AVL nodes are immutable; cannot set {name!r}")

    def __delattr__(self, name):
        if name.startswith("_"):
            object.__delattr__(self, name)
            return
        raise ProtoUserError(f"AVL nodes are immutable; cannot delete {name!r}")


class PosNode(_Node):
    """Node of a positional tree: ``value``, ``left``, ``right``, ``height``, ``count``.

    ``height`` and ``count`` are computed from the children; ``context`` is the
    optional persistence context (contexts adopt unbound nodes when saving).
    """

    __type_name__ = "proto_db.avl.PosNode"

    def __init__(self, value, left: "PosNode | None" = None, right: "PosNode | None" = None, *,
                 context: PersistenceContext | None = None):
        super().__init__(context=context)
        object.__setattr__(self, "value", value)
        self._init_shape(left, right)

    def _with_children(self, left, right) -> "PosNode":
        return PosNode(self.value, left, right, context=self.context)


class KeyNode(_Node):
    """Node of a keyed tree: ``key``, ``value``, ``left``, ``right``, ``height``, ``count``.

    Ordered by ``order_key(key)``, which is cached per node at run time (never persisted).
    """

    __type_name__ = "proto_db.avl.KeyNode"

    def __init__(self, key, value, left: "KeyNode | None" = None, right: "KeyNode | None" = None, *,
                 context: PersistenceContext | None = None, _okey=None):
        super().__init__(context=context)
        object.__setattr__(self, "_okey", order_key(key) if _okey is None else _okey)
        object.__setattr__(self, "key", key)
        object.__setattr__(self, "value", value)
        self._init_shape(left, right)

    def _order_key(self):
        """Return ``order_key(self.key)``, computing and caching it on first use (stubs)."""
        okey = self.__dict__.get("_okey")
        if okey is None:
            okey = order_key(self.key)
            # Benign race: every thread computes the same immutable tuple.
            object.__setattr__(self, "_okey", okey)
        return okey

    def _with_children(self, left, right) -> "KeyNode":
        return KeyNode(self.key, self.value, left, right, context=self.context, _okey=self._order_key())

    def _with_value(self, value) -> "KeyNode":
        return KeyNode(self.key, value, self.left, self.right, context=self.context,
                       _okey=self._order_key())


# -- balancing core -----------------------------------------------------------------------

def _height(node) -> int:
    return 0 if node is None else node.height


def _count(node) -> int:
    return 0 if node is None else node.count


def _balance(pivot: _Node, left, right) -> _Node:
    """Node with ``pivot``'s payload over ``left``/``right`` whose heights differ by at most 2."""
    hl, hr = _height(left), _height(right)
    if hl > hr + 1:
        if _height(left.left) >= _height(left.right):
            return left._with_children(left.left, pivot._with_children(left.right, right))
        lr = left.right
        return lr._with_children(left._with_children(left.left, lr.left),
                                 pivot._with_children(lr.right, right))
    if hr > hl + 1:
        if _height(right.right) >= _height(right.left):
            return right._with_children(pivot._with_children(left, right.left), right.right)
        rl = right.left
        return rl._with_children(pivot._with_children(left, rl.left),
                                 right._with_children(rl.right, right.right))
    return pivot._with_children(left, right)


def _join(pivot: _Node, left, right) -> _Node:
    """Tree of ``left`` + pivot payload + ``right`` for trees of any heights (all left < pivot < right).

    Descends the spine of the taller tree only: O(|height(left) - height(right)| + 1).
    """
    hl, hr = _height(left), _height(right)
    if hl > hr + 1:
        return _balance(left, left.left, _join(pivot, left.right, right))
    if hr > hl + 1:
        return _balance(right, _join(pivot, left, right.left), right.right)
    return pivot._with_children(left, right)


def _pop_first(node: _Node):
    """Return ``(tree without its first node, first node)``."""
    if node.left is None:
        return node.right, node
    rest, first = _pop_first(node.left)
    return _balance(node, rest, node.right), first


def _join2(left, right):
    """Concatenate two trees (all of ``left`` before all of ``right``) in O(log n)."""
    if right is None:
        return left
    if left is None:
        return right
    rest, first = _pop_first(right)
    return _join(first, left, rest)


def _remove_node(node: _Node):
    """Tree that replaces ``node`` once its own payload is deleted."""
    left, right = node.left, node.right
    if left is None:
        return right
    if right is None:
        return left
    rest, successor = _pop_first(right)
    return _balance(successor, left, rest)


def _check_kind(root, cls) -> None:
    if root is not None and not isinstance(root, cls):
        raise ProtoValidationError(f"Expected a {cls.__name__} tree, got {type(root).__name__}")


# -- positional ---------------------------------------------------------------------------

def _check_index(index, upper: int, what: str) -> None:
    if type(index) is not int:
        raise ProtoValidationError(f"{what} must be an int, not {type(index).__name__}")
    if not 0 <= index <= upper:
        raise ProtoValidationError(f"{what} {index} out of range 0..{upper}")


def pos_count(root: PosNode | None) -> int:
    """Number of elements in the positional tree ``root``."""
    return _count(root)


def pos_get(root: PosNode | None, index: int):
    """Return the element at ``index`` (``0 <= index < count``)."""
    _check_kind(root, PosNode)
    _check_index(index, _count(root) - 1, "Index")
    node = root
    while True:
        lc = _count(node.left)
        if index < lc:
            node = node.left
        elif index == lc:
            return node.value
        else:
            index -= lc + 1
            node = node.right


def pos_insert(root: PosNode | None, index: int, value) -> PosNode:
    """Return a tree with ``value`` inserted before position ``index`` (``0 <= index <= count``)."""
    _check_kind(root, PosNode)
    _check_index(index, _count(root), "Index")
    return _pos_insert(root, index, value)


def _pos_insert(node, index, value):
    if node is None:
        return PosNode(value)
    lc = _count(node.left)
    if index <= lc:
        return _balance(node, _pos_insert(node.left, index, value), node.right)
    return _balance(node, node.left, _pos_insert(node.right, index - lc - 1, value))


def pos_set(root: PosNode | None, index: int, value) -> PosNode:
    """Return a tree whose element at ``index`` is ``value`` (``0 <= index < count``)."""
    _check_kind(root, PosNode)
    _check_index(index, _count(root) - 1, "Index")
    return _pos_set(root, index, value)


def _pos_set(node, index, value):
    lc = _count(node.left)
    if index < lc:
        return PosNode(node.value, _pos_set(node.left, index, value), node.right, context=node.context)
    if index == lc:
        return PosNode(value, node.left, node.right, context=node.context)
    return PosNode(node.value, node.left, _pos_set(node.right, index - lc - 1, value), context=node.context)


def pos_remove(root: PosNode | None, index: int) -> PosNode | None:
    """Return a tree without the element at ``index`` (``0 <= index < count``)."""
    _check_kind(root, PosNode)
    _check_index(index, _count(root) - 1, "Index")
    return _pos_remove(root, index)


def _pos_remove(node, index):
    lc = _count(node.left)
    if index < lc:
        return _balance(node, _pos_remove(node.left, index), node.right)
    if index == lc:
        return _remove_node(node)
    return _balance(node, node.left, _pos_remove(node.right, index - lc - 1))


def _pos_split(node, index):
    """Return ``(first index elements, remaining elements)`` of ``node``."""
    if node is None:
        return None, None
    lc = _count(node.left)
    if index <= lc:
        if index == lc:
            return node.left, _join(node, None, node.right)
        ll, lr = _pos_split(node.left, index)
        return ll, _join(node, lr, node.right)
    rl, rr = _pos_split(node.right, index - lc - 1)
    return _join(node, node.left, rl), rr


def pos_slice(root: PosNode | None, start: int, stop: int) -> PosNode | None:
    """Return the tree of elements ``start .. stop-1`` (``0 <= start <= stop <= count``) in O(log n)."""
    _check_kind(root, PosNode)
    n = _count(root)
    _check_index(start, n, "Slice start")
    _check_index(stop, n, "Slice stop")
    if stop < start:
        raise ProtoValidationError(f"Slice stop {stop} is before start {start}")
    if start == 0 and stop == n:
        return root
    _, tail = _pos_split(root, start)
    middle, _ = _pos_split(tail, stop - start)
    return middle


def pos_concat(left: PosNode | None, right: PosNode | None) -> PosNode | None:
    """Return the tree of ``left``'s elements followed by ``right``'s, in O(log n)."""
    _check_kind(left, PosNode)
    _check_kind(right, PosNode)
    return _join2(left, right)


def _inorder(root) -> Iterator:
    """Yield the nodes of ``root`` in order, with an explicit stack."""
    stack = []
    node = root
    while stack or node is not None:
        while node is not None:
            stack.append(node)
            node = node.left
        node = stack.pop()
        yield node
        node = node.right


def pos_iter(root: PosNode | None) -> Iterator:
    """Yield the elements of a positional tree in order (no recursion)."""
    _check_kind(root, PosNode)
    for node in _inorder(root):
        yield node.value


def _build_balanced(items: list, make) -> _Node | None:
    """Perfectly balanced tree over ``items``; ``make(item, left, right)`` builds a node.

    Subtree sizes differ by at most one at every node, so heights do too.  Recursion
    depth is ``ceil(log2(n + 1))``.
    """
    def build(lo, hi):
        if lo >= hi:
            return None
        mid = (lo + hi) // 2
        return make(items[mid], build(lo, mid), build(mid + 1, hi))
    return build(0, len(items))


def pos_from_iterable(items: Iterable) -> PosNode | None:
    """Return a balanced positional tree of ``items`` in O(n)."""
    return _build_balanced(list(items), lambda v, l, r: PosNode(v, l, r))


# -- keyed --------------------------------------------------------------------------------

def key_get(root: KeyNode | None, key, default=None):
    """Return the value stored under ``key`` or ``default`` when absent."""
    _check_kind(root, KeyNode)
    k = order_key(key)
    node = root
    while node is not None:
        nk = node._order_key()
        if k < nk:
            node = node.left
        elif k > nk:
            node = node.right
        else:
            return node.value
    return default


def key_put(root: KeyNode | None, key, value):
    """Return ``(new_root, replaced, old_value)`` with ``key`` mapped to ``value``.

    For an existing key the stored key object is kept (as ``dict`` does) and
    ``old_value`` is its previous value; otherwise ``replaced`` is False and
    ``old_value`` is None.
    """
    _check_kind(root, KeyNode)
    k = order_key(key)
    out = [False, None]

    def put(node):
        if node is None:
            return KeyNode(key, value, _okey=k)
        nk = node._order_key()
        if k < nk:
            return _balance(node, put(node.left), node.right)
        if k > nk:
            return _balance(node, node.left, put(node.right))
        out[0], out[1] = True, node.value
        return node._with_value(value)

    new_root = put(root)
    return new_root, out[0], out[1]


def key_remove(root: KeyNode | None, key):
    """Return ``(new_root, removed, old_value)``; ``root`` itself is returned when ``key`` is absent."""
    _check_kind(root, KeyNode)
    k = order_key(key)
    out = [False, None]

    def remove(node):
        if node is None:
            return None
        nk = node._order_key()
        if k < nk:
            child = remove(node.left)
            return node if child is node.left else _balance(node, child, node.right)
        if k > nk:
            child = remove(node.right)
            return node if child is node.right else _balance(node, node.left, child)
        out[0], out[1] = True, node.value
        return _remove_node(node)

    new_root = remove(root)
    return new_root, out[0], out[1]


def key_iter(root: KeyNode | None) -> Iterator[tuple]:
    """Yield ``(key, value)`` pairs in key order (no recursion)."""
    _check_kind(root, KeyNode)
    for node in _inorder(root):
        yield node.key, node.value


def key_iter_range(root: KeyNode | None, lo, hi, lo_inc: bool = True, hi_inc: bool = True) -> Iterator[tuple]:
    """Yield ``(key, value)`` pairs with ``lo <(=) key <(=) hi`` in key order, in O(log n + k).

    ``lo`` or ``hi`` set to ``None`` means unbounded on that side (so ``None``
    itself cannot serve as a bound, D20).  ``lo_inc``/``hi_inc`` select
    inclusive bounds.
    """
    _check_kind(root, KeyNode)
    lok = None if lo is None else order_key(lo)
    hik = None if hi is None else order_key(hi)
    stack = []
    node = root
    while stack or node is not None:
        while node is not None:
            if lok is not None:
                nk = node._order_key()
                if nk < lok or (nk == lok and not lo_inc):
                    node = node.right  # node and its left subtree are below the range
                    continue
            stack.append(node)
            node = node.left
        if not stack:
            return
        node = stack.pop()
        if hik is not None:
            nk = node._order_key()
            if nk > hik or (nk == hik and not hi_inc):
                return
        yield node.key, node.value
        node = node.right


def key_from_sorted(pairs: Iterable[tuple]) -> KeyNode | None:
    """Return a balanced keyed tree of ``(key, value)`` pairs in O(n).

    The keys must be strictly increasing by ``order_key``; otherwise (unsorted or
    duplicated keys) ``ProtoValidationError`` is raised.
    """
    items = []
    prev = None
    for key, value in pairs:
        k = order_key(key)
        if prev is not None and not prev < k:
            raise ProtoValidationError(f"key_from_sorted needs strictly increasing keys; {key!r} is out of order")
        items.append((k, key, value))
        prev = k
    return _build_balanced(items, lambda t, l, r: KeyNode(t[1], t[2], l, r, _okey=t[0]))


# -- invariants ---------------------------------------------------------------------------

def check_invariants(root: _Node | None) -> None:
    """Verify a tree; raise ``ProtoCorruptionError`` describing the first violation.

    Checks, for every node: one node kind throughout, ``height`` and ``count``
    consistent with the children, AVL balance (child heights differ by at most
    one) and, for keyed trees, strictly increasing ``order_key`` in order.
    Post-order traversal with an explicit stack: loads every node of a stored tree.
    """
    if root is None:
        return
    kind = type(root)
    if kind not in (PosNode, KeyNode):
        raise ProtoCorruptionError(f"Not an AVL node: {kind.__name__}")
    keyed = kind is KeyNode
    # Each result: (height, count, min order key, max order key); keys are None for PosNode.
    results: dict[int, tuple] = {}
    stack = [(root, False)]
    while stack:
        node, expanded = stack.pop()
        if not expanded:
            stack.append((node, True))
            for child in (node.right, node.left):
                if child is not None:
                    if type(child) is not kind:
                        raise ProtoCorruptionError(
                            f"Mixed node kind: {type(child).__name__} under a {kind.__name__} tree")
                    stack.append((child, False))
            continue
        left, right = node.left, node.right
        lh, lc, lmin, lmax = results.pop(id(left)) if left is not None else (0, 0, None, None)
        rh, rc, rmin, rmax = results.pop(id(right)) if right is not None else (0, 0, None, None)
        height, count = node.height, node.count
        if height != 1 + max(lh, rh):
            raise ProtoCorruptionError(
                f"Wrong height {height!r} at {node!r}: children heights are {lh} and {rh}")
        if count != 1 + lc + rc:
            raise ProtoCorruptionError(f"Wrong count {count!r} at {node!r}: children counts are {lc} and {rc}")
        if abs(lh - rh) > 1:
            raise ProtoCorruptionError(f"AVL balance violated at {node!r}: child heights {lh} and {rh}")
        nmin = nmax = None
        if keyed:
            k = order_key(node.key)
            if (lmax is not None and not lmax < k) or (rmin is not None and not k < rmin):
                raise ProtoCorruptionError(f"Key order violated at {node!r} (key {node.key!r})")
            nmin = k if lmin is None else lmin
            nmax = k if rmax is None else rmax
        results[id(node)] = (height, count, nmin, nmax)
