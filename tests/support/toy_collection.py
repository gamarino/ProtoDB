"""ToySet: a minimal set-like IndexedCollection over a KeyNode tree, used to prove the contract.

Members are scalar values (the key of each node; the node value is ``True``).
It is ConcurrentOptimized so that the op-log and ``rebase_onto`` machinery is
exercised before the real collections (Tasks 3.3a/3.3b) exist.
"""
from proto_db.collections import avl
from proto_db.collections.base import ConcurrentOptimized, IndexedCollection
from proto_db.errors import ProtoValidationError


class ToySet(ConcurrentOptimized, IndexedCollection):
    """Set of orderable scalars; every mutator maintains the indexes and the op log."""

    __type_name__ = "tests.support.ToySet"
    _fields = ("root", "count", "indexes")

    def __init__(self, *, context=None):
        super().__init__(context=context)
        object.__setattr__(self, "root", None)

    def elements(self):
        """Members in key order."""
        for key, _ in avl.key_iter(self.root):
            yield key

    def has(self, e) -> bool:
        """True iff ``e`` is a member."""
        return avl.key_get(self.root, e, False)

    def add(self, e) -> "ToySet":
        """Return a set with ``e`` added (``self`` when already present)."""
        root, replaced, _ = avl.key_put(self.root, e, True)
        if replaced:
            return self
        return self._with(root=root, count=self.count + 1, indexes=self._after_add(e),
                          _op_log=self._logged(("add", e)))

    def discard(self, e) -> "ToySet":
        """Return a set without ``e`` (``self`` when absent)."""
        root, removed, _ = avl.key_remove(self.root, e)
        if not removed:
            return self
        return self._with(root=root, count=self.count - 1, indexes=self._after_remove(e),
                          _op_log=self._logged(("remove", e)))

    def replace(self, old, new) -> "ToySet":
        """Return a set where member ``old`` is replaced by non-member ``new`` (one index event)."""
        if not self.has(old) or self.has(new):
            raise ProtoValidationError("replace needs a present old member and an absent new one")
        root, _, _ = avl.key_remove(self.root, old)
        root, _, _ = avl.key_put(root, new, True)
        return self._with(root=root, indexes=self._after_replace(old, new),
                          _op_log=self._logged(("remove", old), ("add", new)))

    def _replay_op(self, target: "ToySet", op) -> "ToySet":
        kind, e = op
        if kind == "add":
            return target.add(e)
        if kind == "remove":
            return target.discard(e)
        raise ProtoValidationError(f"unknown op {op!r}")
