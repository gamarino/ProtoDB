"""Recount checker for the occurrence weights of key-group indexes (Task 8.4b, D56).

``assert_exact_weights(index)`` recounts every occurrence of an index from its members
and compares the recount with every stored weight: each members tree's weight with its
group's total and with the sum of its member counts, the groups tree's weight plus the
side group with ``total_count``.  It also requires the trees to be weighted
(``CountedKeyNode``): an index written by this code must offer rank access.
"""
from proto_db.collections import avl
from proto_db.collections.avl import CountedKeyNode


def _recount_members(members) -> int:
    return sum(entry[1] for _, entry in avl.key_iter(members))


def assert_exact_weights(index) -> None:
    groups, side = index.groups, index.side
    for tree in (groups, side[0]):
        assert tree is None or isinstance(tree, CountedKeyNode), f"unweighted tree {type(tree).__name__}"
    avl.check_invariants(groups)
    recount = 0
    for key, (members, total) in avl.key_iter(groups):
        assert members is not None and isinstance(members, CountedKeyNode)
        avl.check_invariants(members)
        n = _recount_members(members)
        assert n == total == avl.key_weight(members), (key, n, total, avl.key_weight(members))
        recount += n
    assert avl.key_weight(groups) == recount
    avl.check_invariants(side[0])
    side_n = _recount_members(side[0])
    assert side_n == side[1] == avl.key_weight(side[0])
    assert recount + side_n == index.total_count
