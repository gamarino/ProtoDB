"""Index occurrence weights stay exact under concurrent commits that merge and retry (Task 8.4b, D56).

Threads insert, update the indexed field of, and remove entries of one indexed
``Dictionary`` (``ConcurrentOptimized``: a commit that loses the race replays its op log on
the newer root, D33) and of one indexed ``List`` (whole-value conflicts, retried by
``Database.run``).  At the end, and on intermediate snapshots, the recount checker
compares every weight with the occurrences actually stored, the index is validated
against the elements, and every rank window equals the plan's execution.
"""
import random
import threading

from proto_db.indexing import RangeIndex
from proto_db.query.terms import Term
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.support.index_weights import assert_exact_weights

RUN_KW = dict(retries=1000, backoff=(0.0002, 0.003))


def _check(collection):
    index = collection.get_index("k")
    assert_exact_weights(index)
    index.validate(list(collection.elements()))
    for plan in (index.ordered_plan(), index.ordered_plan(True), index.plan(Term("k", ">=", 5)),
                 index.plan(Term("k", "==", 3))):
        rows = list(plan.execute())
        assert plan.exact_rows() == len(rows)
        for start in (0, len(rows) // 3, len(rows) - 1):
            assert list(plan.window(max(start, 0), max(start, 0) + 7)) == rows[max(start, 0):max(start, 0) + 7]


def test_weights_are_exact_after_concurrent_merges_and_retries(monkeypatch):
    from proto_db.collections.dictionaries import Dictionary
    replayed = [0]
    replay = Dictionary._replay_op

    def counting_replay(self, target, op):
        replayed[0] += 1
        return replay(self, target, op)
    monkeypatch.setattr(Dictionary, "_replay_op", counting_replay)
    space = ObjectSpace(MemoryStorage())
    db = space.new_database("db")

    def seed(tx):
        d = tx.new_dictionary().add_index(RangeIndex("k", "k"))
        tx.set_root_object("d", d)
        tx.set_root_object("l", tx.new_list().add_index(RangeIndex("k", "k")))
    db.run(seed)
    errors = []
    snapshots = []

    def worker(seed_):
        rnd = random.Random(seed_)
        try:
            for i in range(60):
                key = rnd.randrange(40)

                def change(tx, i=i, key=key):
                    roll = rnd.random()
                    if seed_ % 2:
                        # Blind writer: an op log built from an empty collection, written without
                        # reading the root, is replayed onto the newest root at commit (D33 merge).
                        tx.set_root_object("d", tx.new_dictionary()
                                           .set_at(key, {"k": rnd.randrange(10), "w": seed_, "i": i})
                                           .set_at(2000 + 100 * seed_ + i, {"k": rnd.randrange(10)}))
                        return
                    d = tx.get_root_object("d")
                    if roll < 0.5:
                        d = d.set_at(key, {"k": rnd.randrange(10), "w": seed_, "i": i})   # insert or update k
                    elif roll < 0.7 and key in d:
                        d = d.remove_at(key)
                    else:
                        d = d.set_at(1000 * seed_ + i, {"k": None if roll > 0.95 else rnd.randrange(10)})
                    tx.set_root_object("d", d)
                    if i % 4 == 0:
                        lst = tx.get_root_object("l")
                        lst = lst.append_last({"k": rnd.randrange(10), "w": seed_})
                        if lst.count > 5 and roll < 0.3:
                            lst = lst.remove_at(rnd.randrange(lst.count))
                        tx.set_root_object("l", lst)
                db.run(change, **RUN_KW)
                if i % 20 == 0:
                    with db.new_transaction() as tx:
                        snapshots.append((tx.get_root_object("d"), tx.get_root_object("l")))
        except Exception as e:  # re-raised by the main thread
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(s,)) for s in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=300)
    assert not any(t.is_alive() for t in threads)
    if errors:
        raise errors[0]
    with db.new_transaction() as tx:
        final = (tx.get_root_object("d"), tx.get_root_object("l"))
    for d, lst in snapshots + [final]:
        _check(d)
        _check(lst)
    assert final[0].count > 0 and final[1].count > 0
    assert replayed[0] > 0, "no commit went through the merge (replay) path"
