"""Query cost against the depth of a versioned-entity history (Task 9c CARRY; ProtoDB only, R112).

The documented pattern (2026-11-08 entry of the plan's ledger): an entity is a chain of immutable
versions, each holding its predecessor in a ``previous`` attribute set by the program; the current
collection holds only the newest version of each entity, so a superseded version leaves the
current collection and its indexes when its successor replaces it, and stays reachable through
``previous``.  The question measured: does a current-state query cost more when every entity has
a longer history?

For each depth ``D`` in ``depths``, a separate database (``FileStorage``, ``durability="none"``:
the build is not what is measured) holds ``entities`` orders in a Dictionary ``orders`` with an
EqualityIndex on ``customer`` (``customer = id % customers``).  Version 1 of every order is
committed first; then ``D - 1`` commits each replace every order by its next version
(``order.with_(version=v, status=..., previous=order)``).  Every depth ends with the same
``entities`` current orders; only the history behind them differs.

The query: ``orders where customer == c`` for a random ``c`` (``entities / customers`` rows), each in
a new transaction, the query result cache disabled (``query_cache_bytes=0``: the query runs every
time).  Two settings, both measured at every depth, interleaved repetition by repetition:

* warm: the space's decoded-atom cache at its default size, warmed by one pass over every customer;
* uncached: ``atom_cache_entries=0`` (a second space over the same files), so each query decodes
  every atom it touches: its decodes per query are the work the query does.

Each query's answer is checked against the model (the ids of that customer, every one at version
``D``); once per depth, outside the timing, the ``previous`` chain of one order is walked and must
hold versions ``D .. 1``.  Reported per depth: latency per query, atom decodes per query
(:mod:`benchmarks.decode_counter`), the entries of the current index, the versions stored, and the
database's size on disk.
"""
import os
import random
import time

from benchmarks.harness import BenchResult, benchmark, measure


def _status(oid: int, version: int) -> str:
    return f"s{version}-{(oid * 31 + version) % 97}"


class _History:
    """One database of the given depth (module docstring)."""

    def __init__(self, path: str, entities: int, customers: int, depth: int, batch: int):
        from proto_db import EqualityIndex, FileStorage, ObjectSpace
        self.path, self.entities, self.customers, self.depth = path, entities, customers, depth
        space = ObjectSpace(FileStorage(path, durability="none"), query_cache_bytes=0)
        db = space.new_database("h")

        def init(tr):
            d = tr.new_dictionary().add_index(EqualityIndex("by_customer", "customer"))
            for i in range(entities):
                d = d.set_at(i, tr.new_object(id=i, customer=i % customers, version=1, status=_status(i, 1),
                                              previous=None))
            tr.set_root_object("orders", d)
        t0 = time.perf_counter()
        db.run(init)
        for v in range(2, depth + 1):
            for lo in range(0, entities, batch):
                def step(tr, v=v, lo=lo):
                    d = tr.get_root_object("orders")
                    for i in range(lo, min(entities, lo + batch)):
                        o = d.get_at(i)
                        d = d.set_at(i, o.with_(version=v, status=_status(i, v), previous=o))
                    tr.set_root_object("orders", d)
                db.run(step)
        self.build_s = time.perf_counter() - t0
        space.close()
        # two spaces over the same files: warm (default atom cache) and uncached (no atom cache)
        self.warm = ObjectSpace(FileStorage(path, durability="none"), query_cache_bytes=0)
        self.cold = ObjectSpace(FileStorage(path, durability="none"), query_cache_bytes=0, atom_cache_entries=0)
        self.warm_db, self.cold_db = self.warm.open_database("h"), self.cold.open_database("h")

    def query(self, db, customer: int) -> tuple:
        from proto_db import F, from_collection
        with db.new_transaction() as tr:
            rows = list(from_collection(tr.get_root_object("orders")).where(F.customer == customer))
            return tuple(sorted((r.id, r.version, r.status) for r in rows))

    def want(self, customer: int) -> tuple:
        return tuple((i, self.depth, _status(i, self.depth)) for i in range(customer, self.entities, self.customers))

    def chain(self, oid: int) -> list:
        with self.warm_db.new_transaction() as tr:
            o, out = tr.get_root_object("orders").get_at(oid), []
            while o is not None:
                out.append(o.version)
                o = o.previous
            return out

    def index_entries(self) -> int:
        from proto_db import F, from_collection
        with self.warm_db.new_transaction() as tr:
            info = from_collection(tr.get_root_object("orders")).where(F.customer == 0).explain()
            return info["index"]["entries"]

    def close(self):
        self.warm.close()
        self.cold.close()


@benchmark("history_depth",
           quick={"entities": 60, "customers": 6, "depths": [1, 3, 9], "queries": 12, "batch": 60},
           full={"entities": 1_000, "customers": 100, "depths": [1, 10, 100, 1000], "queries": 200, "batch": 1_000})
def history_depth(ctx, p):
    """Current-state query at history depths 1-1000 per entity (versioned entities, ``previous``)."""
    from benchmarks import decode_counter
    from benchmarks.workloads.inventory import _apparent, _settled_du
    decode_counter.install()
    entities, customers = p["entities"], p["customers"]
    result = BenchResult({"entities": entities, "customers": customers, "rows per query": entities // customers,
                          "depths": p["depths"], "queries per sample": p["queries"],
                          "query cache": "off (query_cache_bytes=0)"}, None, None)
    rng = random.Random(1)
    asks = [rng.randrange(customers) for _ in range(p["queries"])]
    dbs = {}
    try:
        for depth in p["depths"]:
            h = _History(os.path.join(ctx.tempdir(f"history-{depth}"), "db"), entities, customers, depth, p["batch"])
            dbs[depth] = h
            chain = h.chain(entities // 2)
            result.check(f"depth {depth}: the previous chain holds every version", chain == list(range(depth, 0, -1)),
                         str(chain[:5]))
            entries = h.index_entries()
            result.check(f"depth {depth}: the current index holds only current versions", entries == entities,
                         f"{entries} != {entities}")
            result.metrics[f"depth {depth}"] = {
                "versions stored": entities * depth, "index entries": entries, "build_s": round(h.build_s, 1),
                "on-disk bytes (allocated, apparent)": (_settled_du(h.path), _apparent(h.path))}
            for c in range(customers):          # warm-up pass of the warm space
                h.query(h.warm_db, c)
        want = {d: [h.want(c) for c in asks] for d, h in dbs.items()}
        samples = {(d, s): [] for d in dbs for s in ("warm", "uncached")}
        decodes = {(d, s): [] for d in dbs for s in ("warm", "uncached")}
        for _ in range(ctx.repeat):             # interleaved: every depth and setting in each repetition
            for d, h in dbs.items():
                for setting, db in (("warm", h.warm_db), ("uncached", h.cold_db)):
                    before = decode_counter.count()
                    sample, _ = measure(lambda h=h, db=db: [h.query(db, c) for c in asks], 1, 0,
                                        expect=want[d])
                    samples[(d, setting)].extend(sample)
                    decodes[(d, setting)].append((decode_counter.count() - before) / len(asks))
        for (d, setting), xs in samples.items():
            result.add_timing(f"depth {d}: query, {setting}", xs, per=len(asks))
            result.metrics[f"depth {d}: decodes per query, {setting}, each sample"] = [round(x, 2) for x in
                                                                                       decodes[(d, setting)]]
        result.checksum = {d: [h.query(h.warm_db, c) for c in asks[:3]] for d, h in dbs.items()}
        result.expected = {d: want[d][:3] for d in dbs}
    finally:
        for h in dbs.values():
            h.close()
    return result
