"""One TCP cluster peer of the cross-transaction cache profile (Task 9c CARRY, ruling R110).

Usage: ``python cache_peer.py CONFIG_JSON``, where the config holds ``node``, ``local``, ``store``,
``writers``, ``readers``, ``seconds``, ``catalog``, ``products``, ``categories``, ``read`` (``"point"``
or ``"query"``), ``atom_cache_entries`` and ``query_cache_bytes``.

The peer runs a :class:`~proto_db.ClusterCoordinator` over a :class:`~proto_db.TcpTransport` and a
:class:`~proto_db.TieredStorage` (``durability="fsync"``) over the directory object store shared by
every peer, as :mod:`benchmarks.cluster_peer` does, with an :class:`~proto_db.ObjectSpace` built
with the given ``atom_cache_entries`` and ``query_cache_bytes``.

Data, committed by ``n0`` before the window and visible on every peer before it starts:

* ``catalog``: a Dictionary of ``catalog`` entries, ``k -> catalog_value(k, 0)``;
* ``products``: a Dictionary of ``products`` DBObjects (``id``, ``cat = id % categories``, ``name``)
  with an EqualityIndex on ``cat``.

The window: ``writers`` threads (on ``n0`` only, in this profile) each commit, one at a time, an
update of one catalog key of their own partition (``k % writers == t``) to ``catalog_value(k,
gen)``, ``gen`` the thread's commit number; ``products`` is never written.  ``readers`` threads
(on ``n1`` and ``n2``) read, each read in a new transaction:

* ``point``: one random catalog key; the value must be well formed for that key (any
  generation: a reader sees the root its node last learned);
* ``query``: ``products where cat == c`` for a random ``c``, through ``space.query_cache.result``;
  the ids must be exactly the model's.

Before the window, each reader node reads every catalog key (``point``) or runs every category's
query (``query``) once, untimed, so the window measures reads across transactions on a node that
has served reads before, not a first pass.

Each reader node reports its reads, every read latency, the database versions its readers saw
(their minimum and maximum: proof that the reads crossed roots written on another node), the
decoded-atom and query cache counters over the window, and the atom decodes counted in the process
over the window (:mod:`benchmarks.decode_counter`).  ``n0`` reports, per writer thread, the last
generation it committed for each key, so the parent can rebuild the exact final catalog.

Protocol with the parent, one JSON line each way (as :mod:`benchmarks.cluster_peer`): ``{"port"}``;
``{"peers"}``; ``{"ready"}``; ``{"go"}``; ``{"loaded"}``; ``{"go2"}``; the report; ``{"stop"}``.
"""
import json
import os
import random
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from benchmarks import decode_counter  # noqa: E402
from benchmarks.cluster_peer import COORDINATOR_KW, RUN_KW, _hear, _say  # noqa: E402
from benchmarks.support import DirObjectStore  # noqa: E402
from proto_db import (ClusterCoordinator, EqualityIndex, F, ObjectSpace, TcpTransport,  # noqa: E402
                      TieredStorage, from_collection)
from proto_db.errors import ProtoValidationError  # noqa: E402


def catalog_value(key: int, gen: int) -> str:
    """The catalog value of ``key`` at generation ``gen`` (short: stored inline in its tree node)."""
    return f"v{key}:{gen}:{(key * 7919 + gen * 104729) % 1000003}"


def value_ok(key: int, value) -> bool:
    """Whether ``value`` is ``catalog_value(key, gen)`` for some generation."""
    if not isinstance(value, str):
        return False
    parts = value.split(":")
    if len(parts) != 3 or parts[0] != f"v{key}" or not parts[1].isdigit():
        return False
    return value == catalog_value(key, int(parts[1]))


def category_ids(products: int, categories: int, cat: int) -> list:
    """The model's answer to ``products where cat == c``: the sorted ids."""
    return list(range(cat, products, categories))


def _cache_stats(space) -> dict:
    atoms = space.atom_cache_stats()
    return {"atoms": atoms, "queries": space.query_cache_stats(), "decodes": decode_counter.count()}


def _delta(after: dict, before: dict) -> dict:
    out = {"decodes": after["decodes"] - before["decodes"]}
    for part in ("atoms", "queries"):
        a, b = after[part], before[part]
        out[part] = None if a is None else {k: a[k] - b.get(k, 0) for k in a
                                            if isinstance(a[k], int) and k not in ("capacity", "entries", "bytes",
                                                                                   "identity_map")}
    return out


def _load(db, cfg: dict) -> None:
    """``n0`` commits the data; every peer then waits until its own reads see all of it."""
    if cfg["node"] == "n0":
        def fill(tr):
            d = tr.new_dictionary()
            for k in range(cfg["catalog"]):
                d = d.set_at(k, catalog_value(k, 0))
            tr.set_root_object("catalog", d)
            p = tr.new_dictionary().add_index(EqualityIndex("by_cat", "cat"))
            for i in range(cfg["products"]):
                p = p.set_at(i, tr.new_object(id=i, cat=i % cfg["categories"], name=f"product-{i}"))
            tr.set_root_object("products", p)
        db.run(fill, **RUN_KW)
    deadline = time.monotonic() + 120
    while True:
        tr = db.new_transaction()
        try:
            cat, prod = tr.get_root_object("catalog"), tr.get_root_object("products")
            if cat is not None and prod is not None and cat.count == cfg["catalog"] and prod.count == cfg["products"]:
                return
        finally:
            tr.abort()
        if time.monotonic() > deadline:
            raise RuntimeError("the data did not become visible on this node")
        time.sleep(0.01)


def _query(space, tr, cat: int) -> list:
    q = from_collection(tr.get_root_object("products")).where(F.cat == cat)
    return sorted(row.id for row in space.query_cache.result(q))


def _warm(db, space, cfg: dict) -> None:
    """One untimed pass over what the readers read (module docstring)."""
    tr = db.new_transaction()
    try:
        if cfg["read"] == "point":
            catalog = tr.get_root_object("catalog")
            for k in range(cfg["catalog"]):
                catalog.get_at(k)
        else:
            for c in range(cfg["categories"]):
                _query(space, tr, c)
    finally:
        tr.abort()


def read_loop(db, space, cfg: dict, r: int, start: float) -> dict:
    """One reader thread: reads of the configured kind, each in its own transaction, verified."""
    rng = random.Random(int(cfg["node"][1:]) * 1000 + r)
    latencies, bad, versions = [], 0, []
    stop_at = start + cfg["seconds"]
    clock = time.perf_counter
    point = cfg["read"] == "point"
    while time.monotonic() < stop_at:
        if point:
            key = rng.randrange(cfg["catalog"])
        else:
            cat = rng.randrange(cfg["categories"])
        t0 = clock()
        tr = db.new_transaction()
        if point:
            got = tr.get_root_object("catalog").get_at(key)
        else:
            got = _query(space, tr, cat)
        version = tr._db_root.version            # benchmark-only: which committed version was read
        tr.abort()
        latencies.append(round((clock() - t0) * 1e6))
        versions.append(version)
        if not (value_ok(key, got) if point else got == category_ids(cfg["products"], cfg["categories"], cat)):
            bad += 1
    return {"reads": len(latencies), "bad": bad, "latencies_us": latencies,
            "versions": [min(versions), max(versions)] if versions else None}


def main() -> None:
    """Run one peer (module docstring)."""
    cfg = json.loads(sys.argv[1])
    decode_counter.install()
    transport = TcpTransport("127.0.0.1", 0)
    _say({"port": transport.address[1]})
    peers = [tuple(p) for p in _hear()["peers"]]
    coordinator = ClusterCoordinator(cfg["node"], peers, transport=transport, **COORDINATOR_KW)
    with coordinator:
        storage = TieredStorage(cfg["local"], DirObjectStore(cfg["store"]), durability="fsync", ship_interval=0.05,
                                backoff=(0.001, 0.05))
        space = ObjectSpace(storage, coordinator=coordinator, atom_cache_entries=cfg["atom_cache_entries"],
                            query_cache_bytes=cfg["query_cache_bytes"])
        deadline = time.monotonic() + 30
        while coordinator.stats()["rejoining"] and time.monotonic() < deadline:
            time.sleep(0.005)
        _say({"ready": True})
        _hear()
        if cfg["node"] == "n0":
            try:
                space.new_database("db")
            except ProtoValidationError:
                pass                        # already created
        db = None
        deadline = time.monotonic() + 30
        while db is None:
            try:
                db = space.open_database("db")
            except ProtoValidationError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.01)
        _load(db, cfg)
        if cfg["readers"]:
            _warm(db, space, cfg)
        _say({"loaded": True})
        _hear()

        writers, readers = cfg["writers"], cfg["readers"]
        last = [dict() for _ in range(writers)]          # per writer thread: key -> last committed gen
        counts = [0] * writers
        commit_lat = []
        read_stats = [None] * readers
        thread_errors = []
        lock = threading.Lock()

        def writer(t):
            rng = random.Random(t)
            keys = list(range(t, cfg["catalog"], writers))
            stop_at = start + cfg["seconds"]
            lat, gen = [], 0
            try:
                while time.monotonic() < stop_at:
                    gen += 1
                    key = rng.choice(keys)

                    def fn(tr, key=key, gen=gen):
                        tr.set_root_object("catalog", tr.get_root_object("catalog").set_at(key, catalog_value(key, gen)))
                    t0 = time.perf_counter()
                    db.run(fn, **RUN_KW)
                    lat.append(time.perf_counter() - t0)
                    last[t][key] = gen
                    counts[t] += 1
            except BaseException as e:      # reported, not swallowed: the run then fails its check
                thread_errors.append(f"writer {type(e).__module__}.{type(e).__qualname__}: {str(e)[:300]}")
                raise
            finally:
                with lock:
                    commit_lat.extend(lat)

        def reader(r):
            try:
                read_stats[r] = read_loop(db, space, cfg, r, start)
            except BaseException as e:      # reported, not swallowed: the run then fails its check
                thread_errors.append(f"reader {type(e).__module__}.{type(e).__qualname__}: {str(e)[:300]}")
                raise

        before = _cache_stats(space)
        start = time.monotonic()
        threads = [threading.Thread(target=writer, args=(t,)) for t in range(writers)]
        threads += [threading.Thread(target=reader, args=(r,)) for r in range(readers)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        elapsed = time.monotonic() - start
        window = _delta(_cache_stats(space), before)
        space.sync(timeout=120)
        st = coordinator.stats()
        _say({"node": cfg["node"], "seconds": elapsed, "counts": counts,
              "last": [{str(k): g for k, g in d.items()} for d in last],
              "commit_latencies": [round(x, 6) for x in commit_lat],
              "reads": [x for x in read_stats if x is not None], "window": window,
              "atom_cache_end": space.atom_cache_stats(), "query_cache_end": space.query_cache_stats(),
              "failures": st["replication_failures"], "healthy": st["healthy"],
              "thread_errors": thread_errors, "pid": os.getpid()})
        _hear()
        space.close()


if __name__ == "__main__":
    main()
