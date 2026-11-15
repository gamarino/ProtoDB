"""The registry per database under real parallelism (runtime registry, Task 8 review minor 6).

8 threads start transactions of one database whose list no registry was built for yet and
resolve through it at once: they must share one registry (the process cache keeps the first
build published).  Then resolving threads run while another thread reloads the database's
registry over and over: every resolution must give the right definition, and nothing may
raise (the list never changes, so not even the mismatch error is expected).

Catches: two registries for one list after a racing first build, a resolution that sees a
half-made binding, a reload that makes a concurrent resolution fail or use another list.
"""
import threading

import pytest

from proto_db import registry as registry_module
from proto_db.storage.memory import MemoryStorage
from proto_db.registry import BASE
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import report, run_threads

THREADS = 8
ROUNDS = 40
MAPPING = {name: f"tests.unit.registry_modules.{name}" for name in ("reg8_base", "reg8_extra")}

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(120)]


def _database():
    space = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING)
    db = space.new_database("db")
    tr = db.new_transaction()
    tr.install_module("reg8_extra")
    tr.commit()
    tr = db.new_transaction()
    tr.set_root_object("thing", tr.new_object(parents=("reg8.Thing",)))
    tr.commit()
    return space, db


def test_concurrent_first_builds_share_one_registry(monkeypatch):
    space, db = _database()
    monkeypatch.setattr(registry_module, "_REGISTRIES", {})        # nothing built yet
    transactions = [db.new_transaction() for _ in range(THREADS)]
    found = [None] * THREADS
    greetings = [None] * THREADS

    def worker(i):
        def body():
            found[i] = transactions[i].registry
            greetings[i] = transactions[i].get_root_object("thing").greet()
        return body

    elapsed = run_threads([worker(i) for i in range(THREADS)])
    assert all(registry is found[0] for registry in found)
    assert greetings == ["extra+base"] * THREADS
    report("registry first builds", elapsed, f"{THREADS} threads, one registry")
    space.close()


def test_reload_racing_with_resolutions():
    space, db = _database()
    stop = threading.Event()
    results = []
    lock = threading.Lock()

    def resolver():
        seen = []
        for _ in range(ROUNDS):
            tr = db.new_transaction()
            seen.append(tr.get_root_object("thing").greet())
            seen.append(tr.new_object(parents=("reg8.Thing",)).greet())
            with tr:
                seen.append(tr.get_root_object("thing").greet())
        with lock:
            results.extend(seen)

    def reloader():
        reloads = 0
        while not stop.is_set() and reloads < ROUNDS * 4:
            assert space.reload_registry("db").key == (BASE, "reg8_base", "reg8_extra")
            reloads += 1

    def resolvers_then_stop():
        try:
            run_threads([resolver for _ in range(THREADS - 1)])
        finally:
            stop.set()

    elapsed = run_threads([resolvers_then_stop, reloader])
    assert results and set(results) == {"extra+base"}
    report("registry reload race", elapsed, f"{len(results)} resolutions")
    space.close()
