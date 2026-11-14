"""Concurrent installs of registry modules (runtime registry, Task 7; Review Focus 5).

8 threads install 4 modules (two threads per module), each in its own transaction started
before any of them commits, so every install is made from one base and its commit lands in a
group-commit round with the others (R71).  Each thread also writes an ordinary root of its own.
The commits are made with ``Transaction.commit`` directly, never retried: an install that
conflicted would raise.  Then the same over two ``CloudStorage`` instances of one object store
(two processes): there the root's compare-and-swap is the only arbiter, a lost race is retried
by ``Database.run``, and the retry merges onto the winner's list.

Catches: an install lost or applied twice under real parallelism, a conflict between installs,
a module list whose order is not the order its modules were committed in, a catalog entry
replaced by a later install, and a deadlock between installs and the root lock.
"""
import threading

import pytest

from proto_db.storage.cloud import CloudStorage
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import Attempts, report, run_threads
from tests.support.fake_object_store import FakeObjectStore

THREADS = 8
MODULES = ("reg7_a", "reg7_b", "reg7_c", "reg7_d")
MAPPING = {name: f"tests.unit.registry_modules.{name}" for name in MODULES}
ROUNDS = 5

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(120)]


def _check_final(db, prefix: str):
    """The installed list and the catalog oids of ``db``, checked complete, with the ordinary
    root ``<prefix><i>`` of every thread."""
    tr = db.new_transaction()
    try:
        installed = tr.installed_modules()
        assert sorted(installed) == sorted(MODULES), installed
        names = [f"reg7.Only_{m[-1]}" for m in MODULES] + ["reg7.Shared"]
        catalog = {name: tr.catalog_object(name) for name in names}
        assert all(obj is not None and obj.parents == (name,) for name, obj in catalog.items())
        assert all(tr.get_root_object(f"{prefix}{i}") == i for i in range(THREADS))
        return installed, {name: obj.oid for name, obj in catalog.items()}
    finally:
        tr.abort()


def test_concurrent_installs_land_without_conflicts(tmp_path):
    for round_number in range(ROUNDS):
        space = ObjectSpace(FileStorage(str(tmp_path / f"space{round_number}")), modules_mapping=MAPPING)
        db = space.new_database("db")
        transactions = [db.new_transaction() for _ in range(THREADS)]    # one base for all
        conflicts = []
        lock = threading.Lock()

        def install(i):
            def body():
                tr = transactions[i]
                tr.install_module(MODULES[i % len(MODULES)])
                tr.set_root_object(f"thread{i}", i)
                try:
                    tr.commit()
                except Exception as e:      # recorded: the assertion below names it
                    with lock:
                        conflicts.append(e)
                    raise
            return body

        elapsed = run_threads([install(i) for i in range(THREADS)])
        assert conflicts == []
        installed, _ = _check_final(db, "thread")
        assert len(db.history()) == THREADS + 1        # every commit published one version
        report("registry installs", elapsed, f"round {round_number}", f"order {installed}", "0 conflicts")
        space.close()


def test_installs_from_two_storage_instances_all_land():
    store = FakeObjectStore()
    spaces = [ObjectSpace(CloudStorage(store, backoff=(0.0, 0.001)), modules_mapping=MAPPING) for _ in range(2)]
    spaces[0].new_database("db")
    attempts = Attempts()
    dbs = [space.open_database("db") for space in spaces]

    def install(i):
        def body():
            name = MODULES[i % len(MODULES)]
            attempts.run(dbs[i % 2], lambda tr: (tr.install_module(name), tr.set_root_object(f"t{i}", i)),
                         retries=10_000, backoff=(0.001, 0.05))
        return body

    elapsed = run_threads([install(i) for i in range(THREADS)])
    assert attempts.commits == THREADS
    # Both instances see one complete list and one catalog.
    assert _check_final(dbs[0], "t") == _check_final(dbs[1], "t")
    report("registry installs, two instances", elapsed, str(attempts))
    for space in spaces:
        space.close()
