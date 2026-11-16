"""Concurrent installs of semantic modules (Task 5; Review Focus 5).

From one base (a database with ``acme_sales`` installed, so with a semantic catalog), 2 and then 8
threads each install a different semantic module (``semc_01`` .. ``semc_08``, each adding a field to an
entity of ``acme_sales``; two of them extend the same entity), each in its own transaction started before
any of them commits, so the commits land in group-commit rounds with each other (R71).  The commits are
made with ``Transaction.commit`` directly, never retried: an install that conflicted would raise.

Catches: a semantic catalog that is last-writer-wins instead of the composition of the union of the
installed modules; a conflict between installs caused by the catalog root; a module's contributions lost
or composed twice; a deadlock between installs and the root lock.  No sleep decides anything (R82): the
threads are released by a barrier and every commit is joined.
"""
import threading

import pytest

from proto_db.catalog.atoms import semantic_catalog_root
from proto_db.catalog.model import canonical_form, from_atoms
from proto_db.registry import Registry
from proto_db.semantic import semantic
from proto_db.semantic.compose import compose
from proto_db.storage.file import FileStorage
from tests.concurrency.harness import report, run_threads
from tests.unit.semantic_modules import MAPPING
from proto_db.transactions import ObjectSpace

MODULES = tuple(f"semc_0{i}" for i in range(1, 9))
ROUNDS = 3

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(240)]


def _round(directory, modules: tuple) -> None:
    space = ObjectSpace(FileStorage(str(directory)), modules_mapping=MAPPING)
    try:
        db = space.new_database("db")
        db.run(lambda tr: tr.install_module("acme_sales"))
        transactions = [db.new_transaction() for _ in modules]         # one base for all
        errors = []
        lock = threading.Lock()

        def install(i):
            def body():
                tr = transactions[i]
                tr.install_module(modules[i])
                tr.set_root_object(f"thread{i}", i)
                try:
                    tr.commit()
                except Exception as e:      # recorded: the assertion below names it
                    with lock:
                        errors.append(e)
                    raise
            return body

        elapsed = run_threads([install(i) for i in range(len(modules))])
        assert errors == []
        tr = db.new_transaction()
        try:
            installed = tr.installed_modules()
            assert sorted(installed) == sorted(("base", "acme_sales") + modules)
            assert all(tr.get_root_object(f"thread{i}") == i for i in range(len(modules)))
            stored = from_atoms(semantic_catalog_root(tr._objects))
            union = compose(Registry.build(installed, mapping=MAPPING))
            assert canonical_form(stored) == canonical_form(union)
            assert canonical_form(semantic(tr).catalog()) == canonical_form(union)
            for module in modules:
                extends = union["modules"][module]["extensions"][0]["extends"]
                entity = semantic(tr).describe(extends)
                field = next(a for a in entity["attributes"] if a["name"] == f"{module}_note")
                assert field["provenance"] == [module]
        finally:
            tr.abort()
        assert len(db.history()) == len(modules) + 2       # every commit published one version
        report("semantic installs", elapsed, f"{len(modules)} threads", f"order {installed}", "0 conflicts")
    finally:
        space.close()


@pytest.mark.parametrize("count", [2, 8])
def test_concurrent_semantic_installs_compose_the_union(tmp_path, count):
    for round_number in range(ROUNDS):
        _round(tmp_path / f"space{round_number}", MODULES[:count])
