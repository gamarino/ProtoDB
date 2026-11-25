"""The semantic catalog in the database: written at install, verified on use, the session and as-of
reads (spec sections 5 and 8; Task 5; Review Focus 1 and 2).

Catches: an install that writes no catalog, or one that is not the composition of the installed code; a
stored composition that depends on the installation order; a reinstall that writes a new version; a
database that installs no semantic module and still writes format 6; a database written before the
semantic layer (modules listed, no catalog) that breaks plain resolution, that the session reads as if it
had a catalog, or that the next install does not repair; code that differs from the stored catalog read
silently; a stored catalog read back unchecked (expressions, composition, module set); ``describe`` that
reads the current composition instead of the snapshot's, or another database's; an install refused for a
forbidden change that leaves something pending; reading the catalog that enters the read set.
"""
from __future__ import annotations

import base64
import os
import subprocess
import threading
import sys
from pathlib import Path

import pytest

from proto_db.catalog.atoms import SEMANTIC_CATALOG_ROOT, SemanticCatalog, semantic_catalog_root
from proto_db.catalog.model import canonical_form, from_atoms, to_atoms
from proto_db.errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from proto_db.registry import Registry
from proto_db.semantic import SemanticSession, semantic
from proto_db.preconditions import Unit, UnitKind
from proto_db.semantic import store
from proto_db.semantic.compose import compose
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace
from tests.unit.semantic_modules import MAPPING, RC4_MAPPING

from docs.kit.semantic.acme_sales import Order

pytestmark = [pytest.mark.timeout(120)]

ROOT = Path(__file__).resolve().parents[2]
PLAIN = {**MAPPING, "reg8_base": "tests.unit.registry_modules.reg8_base"}


def _space(path=None, mapping=MAPPING):
    storage = FileStorage(str(path)) if path is not None else MemoryStorage()
    return ObjectSpace(storage, modules_mapping=mapping)


def _install(db, *names):
    def body(tr):
        for name in names:
            tr.install_module(name)
    db.run(body)


def _stored(db) -> dict | None:
    tr = db.new_transaction()
    try:
        found = semantic_catalog_root(tr._objects)
        return None if found is None else from_atoms(found)
    finally:
        tr.abort()


def _code(*names) -> dict:
    return compose(Registry.build(("base",) + names, mapping=MAPPING))


def _attributes(described: dict) -> list:
    return [a["name"] for a in described.get("attributes", [])]


def _segment_versions(path: Path) -> list:
    return sorted(int.from_bytes(f.read_bytes()[8:10], "little") for f in path.glob("*.seg"))


# -- written at install -------------------------------------------------------------------------------------

def test_install_writes_the_semantic_catalog(tmp_path):
    space = _space(tmp_path / "s")
    try:
        db = space.new_database("db")
        db.run(lambda tr: tr.set_root_object("plain", 1))
        assert _stored(db) is None
        _install(db, "acme_sales")
        assert space.storage.read_root().version == 6
        tr = db.new_transaction()
        try:
            assert isinstance(tr._objects.get_at(SEMANTIC_CATALOG_ROOT), SemanticCatalog)
            stored = from_atoms(semantic_catalog_root(tr._objects))
            assert stored["module_set"] == ["acme_sales", "base"]           # the full module list (F24)
            assert stored["modules"]["base"] == {"name": "base", "version": "1"}
            assert canonical_form(stored) == canonical_form(_code("acme_sales"))
            session = semantic(tr)
            assert isinstance(session, SemanticSession)
            assert canonical_form(session.catalog()) == canonical_form(stored)
            assert "delivery_date" not in _attributes(session.describe("acme.sales.Order"))
        finally:
            tr.abort()
    finally:
        space.close()


def test_install_order_does_not_change_the_stored_composition():
    space = _space()
    try:
        first, second = space.new_database("first"), space.new_database("second")
        _install(first, "acme_sales")
        _install(first, "acme_delivery")
        _install(first, "semc_05")
        _install(second, "semc_05", "acme_delivery")         # acme_sales comes with semc_05
        assert first.run(lambda tr: tr.installed_modules()) != second.run(lambda tr: tr.installed_modules())
        expected = canonical_form(_code("acme_sales", "acme_delivery", "semc_05"))
        assert canonical_form(_stored(first)) == canonical_form(_stored(second)) == expected
    finally:
        space.close()


def test_reinstall_adds_nothing():
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_delivery")
        versions = len(db.history())
        pointer = db.run(lambda tr: tr._objects.get_at(SEMANTIC_CATALOG_ROOT).atom_pointer)
        tr = db.new_transaction()
        tr.install_module("acme_delivery")
        tr.install_module("acme_sales")
        assert not tr._registry_pending()
        tr.commit()
        assert len(db.history()) == versions
        assert db.run(lambda tr: tr._objects.get_at(SEMANTIC_CATALOG_ROOT).atom_pointer) == pointer
    finally:
        space.close()


def test_a_database_without_semantic_modules_writes_no_v6_segment(tmp_path):
    space = _space(tmp_path / "s", mapping=PLAIN)
    try:
        db = space.new_database("db")
        _install(db, "reg8_base")
        db.run(lambda tr: tr.set_root_object("thing", tr.new_object(parents=("reg8.Thing",))))
        _install(db, "reg8_base")
        assert _stored(db) is None
        assert space.storage.read_root().version == 5
        with pytest.raises(ProtoNotSupportedError, match="no semantic catalog; install a module to add it"):
            db.run(lambda tr: semantic(tr).catalog())
    finally:
        space.close()
    assert max(_segment_versions(tmp_path / "s")) == 5


# -- a database written before the semantic layer (Review Focus 1) ------------------------------------------

_RC4_WRITER = """
import sys
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace
from tests.unit.semantic_modules import RC4_MAPPING
space = ObjectSpace(FileStorage(sys.argv[1]), modules_mapping=RC4_MAPPING)
db = space.new_database("db")
def write(tr):
    tr.install_module("acme_sales")
    tr.set_root_object("order", tr.new_object(number=7, parents=("acme.sales.Order",)))
db.run(write)
assert db.run(lambda tr: tr.get_root_object("order").describe_number()) == "order 7"
space.close()
"""


def _gil_flag() -> list:
    if hasattr(sys, "_is_gil_enabled"):
        return ["-X", f"gil={int(sys._is_gil_enabled())}"]
    return []


def test_rc4_database_without_semantic_catalog(tmp_path):
    # Another process writes the database with code that declares no semantic element (the rc4 shape).
    subprocess.run([sys.executable, *_gil_flag(), "-c", _RC4_WRITER, str(tmp_path / "s")], cwd=ROOT, check=True,
                   timeout=60, env={**os.environ, "PYTHONPATH": str(ROOT)})
    assert RC4_MAPPING["acme_sales"] != MAPPING["acme_sales"]
    assert _segment_versions(tmp_path / "s") and max(_segment_versions(tmp_path / "s")) == 5
    space = _space(tmp_path / "s")
    try:
        db = space.open_database("db")
        assert _stored(db) is None
        tr = db.new_transaction()
        try:
            order = tr.get_root_object("order")              # plain resolution, now with the semantic code
            assert order.number == 7 and order.state == "open"
            with pytest.raises(ProtoNotSupportedError, match="no semantic catalog; install a module to add it"):
                semantic(tr).catalog()
            with pytest.raises(ProtoNotSupportedError, match="no semantic catalog"):
                Order.describe(tr)
            assert Unit(UnitKind.ROOT, SEMANTIC_CATALOG_ROOT) not in tr._read_log
        finally:
            tr.abort()
        _install(db, "acme_delivery")
        stored = _stored(db)
        assert stored["module_set"] == ["acme_delivery", "acme_sales", "base"]
        assert canonical_form(stored) == canonical_form(_code("acme_sales", "acme_delivery"))
        assert "delivery_date" in _attributes(db.run(lambda tr: Order.describe(tr)))
        assert db.run(lambda tr: tr.get_root_object("order").number) == 7
    finally:
        space.close()


# -- verified on use ----------------------------------------------------------------------------------------

def _relabelled(registry, module_name):
    """The contributions of ``module_name`` with the label of ``Order.number`` changed: a stored catalog
    written by other code than this process's."""
    item = compose_module(registry, module_name)
    if module_name == "acme_sales":
        for extension in item["extensions"]:
            for element in extension.get("elements", []):
                if extension["extends"] == "acme.sales.Order" and element["name"] == "number":
                    element["label"] = {"und": "No."}
    return item


compose_module = store.module_contributions


def test_code_that_differs_from_the_stored_catalog_raises(monkeypatch):
    space = _space()
    try:
        db = space.new_database("db")
        with monkeypatch.context() as patched:
            patched.setattr(store, "module_contributions", _relabelled)
            _install(db, "acme_sales")
        assert _stored(db)["modules"]["acme_sales"] != _code("acme_sales")["modules"]["acme_sales"]
        with pytest.raises(ProtoValidationError, match=r"acme_sales.*acme\.sales\.Order.*number.*label.*R263"):
            db.run(lambda tr: semantic(tr).catalog())
        with pytest.raises(ProtoValidationError, match="R263"):
            db.run(lambda tr: Order.describe(tr))
    finally:
        space.close()


def _overwrite(db, model):
    """Store ``model`` as the catalog through the transaction's internal write set (the semantic layer's
    own write path; applications cannot name a ``__`` root), as another writer could have."""
    def body(tr):
        tr._writes[SEMANTIC_CATALOG_ROOT] = to_atoms(model, tr)
    db.run(body)


def test_a_catalog_whose_module_set_is_not_the_installed_list_is_not_supported_until_the_next_install():
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_delivery")
        _overwrite(db, _code("acme_sales"))                 # as a writer that installed without the layer
        with pytest.raises(ProtoNotSupportedError, match="acme_delivery"):
            db.run(lambda tr: semantic(tr).catalog())
        _install(db, "acme_delivery")                        # the next install adds what the catalog lacks
        assert canonical_form(_stored(db)) == canonical_form(_code("acme_sales", "acme_delivery"))
        assert "delivery_date" in _attributes(db.run(lambda tr: semantic(tr).describe("acme.sales.Order")))
    finally:
        space.close()


def test_a_stored_catalog_is_checked_when_read():
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        good = _code("acme_sales")
        # Entities that are not the composition of the stored modules: the cache is corrupt.
        bad = _code("acme_sales")
        bad["entities"]["acme.sales.Order"]["meaning"] = "Something else"
        _overwrite(db, bad)
        with pytest.raises(ProtoCorruptionError, match="not the composition"):
            db.run(lambda tr: semantic(tr).catalog())
        # An invalid expression tree in a stored contribution (and in the entity composed from it).
        for tree, error in ((["+", ["attr", "quantity"]], ProtoCorruptionError),
                            (["min", ["attr", "quantity"]], ProtoNotSupportedError)):
            bad = _code("acme_sales")
            for item in (_element(bad["modules"]["acme_sales"], "acme.sales.OrderLine", "amount"),
                         next(a for a in bad["entities"]["acme.sales.OrderLine"]["attributes"]
                              if a["name"] == "amount")):
                item["expression"] = {"ast": tree}
            _overwrite(db, bad)
            with pytest.raises(error):
                db.run(lambda tr: semantic(tr).catalog())
        _overwrite(db, good)
        assert canonical_form(db.run(lambda tr: semantic(tr).catalog())) == canonical_form(good)
    finally:
        space.close()


def _element(module_item: dict, extends: str, name: str) -> dict:
    extension = next(e for e in module_item["extensions"] if e["extends"] == extends)
    return next(e for e in extension["elements"] if e["name"] == name)


def test_an_extending_module_with_a_forbidden_change_fails_the_install():
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        versions = len(db.history())
        tr = db.new_transaction()
        with pytest.raises(ProtoValidationError, match="R282"):
            tr.install_module("r282_value_type")
        assert tr.installed_modules() == ("base", "acme_sales")
        assert not tr._registry_pending()
        tr.commit()
        assert len(db.history()) == versions
        assert canonical_form(_stored(db)) == canonical_form(_code("acme_sales"))
    finally:
        space.close()


# -- the session ----------------------------------------------------------------------------------------------

def test_reading_the_catalog_does_not_enter_the_read_set():
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        reader = db.new_transaction()
        assert semantic(reader).list_entities("order")
        reader.set_root_object("mine", 1)
        _install(db, "acme_delivery")                        # a concurrent install commits first
        assert Unit(UnitKind.ROOT, SEMANTIC_CATALOG_ROOT) not in reader._read_log
        reader.commit()                                      # no conflict
        assert db.run(lambda tr: tr.get_root_object("mine")) == 1
    finally:
        space.close()


def test_a_transaction_sees_its_own_pending_install():
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        tr = db.new_transaction()
        tr.install_module("acme_delivery")
        assert "delivery_date" in _attributes(semantic(tr).describe("acme.sales.Order"))
        assert "delivery_date" in _attributes(Order.describe(tr))
        tr.abort()
        assert "delivery_date" not in _attributes(db.run(lambda tr: Order.describe(tr)))
    finally:
        space.close()


def test_list_entities_gives_type_name_label_and_meaning():
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        session = semantic(db.snapshot_at(db.run(lambda tr: tr.snapshot_ref)))
        listed = session.list_entities()
        assert [e["type_name"] for e in listed] == sorted(e["type_name"] for e in listed)
        order = next(e for e in listed if e["type_name"] == "acme.sales.Order")
        assert order == {"type_name": "acme.sales.Order", "label": {"und": "Order"},
                         "meaning": "A customer's order to the company; open, confirmed or cancelled"}
        line = next(e for e in listed if e["type_name"] == "acme.sales.OrderLine")
        assert line["label"] is None
        # Case-folded substring of the type name, a label, a synonym or the meaning.
        assert [e["type_name"] for e in session.list_entities("ORDER")] == [
            "acme.core.Company", "acme.sales.Order", "acme.sales.OrderLine"]
        assert [e["type_name"] for e in session.list_entities("purchase")] == ["acme.sales.Order"]
        assert [e["type_name"] for e in session.list_entities("credit")] == []
    finally:
        space.close()


def test_semantic_takes_a_transaction_or_a_snapshot():
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        with pytest.raises(ProtoUserError, match="read snapshot"):
            semantic(db)
        snapshot = db.snapshot_at(db.run(lambda tr: tr.snapshot_ref))
        with pytest.raises(ProtoUserError, match="read-only"):
            semantic(snapshot)._transaction()                # what a write operation goes through (F24)
        tr = db.new_transaction()
        assert semantic(tr)._transaction() is tr
        tr.abort()
        with pytest.raises(ProtoUserError):
            semantic(tr).catalog()                           # an ended transaction
    finally:
        space.close()


# -- describe on a snapshot, as of a version ------------------------------------------------------------------

def test_class_describe_reads_the_snapshots_composition():
    space = _space()
    try:
        with_delivery, without = space.new_database("with_delivery"), space.new_database("without")
        _install(with_delivery, "acme_delivery")
        _install(without, "acme_sales")
        for db, expected in ((with_delivery, True), (without, False)):
            tr = db.new_transaction()
            try:
                assert ("delivery_date" in _attributes(Order.describe(tr))) is expected
                snapshot = db.snapshot_at(tr.snapshot_ref)
                assert ("delivery_date" in _attributes(Order.describe(snapshot))) is expected
                assert Order.describe(snapshot) == Order.describe(tr) == semantic(snapshot).describe(
                    "acme.sales.Order")
            finally:
                tr.abort()
    finally:
        space.close()


def test_as_of_describe_uses_that_versions_catalog():
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        before = db.run(lambda tr: semantic(tr).version())
        _install(db, "acme_delivery")
        after = db.run(lambda tr: semantic(tr).version())
        assert after["number"] > before["number"]
        old = db.snapshot_at(base64.b64decode(before["ref"]))       # the version's ref (F23)
        assert old.version == before["number"] and semantic(old).version() == before
        assert "delivery_date" not in _attributes(Order.describe(old))
        assert "delivery_date" not in _attributes(semantic(old).describe("acme.sales.Order"))
        assert canonical_form(semantic(old).catalog()) == canonical_form(_code("acme_sales"))
        new = db.snapshot_at(base64.b64decode(after["ref"]))
        assert "delivery_date" in _attributes(Order.describe(new))
        with old:
            assert "delivery_date" not in _attributes(Order.describe(old))
    finally:
        space.close()


def test_the_binding_ends_on_the_newest_committed_list():
    # Concurrent commits of one space rebind the database after their own publish, in any order: the
    # rebind of an older version, run last, must not bind the database to its older list (found by the
    # concurrent-install test: the session of the next transaction then raised the mismatch).
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")                           # version 1
        _install(db, "acme_delivery")                        # version 2
        space._registries.rebind("db", ("base", "acme_sales"), key=(0, 1))
        assert "delivery_date" in _attributes(db.run(lambda tr: Order.describe(tr)))
    finally:
        space.close()


# -- fix round 1 ----------------------------------------------------------------------------------------------

def test_a_transaction_started_between_publish_and_rebind_resolves():
    # Review I1 (R82): the rebind of an install commit runs after its publish; a transaction of this space
    # that starts in between is on the published list and must resolve with it.  The order is decided by
    # events, never by a sleep: the committing thread is held right before its rebind.
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        registries = space._registries
        original = registries.rebind
        held, release = threading.Event(), threading.Event()

        def gated(*args, **kwargs):
            held.set()
            assert release.wait(60)
            return original(*args, **kwargs)

        registries.rebind = gated
        writer = threading.Thread(target=lambda: _install(db, "acme_delivery"))
        writer.start()
        try:
            assert held.wait(60)                             # published, not yet rebound
            tr = db.new_transaction()
            try:
                assert tr.installed_modules()[-1] == "acme_delivery"
                assert "delivery_date" in _attributes(Order.describe(tr))
            finally:
                tr.abort()
        finally:
            release.set()
            writer.join(60)
        assert not writer.is_alive()
        assert "delivery_date" in _attributes(db.run(lambda tr: Order.describe(tr)))
    finally:
        space.close()


def test_a_reload_that_finds_a_newer_binding_reloads_the_newer_list():
    # Review minor 1, R243: a reload invalidates every registry of the database built before it, also when
    # a newer commit's binding won meanwhile: the reload reads again and binds the newer list, built fresh.
    from proto_db.transactions import _SpaceRegistries
    registries = _SpaceRegistries(MAPPING, None)
    older, newer = ("base", "acme_sales"), ("base", "acme_sales", "acme_delivery")
    registries.rebind("x", newer, None, (0, 7))
    before = registries.registry("x", registries._bound["x"])
    reads = iter([(older, (0, 6), None), (newer, (0, 7), None)])
    registries.reload("x", lambda: next(reads))
    binding = registries._bound["x"]
    assert binding.names == newer and binding.key == (0, 7)
    assert registries.registry("x", binding) is not before


def test_reinstall_on_a_database_without_a_catalog_keeps_rc4_behaviour(tmp_path):
    # Review I2: a database with no semantic catalog reinstalls exactly as before the semantic layer, even
    # when a listed module cannot be imported in this process.
    space = _space(tmp_path / "s", mapping=PLAIN)
    try:
        _install(space.new_database("db"), "reg8_base")
    finally:
        space.close()
    space = _space(tmp_path / "s", mapping=MAPPING)          # this process lacks reg8_base's code
    try:
        db = space.open_database("db")
        versions = len(db.history())
        _install(db, "base")
        assert len(db.history()) == versions and _stored(db) is None
    finally:
        space.close()


def test_a_session_kept_across_an_install_sees_the_new_catalog():
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        tr = db.new_transaction()
        try:
            session = semantic(tr)
            assert "delivery_date" not in _attributes(session.describe("acme.sales.Order"))
            tr.install_module("acme_delivery")
            assert "delivery_date" in _attributes(session.describe("acme.sales.Order"))
        finally:
            tr.abort()
    finally:
        space.close()


def test_a_catalog_holding_modules_the_database_does_not_list_names_them():
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        _overwrite(db, _code("acme_sales", "acme_delivery"))
        with pytest.raises(ProtoNotSupportedError, match=r"does not list \['acme_delivery'\]"):
            db.run(lambda tr: semantic(tr).catalog())
    finally:
        space.close()


# -- fix round 2 ----------------------------------------------------------------------------------------------

def test_overlapping_install_commits_each_keep_their_publish_record():
    # Re-review I1 (R82): T1 has published L1 (version V+1) and not rebound yet; T2, started on V, records
    # L2 (V+2) under the root lock and is held there; T3 starts on V+1, whose list is L1, and must resolve.
    # Events decide the order; the waits only bound a hang.
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        registries = space._registries
        rebind, committing = registries.rebind, registries.committing
        t1_held, t1_release = threading.Event(), threading.Event()
        t2_held, t2_release = threading.Event(), threading.Event()

        def gated_rebind(database, names, *args, **kwargs):
            if "acme_delivery" in names and "semc_01" not in names:
                t1_held.set()
                assert t1_release.wait(60)
            return rebind(database, names, *args, **kwargs)

        def gated_committing(database, names):
            committing(database, names)
            if "semc_01" in names:
                t2_held.set()
                assert t2_release.wait(60)

        registries.rebind, registries.committing = gated_rebind, gated_committing
        t2 = db.new_transaction()                            # started on V
        t2.install_module("semc_01")
        first = threading.Thread(target=lambda: _install(db, "acme_delivery"))
        second = threading.Thread(target=t2.commit)
        first.start()
        try:
            assert t1_held.wait(60)                          # T1 published, not rebound
            second.start()
            assert t2_held.wait(60)                          # T2 recorded, holding the root lock
            tr = db.new_transaction()
            try:
                assert tr.installed_modules()[-1] == "acme_delivery" and "semc_01" not in tr.installed_modules()
                assert "delivery_date" in _attributes(Order.describe(tr))
            finally:
                tr.abort()
        finally:
            t2_release.set()
            t1_release.set()
            first.join(60)
            second.join(60)
        assert not first.is_alive() and not second.is_alive()
        # Nothing stays pending; the first start's list and every published list are adopted.
        assert not registries._pending["db"]
        assert registries._adopted["db"] == {
            (), ("base", "acme_sales"), ("base", "acme_sales", "acme_delivery"),
            ("base", "acme_sales", "acme_delivery", "semc_01")}
        assert "semc_01_note" in _attributes(db.run(lambda tr: semantic(tr).describe("acme.core.Company")))
    finally:
        space.close()


def test_a_registry_build_in_flight_across_a_losing_reload_is_not_stored(monkeypatch):
    # Re-review minor 1, R243, ``probe_reload_race.py``: a registry() build that started before a reload
    # must not store its pre-reload registry in the binding the database has after the reload.  The reload
    # first reads an older list, so it finds the newer binding and reads again.
    from proto_db import registry as registry_module
    from proto_db.transactions import _SpaceRegistries
    registries = _SpaceRegistries(MAPPING, None)
    older, newer = ("base", "acme_sales"), ("base", "acme_sales", "acme_delivery")
    registries.rebind("x", newer, None, (0, 7))
    before = registry_module.registry_for(newer, mapping=MAPPING, allowed=None)     # the process cache
    original = registry_module.registry_for
    built, go = threading.Event(), threading.Event()

    def held(*args, **kwargs):
        found = original(*args, **kwargs)
        built.set()
        assert go.wait(60)
        return found

    monkeypatch.setattr(registry_module, "registry_for", held)
    out = {}
    binding = registries._bound["x"]
    reader = threading.Thread(target=lambda: out.setdefault("registry", registries.registry("x", binding)))
    reader.start()
    try:
        assert built.wait(60)                                # built the pre-reload registry, not stored yet
        monkeypatch.setattr(registry_module, "registry_for", original)
        reads = iter([(older, (0, 6), None), (newer, (0, 7), None)])
        registries.reload("x", lambda: next(reads))
    finally:
        go.set()
        reader.join(60)
    assert not reader.is_alive()
    current = registries._bound["x"]
    assert current is not binding and current.names == newer
    assert current.registry is not before and registries.registry("x", current) is current.registry
    assert out["registry"] is before                         # the build that predates the reload, unstored


def test_a_transaction_that_read_its_snapshot_before_a_rebind_keeps_its_list():
    # Found by the strengthened concurrency test (gil=1): a transaction reads its snapshot, then binds; an
    # install commit of this space may publish and rebind in between, so the transaction's list is older
    # than the bound one.  That list is one this space committed: the transaction gets a binding of its
    # own, and the database's binding stays on the newer list.  A list this space never committed is
    # still the mismatch (spec Q2).
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")                           # version 1
        _install(db, "acme_delivery")                        # version 2: rebound
        registries = space._registries
        older = ("base", "acme_sales")
        own = registries.started("db", older, (0, 1))
        assert own is not None and own.names == older
        assert registries._bound["db"].names == older + ("acme_delivery",)
        assert "delivery_date" not in _attributes(compose(registries.registry("db", own))["entities"][
            "acme.sales.Order"])
        assert registries.started("db", older + ("semc_01",), (0, 2)) is None
    finally:
        space.close()


# -- fix round 3 ----------------------------------------------------------------------------------------------

def _read_then_bind_after_an_install(tmp_path, mode: str) -> dict:
    """Re-review 2 (R82), ``probe_bound_list.py``: transaction A reads its snapshot on the list the space is
    bound to without having committed it (bound at the first start after reopening, or by
    ``reload_registry`` after another space installed), then an install commit of this space publishes and
    rebinds, then A binds.  Events decide the order; the waits only bound a hang."""
    path = tmp_path / "s"
    space = _space(path)
    db = space.new_database("db")
    _install(db, "acme_sales")
    if mode == "reopen":
        space.close()
        space = _space(path)
        db = space.open_database("db")
        db.run(lambda tr: tr.installed_modules())           # binds at the first start
        install = "acme_delivery"
    elif mode == "reload":
        space.reload_registry("db")                          # binds the list it reads
        install = "acme_delivery"
    else:
        db.run(lambda tr: tr.installed_modules())
        other = _space(path)
        try:
            _install(other.open_database("db"), "acme_delivery")
        finally:
            other.close()
        space.reload_registry("db")
        install = "semc_01"
    try:
        registries = space._registries
        started = registries.started
        read, go = threading.Event(), threading.Event()

        def gated(database, names, *args):
            if threading.current_thread().name == "A":
                read.set()
                assert go.wait(60)
            return started(database, names, *args)

        registries.started = gated
        out = {}

        def a():
            tr = db.new_transaction()
            try:
                out["listed"] = tr.installed_modules()
                out["attributes"] = _attributes(Order.describe(tr))
            except Exception as e:      # recorded: the assertion names it
                out["error"] = e
            finally:
                tr.abort()

        thread = threading.Thread(target=a, name="A")
        thread.start()
        try:
            assert read.wait(60)                             # A read its snapshot, has not bound
            _install(db, install)                            # publish and rebind meanwhile
        finally:
            go.set()
            thread.join(60)
        assert not thread.is_alive()
        out["bound"] = registries._bound["db"].names
        return out
    finally:
        space.close()


@pytest.mark.parametrize("mode", ["reopen", "reload", "other"])
def test_a_transaction_on_a_bound_list_that_binds_after_an_install_keeps_its_list(tmp_path, mode):
    out = _read_then_bind_after_an_install(tmp_path, mode)
    assert "error" not in out, out.get("error")
    if mode in ("reopen", "reload"):
        assert out["listed"] == ("base", "acme_sales") and "delivery_date" not in out["attributes"]
        assert out["bound"] == ("base", "acme_sales", "acme_delivery")
    else:
        assert out["listed"] == ("base", "acme_sales", "acme_delivery") and "delivery_date" in out["attributes"]
        assert out["bound"][-1] == "semc_01"


def test_an_install_publish_of_unknown_outcome_that_landed_binds_the_next_transaction(monkeypatch):
    # Re-review 2 (b): the publish landed but reported an unknown outcome, so no rebind ran.  The build's
    # record is kept: the next transaction on the landed list binds to it (the missing rebind).
    import proto_db.transactions as transactions
    from proto_db.errors import ProtoOutcomeUnknownError
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        original = transactions._commit_root

        def landed_but_unknown(*args, **kwargs):
            original(*args, **kwargs)
            raise ProtoOutcomeUnknownError("injected: the publish landed, its outcome is reported unknown")

        monkeypatch.setattr(transactions, "_commit_root", landed_but_unknown)
        tr = db.new_transaction()
        tr.install_module("acme_delivery")
        with pytest.raises(ProtoOutcomeUnknownError):
            tr.commit()
        monkeypatch.setattr(transactions, "_commit_root", original)
        assert space._registries._bound["db"].names == ("base", "acme_sales")          # no rebind ran
        tr = db.new_transaction()
        try:
            assert tr.installed_modules()[-1] == "acme_delivery"
            assert "delivery_date" in _attributes(Order.describe(tr))
        finally:
            tr.abort()
        assert space._registries._bound["db"].names[-1] == "acme_delivery"
    finally:
        space.close()


# -- fix round 4: adopted lists, the binding ordered by (label, version) --------------------------------------

@pytest.mark.parametrize("mode", ["first", "reload"])
def test_a_transaction_on_a_list_read_before_the_space_bound_it_keeps_it(tmp_path, mode):
    # Re-review 3 (R82), ``probe_first_bind_later.py``: A reads its snapshot on the list L at version 1 of a
    # reopened, unbound space; another space commits plain data (version 2, still L); this space binds L at
    # version 2 (a first start, or reload_registry); this space installs (version 3) and rebinds; A binds.  L
    # is a list this space adopted, whatever the version A read it at.  Events decide the order.
    path = tmp_path / "s"
    space = _space(path)
    try:
        _install(space.new_database("db"), "acme_sales")
    finally:
        space.close()
    space = _space(path)
    try:
        db = space.open_database("db")
        registries = space._registries
        started = registries.started
        read, go = threading.Event(), threading.Event()

        def gated(database, names, *args):
            if threading.current_thread().name == "A":
                read.set()
                assert go.wait(60)
            return started(database, names, *args)

        registries.started = gated
        out = {}

        def a():
            tr = db.new_transaction()
            try:
                out["listed"] = tr.installed_modules()
                out["attributes"] = _attributes(Order.describe(tr))
            except Exception as e:      # recorded: the assertion names it
                out["error"] = e
            finally:
                tr.abort()

        thread = threading.Thread(target=a, name="A")
        thread.start()
        try:
            assert read.wait(60)                             # A read version 1; the space is unbound
            other = _space(path)
            try:
                other.open_database("db").run(lambda tr: tr.set_root_object("x", 1))     # version 2, list L
            finally:
                other.close()
            if mode == "reload":
                space.reload_registry("db")
            else:
                db.run(lambda tr: tr.installed_modules())
            _install(db, "acme_delivery")                    # version 3: published and rebound
        finally:
            go.set()
            thread.join(60)
        assert not thread.is_alive()
        assert "error" not in out, out.get("error")
        assert out["listed"] == ("base", "acme_sales") and "delivery_date" not in out["attributes"]
        assert registries._bound["db"].names == ("base", "acme_sales", "acme_delivery")
    finally:
        space.close()


#: The label of a refounded lineage's roots (``proto_db.cluster.coordinator.REFOUND_TERM_STEP``): above every
#: label of the lineage it replaces.
_LATER_LINEAGE = 1 << 44


def _snapshot_pointer(db):
    tr = db.new_transaction()
    try:
        return tr._snapshot_pointer
    finally:
        tr.abort()


def _move_to_lineage(space, base_pointer, term):
    """Make the storage's current root a space root over the databases of the space root at
    ``base_pointer``, labelled ``term`` (``None``: unlabelled): what a node's snapshots start from after a
    failover beyond the failure model (``refound``, ``force_rejoin``) moved the cluster to a lineage that
    lost the later commits.  A cluster's new lineage carries a label above the lost one's; an unlabelled
    root stands for the persisted root a node reads when its confirmation under a lease failed."""
    from proto_db.transactions import SpaceRoot, _ClusterStamp, _SpaceContext
    storage = space.storage
    context = _SpaceContext(storage)
    base = context.load(base_pointer, SpaceRoot.__type_name__)
    current = storage.read_root()
    stamp = None if term is None else _ClusterStamp(term, current.epoch + 1)
    pointer = context.save(SpaceRoot(databases=base.databases, context=context, cluster=stamp, previous=base))
    storage.flush()
    with storage.root_lock():
        return storage.publish_root(pointer, current.epoch)


def _lost_lineage(space):
    """``db`` at version 3 (bound to base, acme_sales, acme_delivery, semc_01), and the pointer of the space
    root of its version 1 (base, acme_sales)."""
    db = space.new_database("db")
    _install(db, "acme_sales")                               # version 1
    base = _snapshot_pointer(db)
    _install(db, "acme_delivery")                            # version 2
    _install(db, "semc_01")                                  # version 3
    assert space._registries._bound["db"].names[-1] == "semc_01"
    return db, base


def test_a_database_moved_to_a_later_lineage_on_a_list_never_adopted_mismatches_until_reloaded(tmp_path):
    # The later lineage installs a module elsewhere: a list this space never adopted is the mismatch (Q2),
    # also for a loaded object outside any block; reload_registry rebinds although the versions went back.
    path = tmp_path / "s"
    space = _space(path)
    try:
        db, base = _lost_lineage(space)
        _move_to_lineage(space, base, _LATER_LINEAGE)
        other = _space(path)
        try:
            _install(other.open_database("db"), "semc_02")
        finally:
            other.close()
        _move_to_lineage(space, space.storage.read_root().pointer, _LATER_LINEAGE + 1)
        tr = db.new_transaction()
        try:
            assert tr.installed_modules() == ("base", "acme_sales", "semc_02")
            with pytest.raises(ProtoValidationError, match="no automatic rebuild"):
                Order.describe(tr)
        finally:
            tr.abort()
        with pytest.raises(ProtoValidationError, match="no automatic rebuild"):
            space._registries.fallback()
        space.reload_registry("db")
        assert tuple(space._registries.fallback().key) == ("base", "acme_sales", "semc_02")
        assert "delivery_date" not in _attributes(db.run(lambda tr: Order.describe(tr)))
    finally:
        space.close()


def test_reload_rebinds_a_database_whose_current_root_orders_below_its_binding():
    # The node reads a root that orders below the one its binding came from (the persisted root, when the
    # confirmation under a lease failed): reload_registry, the explicit rebuild, rebinds to it.
    space = _space()
    try:
        db, base = _lost_lineage(space)
        _move_to_lineage(space, base, None)
        space.reload_registry("db")
        assert space._registries._bound["db"].names == ("base", "acme_sales")
        assert tuple(space._registries.fallback().key) == ("base", "acme_sales")
        assert "delivery_date" not in _attributes(db.run(lambda tr: Order.describe(tr)))
    finally:
        space.close()


def test_a_reload_that_read_before_an_own_install_published_ends_on_the_newest_list():
    # reload_registry reads the current root (L), and an install commit of this space publishes L+m and
    # rebinds before the reload binds: the binding must end on L+m (never back on L), built fresh (R243).
    from proto_db import registry as registry_module
    space = _space()
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        registries = space._registries
        build_fresh = registries.build_fresh
        held, go = threading.Event(), threading.Event()

        def gated(database, names):
            if threading.current_thread().name == "reload" and not held.is_set():
                held.set()
                assert go.wait(60)
            return build_fresh(database, names)

        registries.build_fresh = gated
        reloader = threading.Thread(target=lambda: space.reload_registry("db"), name="reload")
        reloader.start()
        try:
            assert held.wait(60)                             # read the list L, building its registry
            _install(db, "acme_delivery")                    # published and rebound meanwhile
        finally:
            go.set()
            reloader.join(60)
        assert not reloader.is_alive()
        newest = ("base", "acme_sales", "acme_delivery")
        binding = registries._bound["db"]
        assert binding.names == newest
        found = registries.registry("db", binding)
        assert found is not registry_module.registry_for(newest, mapping=MAPPING, allowed=None)
        assert registries.fallback() is found
    finally:
        space.close()


# -- fix round 5: label 0 without a coordinator; a binding whose root the node no longer follows ------------

def test_a_space_without_a_coordinator_over_a_cluster_labelled_root_follows_its_own_install(tmp_path):
    # Re-review 4, ``probe_label_drop.py``: a cluster's storage opened without a coordinator.  Its current
    # root carries a cluster label; a local commit writes an unlabelled root.  Outside cluster mode the label
    # is 0 for every root, so the space's own install moves the binding forward.
    path = tmp_path / "s"
    space = _space(path)
    try:
        _install(space.new_database("db"), "acme_sales")                           # version 1
        _move_to_lineage(space, space.storage.read_root().pointer, 5)               # the cluster's root
    finally:
        space.close()
    space = _space(path)
    try:
        db = space.open_database("db")
        registries = space._registries
        db.run(lambda tr: tr.installed_modules())                                   # binds the list at version 1
        assert registries._bound["db"].key == (0, 1)
        _install(db, "acme_delivery")                                              # a local commit: version 2
        newest = ("base", "acme_sales", "acme_delivery")
        assert registries._bound["db"].names == newest and registries._bound["db"].key == (0, 2)
        db.run(lambda tr: tr.installed_modules())
        assert tuple(registries.fallback().key) == newest
        space.reload_registry("db")
        assert registries._bound["db"].key == (0, 2)
    finally:
        space.close()


class _NodeRoot:
    """The read side of a cluster coordinator for a space of node ``n1``: the committed root it knows,
    ``(label, RootRecord)``, which the test sets.  ``force_rejoin()`` on a survivor drops the committed root
    it knows to a root that orders below it (coordinator.py, ``force_rejoin``; tests/unit/
    test_cluster_rejoin_records.py, Y3), and the space then starts its snapshots from there; the stand-in
    makes that step a plain assignment, so no timing decides it.  The roots are real space roots of the
    storage (``_cluster_roots``); nothing is committed through it."""

    node_id = "n1"
    needs_confirmation = False
    persister = None

    def __init__(self, root):
        self.root = root

    def attach(self, tier):
        pass

    def detach(self, tier):
        pass

    def known_root(self):
        return True, self.root[1]

    def current_root(self):
        return True, self.root[1]

    def committed_root(self):
        label, record = self.root
        return True, record, label, None, 0.0, 0


def _cluster_roots(path) -> dict:
    """Labelled space roots of ``db`` in the storage at ``path``, by name, as ``(label, RootRecord)``:
    ``r1`` (label 10, version 1: base, acme_sales), ``r1b`` (label 11, the same version), ``lost`` (label 20,
    version 3: with acme_delivery and semc_01), ``later`` (label 30, version 1 again: a later lineage) and
    ``other`` (label 31, version 2 of a branch from version 1 that installed semc_02)."""
    space = _space(path)
    try:
        db = space.new_database("db")
        _install(db, "acme_sales")
        v1 = _snapshot_pointer(db)
        _install(db, "acme_delivery")
        _install(db, "semc_01")
        v3 = _snapshot_pointer(db)
        roots = {"r1": (10, _move_to_lineage(space, v1, 10)), "r1b": (11, _move_to_lineage(space, v1, 11)),
                 "lost": (20, _move_to_lineage(space, v3, 20)), "later": (30, _move_to_lineage(space, v1, 30))}
        _install(db, "semc_02")                              # on version 1 again: a branch
        roots["other"] = (31, _move_to_lineage(space, space.storage.read_root().pointer, 31))
        return roots
    finally:
        space.close()


def _node_space(path, root):
    node = _NodeRoot(root)
    return ObjectSpace(FileStorage(str(path)), modules_mapping=MAPPING, coordinator=node), node


L1 = ("base", "acme_sales")
LOST = ("base", "acme_sales", "acme_delivery", "semc_01")


def _bound_to_the_lost_root(path):
    """A node space bound at r1 (adopting L1), then bound to the lost root's list by reload_registry."""
    roots = _cluster_roots(path)
    space, node = _node_space(path, roots["r1"])
    db = space.open_database("db")
    db.run(lambda tr: tr.installed_modules())                # first start: L1 at (10, 1)
    node.root = roots["lost"]
    space.reload_registry("db")                              # LOST at (20, 3), adopted
    assert space._registries._bound["db"].names == LOST
    return roots, space, node, db


def test_a_database_moved_to_a_later_lineage_binds_its_adopted_list(tmp_path):
    # Re-review 3 (refound): the database is back at version 1 (L1, adopted) on a later lineage.  A transaction
    # on L1 binds the database to it; a loaded object resolved outside any block never uses the lost list.
    roots, space, node, db = _bound_to_the_lost_root(tmp_path / "s")
    try:
        node.root = roots["later"]
        tr = db.new_transaction()
        try:
            assert tr.installed_modules() == L1 and "delivery_date" not in _attributes(Order.describe(tr))
        finally:
            tr.abort()
        assert space._registries._bound["db"].names == L1
        assert tuple(space._registries.fallback().key) == L1
    finally:
        space.close()


def test_a_force_rejoin_survivor_rebinds_to_the_root_it_now_follows(tmp_path):
    # Re-review 4 residual: force_rejoin() on a survivor drops its committed root to r1, which orders below
    # the lost root the binding came from.  A transaction on r1 (L1, adopted, an older key) finds the node's
    # committed root below the binding's root, and its snapshot is that root: the database binds to L1.
    roots, space, node, db = _bound_to_the_lost_root(tmp_path / "s")
    try:
        node.root = roots["r1"]                              # force_rejoin(): the lost root is forgotten
        tr = db.new_transaction()
        try:
            assert tr.installed_modules() == L1 and "delivery_date" not in _attributes(Order.describe(tr))
        finally:
            tr.abort()
        assert space._registries._bound["db"].names == L1
        assert tuple(space._registries.fallback().key) == L1
    finally:
        space.close()


def _read_then_move(space, node, db, read_root, then_root) -> dict:
    """Transaction A reads its snapshot at ``read_root``; the node's committed root becomes ``then_root``;
    then A binds.  Events decide the order."""
    registries = space._registries
    started = registries.started
    read, go = threading.Event(), threading.Event()

    def gated(database, names, *args):
        if threading.current_thread().name == "A":
            read.set()
            assert go.wait(60)
        return started(database, names, *args)

    registries.started = gated
    out = {}

    def a():
        tr = db.new_transaction()
        try:
            out["listed"] = tr.installed_modules()
            out["attributes"] = _attributes(Order.describe(tr))
        except Exception as e:      # recorded: the assertion names it
            out["error"] = e
        finally:
            tr.abort()

    node.root = read_root
    thread = threading.Thread(target=a, name="A")
    thread.start()
    try:
        assert read.wait(60)
        node.root = then_root
    finally:
        go.set()
        thread.join(60)
    registries.started = started
    assert not thread.is_alive()
    return out


def test_a_survivor_transaction_on_another_root_marks_the_database_stale_until_reloaded(tmp_path):
    # As above, but the transaction's snapshot (r1) is not the node's committed root (r1b) when it binds:
    # the binding's root is no longer followed and the transaction cannot rebind it, so a loaded object
    # outside any block raises the mismatch instead of resolving with the lost list; reload_registry clears it.
    roots, space, node, db = _bound_to_the_lost_root(tmp_path / "s")
    try:
        out = _read_then_move(space, node, db, roots["r1"], roots["r1b"])
        assert "error" not in out, out.get("error")
        assert out["listed"] == L1 and "delivery_date" not in out["attributes"]
        assert space._registries._bound["db"].names == LOST
        with pytest.raises(ProtoValidationError, match="no automatic rebuild"):
            space._registries.fallback()
        space.reload_registry("db")
        assert tuple(space._registries.fallback().key) == L1
    finally:
        space.close()


def test_inside_the_failure_model_an_older_snapshot_changes_nothing(tmp_path):
    # The private-binding branch inside the model: A read r1, then the database was rebound to a newer root
    # the node still follows.  A keeps its list; the database's binding and the fallback are unchanged.
    path = tmp_path / "s"
    roots = _cluster_roots(path)
    space, node = _node_space(path, roots["r1"])
    try:
        db = space.open_database("db")
        db.run(lambda tr: tr.installed_modules())            # L1 at (10, 1)
        registries = space._registries
        started = registries.started
        read, go = threading.Event(), threading.Event()

        def gated(database, names, *args):
            if threading.current_thread().name == "A":
                read.set()
                assert go.wait(60)
            return started(database, names, *args)

        registries.started = gated
        out = {}

        def a():
            tr = db.new_transaction()
            try:
                out["attributes"] = _attributes(Order.describe(tr))
            except Exception as e:      # recorded: the assertion names it
                out["error"] = e
            finally:
                tr.abort()

        thread = threading.Thread(target=a, name="A")
        thread.start()
        try:
            assert read.wait(60)                             # A read r1
            node.root = roots["lost"]
            space.reload_registry("db")                      # LOST at (20, 3), the root the node follows
        finally:
            go.set()
            thread.join(60)
        assert not thread.is_alive()
        assert "error" not in out, out.get("error")
        assert "delivery_date" not in out["attributes"]
        assert registries._bound["db"].names == LOST and "db" not in registries._stale
        assert tuple(registries.fallback().key) == LOST
    finally:
        space.close()
