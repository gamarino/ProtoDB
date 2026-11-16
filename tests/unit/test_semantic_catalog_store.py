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
import sys
from pathlib import Path

import pytest

from proto_db.catalog.atoms import SEMANTIC_CATALOG_ROOT, SemanticCatalog, semantic_catalog_root
from proto_db.catalog.model import canonical_form, from_atoms, to_atoms
from proto_db.errors import ProtoCorruptionError, ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from proto_db.registry import Registry
from proto_db.semantic import SemanticSession, semantic
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
            assert SEMANTIC_CATALOG_ROOT not in tr._reads
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
        assert SEMANTIC_CATALOG_ROOT not in reader._reads
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
        space._registries.rebind("db", ("base", "acme_sales"), version=1)
        assert "delivery_date" in _attributes(db.run(lambda tr: Order.describe(tr)))
    finally:
        space.close()
