"""Registry roots, ``install_module`` and the add-if-absent commit (runtime registry, Task 7).

Catches: an install that misses a dependency, adds one twice or in the wrong order; an install
that writes when nothing is missing; concurrent installs of different modules where one
conflicts or overwrites the other (last writer wins); a read of the registry roots that enters
the read set, so an install makes ordinary work conflict; a ``__`` root of the wrong type read
as registry state; an application write, or removal, of a ``__`` root; a catalog object that is
missing, recreated or replaced; a catalog object handed out by a transaction whose commit then
silently keeps another one; a union of two concurrent installs that cannot be built committed
anyway; a module outside the runtime's allowed set, or one that cannot be imported, that
leaves part of its install pending; registry records that do not declare their format feature;
and a cluster commit that does not merge against the committed root.
"""
from __future__ import annotations

import pytest

from proto_db.core.atom import type_tag
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoConflictError, ProtoCorruptionError, ProtoNotSupportedError, ProtoValidationError
from proto_db.registry import CATALOG_ROOT, MODULES_ROOT, Catalog, ModuleList
from proto_db.storage import format as fmt
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace

pytestmark = [pytest.mark.timeout(120)]

_PACKAGE = "tests.unit.registry_modules"
_FIXTURES = ("reg7_base", "reg7_contacts", "reg7_crm", "reg7_a", "reg7_b", "reg7_c", "reg7_d",
             "reg7_cyc_x", "reg7_cyc_y")
MAPPING = {name: f"{_PACKAGE}.{name}" for name in _FIXTURES}


def _space(storage=None, **kw):
    return ObjectSpace(storage if storage is not None else MemoryStorage(), modules_mapping=MAPPING, **kw)


@pytest.fixture
def db():
    space = _space()
    yield space.new_database("db")
    space.close()


def _installed(db) -> tuple:
    tr = db.new_transaction()
    try:
        return tr.installed_modules()
    finally:
        tr.abort()


def _module_list(db) -> ModuleList:
    tr = db.new_transaction()
    try:
        return tr._objects.get_at(MODULES_ROOT)
    finally:
        tr.abort()


def _versions(db) -> int:
    return len(db.history())


# -- install --------------------------------------------------------------------------------------

def test_install_adds_module_and_deps(db):
    tr = db.new_transaction()
    assert tr.installed_modules() == ()
    tr.install_module("reg7_crm")
    # Dependencies first, in dependency order; visible to the installing transaction at once.
    assert tr.installed_modules() == ("reg7_base", "reg7_contacts", "reg7_crm")
    tr.commit()
    assert _installed(db) == ("reg7_base", "reg7_contacts", "reg7_crm")
    # The persisted list records each module's declared dependencies (spec section 3).
    assert _module_list(db).entries() == (("reg7_base", ()), ("reg7_contacts", ("reg7_base",)),
                                         ("reg7_crm", ("reg7_contacts",)))


def test_install_is_idempotent(db):
    tr = db.new_transaction()
    tr.install_module("reg7_contacts")
    tr.install_module("reg7_contacts")
    tr.install_module("reg7_base")
    assert tr.installed_modules() == ("reg7_base", "reg7_contacts")
    tr.commit()
    before = _versions(db)
    tr = db.new_transaction()
    tr.install_module("reg7_contacts")
    tr.install_module("reg7_base")
    tr.commit()
    assert _versions(db) == before          # nothing was missing: nothing was written
    tr = db.new_transaction()
    tr.install_module("reg7_crm")           # only the missing module is added, after the others
    tr.commit()
    assert _installed(db) == ("reg7_base", "reg7_contacts", "reg7_crm")


def test_concurrent_installs_both_land(db):
    first, second = db.new_transaction(), db.new_transaction()
    first.install_module("reg7_a")
    second.install_module("reg7_b")
    first.commit()
    second.commit()                         # no conflict: merged against the current root
    assert _installed(db) == ("reg7_a", "reg7_b")       # commit order
    tr = db.new_transaction()
    try:
        for name in ("reg7.Only_a", "reg7.Only_b", "reg7.Shared"):
            assert isinstance(tr.catalog_object(name), DBObject)
    finally:
        tr.abort()


def test_concurrent_installs_of_one_module_land_once(db):
    first, second = db.new_transaction(), db.new_transaction()
    first.install_module("reg7_crm")
    second.install_module("reg7_contacts")
    first.commit()
    second.commit()
    assert _installed(db) == ("reg7_base", "reg7_contacts", "reg7_crm")


def test_reading_modules_does_not_conflict_with_ordinary_writes(db):
    seed = db.new_transaction()
    seed.install_module("reg7_base")
    seed.commit()
    reader = db.new_transaction()
    assert reader.installed_modules() == ("reg7_base",)
    assert reader.catalog_object("reg7.Partner") is not None
    assert reader.catalog_object("reg7.Contact") is None
    reader.set_root_object("x", 1)
    installer = db.new_transaction()
    installer.install_module("reg7_crm")
    installer.commit()
    reader.commit()                         # the registry roots were not in its read set
    tr = db.new_transaction()
    try:
        assert tr.get_root_object("x") == 1
        assert tr.installed_modules() == ("reg7_base", "reg7_contacts", "reg7_crm")
    finally:
        tr.abort()
    # The other way round: an installer whose base moved because of ordinary work commits too.
    installer = db.new_transaction()
    installer.install_module("reg7_a")
    writer = db.new_transaction()
    writer.set_root_object("x", writer.get_root_object("x") + 1)
    writer.commit()
    installer.commit()
    assert _installed(db)[-1] == "reg7_a"


def test_installing_transaction_keeps_its_own_ordinary_conflicts(db):
    db.run(lambda tr: tr.set_root_object("x", 1))
    tr = db.new_transaction()
    tr.install_module("reg7_base")
    tr.set_root_object("x", tr.get_root_object("x") + 1)
    db.run(lambda other: other.set_root_object("x", 10))
    with pytest.raises(ProtoConflictError):
        tr.commit()
    assert _installed(db) == ()             # nothing of the failed commit was published


def test_install_is_discarded_with_an_aborted_transaction(db):
    tr = db.new_transaction()
    tr.install_module("reg7_crm")
    tr.abort()
    assert _installed(db) == ()


# -- refusals -------------------------------------------------------------------------------------

def test_wrong_typed_registry_root_is_corrupt(db):
    tr = db.new_transaction()
    tr._writes[MODULES_ROOT] = 5            # data written before the names were reserved
    tr._writes[CATALOG_ROOT] = DBObject(kind="not a catalog")
    tr.commit()
    tr = db.new_transaction()
    with pytest.raises(ProtoCorruptionError, match="__currentModules"):
        tr.installed_modules()
    with pytest.raises(ProtoCorruptionError, match="__currentModules"):
        tr.install_module("reg7_base")
    with pytest.raises(ProtoCorruptionError, match="__catalog"):
        tr.catalog_object("reg7.Partner")
    tr.abort()


def test_wrong_typed_registry_root_written_concurrently_fails_the_install_commit(db):
    installer = db.new_transaction()
    installer.install_module("reg7_base")
    corrupter = db.new_transaction()
    corrupter._writes[MODULES_ROOT] = "a list?"
    corrupter.commit()
    with pytest.raises(ProtoCorruptionError, match="__currentModules"):
        installer.commit()


def test_malformed_module_list_is_corrupt(db):
    tr = db.new_transaction()
    tr._writes[MODULES_ROOT] = ModuleList(modules=(("reg7_base", ()),), context=tr)
    tr.commit()
    malformed = [(("reg7_base",),), (("reg7_base", ()), ("reg7_base", ())), (("", ()),),
                 (("reg7_base", ("",)),), (("reg7_base", "reg7_x"),), ((1, ()),)]
    for modules in malformed:
        listed = ModuleList.__new__(ModuleList)
        object.__setattr__(listed, "modules", modules)
        with pytest.raises(ProtoCorruptionError):
            listed.entries()


def test_applications_still_cannot_write_dunder_roots(db):
    tr = db.new_transaction()
    for name in (MODULES_ROOT, CATALOG_ROOT, "__currentSymbols", "__anything"):
        with pytest.raises(ProtoValidationError, match="reserved"):
            tr.set_root_object(name, 1)
        with pytest.raises(ProtoValidationError, match="reserved"):
            tr.set_root_object(name, 1, replace=True)
        with pytest.raises(ProtoValidationError, match="reserved"):
            tr.remove_root_object(name)
    tr.abort()


def test_module_outside_the_allowed_set_is_refused_and_leaves_nothing_pending():
    space = _space(modules_allowed=("reg7_contacts", "reg7_crm"))
    db = space.new_database("db")
    tr = db.new_transaction()
    with pytest.raises(ProtoValidationError, match="reg7_base"):
        tr.install_module("reg7_crm")       # its dependency is not allowed
    assert tr.installed_modules() == ()
    assert tr.catalog_object("reg7.Lead") is None
    tr.commit()
    assert _versions(db) == 1
    space.close()


def test_allowed_and_mapping_are_validated():
    with pytest.raises(ProtoValidationError):
        ObjectSpace(MemoryStorage(), modules_allowed="reg7_base")
    with pytest.raises(ProtoValidationError):
        ObjectSpace(MemoryStorage(), modules_allowed=("",))
    with pytest.raises(ProtoValidationError):
        ObjectSpace(MemoryStorage(), modules_mapping={"reg7_base": 3})
    with pytest.raises(ProtoValidationError):
        ObjectSpace(MemoryStorage(), modules_mapping=[("reg7_base", "x")])


def test_unimportable_module_is_refused_and_leaves_nothing_pending(db):
    tr = db.new_transaction()
    with pytest.raises(ProtoNotSupportedError, match="reg7_missing"):
        tr.install_module("reg7_missing")
    with pytest.raises(ProtoValidationError):
        tr.install_module("")
    assert tr.installed_modules() == ()
    tr.abort()


def test_union_that_cannot_be_built_is_refused_at_commit(db):
    # Each module alone is a valid registry; together their registry-name parents form a cycle.
    first, second = db.new_transaction(), db.new_transaction()
    first.install_module("reg7_cyc_x")
    second.install_module("reg7_cyc_y")
    first.commit()
    with pytest.raises(ProtoValidationError, match="cycle"):
        second.commit()
    assert _installed(db) == ("reg7_cyc_x",)
    tr = db.new_transaction()
    with pytest.raises(ProtoValidationError, match="cycle"):
        tr.install_module("reg7_cyc_y")     # refused at install as well, once it can be seen
    tr.abort()


# -- the catalog ----------------------------------------------------------------------------------

def test_catalog_object_created_for_new_registry_names(db):
    tr = db.new_transaction()
    tr.install_module("reg7_crm")
    pending = tr.catalog_object("reg7.Lead")
    assert isinstance(pending, DBObject) and pending.parents == ("reg7.Lead",)
    tr.commit()
    tr = db.new_transaction()
    try:
        catalog = {name: tr.catalog_object(name) for name in ("reg7.Partner", "reg7.Contact", "reg7.Lead")}
        for name, obj in catalog.items():
            assert type(obj) is DBObject and obj.parents == (name,)
        assert catalog["reg7.Lead"].oid == pending.oid
        assert tr.catalog_object("reg7.Unknown") is None
        with pytest.raises(ProtoValidationError):
            tr.catalog_object("")
    finally:
        tr.abort()
    # A later install that extends a name already listed keeps its catalog object.
    tr = db.new_transaction()
    tr.install_module("reg7_a")
    tr.commit()
    tr = db.new_transaction()
    try:
        assert tr.catalog_object("reg7.Lead").oid == pending.oid
        assert tr.catalog_object("reg7.Partner").oid == catalog["reg7.Partner"].oid
        assert tr.catalog_object("reg7.Only_a") is not None
        assert tr._objects.get_at(CATALOG_ROOT).names() == (
            "reg7.Contact", "reg7.Lead", "reg7.Only_a", "reg7.Partner", "reg7.Shared")
    finally:
        tr.abort()


def test_catalog_object_comes_before_its_registry_name(db):
    from proto_db.registry import Registry, using
    tr = db.new_transaction()
    tr.install_module("reg7_crm")
    lead_class = tr.catalog_object("reg7.Lead")
    lead = tr.new_object(parents=(lead_class.with_(stage="catalog"),))
    with using(Registry.build(tr.installed_modules(), mapping=MAPPING)):
        assert lead.stage == "catalog" and lead.role == "contact" and lead.kind == "partner"
    tr.abort()


def test_a_handed_out_catalog_object_never_silently_loses(db):
    # Both create a catalog object for reg7.Only_a; the second one handed its own out.
    first, second = db.new_transaction(), db.new_transaction()
    first.install_module("reg7_a")
    second.install_module("reg7_a")
    second.catalog_object("reg7.Only_a")
    first.commit()
    with pytest.raises(ProtoConflictError, match="reg7.Only_a"):
        second.commit()
    # One that did not hand its own out commits, and the first committed catalog object stays.
    first, second = db.new_transaction(), db.new_transaction()
    first.install_module("reg7_b")
    second.install_module("reg7_b")
    first.commit()
    tr = db.new_transaction()
    kept = tr.catalog_object("reg7.Only_b").oid
    tr.abort()
    second.commit()
    tr = db.new_transaction()
    try:
        assert tr.catalog_object("reg7.Only_b").oid == kept
        assert tr.installed_modules() == ("reg7_a", "reg7_b")
    finally:
        tr.abort()


# -- format ---------------------------------------------------------------------------------------

def test_registry_records_use_the_registry_feature():
    for cls in (ModuleList, Catalog):
        tag = type_tag(cls.__type_name__)
        assert type(tag) is int
        assert fmt.payload_features({"t": tag, "oid": b"\0" * 16, "s": {}}) == {"registry"}
        assert fmt.payload_features({"t": cls.__type_name__, "oid": b"\0" * 16, "s": {}}) == {"registry"}
    assert fmt.FEATURE_VERSIONS["registry"] == 5
    assert fmt.payload_features({"t": [1], "oid": b"\0" * 16, "s": {}}) == frozenset()


def test_a_space_with_an_install_writes_format_5_and_reopens(tmp_path):
    space = _space(FileStorage(str(tmp_path / "s")))
    db = space.new_database("db")
    db.run(lambda tr: tr.set_root_object("x", 1))
    assert space.storage.read_root().version == 4
    db.run(lambda tr: tr.install_module("reg7_crm"))
    assert space.storage.read_root().version == 5
    space.close()
    space = _space(FileStorage(str(tmp_path / "s")))
    try:
        db = space.open_database("db")
        assert _installed(db) == ("reg7_base", "reg7_contacts", "reg7_crm")
        tr = db.new_transaction()
        assert tr.catalog_object("reg7.Lead").parents == ("reg7.Lead",)
        tr.abort()
    finally:
        space.close()


# -- the cluster commit path ----------------------------------------------------------------------

def test_concurrent_installs_on_two_cluster_nodes_both_land():
    from tests.support.cluster import Cluster
    from tests.support.outcomes import new_database_once
    fast = dict(lease_duration=0.9, request_timeout=0.1, startup_grace=0.0, backoff=(0.001, 0.02))
    with Cluster(3, **fast) as cl:
        shared = MemoryStorage()
        spaces = [ObjectSpace(shared, coordinator=cl[n], modules_mapping=MAPPING) for n in cl.ids]
        cl.wait_rejoined()
        new_database_once(spaces[0])
        dbs = [space.open_database("db") for space in spaces[:2]]
        first, second = dbs[0].new_transaction(), dbs[1].new_transaction()
        assert first._snapshot_pointer == second._snapshot_pointer      # one base
        first.install_module("reg7_a")
        second.install_module("reg7_b")
        first.commit()
        second.commit()                     # built on the committed root, merged: no conflict
        assert _installed(dbs[1]) == ("reg7_a", "reg7_b")
