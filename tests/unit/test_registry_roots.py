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

import importlib
import os
import subprocess
import sys
import threading

import pytest

from proto_db.collections.dictionaries import Dictionary
from proto_db.core.atom import type_tag
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoConflictError, ProtoCorruptionError, ProtoNotSupportedError, ProtoValidationError
from proto_db.errors import ProtoUserError
from proto_db.registry import CATALOG_ROOT, MODULES_ROOT, Catalog, ModuleList, Registry
from proto_db.storage import format as fmt
from proto_db.storage.file import FileStorage
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace

pytestmark = [pytest.mark.timeout(120)]

_PACKAGE = "tests.unit.registry_modules"
_FIXTURES = ("reg7_base", "reg7_contacts", "reg7_crm", "reg7_a", "reg7_b", "reg7_c", "reg7_d",
             "reg7_cyc_x", "reg7_cyc_y", "reg7_y", "reg7_importcommit", "reg7_peek", "reg7_peekcommit",
             "reg7_selfinstall")
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
    # Review N-4: the union is unbuildable only because of the concurrent install: retryable, naming it.
    with pytest.raises(ProtoConflictError, match="concurrently installed 'reg7_cyc_x'.*cycle"):
        second.commit()
    assert _installed(db) == ("reg7_cyc_x",)
    # Retried, the install sees the list as it is now and states the real refusal.
    with pytest.raises(ProtoValidationError, match="cycle"):
        db.run(lambda tr: tr.install_module("reg7_cyc_y"))
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


# -- fix round 1 (review of Task 7) -----------------------------------------------------------------

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _install_in_another_process(path: str, name: str) -> None:
    subprocess.run([sys.executable, "-m", "tests.unit.registry_modules.reg7_installer", path, name],
                   check=True, timeout=60, cwd=REPO)


def _guarded(fn, errors):
    def body():
        try:
            fn()
        except BaseException as e:      # reported by the test
            errors.append(e)
    return body


def _signal_on_first_build(monkeypatch, event):
    """Set ``event`` when ``Registry.build`` is next called: from a commit, that is its lock step
    (review N-5: explicit synchronization instead of a sleep)."""
    original = Registry.build.__func__

    def build(cls, *args, **kwargs):
        event.set()
        return original(cls, *args, **kwargs)

    monkeypatch.setattr(Registry, "build", classmethod(build))


@pytest.fixture
def driver():
    from tests.unit.registry_modules import reg7_driver
    reg7_driver.importing.clear()
    reg7_driver.go.clear()
    yield reg7_driver
    reg7_driver.db = reg7_driver.tr = reg7_driver.committing = reg7_driver.selfinstall = None
    del reg7_driver.peeked[:], reg7_driver.seen[:], reg7_driver.selfinstall_errors[:]


def test_an_import_whose_code_commits_never_deadlocks_a_commit_that_needs_the_module(tmp_path, monkeypatch,
                                                                                     driver):
    """Review I-1: the commit that finds a module of the merged list not yet imported must import it
    outside the root lock.  Thread A imports ``reg7_importcommit``, whose import-time code commits;
    meanwhile another process installs that module, and thread B commits an install it validated
    before.  B's lock step meets the module A is still importing: importing it there waited for A's
    import lock while A waited for the root lock, forever.  A resumes only once B is in its lock
    step (the build made there signals it), so the scenario is exercised on every run; the joins
    only turn a regression into a failure instead of a hang."""
    path = str(tmp_path / "space")
    space = _space(FileStorage(path))
    try:
        db = space.new_database("db")
        late = db.new_transaction()
        late.install_module("reg7_y")                  # validated against an empty list
        _install_in_another_process(path, "reg7_importcommit")
        driver.db = db
        _signal_on_first_build(monkeypatch, driver.go)
        errors = []
        importer = threading.Thread(
            target=_guarded(lambda: importlib.import_module(MAPPING["reg7_importcommit"]), errors), daemon=True)
        importer.start()
        assert driver.importing.wait(10)
        committer = threading.Thread(target=_guarded(late.commit, errors), daemon=True)
        committer.start()
        committer.join(30)
        importer.join(30)
        assert not committer.is_alive() and not importer.is_alive(), "deadlock: commit and import still blocked"
        assert errors == []
        tr = db.new_transaction()
        try:
            assert tr.installed_modules() == ("reg7_importcommit", "reg7_y")
            assert tr.get_root_object("from_import") == 1
        finally:
            tr.abort()
    finally:
        space.close()


def test_the_commit_imports_without_the_transaction_lock(tmp_path, monkeypatch, driver):
    """Review N-2: a module the commit imports (outside the root lock) may, from the thread importing
    it, use the transaction being committed: that read fails fast (the transaction is committing)
    instead of waiting for a lock the commit holds while it waits for the import."""
    path = str(tmp_path / "space")
    space = _space(FileStorage(path))
    try:
        db = space.new_database("db")
        late = db.new_transaction()
        late.install_module("reg7_y")
        _install_in_another_process(path, "reg7_peekcommit")
        driver.committing = late
        _signal_on_first_build(monkeypatch, driver.go)
        errors = []
        importer = threading.Thread(
            target=_guarded(lambda: importlib.import_module(MAPPING["reg7_peekcommit"]), errors), daemon=True)
        importer.start()
        assert driver.importing.wait(10)
        committer = threading.Thread(target=_guarded(late.commit, errors), daemon=True)
        committer.start()
        committer.join(30)
        importer.join(30)
        assert not committer.is_alive() and not importer.is_alive(), "deadlock: import and transaction lock"
        assert errors == []
        assert len(driver.seen) == 1 and isinstance(driver.seen[0], ProtoUserError)
        assert "committing" in str(driver.seen[0])
        assert _installed(db) == ("reg7_peekcommit", "reg7_y")
    finally:
        space.close()


def test_install_module_refuses_a_module_this_thread_is_still_importing(db, driver):
    """Review N-3: its declarations so far are not the module's; installing it would plan the
    catalog from half of them."""
    tr = db.new_transaction()
    driver.selfinstall = tr
    importlib.import_module(MAPPING["reg7_selfinstall"])
    driver.selfinstall = None
    assert len(driver.selfinstall_errors) == 1
    error = driver.selfinstall_errors[0]
    assert isinstance(error, ProtoUserError) and "reg7_selfinstall" in str(error)
    assert tr.installed_modules() == ()
    tr.install_module("reg7_selfinstall")       # imported now: every extension is planned
    assert tr.catalog_object("reg7.SelfFirst") is not None and tr.catalog_object("reg7.SelfSecond") is not None
    tr.abort()


def _importing(monkeypatch, module_name):
    """Mark the imported module ``module_name`` as still being imported (as this thread would see a
    module whose import it is inside)."""
    spec = sys.modules[MAPPING[module_name]].__spec__
    monkeypatch.setattr(spec, "_initializing", True, raising=False)


def test_a_union_unbuildable_because_of_a_concurrent_install_is_a_conflict(db, monkeypatch):
    """Review N-4: the commit's import step finds a concurrently installed module that this thread is
    still importing: retryable ``ProtoConflictError`` naming it, not a user error."""
    late = db.new_transaction()
    late.install_module("reg7_y")
    db.run(lambda tr: tr.install_module("reg7_a"))
    _importing(monkeypatch, "reg7_a")
    with pytest.raises(ProtoConflictError, match="concurrently installed 'reg7_a'"):
        late.commit()


def test_an_own_module_still_being_imported_at_commit_is_a_user_error(db, monkeypatch):
    """Review N-4, the other side: the module still being imported is in this transaction's own list."""
    late = db.new_transaction()
    late.install_module("reg7_y")
    db.run(lambda tr: tr.install_module("reg7_a"))      # the merged list differs: the commit builds it
    _importing(monkeypatch, "reg7_y")
    with pytest.raises(ProtoUserError, match="'reg7_y' of this transaction's list"):
        late.commit()


def test_a_catalog_update_of_a_name_a_concurrent_install_adds_first_conflicts(db):
    """Review N-1 (the reviewer's repro): the update of a name this transaction's own install
    introduces is never silently dropped when a concurrent install adds the name first."""
    t1, t2 = db.new_transaction(), db.new_transaction()
    t1.install_module("reg7_base")
    t1.set_catalog_object("reg7.Partner", DBObject(parents=("reg7.Partner",)).with_(note="t1-config"))
    t2.install_module("reg7_base")
    t2.commit()
    with pytest.raises(ProtoConflictError, match="reg7.Partner"):
        t1.commit()
    # Retried, the install is already there and the update applies.
    def install_and_configure(tr):
        tr.install_module("reg7_base")
        tr.set_catalog_object("reg7.Partner", DBObject(parents=("reg7.Partner",)).with_(note="t1-config"))
    db.run(install_and_configure)
    assert _catalog(db, "reg7.Partner").note == "t1-config"


def test_install_module_imports_outside_the_transaction_lock(db, driver):
    """Review M-5: another thread may use the installing transaction while the module is imported."""
    reg7_driver = driver
    tr = db.new_transaction()
    reg7_driver.tr = tr
    try:
        tr.install_module("reg7_peek")
    finally:
        reg7_driver.tr = None
    assert reg7_driver.peeked == [()]           # read while the import ran: nothing pending yet
    assert tr.installed_modules() == ("reg7_peek",)
    tr.abort()


def test_installs_from_two_file_storage_instances_over_one_directory_both_land(tmp_path):
    """Review M-2: two processes over one directory (the file lock is global): raw commits, no retry."""
    path = str(tmp_path / "space")
    first_space = _space(FileStorage(path))
    first_space.new_database("db")
    second_space = _space(FileStorage(path))
    try:
        first, second = (s.open_database("db").new_transaction() for s in (first_space, second_space))
        first.install_module("reg7_a")
        second.install_module("reg7_b")
        first.commit()
        second.commit()
        assert _installed(first_space.open_database("db")) == ("reg7_a", "reg7_b")
        assert _installed(second_space.open_database("db")) == ("reg7_a", "reg7_b")
    finally:
        first_space.close()
        second_space.close()


def test_installing_transaction_sees_its_pending_list_through_both_reads(db):
    """Review M-6: get_root_object of a registry root and the registry reads agree in an installing
    transaction: both show the list (and catalog) with the pending installs."""
    db.run(lambda tr: tr.install_module("reg7_base"))
    tr = db.new_transaction()
    tr.install_module("reg7_crm")
    assert tr.get_root_object(MODULES_ROOT).names() == tr.installed_modules() == (
        "reg7_base", "reg7_contacts", "reg7_crm")
    assert tr.get_root_object(CATALOG_ROOT).names() == ("reg7.Contact", "reg7.Lead", "reg7.Partner")
    tr.abort()
    tr = db.new_transaction()
    assert tr.get_root_object(MODULES_ROOT).names() == ("reg7_base",)     # nothing pending: the snapshot's
    tr.abort()


class _CatalogSubclass(DBObject):
    __type_name__ = "tests.registry_roots.CatalogSubclass"

    @property
    def parents(self):                          # an overridable public member, never read by the registry
        return ("reg7.Partner",)


def test_a_catalog_entry_must_be_a_plain_dbobject_with_persisted_parents(db):
    """Review M-1: exact type, and the persisted parents rather than the overridable ``parents``."""
    for entry in (_CatalogSubclass(), DBObject(parents=("reg7.Other",))):
        tr = db.new_transaction()
        tr._writes[CATALOG_ROOT] = Catalog(entries=Dictionary().set_at("reg7.Partner", entry), context=tr)
        tr.commit()
        tr = db.new_transaction()
        with pytest.raises(ProtoCorruptionError, match="__catalog"):
            tr.catalog_object("reg7.Partner")
        tr.abort()


# -- catalog updates (review I-2, ruling R189) --------------------------------------------------------

def _catalog(db, name):
    tr = db.new_transaction()
    try:
        return tr.catalog_object(name)
    finally:
        tr.abort()


def test_a_catalog_object_update_lands(db):
    db.run(lambda tr: tr.install_module("reg7_base"))
    original = _catalog(db, "reg7.Partner")
    tr = db.new_transaction()
    tr.set_catalog_object("reg7.Partner", tr.catalog_object("reg7.Partner").with_(note="v2"))
    assert tr.catalog_object("reg7.Partner").note == "v2"      # the transaction sees its own update
    tr.commit()
    updated = _catalog(db, "reg7.Partner")
    # A version is a new atom (D18): the entry, not the oid, carries the continuity (R190).
    assert updated.note == "v2" and updated.oid != original.oid and updated.parents == ("reg7.Partner",)


def test_a_catalog_update_of_an_object_created_in_the_same_transaction(db):
    tr = db.new_transaction()
    tr.install_module("reg7_base")
    tr.set_catalog_object("reg7.Partner", tr.catalog_object("reg7.Partner").with_(note="first"))
    tr.commit()
    assert _catalog(db, "reg7.Partner").note == "first"


def test_concurrent_updates_of_one_catalog_entry_conflict_once(db):
    db.run(lambda tr: tr.install_module("reg7_base"))
    first, second = db.new_transaction(), db.new_transaction()
    for tr, note in ((first, "first"), (second, "second")):
        tr.set_catalog_object("reg7.Partner", tr.catalog_object("reg7.Partner").with_(note=note))
    first.commit()
    with pytest.raises(ProtoConflictError, match="reg7.Partner"):
        second.commit()
    assert _catalog(db, "reg7.Partner").note == "first"
    # Database.run retries from the committed version: the second update then lands.
    db.run(lambda tr: tr.set_catalog_object("reg7.Partner", tr.catalog_object("reg7.Partner").with_(note="second")))
    assert _catalog(db, "reg7.Partner").note == "second"


def test_a_catalog_update_and_a_concurrent_install_of_another_name_both_land(db):
    db.run(lambda tr: tr.install_module("reg7_base"))
    updater, installer = db.new_transaction(), db.new_transaction()
    updater.set_catalog_object("reg7.Partner", updater.catalog_object("reg7.Partner").with_(note="v2"))
    installer.install_module("reg7_a")
    installer.commit()
    updater.commit()
    assert _catalog(db, "reg7.Partner").note == "v2"
    assert _catalog(db, "reg7.Only_a") is not None
    # And the other order: an install that commits after an update keeps the update.
    updater, installer = db.new_transaction(), db.new_transaction()
    updater.set_catalog_object("reg7.Partner", updater.catalog_object("reg7.Partner").with_(note="v3"))
    installer.install_module("reg7_b")
    updater.commit()
    installer.commit()
    assert _catalog(db, "reg7.Partner").note == "v3" and _catalog(db, "reg7.Only_b") is not None


def test_a_catalog_update_must_be_a_version_of_the_entry(db):
    db.run(lambda tr: tr.install_module("reg7_base"))
    tr = db.new_transaction()
    current = tr.catalog_object("reg7.Partner")
    refused = [
        ("reg7.Partner", current.with_parents("reg7.Other")),             # its name dropped from its parents
        ("reg7.Partner", current.with_(note=1).with_parents()),           # no parents at all
        ("reg7.Unknown", current),                                        # no entry to update
        ("reg7.Partner", "not an object"),
    ]
    for name, obj in refused:
        with pytest.raises(ProtoValidationError):
            tr.set_catalog_object(name, obj)
    with pytest.raises(ProtoValidationError):
        tr.set_catalog_object("reg7.Partner", _CatalogSubclass())
    tr.commit()                                 # nothing was written
    assert len(db.history()) == 2
