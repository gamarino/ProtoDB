"""The registry per database: open, consistency and mismatch (runtime registry, Task 8).

Catches: a transaction that resolves without its database's registry (``registry=None``, ruling
M3 of the Task 4 review), through its own objects or through objects loaded via the space's
shared atom cache; a registry rebuilt automatically when the database's module list changed
(spec Q2: a mismatch is an error, ``reload_registry`` rebuilds explicitly); a registry built per
transaction instead of once per module list per process; an unimportable listed module that
breaks opening the database or reading plain objects (Review Focus 4); an as-of read that
resolves with the current list instead of its own version's; an installing transaction that
does not see its own module; and two databases of one space with different lists whose loaded
objects silently resolve with the wrong one.
"""
from __future__ import annotations

import threading

import pytest

from proto_db import resolution
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from proto_db.registry import Registry, using
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace

pytestmark = [pytest.mark.timeout(120)]

_PACKAGE = "tests.unit.registry_modules"
MAPPING = {name: f"{_PACKAGE}.{name}" for name in ("reg8_base", "reg8_extra", "reg8_other")}
#: A mapping under which ``reg8_base`` cannot be imported (the module path does not exist).
BROKEN = dict(MAPPING, reg8_base=f"{_PACKAGE}.reg8_absent_module")
CACHES = pytest.mark.parametrize("entries", [50_000, 0], ids=["atom-cache", "no-atom-cache"])


def _space(storage=None, mapping=MAPPING, **kw):
    return ObjectSpace(storage if storage is not None else MemoryStorage(), modules_mapping=mapping, **kw)


def _install(db, *names):
    tr = db.new_transaction()
    for name in names:
        tr.install_module(name)
    tr.commit()


def _put(db, **roots):
    """Commit each ``name=factory(tr)`` as a root; factories build the value in the transaction."""
    tr = db.new_transaction()
    for name, factory in roots.items():
        tr.set_root_object(name, factory(tr))
    tr.commit()


def _thing(tr):
    return tr.new_object(label="t", parents=("reg8.Thing",))


# -- the registry comes from the database's list ---------------------------------------------------

@CACHES
def test_registry_built_from_the_database_list(entries):
    space = _space(atom_cache_entries=entries)
    db = space.new_database("db")
    _install(db, "reg8_base")
    _put(db, thing=_thing)
    first, second = db.new_transaction(), db.new_transaction()
    assert isinstance(first.registry, Registry)
    assert first.registry.key == ("reg8_base",)
    assert second.registry is first.registry            # one registry per list per process
    fresh = first.new_object(parents=("reg8.Thing",))
    assert fresh.kind == "thing" and fresh.greet() == "base"
    loaded = first.get_root_object("thing")             # the space's shared atom cache, or first
    assert loaded.greet() == "base" and loaded["kind"] == "thing" and "greet" in loaded
    assert resolution.linearize(loaded) == (loaded, "reg8.Thing")
    first.abort()
    second.abort()
    other = _space(MemoryStorage())
    assert other.new_database("db").new_transaction().registry is None     # no modules: no registry
    space.close()
    other.close()


def test_registry_shared_across_spaces_with_the_same_list():
    left, right = _space(), _space()
    for space in (left, right):
        _install(space.new_database("db"), "reg8_base")
    a = left.open_database("db").new_transaction()
    b = right.open_database("db").new_transaction()
    assert a.registry is b.registry
    a.abort()
    b.abort()
    left.close()
    right.close()


@CACHES
def test_resolution_never_runs_without_the_database_registry(entries, monkeypatch):
    # M3 (Task 4 review): with a registry, resolution never runs with registry=None for an object of
    # the database, own or loaded, outside any scope and without using().
    space = _space(atom_cache_entries=entries)
    db = space.new_database("db")
    _install(db, "reg8_base")
    _put(db, thing=_thing, child=lambda tr: tr.new_object(parents=(tr.new_object(n=1),)))
    seen = []
    check = resolution._check_python_members

    def spy(cls, entry, registry):           # runs on every inherited lookup, cached or not
        seen.append((entry.names, registry))
        return check(cls, entry, registry)

    monkeypatch.setattr(resolution, "_check_python_members", spy)
    tr = db.new_transaction()
    assert resolution.current_registry() is None
    assert tr.get_root_object("thing").greet() == "base"
    assert tr.new_object(parents=("reg8.Thing",)).missing is None
    assert len(seen) == 2 and all(names and registry is tr.registry for names, registry in seen)
    # A names-free linearization does not depend on any registry, and needs none.
    assert tr.get_root_object("child").n == 1
    tr.abort()
    space.close()


def test_the_database_registry_wins_over_using():
    # using() is a fallback for tests: an object of a database with a registry resolves with it.
    space = _space()
    db = space.new_database("db")
    _install(db, "reg8_base")
    tr = db.new_transaction()
    with using(Registry.build(("reg8_other",), mapping=MAPPING)):
        assert tr.new_object(parents=("reg8.Thing",)).greet() == "base"
    tr.abort()
    space.close()


# -- mismatch and reload -------------------------------------------------------------------------

def _two_spaces():
    storage = MemoryStorage()
    first = _space(storage)
    db1 = first.new_database("db")
    _install(db1, "reg8_base")
    _put(db1, thing=_thing)
    second = _space(storage)
    db2 = second.open_database("db")
    tr = db2.new_transaction()
    assert tr.get_root_object("thing").greet() == "base"        # binds reg8_base in `second`
    tr.abort()
    _install(db1, "reg8_extra")                                 # a foreign install
    return first, second, db1, db2


def test_mismatch_raises_after_a_foreign_install():
    first, second, db1, db2 = _two_spaces()
    message = r"registry built for modules \['reg8_base'\] but the database lists \['reg8_base', 'reg8_extra'\]"
    tr = db2.new_transaction()
    with pytest.raises(ProtoValidationError, match=message):
        tr.registry
    with pytest.raises(ProtoValidationError, match=message):
        tr.new_object(parents=("reg8.Thing",)).greet()
    with pytest.raises(ProtoValidationError, match=message):
        tr.get_root_object("thing").greet()                     # loaded: no silent old registry
    with pytest.raises(ProtoValidationError, match=message), tr:
        tr.get_root_object("thing").greet()
    # Reading plain data still works; no automatic rebuild happened.
    again = db2.new_transaction()
    assert again.get_root_object("thing").label == "t"
    with pytest.raises(ProtoValidationError, match=message):
        again.registry
    again.abort()
    # The installing space never mismatches itself.
    mine = db1.new_transaction()
    assert mine.registry.key == ("reg8_base", "reg8_extra")
    assert mine.get_root_object("thing").greet() == "extra+base"
    mine.abort()
    first.close()
    second.close()


def test_reload_registry_clears_the_mismatch():
    first, second, db1, db2 = _two_spaces()
    reloaded = second.reload_registry("db")
    assert reloaded.key == ("reg8_base", "reg8_extra")
    tr = db2.new_transaction()
    assert tr.registry is reloaded
    assert tr.get_root_object("thing").greet() == "extra+base"
    assert tr.new_object(parents=("reg8.Thing",)).greet() == "extra+base"
    tr.abort()
    with pytest.raises(ProtoValidationError, match="does not exist"):
        second.reload_registry("nope")
    first.close()
    second.close()


def test_a_transaction_keeps_the_registry_it_started_with():
    # A transaction that started on the bound registry keeps it after a reload rebuilt another.
    first, second, db1, db2 = _two_spaces()
    bound = second.reload_registry("db")
    old = db2.new_transaction()
    rebuilt = second.reload_registry("db")
    assert rebuilt is not bound and rebuilt.key == bound.key        # an explicit, fresh build
    assert old.registry is bound
    new = db2.new_transaction()
    assert new.registry is rebuilt
    old.abort()
    new.abort()
    first.close()
    second.close()


# -- Review Focus 4: an unimportable listed module ---------------------------------------------------

@CACHES
def test_unimportable_listed_module_raises_at_first_resolution(entries):
    storage = MemoryStorage()
    good = _space(storage)
    db = good.new_database("db")
    _install(db, "reg8_base")
    _put(db, plain=lambda tr: tr.new_object(x=1),
         child=lambda tr: tr.new_object(parents=(tr.new_object(y=2),)),
         thing=_thing)
    broken = _space(storage, mapping=BROKEN, atom_cache_entries=entries)
    opened = broken.open_database("db")                         # opening never imports
    tr = opened.new_transaction()
    assert tr.get_root_object("plain").x == 1                   # loading a plain object works
    assert tr.get_root_object("child").y == 2                   # so does an object-only chain
    thing = tr.get_root_object("thing")
    assert thing.label == "t"                                   # own attributes need no registry
    for read in (lambda: thing.greet, lambda: thing["kind"], lambda: "kind" in thing, lambda: tr.registry):
        with pytest.raises(ProtoNotSupportedError, match=r"reg8_base.*reg8_absent_module|reg8_absent_module"):
            read()
    with pytest.raises(ProtoNotSupportedError, match="'db'"):
        tr.registry                                             # names the database too
    tr.abort()
    good.close()
    broken.close()


# -- as-of reads ------------------------------------------------------------------------------------

@CACHES
def test_as_of_read_uses_that_version_list(entries):
    space = _space(atom_cache_entries=entries)
    db = space.new_database("db")
    _install(db, "reg8_base")
    _put(db, thing=_thing)
    tr = db.new_transaction()
    old_ref = tr.snapshot_ref
    tr.abort()
    _install(db, "reg8_extra")
    old = db.snapshot_at(old_ref)
    assert old.registry.key == ("reg8_base",)
    with old:
        assert old.get_root_object("thing").greet() == "base"
    current = db.new_transaction()
    assert current.registry.key == ("reg8_base", "reg8_extra")
    with current:
        assert current.get_root_object("thing").greet() == "extra+base"
    assert db.snapshot_at(db.history()[0]).registry.key == ("reg8_base", "reg8_extra")
    # Ruling (a): an as-of read never changes how other code resolves.  Outside any block, an
    # object (of the old snapshot too) resolves with the database's current binding.
    loose = db.new_transaction()
    assert loose.get_root_object("thing").greet() == "extra+base"
    assert old.get_root_object("thing").greet() == "extra+base"
    loose.abort()
    space.close()


def test_as_of_read_of_the_bound_list_needs_no_scope():
    space = _space()
    db = space.new_database("db")
    _install(db, "reg8_base")
    _put(db, thing=_thing)
    snapshot = db.snapshot_at(db.history()[0])          # the current version: the bound list
    assert snapshot.get_root_object("thing").greet() == "base"
    loose = db.new_transaction()
    assert loose.get_root_object("thing").greet() == "base"
    loose.abort()
    space.close()


@CACHES
def test_history_values_are_as_of_reads(entries):
    space = _space(atom_cache_entries=entries)
    db = space.new_database("db")
    _install(db, "reg8_base")
    tr = db.new_transaction()
    tr.new_mutable("m", _thing(tr))
    tr.commit()
    _install(db, "reg8_extra")
    entry = db.history()[1]                             # the version before the second install
    value = entry.get_mutable("m")
    assert value.greet() == "extra+base"                # outside a block: the current binding
    with db.snapshot_at(entry):
        assert value.greet() == "base"                  # inside: that version's list
    space.close()


# -- review P3/P4: as-of reads never change the binding nor the fallback (ruling a) ----------------

def test_a_snapshot_never_changes_unscoped_resolution():
    space = _space()
    db = space.new_database("db")
    _install(db, "reg8_base")
    _put(db, thing=_thing)
    tr = db.new_transaction()
    ref = tr.snapshot_ref
    tr.abort()
    _install(db, "reg8_extra")

    def unscoped():
        t = db.new_transaction()
        try:
            return t.get_root_object("thing").greet()
        finally:
            t.abort()

    assert unscoped() == "extra+base"
    db.snapshot_at(ref)                                 # only made, nothing read
    assert unscoped() == "extra+base"
    space.reload_registry("db")
    assert unscoped() == "extra+base"
    space.close()


def test_a_pager_on_a_stale_list_keeps_the_mismatch():
    first, second, db1, db2 = _two_spaces()
    tr = db2.new_transaction()                          # stale: the database lists reg8_extra now
    tr.set_root_object("items", tr.new_list().append_last(1))
    tr.commit()
    reader = db2.new_transaction()
    reader.pager("items", page_size=1)                  # builds a Snapshot of the stale root
    with pytest.raises(ProtoValidationError, match="registry built for modules"):
        reader.get_root_object("thing").greet()         # the specific error, not a generic one
    reader.abort()
    first.close()
    second.close()


# -- the installing transaction -----------------------------------------------------------------------

def test_installing_transaction_sees_its_module():
    space = _space()
    db = space.new_database("db")
    _install(db, "reg8_base")
    _put(db, thing=_thing)
    installer, bystander = db.new_transaction(), db.new_transaction()
    installer.install_module("reg8_extra")
    assert installer.registry.key == ("reg8_base", "reg8_extra")
    assert installer.new_object(parents=("reg8.Thing",)).greet() == "extra+base"
    with installer:
        assert installer.get_root_object("thing").greet() == "extra+base"
        assert bystander.new_object(parents=("reg8.Thing",)).greet() == "base"
    assert bystander.registry.key == ("reg8_base",)
    bystander.abort()
    after = db.new_transaction()
    assert after.registry.key == ("reg8_base", "reg8_extra")      # the installer's space rebinds
    assert after.get_root_object("thing").greet() == "extra+base"
    after.abort()
    space.close()


def test_run_scopes_its_transaction():
    space = _space()
    db = space.new_database("db")
    _install(db, "reg8_base")
    _put(db, thing=_thing)
    assert db.run(lambda tr: tr.get_root_object("thing").greet()) == "base"
    assert resolution.current_reader() is None
    space.close()


# -- several databases in one space -----------------------------------------------------------------

def test_two_databases_with_different_lists_resolve_by_their_database():
    space = _space()
    a, b = space.new_database("a"), space.new_database("b")
    _install(a, "reg8_base")
    _install(b, "reg8_other")
    _put(a, thing=_thing)
    _put(b, thing=_thing)
    ta, tb = a.new_transaction(), b.new_transaction()
    assert ta.new_object(parents=("reg8.Thing",)).greet() == "base"
    assert tb.new_object(parents=("reg8.Thing",)).greet() == "other"
    with ta:
        assert ta.get_root_object("thing").greet() == "base"
    with tb:
        assert tb.get_root_object("thing").greet() == "other"
    loose_a, loose_b = a.new_transaction(), b.new_transaction()
    assert loose_a.get_root_object("thing").greet() == "base"       # its database's binding
    assert loose_b.get_root_object("thing").greet() == "other"
    loose_a.abort()
    loose_b.abort()
    space.close()


def test_scope_is_per_thread():
    space = _space()
    db = space.new_database("db")
    _install(db, "reg8_base")
    tr = db.new_transaction()
    seen = []
    with tr:
        assert resolution.current_reader() is tr
        worker = threading.Thread(target=lambda: seen.append(resolution.current_reader()))
        worker.start()
        worker.join()
    assert resolution.current_reader() is None
    assert seen in ([None], [tr])        # a thread inherits the starting context only on 3.14t
    space.close()


def test_plain_objects_in_a_database_without_modules_are_unchanged():
    space = _space()
    db = space.new_database("db")
    _put(db, plain=lambda tr: tr.new_object(x=1))
    tr = db.new_transaction()
    plain = tr.get_root_object("plain")
    assert plain.x == 1 and plain.missing is None and tr.registry is None
    with pytest.raises(ProtoNotSupportedError, match="needs a registry"):
        DBObject(parents=("reg8.Thing",)).greet
    tr.abort()
    space.close()


# -- review P1/P2: a reader block reads only its own space and database (ruling R198) -------------

def _two_databases(other_module):
    space = _space()
    a, b = space.new_database("a"), space.new_database("b")
    _install(a, "reg8_base")
    _install(b, other_module)
    _put(a, thing=_thing)
    return space, a, b


def test_an_object_of_another_database_inside_a_block_raises_when_lists_differ():
    space, a, b = _two_databases("reg8_other")
    ta = a.new_transaction()
    obj = ta.get_root_object("thing")
    tb = b.new_transaction()
    with pytest.raises(ProtoUserError, match="database 'a' resolved inside a block of database 'b'"), tb:
        obj.greet()
    assert obj.greet() == "base"                        # outside the block: its own database
    ta.abort()
    space.close()


def test_an_object_of_another_database_inside_a_block_resolves_when_lists_match():
    space, a, b = _two_databases("reg8_base")
    ta = a.new_transaction()
    obj = ta.get_root_object("thing")
    tb = b.new_transaction()
    with tb:
        assert obj.greet() == "base"
    ta.abort()
    space.close()


def test_an_object_of_another_space_ignores_the_block():
    x, y = _space(), _space()
    dx, dy = x.new_database("d"), y.new_database("d")
    _install(dx, "reg8_base")
    _install(dy, "reg8_other")
    _put(dx, thing=_thing)
    tx = dx.new_transaction()
    obj = tx.get_root_object("thing")
    ty = dy.new_transaction()
    with ty:
        assert obj.greet() == "base"                    # its own space's binding, never the block's
        assert tx.new_object(parents=("reg8.Thing",)).greet() == "base"
    tx.abort()
    x.close()
    y.close()


def _pointer(db, name):
    tr = db.new_transaction()
    try:
        return tr.get_root_object(name).atom_pointer
    finally:
        tr.abort()


def _undetermined(space, ptr):
    """The DBObject at ``ptr`` loaded through ``space``'s own shared atom cache, not below a
    database's tables: what a query result loaded by pointer is (ruling R199).  ``ptr`` must not
    have been read through ``space`` before."""
    obj = space._space_load(ptr, DBObject.__type_name__)
    assert obj.context is space._atoms
    return obj


def test_an_undetermined_database_uses_the_one_bound_list():
    storage = MemoryStorage()
    writer = _space(storage)
    db = writer.new_database("db")
    _install(db, "reg8_base")
    _put(db, thing=_thing)
    reader = _space(storage)
    reader.open_database("db").new_transaction().abort()            # binds db, reads nothing else
    assert _undetermined(reader, _pointer(db, "thing")).greet() == "base"
    writer.close()
    reader.close()


def test_an_undetermined_database_in_a_space_of_several_lists_raises():
    storage = MemoryStorage()
    writer = _space(storage)
    a, b = writer.new_database("a"), writer.new_database("b")
    _install(a, "reg8_base")
    _install(b, "reg8_other")
    _put(a, thing=_thing)
    reader = _space(storage)
    ra, rb = reader.open_database("a"), reader.open_database("b")
    ra.new_transaction().abort()
    rb.new_transaction().abort()
    obj = _undetermined(reader, _pointer(a, "thing"))
    with pytest.raises(ProtoUserError, match="undetermined.*list different modules"):
        obj.greet()
    tb = rb.new_transaction()
    with pytest.raises(ProtoUserError, match="undetermined.*inside a block of database 'b'"), tb:
        obj.greet()
    writer.close()
    reader.close()


# -- review Important 3: the transaction's registry is validated once ------------------------------

def test_a_second_inherited_lookup_does_not_revalidate(monkeypatch):
    space = _space()
    db = space.new_database("db")
    _install(db, "reg8_base")
    _put(db, thing=_thing)
    tr = db.new_transaction()
    fresh = tr.new_object(parents=("reg8.Thing",))
    assert fresh.greet() == "base"
    calls = []
    original = type(tr)._installed_names
    monkeypatch.setattr(type(tr), "_installed_names", lambda self: calls.append(1) or original(self))
    assert fresh.greet() == "base" and fresh.kind == "thing"
    assert tr.get_root_object("thing").greet() == "base"
    assert calls == []
    tr.install_module("reg8_extra")                     # its own install: the registry changes
    calls.clear()
    assert fresh.greet() == "extra+base"
    assert calls
    tr.abort()
    space.close()


# -- review minor 1: reader blocks per context -------------------------------------------------------

def test_one_transaction_entered_by_two_threads():
    space = _space()
    db = space.new_database("db")
    _install(db, "reg8_base")
    _put(db, thing=_thing)
    tr = db.new_transaction()
    inside = threading.Barrier(2)
    errors, seen = [], []

    def body():
        try:
            with tr:
                inside.wait(timeout=10)
                seen.append(resolution.current_reader() is tr)
                seen.append(tr.get_root_object("thing").greet())
                inside.wait(timeout=10)
            seen.append(resolution.current_reader())
        except Exception as e:          # reported below
            errors.append(e)

    workers = [threading.Thread(target=body) for _ in range(2)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    assert errors == []
    assert sorted(map(str, seen)) == sorted(map(str, [True, "base", None] * 2))
    space.close()


def test_one_snapshot_entered_by_two_asyncio_tasks():
    import asyncio
    space = _space()
    db = space.new_database("db")
    _install(db, "reg8_base")
    _put(db, thing=_thing)
    snapshot = db.snapshot_at(db.history()[0])
    order = []

    async def task(name, first_pause, second_pause):
        with snapshot:
            order.append((name, "in", resolution.current_reader() is snapshot))
            await asyncio.sleep(first_pause)
            order.append((name, snapshot.get_root_object("thing").greet()))
            await asyncio.sleep(second_pause)
        order.append((name, "out", resolution.current_reader()))

    async def main():
        await asyncio.gather(task("a", 0.01, 0.03), task("b", 0.02, 0.0))

    asyncio.run(main())
    assert ("a", "out", None) in order and ("b", "out", None) in order
    assert order.index(("b", "out", None)) < order.index(("a", "out", None))   # they interleaved
    assert all(item[2] is True for item in order if item[1] == "in")
    space.close()


def test_reader_blocks_must_nest():
    space = _space()
    db = space.new_database("db")
    one, two = db.snapshot_at(db.history()[0]), db.snapshot_at(db.history()[0])
    one.__enter__()
    two.__enter__()
    with pytest.raises(ProtoUserError, match="innermost"):
        one.__exit__(None, None, None)
    two.__exit__(None, None, None)
    one.__exit__(None, None, None)
    assert resolution.current_reader() is None
    space.close()


# -- review minor 2: a reload touches one database only -------------------------------------------

def test_reload_of_one_database_leaves_the_others():
    from proto_db.registry import registry_for
    space = _space()
    a, b = space.new_database("a"), space.new_database("b")
    for db in (a, b):
        _install(db, "reg8_base")
        _put(db, thing=_thing)
    tb = b.new_transaction()
    shared = tb.registry
    tb.abort()
    reloaded = space.reload_registry("a")
    assert reloaded is not shared
    assert registry_for(("reg8_base",), mapping=MAPPING) is shared          # the process cache is unchanged
    ta, tb = a.new_transaction(), b.new_transaction()
    assert ta.registry is reloaded and tb.registry is shared
    assert ta.get_root_object("thing").greet() == "base" and tb.get_root_object("thing").greet() == "base"
    other = _space(space.storage)
    other.open_database("a").new_transaction().abort()
    other.open_database("b").new_transaction().abort()
    other.reload_registry("a")
    obj = _undetermined(other, _pointer(a, "thing"))
    assert obj.greet() == "base"                        # one list, two builds: no error (R200)
    ta.abort()
    tb.abort()
    space.close()


# -- review minor 4: the commit's lock step never builds a registry ----------------------------------

def test_the_commit_builds_the_registry_before_its_lock_step(monkeypatch):
    from proto_db import registry as registry_module
    from proto_db import transactions
    storage = MemoryStorage()
    seed = _space(storage)
    _install(seed.new_database("db"), "reg8_base")
    _put(seed.open_database("db"), thing=_thing)
    # A second space over the storage, with an empty process cache: nothing is built yet.
    space = _space(storage)
    db = space.open_database("db")
    monkeypatch.setattr(registry_module, "_REGISTRIES", {})
    builds_in_lock = []
    original_build = registry_module.Registry.build.__func__

    def counting_build(cls, *args, **kwargs):
        if getattr(registry_module._NO_BUILDS, "depth", 0):
            builds_in_lock.append(args)
        return original_build(cls, *args, **kwargs)

    monkeypatch.setattr(registry_module.Registry, "build", classmethod(counting_build))
    tr = db.new_transaction()
    loaded = tr.get_root_object("thing")                # never resolved before the commit
    tr.set_root_object("other", 1)
    lock_step = transactions.Transaction._build_root
    seen = []

    def resolving_build_root(self, current, stamp=None):
        seen.append(loaded.greet())                     # what an index extractor does in a merge
        return lock_step(self, current, stamp)

    monkeypatch.setattr(transactions.Transaction, "_build_root", resolving_build_root)
    tr.commit()
    assert seen == ["base"] and builds_in_lock == []
    space.close()
    seed.close()


def test_the_lock_step_refuses_to_build():
    from proto_db.registry import no_builds, registry_for
    with no_builds():
        with pytest.raises(ProtoUserError, match="never builds"):
            registry_for(("reg8_base", "reg8_other"), mapping=MAPPING)


def test_a_commit_that_needs_no_registry_ignores_an_unimportable_module():
    storage = MemoryStorage()
    good = _space(storage)
    _install(good.new_database("db"), "reg8_base")
    broken = _space(storage, mapping=BROKEN)
    db = broken.open_database("db")
    tr = db.new_transaction()
    tr.set_root_object("plain", tr.new_object(x=1))
    tr.commit()                                         # the prebuild failed; nothing needed it
    check = db.new_transaction()
    assert check.get_root_object("plain").x == 1
    check.abort()
    good.close()
    broken.close()
