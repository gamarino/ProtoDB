"""The registry module ``base``: implicit install, implicit last parent, ``read_data_file`` (Task 9).

Catches: a database whose first install does not add ``base`` first, or adds it with
dependencies, or twice; a database that installs nothing paying for ``base``; a registry built
from a non-empty list that does not start with ``base``; a registry name that does not reach
``base``'s definitions last, or a module definition that ``base`` shadows; a ``_super()`` from a
module method that cannot reach ``base``; an object with no registry-name parent that reaches
``base``; ``read_data_file`` reading relative to the wrong module's directory (the override's
instead of the asking method's), returning the wrong type, or reading an absolute path, a path
that climbs out of the module's directory with ``..``, or through a symbolic link that leaves it.
"""
from __future__ import annotations

import os
import stat
import sys
import textwrap
import zipfile

import pytest

from proto_db import registry as registry_module
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError, ProtoUserError, ProtoValidationError
from proto_db.modules import base as base_module
from proto_db.registry import BASE, MODULES_ROOT, ModuleList, Registry, using
from proto_db.resolution import linearize
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace

pytestmark = [pytest.mark.timeout(120)]

_PACKAGE = "tests.unit.registry_modules"
_FIXTURES = ("reg7_base", "reg7_contacts", "reg7_crm")
MAPPING = {name: f"{_PACKAGE}.{name}" for name in _FIXTURES}

_FILES = '''
from proto_db.registry import module

files = module("reg9_files")


def _helper(obj):
    return obj.read_data_file("data/hello.txt")


@files.extends("reg9.Thing", parents=("reg9.Named",))
class Thing:
    def hello(self, path="data/hello.txt", binary=False):
        return self.read_data_file(path, binary=binary)

    def via_helper(self):
        return _helper(self)

    def super_reads(self):
        return self._super().read_data_file("data/hello.txt")


@files.extends("reg9.Named")
class Named:
    label = "named"
'''

_THEME = '''
from proto_db.registry import module

theme = module("reg9_theme", depends=("reg9_files",))


@theme.extends("reg9.Thing")
class Thing:
    def read_data_file(self, path, *, module=None, binary=False):
        text = self._super().read_data_file(path, module=module, binary=binary)
        return "themed:" + text
'''

_BASE_PARENT = '''
from proto_db.registry import module

late = module("reg9_base_parent", depends=("reg9_files",))


@late.extends("base", parents=("reg9.Named",))
class Base:
    extra = 1
'''

_BASE_UNEXTENDED_PARENT = '''
from proto_db.registry import module

late = module("reg9_base_unextended")


@late.extends("base", parents=("reg9.Unextended",))
class Base:
    extra = 1
'''

_ZIPPED = '''
from proto_db.registry import module

zipped = module("reg9_zipped")


@zipped.extends("reg9.Zipped")
class Zipped:
    def hello(self):
        return self.read_data_file("data/hello.txt")
'''

_REG9_MODULES = ("reg9_files", "reg9_theme", "reg9_base_parent", "reg9_base_unextended", "reg9_zipped")


@pytest.fixture(scope="module")
def tree(tmp_path_factory):
    """Two registry modules written to a fresh directory: ``reg9_files`` (in ``pkg/``, with its
    data files) and ``reg9_theme`` (in ``theme/``), plus a file outside both."""
    root = tmp_path_factory.mktemp("reg9")
    pkg, theme, outside = root / "pkg", root / "theme", root / "outside"
    for d in (pkg / "data" / "sub", theme / "data", outside):
        d.mkdir(parents=True)
    (pkg / "reg9_files.py").write_text(textwrap.dedent(_FILES))
    (theme / "reg9_theme.py").write_text(textwrap.dedent(_THEME))
    (theme / "reg9_base_parent.py").write_text(textwrap.dedent(_BASE_PARENT))
    (theme / "reg9_base_unextended.py").write_text(textwrap.dedent(_BASE_UNEXTENDED_PARENT))
    with zipfile.ZipFile(root / "zipped.zip", "w") as archive:
        archive.writestr("reg9_zipped.py", textwrap.dedent(_ZIPPED))
        archive.writestr("data/hello.txt", "zipped")
    (pkg / "data" / "hello.txt").write_text("hello é\n", encoding="utf-8")
    (pkg / "data" / "sub" / "deep.txt").write_text("deep", encoding="utf-8")
    (theme / "data" / "hello.txt").write_text("the theme's own file", encoding="utf-8")
    (outside / "secret.txt").write_text("secret", encoding="utf-8")
    os.symlink(outside / "secret.txt", pkg / "data" / "escape.txt")
    os.symlink(outside, pkg / "data" / "escape_dir")
    os.symlink(pkg / "data" / "hello.txt", pkg / "data" / "inner.txt")
    os.symlink(pkg / "data" / "hello.txt", pkg / "data" / "swap.txt")     # retargeted by the swap test only
    os.mkfifo(pkg / "data" / "fifo")
    paths = [str(pkg), str(theme), str(root / "zipped.zip")]
    sys.path[:0] = paths
    try:
        yield {"pkg": pkg, "theme": theme, "outside": outside}
    finally:
        for p in paths:
            sys.path.remove(p)
        for name in _REG9_MODULES:          # review M9: leave no fixture module imported
            sys.modules.pop(name, None)


@pytest.fixture(scope="module")
def plain(tree):
    return Registry.build((BASE, "reg9_files"))


@pytest.fixture(scope="module")
def themed(tree):
    return Registry.build((BASE, "reg9_files", "reg9_theme"))


def _space():
    return ObjectSpace(MemoryStorage(), modules_mapping=MAPPING)


# -- install ----------------------------------------------------------------------------------

def test_base_is_installed_first_and_has_no_deps():
    space = _space()
    try:
        db = space.new_database("db")
        tr = db.new_transaction()
        tr.install_module("reg7_crm")
        assert tr.installed_modules() == (BASE, "reg7_base", "reg7_contacts", "reg7_crm")
        tr.commit()
        tr = db.new_transaction()
        entries = tr._objects.get_at(MODULES_ROOT).entries()
        assert entries[0] == (BASE, ())
        assert [name for name, _ in entries] == [BASE, "reg7_base", "reg7_contacts", "reg7_crm"]
        assert tr.registry.key[0] == BASE
        tr.install_module("reg7_base")                  # already installed: nothing added
        tr.install_module(BASE)
        assert tr.installed_modules() == (BASE, "reg7_base", "reg7_contacts", "reg7_crm")
        tr.abort()
    finally:
        space.close()


def test_installing_base_alone_and_a_database_without_modules():
    space = _space()
    try:
        db = space.new_database("db")
        db.run(lambda tr: tr.set_root_object("x", 1))
        tr = db.new_transaction()
        assert tr.installed_modules() == () and tr._objects.get_at(MODULES_ROOT) is None   # pays nothing
        assert tr.registry is None
        tr.abort()
        db.run(lambda tr: tr.install_module(BASE))
        tr = db.new_transaction()
        assert tr.installed_modules() == (BASE,)
        tr.abort()
    finally:
        space.close()


def test_two_installs_add_base_once():
    space = _space()
    try:
        db = space.new_database("db")
        first, second = db.new_transaction(), db.new_transaction()
        first.install_module("reg7_base")
        second.install_module("reg7_contacts")
        first.commit()
        second.commit()
        tr = db.new_transaction()
        assert tr.installed_modules() == (BASE, "reg7_base", "reg7_contacts")
        tr.abort()
    finally:
        space.close()


def _write_foreign_list(db, names):
    """Commit ``names`` as the module list, as a writer of another language may (final review I1):
    through the internal write path, which only the registry uses."""
    tr = db.new_transaction()
    tr._writes[MODULES_ROOT] = ModuleList(modules=tuple((name, ()) for name in names), context=tr)
    tr.commit()


@pytest.mark.parametrize("foreign", [("reg7_base",), ("reg7_base", BASE)], ids=["no-base", "base-last"])
def test_a_list_without_base_first_reads_with_base_first_and_is_repaired(foreign):
    """Ruling R235: lenient reading, strict writing.  A persisted list that does not start with
    ``base`` is read as if ``base`` preceded it; the next install writes ``base`` first."""
    storage = MemoryStorage()
    writer = ObjectSpace(storage, modules_mapping=MAPPING)
    _write_foreign_list(writer.new_database("db"), foreign)
    space = ObjectSpace(storage, modules_mapping=MAPPING)       # the reading process
    try:
        db = space.open_database("db")
        tr = db.new_transaction()
        assert tr.installed_modules() == (BASE, "reg7_base")
        assert tr.registry.key == (BASE, "reg7_base")
        partner = tr.new_object(parents=("reg7.Partner",))
        assert partner.kind == "partner" and "read_data_file" in partner     # base is reached last
        tr.install_module("reg7_contacts")
        tr.commit()
        tr = db.new_transaction()
        entries = tr._objects.get_at(MODULES_ROOT).entries()
        assert [name for name, _ in entries] == [BASE, "reg7_base", "reg7_contacts"]     # repaired
        assert entries[0] == (BASE, ())
        tr.abort()
    finally:
        space.close()
        writer.close()


def test_build_requires_base_first(tree):
    assert Registry.build(()).key == ()
    with pytest.raises(ProtoValidationError, match="must start with the module 'base'"):
        Registry.build(("reg9_files",))
    with pytest.raises(ProtoValidationError, match="must start with the module 'base'"):
        Registry.build(("reg9_files", BASE))
    assert Registry.build((BASE,)).key == (BASE,)


def test_base_with_dependencies_is_refused(tree, monkeypatch):
    handle = registry_module._DECLARATIONS[BASE] if BASE in registry_module._DECLARATIONS else None
    if handle is None:
        Registry.build((BASE,))
        handle = registry_module._DECLARATIONS[BASE]
    monkeypatch.setattr(handle, "depends", ("reg9_files",))
    with pytest.raises(ProtoValidationError, match="'base' has no dependencies"):
        Registry.build((BASE, "reg9_files"))


def test_an_explicit_install_of_base_obeys_the_allowed_set():
    """The exemption covers only base added implicitly by an install of another module (review
    M8): an embedder that allows no module forbids the registry."""
    space = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING, modules_allowed=set())
    try:
        db = space.new_database("db")
        tr = db.new_transaction()
        with pytest.raises(ProtoValidationError, match="'base' is not allowed"):
            tr.install_module(BASE)
        assert tr.installed_modules() == ()
        tr.abort()
    finally:
        space.close()
    space = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING, modules_allowed={BASE})
    try:
        db = space.new_database("db")
        db.run(lambda tr: tr.install_module(BASE))
        tr = db.new_transaction()
        assert tr.installed_modules() == (BASE,)
        tr.abort()
    finally:
        space.close()


def test_base_is_exempt_from_the_allowed_set(tree):
    assert Registry.build((BASE, "reg9_files"), allowed={"reg9_files"}).key == (BASE, "reg9_files")
    space = ObjectSpace(MemoryStorage(), modules_mapping=MAPPING, modules_allowed={"reg7_base"})
    try:
        db = space.new_database("db")
        db.run(lambda tr: tr.install_module("reg7_base"))
        tr = db.new_transaction()
        assert tr.installed_modules() == (BASE, "reg7_base")
        tr.abort()
    finally:
        space.close()


# -- base as the implicit last parent (R204) -------------------------------------------------

def test_every_registry_name_ends_with_base(plain):
    assert plain.parents_of("reg9.Thing") == ("reg9.Named", BASE)
    assert plain.parents_of("reg9.Named") == (BASE,)
    assert plain.parents_of("reg9.Nobody") == (BASE,)     # a name no module extends reaches base too
    assert plain.parents_of(BASE) == ()
    assert Registry.build(()).parents_of("reg9.Thing") == ()


def test_a_definition_in_base_is_found_from_any_registry_name(plain):
    with using(plain):
        for parents in (("reg9.Thing",), ("reg9.Named",), ("reg9.Nobody",)):
            o = DBObject(parents=parents)
            assert linearize(o)[-1] == BASE and linearize(o).count(BASE) == 1
            assert "read_data_file" in o and callable(o.read_data_file)
        o = DBObject(parents=(DBObject(parents=("reg9.Named",)), "reg9.Thing"))
        assert linearize(o)[-1] == BASE and linearize(o).count(BASE) == 1


def test_base_named_before_another_registry_name_has_no_c3_order(plain):
    with using(plain):
        assert linearize(DBObject(parents=("reg9.Thing", BASE)))[-1] == BASE     # base last: fine
        with pytest.raises(ProtoValidationError):
            linearize(DBObject(parents=(BASE, "reg9.Thing")))


@pytest.mark.parametrize("name", ["reg9_base_parent", "reg9_base_unextended"])
def test_an_extension_of_base_may_not_declare_parents(tree, name):
    """base is the root of the registry-name graph: any parent of it is a cycle, also through a
    name no module extends, which the build's cycle check cannot see (review I1)."""
    with pytest.raises(ProtoValidationError, match=rf"Module {name!r}.*'base' may not declare parents"):
        Registry.build((BASE, "reg9_files", name))


def test_an_object_without_a_registry_name_does_not_reach_base(plain):
    with using(plain):
        o = DBObject(parents=(DBObject(a=1),))
        assert BASE not in linearize(o)
        assert "read_data_file" not in o and o.read_data_file is None


def test_a_module_definition_shadows_base_and_supers_to_it(themed):
    with using(themed):
        o = DBObject(parents=("reg9.Thing",))
        method = o.read_data_file
        assert method.__func__.__module__ == "reg9_theme"
        assert o.hello() == "themed:hello é\n"


# -- read_data_file (R206) -------------------------------------------------------------------

def test_read_data_file_default_reads_the_file(plain, tree):
    with using(plain):
        o = DBObject(parents=("reg9.Thing",))
        assert o.hello() == "hello é\n"
        assert type(o.hello()) is str
        assert o.hello(binary=True) == "hello é\n".encode("utf-8")
        assert o.hello("data/sub/deep.txt") == "deep"
        assert o.hello("data/sub/../hello.txt") == "hello é\n"        # stays inside: allowed
        assert o.hello("data/inner.txt") == "hello é\n"               # a link that stays inside
        assert o.read_data_file("data/sub/deep.txt", module="reg9_files") == "deep"


def test_a_module_overrides_read_data_file_and_supers(themed):
    with using(themed):
        o = DBObject(parents=("reg9.Thing",))
        # The override runs, supers to base, and base reads relative to the module of the method
        # that asked (reg9_files's hello), not to the override's module (reg9_theme, which has its
        # own data/hello.txt).
        assert o.hello() == "themed:hello é\n"
        assert o.read_data_file("data/hello.txt", module="reg9_theme") == "themed:the theme's own file"


@pytest.mark.parametrize("path", [
    "../reg9_files.py/../../outside/secret.txt",
    "../outside/secret.txt",
    "data/../../outside/secret.txt",
    "data/escape.txt",                  # a symbolic link to a file outside the module's directory
    "data/escape_dir/secret.txt",       # a path through a link to a directory outside it
])
def test_read_data_file_refuses_a_path_that_leaves_the_module_directory(plain, path):
    with using(plain):
        o = DBObject(parents=("reg9.Thing",))
        with pytest.raises(ProtoValidationError, match="leaves the directory of module 'reg9_files'"):
            o.hello(path)


def test_read_data_file_refuses_an_absolute_path(plain, tree):
    with using(plain):
        o = DBObject(parents=("reg9.Thing",))
        with pytest.raises(ProtoValidationError, match="absolute"):
            o.hello(str(tree["pkg"] / "data" / "hello.txt"))
        with pytest.raises(ProtoValidationError, match="absolute"):
            o.hello(str(tree["outside"] / "secret.txt"))


@pytest.mark.parametrize("path", ["", "data/\0hello.txt", b"data/hello.txt", 7])
def test_read_data_file_refuses_a_path_that_is_not_a_relative_text_path(plain, path):
    with using(plain):
        o = DBObject(parents=("reg9.Thing",))
        with pytest.raises(ProtoValidationError):
            o.hello(path)


def test_read_data_file_needs_a_registry_method_or_an_explicit_module(plain):
    with using(plain):
        o = DBObject(parents=("reg9.Thing",))
        with pytest.raises(ProtoUserError, match="module="):
            o.read_data_file("data/hello.txt")           # called from a test, not a registry method
        with pytest.raises(ProtoUserError, match="module="):
            o.via_helper()                               # a helper is not a registry method
        with pytest.raises(ProtoValidationError, match="not in the registry"):
            o.read_data_file("data/hello.txt", module="reg9_theme")
        with pytest.raises(ProtoValidationError, match="not in the registry"):
            o.read_data_file("data/hello.txt", module="")


def test_read_data_file_refuses_what_is_not_a_regular_file(plain):
    """A FIFO would block (or stream forever), a directory is not a file (review I2)."""
    with using(plain):
        o = DBObject(parents=("reg9.Thing",))
        with pytest.raises(ProtoValidationError, match="not a regular file"):
            o.hello("data/fifo")
        with pytest.raises(ProtoValidationError, match="not a regular file"):
            o.hello("data/sub")


@pytest.mark.parametrize("locate", ["native", "fallback"])
def test_a_swap_between_the_check_and_the_open_is_refused(plain, tree, monkeypatch, locate):
    """The containment check runs again on the open file (review I2): an in-tree link retargeted
    outside after the path check, just before the open, is refused, whichever way the open file
    is located (the platform's descriptor path, or realpath after the open plus samestat)."""
    pkg, outside = tree["pkg"], tree["outside"]
    link = pkg / "data" / "swap.txt"
    fresh = pkg / "data" / f"swap-{locate}.reset"
    os.symlink(pkg / "data" / "hello.txt", fresh)
    os.replace(fresh, link)                                     # in the tree again (another parameter swapped it)
    real_open = base_module._open
    swapped = []

    def swapping_open(path, flags, *args):
        if os.path.basename(os.fspath(path)) == "swap.txt" and not swapped:
            tmp = pkg / "data" / f"swap-{locate}.tmp"
            os.symlink(outside / "secret.txt", tmp)
            os.replace(tmp, link)                               # atomic retarget
            swapped.append(True)
        return real_open(path, flags, *args)

    if locate == "fallback":
        monkeypatch.setattr(base_module, "_descriptor_path", lambda fd: None)
    with using(plain):
        o = DBObject(parents=("reg9.Thing",))
        if locate == "native":
            assert o.hello("data/swap.txt") == "hello \u00e9\n"     # before the swap: in the tree
        monkeypatch.setattr(base_module, "_open", swapping_open)
        with pytest.raises(ProtoValidationError, match="leaves the directory of module 'reg9_files'"):
            o.hello("data/swap.txt")
    assert swapped


def test_the_fallback_location_refuses_a_file_replaced_after_the_open(plain, tree, monkeypatch):
    """Without a descriptor path, the open file must be the file realpath names (samestat)."""
    pkg = tree["pkg"]
    monkeypatch.setattr(base_module, "_descriptor_path", lambda fd: None)
    real = os.path.realpath
    monkeypatch.setattr(base_module, "_realpath_after_open",
                        lambda p: real(str(pkg / "data" / "sub" / "deep.txt")))
    with using(plain):
        o = DBObject(parents=("reg9.Thing",))
        with pytest.raises(ProtoValidationError, match="changed while it was opened"):
            o.hello()


@pytest.mark.parametrize("path", ["\\Windows\\x.txt", "/etc/passwd", "C:x.txt", "C:\\x.txt", "\\\\host\\share\\x"])
def test_root_relative_and_drive_paths_are_absolute(path):
    """Windows-style root-relative and drive paths are refused as absolute on every platform
    (review M1: ntpath.isabs is False for a root-relative path since Python 3.13)."""
    with pytest.raises(ProtoValidationError, match="is absolute"):
        base_module._checked_path("/some/module/dir", "m", path)


def test_a_containment_check_that_cannot_compare_refuses():
    """os.path.commonpath raising ValueError (another drive, mixed forms) is a refusal (review M2)."""
    with pytest.raises(ProtoValidationError, match="leaves the directory of module 'm'"):
        base_module._check_inside("/some/module/dir", "m", "x.txt", "relative/x.txt")


def test_a_module_imported_from_a_zip_has_no_directory(tree):
    registry = Registry.build((BASE, "reg9_zipped"))
    with using(registry):
        o = DBObject(parents=("reg9.Zipped",))
        with pytest.raises(ProtoNotSupportedError, match="module 'reg9_zipped' has no directory .imported from a zip"):
            o.hello()


def test_a_missing_data_file_raises(plain):
    with using(plain):
        o = DBObject(parents=("reg9.Thing",))
        with pytest.raises(FileNotFoundError):
            o.hello("data/none.txt")


def test_super_from_a_module_method_reaches_base(plain):
    with using(plain):
        o = DBObject(parents=("reg9.Thing",))
        assert o.read_data_file.__func__.__module__ == "proto_db.modules.base"
        # reg9.Thing's chain ends; _super() goes on to reg9.Named, then to base's read_data_file,
        # which reads relative to reg9_files (the module of the method that asked).
        assert o.super_reads() == "hello \u00e9\n"
