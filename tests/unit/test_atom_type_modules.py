"""Built-in atom types load in a fresh process (review fix round 1 of Tasks 4.2/4.3).

Catches: stored atoms of built-in 2.0 types (collections, AVL nodes, indexes)
failing with "Unknown atom type" in a process that never imported their module,
and the type -> module table in proto_db/core/atom.py rotting when a new
built-in Atom subclass is added without an entry.
"""
import importlib
import json
import pkgutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from proto_db.core import atom as atom_module
from proto_db.core.atom import BUILTIN_ATOM_MODULES, Atom
from proto_db.errors import ProtoCorruptionError

ROOT = Path(__file__).resolve().parents[2]
BUILTIN_PACKAGES = ("proto_db.core", "proto_db.collections", "proto_db.indexing", "proto_db.storage",
                    "proto_db.query")
BUILTIN_MODULES = ("proto_db.transactions",)


def _all_subclasses(cls):
    out, stack = [], [cls]
    while stack:
        for sub in stack.pop().__subclasses__():
            out.append(sub)
            stack.append(sub)
    return out


def test_every_builtin_atom_type_is_in_the_table():
    for name in BUILTIN_PACKAGES:
        package = importlib.import_module(name)
        for info in pkgutil.iter_modules(package.__path__):
            importlib.import_module(f"{name}.{info.name}")
    for name in BUILTIN_MODULES:
        importlib.import_module(name)
    builtin = [c for c in _all_subclasses(Atom)
               if c.__module__.startswith(BUILTIN_PACKAGES) or c.__module__ in BUILTIN_MODULES]
    assert builtin, "no built-in atom class found"
    for cls in builtin:
        assert BUILTIN_ATOM_MODULES.get(cls.__type_name__) == cls.__module__, \
            f"{cls.__qualname__} ({cls.__type_name__}) is missing from BUILTIN_ATOM_MODULES"
    for type_name, module in BUILTIN_ATOM_MODULES.items():
        importlib.import_module(module)
        assert Atom._class_for(type_name).__type_name__ == type_name


def test_registry_miss_imports_the_module_and_unknown_names_still_raise(monkeypatch):
    from proto_db.indexing.range import RangeIndex
    registry = dict(atom_module._registry)
    del registry[RangeIndex.__type_name__]
    monkeypatch.setattr(atom_module, "_registry", registry)
    monkeypatch.delitem(sys.modules, "proto_db.indexing.range")
    cls = Atom._class_for(RangeIndex.__type_name__)
    assert cls.__type_name__ == "proto_db.indexing.RangeIndex"
    with pytest.raises(ProtoCorruptionError):
        Atom._class_for("proto_db.nonexistent.Type")


CHILD = textwrap.dedent('''
    import json, sys, uuid
    import proto_db.storage.file
    import proto_db.core.atom
    if len(sys.argv) > 2:  # used once to show the test fails without the table
        proto_db.core.atom.BUILTIN_ATOM_MODULES.clear()
    from proto_db.core.atom import Atom, AtomPointer
    lazy = [m for m in ("proto_db.collections.dictionaries", "proto_db.collections.lists",
                        "proto_db.indexing.equality", "proto_db.indexing.range") if m in sys.modules]
    assert not lazy, f"already imported: {lazy}"

    class Context:
        def __init__(self, storage):
            self.storage = storage
        def save(self, atom):
            raise AssertionError("read-only")
        def load(self, ptr, type_name):
            return Atom._stub(self, ptr, type_name)

    storage = proto_db.storage.file.FileStorage.open_existing(sys.argv[1])
    root = storage.read_root()
    ctx = Context(storage)
    coll = ctx.load(AtomPointer(root.segment, root.offset), "proto_db.collections.Dictionary")
    by_city, by_age = coll.get_index("by_city"), coll.get_index("by_age")
    from proto_db.query.terms import EQ, LT, Term
    out = {
        "types": [type(coll).__name__, type(by_city).__name__, type(by_age).__name__],
        "paris": sorted(e.name for e in by_city.plan(Term("city", EQ, "Paris")).execute()),
        "young": sorted(e.name for e in by_age.plan(Term("age", LT, 30)).execute()),
        "ordered": [type(e).__name__ for e in by_age.ordered_plan().execute()],
        "list": list(coll.get_at("items").elements()),
    }
    storage.close()
    print(json.dumps(out))
''')


def test_fresh_interpreter_loads_collections_and_indexes(tmp_path):
    from proto_db.collections.dictionaries import Dictionary
    from proto_db.collections.lists import List
    from proto_db.core.dbobject import DBObject
    from proto_db.indexing import EqualityIndex, RangeIndex
    from proto_db.storage.file import FileStorage
    from tests.support.persistence import DirectContext

    people = {"a": DBObject(name="ann", city="Paris", age=25), "b": DBObject(name="bob", city="Rome", age=40),
              "c": DBObject(name="cid", city="Paris", age=33), "d": DBObject(name="dan", age=19)}
    coll = Dictionary().add_index(EqualityIndex("by_city", "city")).add_index(RangeIndex("by_age", "age"))
    for key, person in people.items():
        coll = coll.set_at(key, person)
    coll = coll.set_at("items", List.from_iterable([1, "two", 3.0]))
    path = str(tmp_path / "space")
    storage = FileStorage(path)
    ptr = DirectContext(storage).save(coll)
    storage.flush()
    storage.publish_root(ptr, None)
    storage.close()

    result = subprocess.run([sys.executable, "-c", CHILD, path], cwd=ROOT, capture_output=True, text=True,
                            timeout=120)
    assert result.returncode == 0, result.stderr
    out = json.loads(result.stdout)
    assert out["types"] == ["Dictionary", "EqualityIndex", "RangeIndex"]
    assert out["paris"] == ["ann", "cid"]
    assert out["young"] == ["ann", "dan"]
    assert out["ordered"] == ["List"] + ["DBObject"] * 4  # the List has no age: MISSING sorts first
    assert out["list"] == [1, "two", 3.0]
