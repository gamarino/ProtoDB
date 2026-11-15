"""The runtime registry and its modules, in memory (spec sections 6 and 7, Task 5).

Catches: a load order that depends on anything but the dependencies and the installation order;
a dependency cycle, or a cycle of registry-name parents, accepted; a chain in the wrong order; a
registry name's parents lost, reordered or repeated; a definition of a refused kind (a property,
a staticmethod, a classmethod, a callable instance, a lambda, a nested class, a private, reserved
or API name) accepted; one function object or one code object behind two definitions (which
``_super`` could not place); an unimportable module, a module outside the allowed set or a
dependency that is not installed going unnamed; a mutable default shared by reference; a
reload duplicating extensions; one registry module name declared by two Python modules.
"""
from __future__ import annotations

import importlib
import os
import subprocess
import sys
import textwrap
import threading

import pytest

from proto_db.core.atom import RESERVED_NAMES
from proto_db.core.dbobject import DBObject
from proto_db.errors import ProtoNotSupportedError, ProtoValidationError
from proto_db.registry import BASE, Extension, Registry, module
from proto_db.values import FrozenDict, FrozenList

pytestmark = [pytest.mark.timeout(120)]

_PACKAGE = "tests.unit.registry_modules"
_FIXTURES = ("reg_base", "reg_contacts", "reg_notes", "reg_crm", "reg_cycle_a", "reg_cycle_b",
             "reg_bad_property", "reg_reuse_a", "reg_reuse_b", "reg_wrong_name", "reg_name_cycle",
             "reg_raises", "reg_twice")
MAPPING = {name: f"{_PACKAGE}.{name}" for name in _FIXTURES}


def build(*names, **kwargs):
    """The registry of ``names``, after ``base`` (ruling R205: every non-empty list starts with it)."""
    kwargs.setdefault("mapping", MAPPING)
    return Registry.build((BASE,) + names, **kwargs)


def modules_of(chain):
    return tuple(extension.module for extension in chain)


# -- load order ---------------------------------------------------------------------------------

def test_topological_order_with_install_order_ties():
    # Dependencies first, whatever the installation order.
    assert build("reg_crm", "reg_contacts", "reg_base").key == (BASE, "reg_base", "reg_contacts", "reg_crm")
    # reg_contacts and reg_notes both depend on reg_base only: the installation order decides.
    assert build("reg_notes", "reg_contacts", "reg_base").key == (BASE, "reg_base", "reg_notes", "reg_contacts")
    assert build("reg_contacts", "reg_notes", "reg_base").key == (BASE, "reg_base", "reg_contacts", "reg_notes")
    # A later-installed module with no dependency still comes after an earlier one that is ready.
    assert build("reg_base", "reg_crm", "reg_contacts", "reg_notes").key == (
        BASE, "reg_base", "reg_contacts", "reg_crm", "reg_notes")


def test_cycle_is_refused():
    with pytest.raises(ProtoValidationError, match=r"cycle") as info:
        build("reg_cycle_a", "reg_cycle_b")
    assert "reg_cycle_a" in str(info.value) and "reg_cycle_b" in str(info.value)


def test_registry_name_parent_cycle_is_refused():
    with pytest.raises(ProtoValidationError, match=r"cycle") as info:
        build("reg_name_cycle")
    assert "cyc.A" in str(info.value) and "cyc.B" in str(info.value)


def test_dependency_not_installed_is_named():
    # The code's depends wins (spec 14 item 8): reg_contacts needs reg_base, which is not listed.
    with pytest.raises(ProtoNotSupportedError, match=r"reg_contacts.*reg_base.*not installed"):
        build("reg_contacts")


def test_module_list_is_validated():
    with pytest.raises(ProtoValidationError, match=r"more than once"):
        build("reg_base", "reg_base")
    for bad in ("", 3):
        with pytest.raises(ProtoValidationError):
            build(bad)
    with pytest.raises(ProtoValidationError):
        Registry.build("reg_base", mapping=MAPPING)       # a str, not a tuple of names


def test_empty_registry():
    registry = Registry.build(())
    assert registry.key == ()
    assert registry.chain("reg.Partner") == ()
    assert registry.parents_of("reg.Partner") == ()


# -- chains, parents and the code index -----------------------------------------------------------

def test_chain_is_newest_first():
    registry = build("reg_crm", "reg_contacts", "reg_base")
    chain = registry.chain("reg.Partner")
    assert modules_of(chain) == ("reg_crm", "reg_contacts", "reg_base")
    assert tuple(extension.position for extension in chain) == (0, 1, 2)
    assert all(extension.registry_name == "reg.Partner" for extension in chain)
    assert chain[0].definitions["label"][0] == "method"
    assert chain[0].definitions["label"][1](None) == "crm"
    assert chain[2].definitions["kind"] == ("data", "partner")
    assert registry.chain("reg.Unknown") == ()
    assert modules_of(registry.chain("mail.Thread")) == ("reg_base",)


def test_extension_is_immutable():
    extension = build("reg_base").chain("reg.Partner")[0]
    assert isinstance(extension, Extension)
    with pytest.raises(TypeError):
        extension.definitions["new"] = ("data", 1)
    with pytest.raises(AttributeError):
        extension.position = 5
    registry = build("reg_base")
    with pytest.raises(AttributeError):
        registry.key = ()


def test_registry_name_parents_union():
    registry = build("reg_notes", "reg_contacts", "reg_base")
    # Load order reg_base, reg_notes, reg_contacts: notes' parents first, then the new one of contacts.
    # Each ends with base, the implicit last parent of every other registry name (ruling R204).
    assert registry.parents_of("reg.Partner") == ("mail.Activity", "mail.Thread", BASE)
    registry = build("reg_contacts", "reg_notes", "reg_base")
    assert registry.parents_of("reg.Partner") == ("mail.Thread", "mail.Activity", BASE)
    assert registry.parents_of("reg.Contact") == ("reg.Partner", BASE)
    assert registry.parents_of("mail.Thread") == (BASE,)
    assert registry.parents_of("reg.Unknown") == (BASE,)
    assert registry.parents_of(BASE) == ()


def test_extension_of_code():
    registry = build("reg_base", "reg_contacts", "reg_crm")
    for extension in registry.chain("reg.Partner"):
        function = extension.definitions["label"][1]
        assert registry.extension_of_code(function.__code__) is extension
    assert registry.extension_of_code(test_extension_of_code.__code__) is None
    # Every extension, in load order: by module, then by declaration within a module.
    assert [(e.module, e.registry_name) for e in registry.extensions] == [
        (BASE, BASE), ("reg_base", "reg.Partner"), ("reg_base", "mail.Thread"), ("reg_contacts", "reg.Partner"),
        ("reg_contacts", "reg.Contact"), ("reg_crm", "reg.Partner")]


# -- definitions --------------------------------------------------------------------------------

_handle = module("test_registry_modules.refusals")


class _CallableInstance:
    def __call__(self, *args):
        return None


def _refuse(body, *, match):
    with pytest.raises(ProtoValidationError, match=match) as info:
        _handle.extends("reg.Refused")(type("Refused", (), body))
    message = str(info.value)
    assert "test_registry_modules.refusals" in message and "reg.Refused" in message
    return message


@pytest.mark.parametrize("kind, value", [
    ("property", property(lambda self: 1)),
    ("staticmethod", staticmethod(lambda: 1)),
    ("classmethod", classmethod(lambda cls: 1)),
    ("callable", _CallableInstance()),
    ("callable", len),
    ("lambda", lambda self: 1),
    ("class", type("Nested", (), {})),
])
def test_refused_definitions(kind, value):
    message = _refuse({"thing": value}, match=r"'thing'")
    assert kind in message


@pytest.mark.parametrize("name", ["_private", "__init__", *sorted(RESERVED_NAMES),
                                  "with_", "with_parents", "parents", "has_defined_attr"])
def test_refused_names(name):
    def method(self):
        return None
    _refuse({name: method}, match=repr(name))
    _refuse({name: 1}, match=repr(name))


def test_api_names_are_computed_from_the_class():
    from proto_db.core.dbobject import DBObject
    for name in (n for n in dir(DBObject) if not n.startswith("_")):
        _refuse({name: 1}, match=repr(name))


def test_refused_property_in_a_module_names_it():
    # Refused as a property, by name, not merely as a value canonical() cannot encode (Task 9
    # plant ``refusal_of_property_removed``).
    with pytest.raises(ProtoValidationError, match=r"reg_bad_property.*reg\.Bad.*'size'.*a property is refused"):
        build("reg_bad_property")


def test_a_property_is_refused_by_the_registry_even_if_canonical_accepted_it(monkeypatch):
    """The refusal of a property is the registry's own rule (spec Q4), not an accident of
    canonical() not encoding it (review M7; plant ``refusal_of_property_removed``)."""
    from proto_db import registry as registry_module
    monkeypatch.setattr(registry_module, "canonical", lambda value: value)
    _refuse({"size": property(lambda self: 1)}, match=r"'size'.*a property is refused")


def test_refused_container_shapes():
    class Base:
        pass
    with pytest.raises(ProtoValidationError, match=r"base class"):
        _handle.extends("reg.Refused")(type("Refused", (Base,), {}))
    with pytest.raises(ProtoValidationError, match=r"class"):
        _handle.extends("reg.Refused")(lambda: None)
    for name in ("", 7):
        with pytest.raises(ProtoValidationError):
            _handle.extends(name)
    with pytest.raises(ProtoValidationError, match=r"more than once"):
        _handle.extends("reg.Refused", parents=("a.B", "a.B"))
    with pytest.raises(ProtoValidationError, match=r"its own parent"):
        _handle.extends("reg.Refused", parents=("reg.Refused",))
    with pytest.raises(ProtoValidationError):
        _handle.extends("reg.Refused", parents=("",))
    with pytest.raises(ProtoValidationError):
        _handle.extends("reg.Refused", parents="a.B")


def test_reused_function_is_refused():
    with pytest.raises(ProtoValidationError, match=r"'shared'.*'again'|'again'.*'shared'") as info:
        build("reg_reuse_a", "reg_reuse_b")
    assert "reg_reuse_a" in str(info.value) and "reg_reuse_b" in str(info.value)


def test_shared_code_object_is_refused():
    # Two closures of one factory are two function objects with one code object: _super could not
    # tell which extension is running, so the registry refuses them.
    handle = module("test_registry_modules.shared_code")

    def factory():
        def method(self):
            return None
        return method

    handle.extends("reg.SharedOne")(type("One", (), {"run": factory()}))
    handle.extends("reg.SharedTwo")(type("Two", (), {"run": factory()}))
    with pytest.raises(ProtoValidationError, match=r"code object"):
        Registry.build((BASE, "test_registry_modules.shared_code"),
                       mapping={"test_registry_modules.shared_code": __name__})


def test_data_defaults_are_canonical():
    extension = build("reg_base").chain("reg.Partner")[0]
    kind, tags = extension.definitions["tags"]
    assert kind == "data" and isinstance(tags, FrozenList) and isinstance(tags[1], FrozenList)
    assert tags == ["a", ["b"]]
    kind, settings = extension.definitions["settings"]
    assert kind == "data" and isinstance(settings, FrozenDict)
    with pytest.raises(ProtoValidationError):
        tags.append("c")
    # A default canonical() refuses is refused naming the definition.
    _refuse({"blob": object()}, match=r"'blob'")


@pytest.mark.parametrize("make", [
    lambda: DBObject(),                                  # the default itself
    lambda: [1, DBObject()],                             # in a list
    lambda: {"a": (1, DBObject())},                      # in a tuple inside a dict
    lambda: {"a": {"b": [(DBObject(),)]}},               # deeper: dict, dict, list, tuple
    lambda: ((("x", DBObject()),),),                     # nested tuples
], ids=["direct", "list", "dict-tuple", "deep", "tuples"])
def test_atoms_in_defaults_are_refused(make):
    # R166: a default is shared by every object that inherits it; an atom there would be one
    # object (saved or not) shared by them all, so none is allowed at any depth.
    message = _refuse({"holder": make()}, match=r"'holder'")
    assert "atom" in message


# -- import, mapping and the allowed set -------------------------------------------------------------

def test_unimportable_module_names_it():
    with pytest.raises(ProtoNotSupportedError, match=r"reg_ghost") as info:
        Registry.build((BASE, "reg_ghost"))
    assert isinstance(info.value.__cause__, ImportError)
    with pytest.raises(ProtoNotSupportedError, match=r"reg_ghost.*tests\.unit\.registry_modules\.nothing"):
        Registry.build((BASE, "reg_ghost"), mapping={"reg_ghost": f"{_PACKAGE}.nothing"})
    with pytest.raises(ProtoNotSupportedError, match=r"reg_raises") as info:
        build("reg_raises")
    assert isinstance(info.value.__cause__, RuntimeError)


def test_module_that_declares_another_name_is_refused():
    with pytest.raises(ProtoNotSupportedError, match=r"reg_wrong_name.*declares no registry module"):
        build("reg_wrong_name")
    # reg_base is declared, but by another Python module than the one the mapping imports.
    importlib.import_module(f"{_PACKAGE}.reg_base")
    with pytest.raises(ProtoNotSupportedError, match=r"reg_notes.*declares no registry module.*reg_base"):
        Registry.build((BASE, "reg_base"), mapping={"reg_base": f"{_PACKAGE}.reg_notes"})


def test_module_outside_allowed_is_refused():
    allowed = {"reg_base", "reg_contacts"}            # base is always allowed (ruling R207)
    assert build("reg_base", "reg_contacts", allowed=allowed).key == (BASE, "reg_base", "reg_contacts")
    with pytest.raises(ProtoValidationError, match=r"reg_crm.*not allowed"):
        build("reg_base", "reg_contacts", "reg_crm", allowed=allowed)


def test_one_name_declared_by_two_python_modules_is_refused():
    importlib.import_module(f"{_PACKAGE}.reg_base")
    with pytest.raises(ProtoValidationError, match=r"reg_base.*tests\.unit\.registry_modules\.reg_base"):
        module("reg_base")


def test_two_declarations_in_one_file_are_refused():
    with pytest.raises(ProtoValidationError, match=r"reg_twice.*reg_twice\.py:4.*reg_twice\.py:5"):
        build("reg_twice")


def test_two_declarations_from_two_lines_are_refused():
    first = module("test_registry_modules.two_lines")
    assert first.name == "test_registry_modules.two_lines"
    with pytest.raises(ProtoValidationError, match=r"test_registry_modules\.py:\d+.*test_registry_modules\.py:\d+"):
        module("test_registry_modules.two_lines")


def _execute(source: str, namespace: dict) -> None:
    exec(compile(textwrap.dedent(source), "<registry-fixture>", "exec"), namespace)


def test_new_execution_of_a_module_replaces_even_from_another_line():
    # A reload after an edit runs a new code object of the same module scope: it replaces the
    # declaration, even though the call moved to another line.
    namespace = {"__name__": "test_registry_modules.executed"}
    _execute("""
        from proto_db.registry import module
        handle = module("test_registry_modules.executed")
    """, namespace)
    first = namespace["handle"]
    _execute("""
        from proto_db.registry import module


        handle = module("test_registry_modules.executed")
    """, namespace)
    assert namespace["handle"] is not first
    namespace["first"] = first      # the stale handle, used from its own Python module
    with pytest.raises(ProtoValidationError, match=r"replaced"):
        _execute("first.extends('reg.Executed')", namespace)
    # Within one execution, a second call from another line is a duplicate.
    with pytest.raises(ProtoValidationError, match=r"<registry-fixture>:3.*<registry-fixture>:4"):
        _execute("""
            from proto_db.registry import module
            one = module("test_registry_modules.executed_twice")
            two = module("test_registry_modules.executed_twice")
        """, {"__name__": "test_registry_modules.executed_twice"})


def test_declaration_from_another_scope_is_refused():
    # A function of the module is another scope (another code object), not a new execution of it.
    with pytest.raises(ProtoValidationError, match=r"<registry-fixture>:3.*<registry-fixture>:5"):
        _execute("""
            from proto_db.registry import module
            one = module("test_registry_modules.other_scope")
            def declare():
                return module("test_registry_modules.other_scope")
            declare()
        """, {"__name__": "test_registry_modules.other_scope"})


def test_same_site_executed_again_replaces():
    # A call in a loop (or a reload of an unchanged file) is one site: it replaces.
    handles = [module("test_registry_modules.loop") for _ in range(3)]
    assert len({id(h) for h in handles}) == 3


def _xoptions() -> list:
    """This interpreter's ``-X`` options (``gil=0`` or ``gil=1``), for a child interpreter."""
    return [arg for k, v in sys._xoptions.items() for arg in ("-X", k if v is True else f"{k}={v}")]


def test_registry_imported_before_dbobject():
    # Importing the registry as the process's first proto_db import (before proto_db.core.dbobject
    # is imported by anything else) must work and build a registry: no import cycle.
    code = textwrap.dedent(f"""
        import sys
        assert not any(name.startswith("proto_db") for name in sys.modules)
        import proto_db.registry as registry           # the first proto_db import of the process
        built = registry.Registry.build(("base", "reg_base"), mapping={{"reg_base": "{_PACKAGE}.reg_base"}})
        assert built.key == ("base", "reg_base")
        assert [e.module for e in built.chain("reg.Partner")] == ["reg_base"]
        print("ok")
    """)
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    env = dict(os.environ, PYTHONPATH=root + os.pathsep + os.environ.get("PYTHONPATH", ""))
    result = subprocess.run([sys.executable, *_xoptions(), "-c", code], cwd=root, env=env,
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_extends_only_from_the_declaring_module():
    from tests.unit.registry_modules import reg_base
    with pytest.raises(ProtoValidationError, match=r"reg_base"):
        reg_base.base.extends("reg.Partner")(type("Late", (), {"late": 1}))


def test_module_declaration_is_validated():
    for name in ("", 5):
        with pytest.raises(ProtoValidationError):
            module(name)
    with pytest.raises(ProtoValidationError, match=r"itself"):
        module("test_registry_modules.self_dependent", depends=("test_registry_modules.self_dependent",))
    with pytest.raises(ProtoValidationError, match=r"more than once"):
        module("test_registry_modules.twice", depends=("reg_base", "reg_base"))
    with pytest.raises(ProtoValidationError):
        module("test_registry_modules.str_depends", depends="reg_base")


def test_reload_does_not_duplicate_extensions():
    before = build("reg_base", "reg_contacts", "reg_crm")
    old_label = before.chain("reg.Partner")[0].definitions["label"][1]
    importlib.reload(importlib.import_module(f"{_PACKAGE}.reg_crm"))
    after = build("reg_base", "reg_contacts", "reg_crm")
    assert modules_of(after.chain("reg.Partner")) == ("reg_crm", "reg_contacts", "reg_base")
    new_label = after.chain("reg.Partner")[0].definitions["label"][1]
    assert new_label is not old_label
    # A registry built before the reload keeps the definitions it was built with.
    assert before.chain("reg.Partner")[0].definitions["label"][1] is old_label
    assert before.extension_of_code(old_label.__code__) is before.chain("reg.Partner")[0]


def test_concurrent_reads_of_a_built_registry():
    registry = build("reg_base", "reg_contacts", "reg_notes", "reg_crm")
    expected = (registry.chain("reg.Partner"), registry.parents_of("reg.Partner"))
    errors = []

    def read():
        try:
            for _ in range(2000):
                assert (registry.chain("reg.Partner"), registry.parents_of("reg.Partner")) == expected
        except BaseException as e:      # reported to the main thread
            errors.append(e)

    threads = [threading.Thread(target=read) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []


def test_concurrent_builds_agree():
    keys, errors = [], []

    def run():
        try:
            keys.append(build("reg_crm", "reg_notes", "reg_contacts", "reg_base").key)
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=run) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert set(keys) == {(BASE, "reg_base", "reg_notes", "reg_contacts", "reg_crm")}
