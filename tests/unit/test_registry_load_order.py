"""The load order of a registry is a function of the module set (rulings R253-R257).

Catches: a load order, and so extension chains, parents unions, linearizations and method results,
that depends on the installation order persisted in ``__currentModules`` (two databases that
install the same modules in opposite orders composed different registries); a name order that
follows a locale or case-insensitive collation instead of code points; ``base`` placed by its name
instead of explicitly first; a dependency overridden by the name order; and a process cache or a
space fallback that tells apart two lists of the same set.
"""
from __future__ import annotations

import pytest

from proto_db import resolution
from proto_db.errors import ProtoValidationError
from proto_db.registry import BASE, Registry, registry_for
from proto_db.storage.memory import MemoryStorage
from proto_db.transactions import ObjectSpace

pytestmark = [pytest.mark.timeout(120)]

_PACKAGE = "tests.unit.registry_modules"
ALPHA, OMEGA, UPPER, FIRST, ZETA, AAA = (
    "regtb_alpha", "regtb_omega", "Regtb_upper", "regtb_first", "regtb_zeta", "aaa_regtb")
MAPPING = {name: f"{_PACKAGE}.{name}" for name in (ALPHA, OMEGA, UPPER, FIRST, ZETA, AAA)}


def _build(*names) -> Registry:
    return Registry.build((BASE,) + names, mapping=MAPPING)


def _composition(registry: Registry) -> tuple:
    """Everything the load order decides, in comparable form."""
    names = ("regtb.Item", "regtb.FromAlpha", "regtb.FromOmega")
    return (registry.key,
            tuple((e.registry_name, e.module, e.position) for e in registry.extensions),
            {name: tuple(e.module for e in registry.chain(name)) for name in names},
            {name: registry.parents_of(name) for name in names})


# -- the registry alone ---------------------------------------------------------------------------

def test_independent_modules_load_in_name_order_whatever_the_list_order():
    forward, backward = _build(ALPHA, OMEGA), _build(OMEGA, ALPHA)
    assert forward.key == backward.key == (BASE, ALPHA, OMEGA)
    assert _composition(forward) == _composition(backward)
    assert tuple(e.module for e in backward.chain("regtb.Item")) == (OMEGA, ALPHA)   # newest first
    # The parents union follows the load order too (spec Q6), then base (R204).
    assert backward.parents_of("regtb.Item") == ("regtb.FromAlpha", "regtb.FromOmega", BASE)


def test_a_dependency_wins_over_the_name_order():
    # regtb_first < regtb_zeta by name, but regtb_first depends on regtb_zeta.
    for registry in (_build(FIRST, ZETA), _build(ZETA, FIRST)):
        assert registry.key == (BASE, ZETA, FIRST)
        assert tuple(e.module for e in registry.chain("regtb.Item")) == (FIRST, ZETA)


def test_the_name_order_is_code_point_order():
    # "R" (U+0052) < "r" (U+0072): a case-insensitive or locale collation would put alpha first.
    assert _build(ALPHA, UPPER).key == _build(UPPER, ALPHA).key == (BASE, UPPER, ALPHA)


def test_base_is_first_even_when_a_name_sorts_before_it():
    assert AAA < BASE
    assert _build(AAA).key == (BASE, AAA)
    assert _build(OMEGA, AAA, ALPHA).key == (BASE, AAA, ALPHA, OMEGA)


def test_the_process_cache_shares_one_registry_per_set():
    forward = registry_for((BASE, ALPHA, OMEGA, UPPER), mapping=MAPPING)
    assert registry_for((BASE, UPPER, OMEGA, ALPHA), mapping=MAPPING) is forward
    # The list is still checked on a cache hit: base first, each name once (R205).
    with pytest.raises(ProtoValidationError, match="must start with the module"):
        registry_for((ALPHA, BASE, OMEGA, UPPER), mapping=MAPPING)
    with pytest.raises(ProtoValidationError, match="more than once"):
        registry_for((BASE, ALPHA, OMEGA, UPPER, ALPHA), mapping=MAPPING)


# -- databases --------------------------------------------------------------------------------------

def _space(storage=None):
    return ObjectSpace(storage if storage is not None else MemoryStorage(), modules_mapping=MAPPING)


def _install(db, *names):
    tr = db.new_transaction()
    for name in names:
        tr.install_module(name)
    tr.set_root_object("item", tr.new_object(label="i", parents=("regtb.Item",)))
    tr.commit()


@pytest.mark.parametrize("entries", [50_000, 0], ids=["atom-cache", "no-atom-cache"])
def test_two_databases_installing_in_opposite_orders_compose_the_same_registry(entries):
    spaces = (ObjectSpace(MemoryStorage(), modules_mapping=MAPPING, atom_cache_entries=entries),
              ObjectSpace(MemoryStorage(), modules_mapping=MAPPING, atom_cache_entries=entries))
    one, two = spaces[0].new_database("db"), spaces[1].new_database("db")
    _install(one, ALPHA, OMEGA)
    _install(two, OMEGA, ALPHA)
    t1, t2 = one.new_transaction(), two.new_transaction()
    # The persisted list keeps the installation history (R256) ...
    assert t1.installed_modules() == (BASE, ALPHA, OMEGA)
    assert t2.installed_modules() == (BASE, OMEGA, ALPHA)
    # ... and the composition ignores it (R253).
    assert t1.registry.key == t2.registry.key == (BASE, ALPHA, OMEGA)
    assert _composition(t1.registry) == _composition(t2.registry)
    with t1:                                    # a block commits on exit
        i1 = t1.get_root_object("item")
        line1 = tuple(n if isinstance(n, str) else "<object>" for n in resolution.linearize(i1))
        r1 = (i1.who(), i1.tag, i1.alpha_only, i1.omega_only)
    with t2:
        i2 = t2.get_root_object("item")
        line2 = tuple(n if isinstance(n, str) else "<object>" for n in resolution.linearize(i2))
        r2 = (i2.who(), i2.tag, i2.alpha_only, i2.omega_only)
    assert line1 == line2 == ("<object>", "regtb.Item", "regtb.FromAlpha", "regtb.FromOmega", BASE)
    assert r1 == r2 == ("omega>alpha>end", "omega", True, True)
    for space in spaces:
        space.close()


def test_the_space_fallback_treats_two_orders_of_one_set_as_one_list():
    space = _space()
    a, b = space.new_database("a"), space.new_database("b")
    _install(a, ALPHA, OMEGA)
    _install(b, OMEGA, ALPHA)
    loose = a.new_transaction()                 # both databases are bound by their installs
    item = loose.get_root_object("item")        # loaded through the shared atom cache
    assert item.who() == "omega>alpha>end"      # outside any block: the space's one set (R257)
    loose.abort()
    space.close()
