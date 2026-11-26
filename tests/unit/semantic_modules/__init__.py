"""Python modules that declare semantic registry modules, for the ``tests/unit/test_semantic_*.py`` tests.

Each file declares the registry module(s) its docstring names; the tests map those names to these
import paths (:data:`MAPPING`), together with the demo's modules under their canonical import path
``docs.kit.semantic`` (preflight F41).
"""
_PACKAGE = __name__

#: Registry module name -> import path, for every semantic fixture module.
MAPPING = {
    "acme_sales": "docs.kit.semantic.acme_sales",
    "acme_delivery": "docs.kit.semantic.acme_delivery",
    "acme_rush": f"{_PACKAGE}.acme_rush",
    "acme_express": f"{_PACKAGE}.acme_express",
    "acme_zlate": f"{_PACKAGE}.acme_zlate",
    "acme_decorated": f"{_PACKAGE}.acme_decorated",
    "acme_subclassed": f"{_PACKAGE}.acme_subclassed",
    "acme_restates": f"{_PACKAGE}.acme_restates",
    "acme_diamond": f"{_PACKAGE}.acme_diamond",
    "acme_disagree": f"{_PACKAGE}.acme_disagree",
    "acme_lambda_total": f"{_PACKAGE}.acme_lambda_total",
    "acme_reads": f"{_PACKAGE}.acme_reads",
    "acme_deep_python": f"{_PACKAGE}.acme_deep_python",
    "acme_via_python": f"{_PACKAGE}.acme_via_python",
    "acme_a_sub": f"{_PACKAGE}.acme_a_sub",
    "acme_x_foo": f"{_PACKAGE}.acme_x_foo",
    "acme_ghost": f"{_PACKAGE}.acme_ghost",
    "acme_keyed": f"{_PACKAGE}.acme_keyed",
    "acme_leaky": f"{_PACKAGE}.acme_leaky",
    "acme_kinds": f"{_PACKAGE}.acme_kinds",
    "acme_audit": f"{_PACKAGE}.acme_audit",
    "acme_late_count": f"{_PACKAGE}.acme_late_count",
    "acme_credit_cap": f"{_PACKAGE}.acme_credit_cap",
    "acme_tighten": f"{_PACKAGE}.acme_tighten",
    "acme_overrides": f"{_PACKAGE}.acme_overrides",
    **{name: f"{_PACKAGE}.acme_constraints" for name in ("acme_informative", "acme_far_invariant",
                                                          "acme_favourites")},
    **{f"r282_{field}": f"{_PACKAGE}.acme_r282" for field in ("value_type", "cardinality", "target", "binding",
                                                               "kind")},
    **{f"semc_0{i}": f"{_PACKAGE}.semc_0{i}" for i in range(1, 9)},
}
#: The mapping of a writer whose code for ``acme_sales`` declares no semantic element (a database written
#: before the semantic layer existed, Review Focus 1); used in a process of its own.
RC4_MAPPING = {"acme_sales": f"{_PACKAGE}.rc4_acme_sales"}
