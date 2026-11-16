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
    **{f"r282_{field}": f"{_PACKAGE}.acme_r282" for field in ("value_type", "cardinality", "target", "binding",
                                                               "kind")},
}
