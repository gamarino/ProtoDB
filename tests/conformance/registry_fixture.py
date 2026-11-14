"""The registry modules the ``registry`` conformance vector installs (``generate.py``).

``conformance.sales`` depends on ``conformance.base``; both are mapped to this Python module
(:data:`MAPPING`).  Installing ``conformance.sales`` writes the module list, the catalog and a
catalog object for each of the registry names below.
"""
from proto_db.registry import module

#: Registry module name -> the import path of the Python module that declares it.
MAPPING = {"conformance.base": __name__, "conformance.sales": __name__}

base = module("conformance.base")
sales = module("conformance.sales", depends=("conformance.base",))


@base.extends("acme.Partner")
class Partner:
    kind = "partner"


@sales.extends("acme.sales.Order", parents=("acme.Partner",))
class Order:
    status = "draft"
