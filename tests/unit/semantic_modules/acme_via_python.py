"""The registry module ``acme_via_python`` (no dependencies): a maintained aggregate over a neutral computed
whose aggregate goes through a ``via`` from ``self`` to a Python-only computed term (re-review finding 1)."""
from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import Aggregate, Computed, Relation

via_python = module("acme_via_python")


class Part(DBObject):
    __type_name__ = "acme.via.Part"
    py = Computed(lambda self: 5)
    thing = Relation("acme.via.Thing", cardinality="many-to-one", inverse="parts")


class Thing(DBObject):
    __type_name__ = "acme.via.Thing"
    parts = Relation("acme.via.Part", cardinality="one-to-many", inverse="thing")
    via_parts = Computed(["sum", ["via", ["self"], "parts"], ["attr", "py"]])
    owner = Relation("acme.via.Box", cardinality="many-to-one", inverse="things")


class Box(DBObject):
    __type_name__ = "acme.via.Box"
    things = Relation("acme.via.Thing", cardinality="one-to-many", inverse="owner")
    total = Aggregate(over="things", sum="via_parts", maintained=True)
