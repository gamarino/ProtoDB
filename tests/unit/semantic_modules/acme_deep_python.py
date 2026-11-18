"""The registry module ``acme_deep_python`` (no dependencies): a maintained aggregate over a neutral computed
that reads a Python-only computed (review I3: refused, neutral terms all the way down)."""
from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import Aggregate, Computed, Field, Relation

deep_python = module("acme_deep_python")


class Thing(DBObject):
    __type_name__ = "acme.deep.Thing"
    a = Field(int)
    py = Computed(lambda self: 5)
    via_py = Computed("py + 1")
    via_via = Computed("via_py * 2")
    owner = Relation("acme.deep.Box", cardinality="many-to-one", inverse="things")


class Box(DBObject):
    __type_name__ = "acme.deep.Box"
    things = Relation("acme.deep.Thing", cardinality="one-to-many", inverse="owner")
    total = Aggregate(over="things", sum="via_via", maintained=True)
