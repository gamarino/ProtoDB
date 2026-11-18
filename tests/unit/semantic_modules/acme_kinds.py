"""The registry module ``acme_kinds``: derived fields whose expressions compare across D28 type ranks, total
functions (false) under D28 that an agent query must still answer (R283 fix round 1: only the agent's own
expressions follow the agent-query rules)."""
from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import Aggregate, Computed, Field, Relation

kinds = module("acme_kinds")


class Thing(DBObject):
    __type_name__ = "acme.kinds.Thing"
    code = Field(str)
    is_five = Computed(["==", ["attr", "code"], ["const", 5]])          # a text against a number: false
    holder = Relation("acme.kinds.Holder", cardinality="many-to-one", inverse="things")


class Holder(DBObject):
    __type_name__ = "acme.kinds.Holder"
    name = Field(str)
    things = Relation("acme.kinds.Thing", cardinality="one-to-many", inverse="holder")
    fives = Aggregate(over="things", count=True, where=["==", ["attr", "code"], ["const", 5]])
