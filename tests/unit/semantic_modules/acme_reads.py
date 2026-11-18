"""The registry module ``acme_reads`` (no dependencies): computed attributes that read an rc4 data default
and other computed attributes (review I1, I2: the expression reader follows F15, stored value, then the
catalogued default, else MISSING)."""
from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import Aggregate, Computed, Field, Relation

reads = module("acme_reads")


@reads.extends("acme.reads.Thing")
class ThingData:
    rate = 3                                # an rc4 data default in the type's chain


class Thing(DBObject):
    __type_name__ = "acme.reads.Thing"
    a = Field(int)
    b = Field(int, default=10)
    plus_rate = Computed("a + rate")
    plus_b = Computed("a + b")
    twice = Computed("a * 2")
    chained = Computed("twice + 1")
    direct = Computed("a * 2 + 1")
    holder = Relation("acme.reads.Holder", cardinality="many-to-one", inverse="things")


class Holder(DBObject):
    __type_name__ = "acme.reads.Holder"
    things = Relation("acme.reads.Thing", cardinality="one-to-many", inverse="holder")
    total = Aggregate(over="things", sum="twice")
    count = Aggregate(over="things", count=True)
