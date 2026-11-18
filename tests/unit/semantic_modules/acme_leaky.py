"""The registry module ``acme_leaky``: a personal field read by computed attributes that fail on it, one of
them Python-only with a call counter (Task 7 fix round 2: a withheld path is never evaluated)."""
from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import Computed, Field, Relation

leaky = module("acme_leaky")

#: One entry per evaluation of ``Person.py``.
CALLS: list = []


def _python_only(self):
    CALLS.append(1)
    return int(self.name)


class Org(DBObject):
    __type_name__ = "leak.Org"
    name = Field(str)
    oops = Computed("name + 1")
    people = Relation("leak.Person", cardinality="one-to-many", inverse="org")


class Person(DBObject):
    __type_name__ = "leak.Person"
    name = Field(str, sensitivity="personal")
    bad = Computed("name + 1")
    py = Computed(_python_only)
    org = Relation("leak.Org", cardinality="many-to-one", inverse="people")
