"""The registry module ``acme_keyed`` (no dependencies): ends of the shapes the demo does not have, for the
relation tests (Task 6, R262).

- ``Shelf.items`` is a ``many`` end of entity handles with a maintained key through the members' identity
  role ``maker`` (``maker.name``): linking an item's maker, or renaming a maker, re-keys its entries;
- ``Placement`` is a relationship whose role ``shelf`` indexes its inverse end ``Shelf.placements`` by the
  member attribute ``slot`` (``RangeIndex("by_slot", "slot")``);
- ``Maker.tagged`` is a ``many-to-many`` end without an inverse indexed by a member attribute of entity
  handles, a shape the MVP refuses (an index reads stored attributes of records, never through a handle).
"""
from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import Aggregate, Field, Relation, Relationship, Role

keyed = module("acme_keyed")


class Maker(DBObject):
    __type_name__ = "acme.keyed.Maker"
    name = Field(str, required=True)
    items = Relation("acme.keyed.Item", cardinality="one-to-many", inverse="maker")
    tagged = Relation("acme.keyed.Item", cardinality="many-to-many", index_by=["weight"])


class Item(DBObject):
    __type_name__ = "acme.keyed.Item"
    weight = Field(int, required=True)
    shelf = Relation("acme.keyed.Shelf", cardinality="many-to-one", inverse="items")


class Placement(Relationship):
    __type_name__ = "acme.keyed.Placement"
    shelf = Role("acme.keyed.Shelf", inverse="placements", index_by=["slot"])
    item = Role("acme.keyed.Item", inverse="placements")
    slot = Field(int, required=True)


class Shelf(DBObject):
    __type_name__ = "acme.keyed.Shelf"
    label = Field(str)
    items = Relation("acme.keyed.Item", cardinality="one-to-many", inverse="shelf", index_by=["maker.name"])
    weight = Aggregate(over="items", sum="weight", maintained=True)
    count = Aggregate(over="items", count=True, maintained=True)
