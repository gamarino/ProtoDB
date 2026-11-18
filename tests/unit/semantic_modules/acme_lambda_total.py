"""The registry module ``acme_lambda_total`` (no dependencies): a maintained aggregate over a Python-only
computed attribute (refused at composition: maintained terms are neutral all the way down)."""
from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import Aggregate, Computed, Field, Relation

lambda_total = module("acme_lambda_total")


class Item(DBObject):
    __type_name__ = "acme.lambdas.Item"
    price = Field(int)
    doubled = Computed(lambda self: self.price * 2)
    bag = Relation("acme.lambdas.Bag", cardinality="many-to-one", inverse="items")


class Bag(DBObject):
    __type_name__ = "acme.lambdas.Bag"
    items = Relation("acme.lambdas.Item", cardinality="one-to-many", inverse="bag")
    total = Aggregate(over="items", sum="doubled", maintained=True)
