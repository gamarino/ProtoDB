"""The registry module ``acme_disagree`` (no dependencies): a relationship whose role and its declared
inverse end disagree on ``ordered`` (refused at composition)."""
from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import Field, RelationTo, Relationship, Role

disagree = module("acme_disagree")


class Basket(Relationship):
    __type_name__ = "acme.disagree.Basket"
    owner = Role("acme.disagree.Shopper", inverse="baskets", ordered=True)
    item = Role("acme.disagree.Shopper")
    size = Field(int)


class Shopper(DBObject):
    __type_name__ = "acme.disagree.Shopper"
    baskets = RelationTo(Basket, role="owner", ordered=False)
