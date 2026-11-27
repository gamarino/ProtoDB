"""The registry module ``erp_model``: the tutorials' ERP declared with the semantic model.

The same small ERP as ``erp_app.py`` (users, groups, persons and opportunities; a model only, no ERP
features), declared so that the database describes itself: what each entity means, its fields, its relations
in both directions, what is computed and what is checked. Chapter 3 of the complete tutorial introduces it;
the semantic model tutorial (``docs/tutorial/semantic-model``) explains every line.

The registry module name is the import path, ``erp_model``: the tutorials put ``examples/tutorial`` on
``sys.path``. A registry module name is process-wide (one name, one Python module per process).
"""
from decimal import Decimal

from proto_db import DBObject
from proto_db.registry import module
from proto_db.semantic import (Aggregate, Computed, Enum, Field, Invariant, Relation, RelationTo, Relationship, Role,
                               intent)

erp = module("erp_model")


class Group(DBObject):
    __type_name__ = "erp.Group"
    __meaning__ = "A group of users that share permissions"
    __label__ = "Group"
    __key__ = ("name",)
    name = Field(str, required=True, label="Name")
    memberships = RelationTo("erp.Membership", role="group")


class User(DBObject):
    __type_name__ = "erp.User"
    __meaning__ = "A person who signs in to the ERP and owns opportunities"
    __label__ = "User"
    __key__ = ("login",)
    login = Field(str, required=True, label="Login")
    name = Field(str, required=True, label="Name", sensitivity="personal")
    memberships = RelationTo("erp.Membership", role="user")
    opportunities = Relation("erp.Opportunity", cardinality="one-to-many", inverse="owner")
    pipeline = Aggregate(over="opportunities", where="stage != 'lost'", sum="amount", maintained=True,
                         meaning="The total amount of the user's opportunities that are not lost")


class Membership(Relationship):
    __type_name__ = "erp.Membership"
    __meaning__ = "A user belongs to a group, in a role"
    user = Role("erp.User", inverse="memberships")
    group = Role("erp.Group", inverse="memberships")
    role = Field(Enum("member", "manager"), default="member", meaning="What the user may do in the group")


class Person(DBObject):
    __type_name__ = "erp.Person"
    __meaning__ = "A contact at a customer company"
    __label__ = "Person"
    name = Field(str, required=True, label="Name", sensitivity="personal")
    company = Field(str, label="Company")
    opportunities = Relation("erp.Opportunity", cardinality="one-to-many", inverse="contact")
    quotes = RelationTo("erp.Quote", role="recipient")


class Opportunity(DBObject):
    __type_name__ = "erp.Opportunity"
    __meaning__ = "A possible sale to a customer, owned by a user, moving through stages"
    __label__ = "Opportunity"
    __synonyms__ = ["deal"]
    number = Field(int, required=True, label="Number")
    title = Field(str, required=True, label="Title")
    stage = Field(Enum("lead", "qualified", "proposal", "won", "lost"), default="lead", label="Stage")
    amount = Field(Decimal, required=True, unit="currency:company", meaning="The amount of the sale if it is won")
    probability = Field(Decimal, default=Decimal("0.1"), meaning="The chance of winning, from 0 to 1")
    expected = Computed("amount * probability", unit="currency:company",
                        meaning="The amount weighted by the chance of winning")
    owner = Relation("erp.User", cardinality="many-to-one", inverse="opportunities")
    contact = Relation("erp.Person", cardinality="many-to-one", inverse="opportunities")
    quotes = RelationTo("erp.Quote", role="opportunity", ordered=True)

    __constraints__ = [
        Invariant("probability >= 0 and probability <= 1", name="probability_range",
                  meaning="A probability is between 0 and 1", message="The probability {probability} is not "
                                                                       "between 0 and 1"),
    ]

    @intent(meaning="Move the opportunity to another stage; a won or lost opportunity is closed",
            requires="stage != 'won' and stage != 'lost'", parameters={"stage": "str"})
    def move_to(self, stage):
        return self.with_(stage=stage)


class Quote(Relationship):
    __type_name__ = "erp.Quote"
    __meaning__ = "An offer sent to a contact for an opportunity, with the terms of that moment"
    opportunity = Role("erp.Opportunity", inverse="quotes", ordered=True)
    recipient = Role("erp.Person", inverse="quotes", binding="version")
    amount = Field(Decimal, required=True, unit="currency:company", meaning="The amount offered in this quote")
