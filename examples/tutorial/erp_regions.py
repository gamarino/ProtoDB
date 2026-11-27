"""The registry module ``erp_regions``: a schema change installed into a database that already has entities.

Chapter 6 of the semantic model tutorial (``docs/tutorial/semantic-model/06-schema-evolution.md``) installs it.
It adds a required field to the opportunity and a maintained aggregate to the user: what a later module may add,
and what it means for the entities that existed before it.
"""
from proto_db.registry import module
from proto_db.semantic import Aggregate, Extension, Field

regions = module("erp_regions", depends=("erp_model",))


class OpportunityRegion(Extension):
    __extends__ = "erp.Opportunity"
    region = Field(str, required=True, label="Region", meaning="The sales region the opportunity belongs to")


class UserWon(Extension):
    __extends__ = "erp.User"
    won = Aggregate(over="opportunities", where="stage == 'won'", sum="amount", maintained=True,
                    meaning="The total amount of the user's won opportunities")
