"""The registry module ``erp_forecast``: a later module that extends the semantic ERP of ``erp_model``.

Chapter 5 of the semantic model tutorial (``docs/tutorial/semantic-model/05-modules-and-composition.md``)
installs it. Without editing ``erp_model`` it adds a field to the opportunity, relabels its title, and declares
a subtype of the person, a key account, which an opportunity accepts as its contact.
"""
from datetime import date

from proto_db.registry import module
from proto_db.semantic import Enum, Extension, Field

from erp_model import Person

forecast = module("erp_forecast", depends=("erp_model",))


class OpportunityForecast(Extension):
    __extends__ = "erp.Opportunity"
    close_date = Field(date, meaning="The date the sale is expected to close")
    title = Field(str, required=True, label="Subject")        # the same field, a new label


class KeyAccount(Person):
    __type_name__ = "erp.KeyAccount"
    __meaning__ = "A contact at a customer the company manages as a key account"
    __label__ = "Key account"
    tier = Field(Enum("gold", "silver"), default="silver", meaning="The service level agreed with the customer")
