"""The registry module ``erp_crm``: a later module that refines ``erp_labels`` without editing it.

Chapter 8 of the complete tutorial installs it after ``erp_labels``. Its ``label`` calls the definition it
overrides through ``self._super()``, and its ``currency`` default replaces the earlier one.
"""
from proto_db.registry import module

crm = module("erp_crm", depends=("erp_labels",))


@crm.extends("erp.Opportunity")
class Opportunity:
    currency = "USD"

    def label(self):
        return f"{self._super().label()}, expected {self.expected} {self.currency}"
