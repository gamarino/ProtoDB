"""The registry module ``erp_labels``: behaviour added to the opportunities that ``erp_model`` declares.

Chapter 8 of the complete tutorial (``docs/tutorial/08-extensible-models.md``) installs it. It does not touch
``erp_model``: it extends the same registry name, ``erp.Opportunity``, with a method and a data default. The
database stores only the module's name; the code lives here.
"""
from proto_db.registry import module

labels = module("erp_labels", depends=("erp_model",))


@labels.extends("erp.Opportunity")
class Opportunity:
    currency = "ARS"                                  # a data default

    def label(self):                                  # a method
        return f"#{self.number} {self.title} ({self.stage})"
