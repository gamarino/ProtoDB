"""The registry module ``acme_express`` (depends on ``acme_sales``): ``ExpressOrder``, a subclass of
``Order`` that redefines the presentation of ``number`` and the implementation of ``confirm``."""
from proto_db.registry import module
from proto_db.semantic import Field, intent

from docs.kit.semantic.acme_sales import Order

express = module("acme_express", depends=("acme_sales",))


class ExpressOrder(Order):
    __type_name__ = "acme.sales.ExpressOrder"
    number = Field(int, label="Express no.")

    @intent(meaning="Confirm the express order: confirmed, and flagged as expedited")
    def confirm(self):
        confirmed = self._super().confirm()
        return confirmed.with_(expedited=True)
