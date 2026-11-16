"""The registry module ``acme_audit`` (depends on ``acme_sales``), for the intent tests (Task 8):

- it redefines ``Order.add_line``: the redefinition records each run in :data:`RUNS` (the name of the thread
  that ran it) and calls the earlier implementation through ``_super()``, so a test can count how often each
  module's implementation runs at record time and at replay;
- it adds the intent ``Order.follow_up(number)``, which creates another order of the same company and
  customer through ``intent_context().create`` and returns its handle: a creation inside an intent, whose
  cell name is drawn when the call is recorded and reused by the replay (R260).
"""
import threading

from proto_db.registry import module
from proto_db.semantic import Extension, intent, intent_context

audit = module("acme_audit", depends=("acme_sales",))

#: The thread name of every run of the redefined ``add_line``, in order.
RUNS: list = []


class OrderAudit(Extension):
    __extends__ = "acme.sales.Order"

    @intent(meaning="Add a product to the order with an agreed quantity and price; the run is audited",
            parameters={"product": "acme.catalog.Product", "quantity": "decimal", "price": "decimal"})
    def add_line(self, product, quantity, price):
        RUNS.append(threading.current_thread().name)
        return self._super().add_line(product, quantity, price)

    @intent(meaning="Open a follow-up order of the same company and customer", parameters={"number": "int"})
    def follow_up(self, number):
        return intent_context().create("acme.sales.Order", number=number, date=self.date, company=self.company,
                                       customer=self.customer)
