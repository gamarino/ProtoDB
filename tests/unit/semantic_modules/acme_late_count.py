"""The registry module ``acme_late_count`` (depends on ``acme_sales``), for the final-review tests (I1): it
adds maintained aggregates to types that may already have entities when it is installed: ``Order.line_count``
(the number of the order's lines) and ``Customer.open_orders`` (the number of its orders not cancelled).
"""
from proto_db.registry import module
from proto_db.semantic import Aggregate, Extension

late_count = module("acme_late_count", depends=("acme_sales",))


class OrderLineCount(Extension):
    __extends__ = "acme.sales.Order"

    line_count = Aggregate(over="lines", count=True, maintained=True)


class CustomerOpenOrders(Extension):
    __extends__ = "acme.sales.Customer"

    open_orders = Aggregate(over="orders", where="state != 'cancelled'", count=True, maintained=True)
