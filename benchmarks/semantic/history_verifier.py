"""The history verifier of the credit experiment (spec section 15, Task 10; preflight row 48).

:func:`verify_history` walks every committed version of a database that holds the demo's order-entry model
(``docs/kit/semantic/acme_sales.py``) and, in each version, recomputes from the order lines what every customer
has committed, then compares it with the customer's credit limit and with the aggregate the database maintains.

It is an independent oracle, and deliberately duplicates what the semantic layer computes:

- it reads the stored records raw (``HistoryEntry.get_mutable``), never through the semantic session, its
  navigation, its queries or its computed attributes;
- it starts from the order cells, not from the customer's ``orders`` collection, so an order missing from that
  collection still weighs on the customer its ``customer`` role names;
- it re-states the model's rules here, by hand: a line's amount is ``quantity * price``; an order's total is the
  sum of its lines' amounts; a customer's committed amount is the sum of the totals of its orders whose state is
  not ``cancelled`` (an order with no stored state is ``open``, the declared default); the credit rule is
  ``committed <= credit_limit``, false when the limit is absent;
- it never calls the code that maintains aggregates by deltas (``proto_db.semantic.entities``) or checks
  invariants (``proto_db.semantic.constraints``).

The duplication is the point: a verifier that reused the maintained-aggregate code would agree with any error of
that code, and could never show it.  If the model's rules change, this module must be changed by hand.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

ORDER_TYPE = "acme.sales.Order"
CUSTOMER_TYPE = "acme.sales.Customer"
#: The state that takes an order out of its customer's committed amount (the aggregate's ``where``).
CANCELLED = "cancelled"


@dataclass
class VerifierReport:
    """What :func:`verify_history` found.

    ``versions`` is the number of versions walked (every entry of ``Database.history()``).  ``violations``
    lists ``(version, customer cell, committed, limit)`` for every version in which a customer's recomputed
    committed amount is above its limit (``limit`` is ``None`` when absent).  ``aggregate_mismatches`` lists
    ``(version, cell, attribute, recomputed, stored)`` for every version in which a maintained aggregate (a
    customer's ``committed``, an order's ``total``) differs from the recomputation."""

    versions: int = 0
    violations: list = field(default_factory=list)
    aggregate_mismatches: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """True when no version holds a violation or a mismatch."""
        return not self.violations and not self.aggregate_mismatches


def _cells(entry, type_name: str):
    """``(name, record)`` of every cell of ``type_name`` in ``entry`` (a cell is ``<type name>#<hex>``)."""
    prefix = type_name + "#"
    for name in entry.iter_mutable_names(after=type_name):
        if not name.startswith(prefix):
            break                       # names come in code point order: the type's cells are contiguous
        yield name, entry.get_mutable(name)


def _order_total(order) -> Decimal:
    """The sum of ``quantity * price`` over the order's lines (an order with no lines totals 0)."""
    total = Decimal(0)
    for line in order.lines or ():
        total += line.quantity * line.price
    return total


def _check_version(entry, report: VerifierReport) -> None:
    committed = {}
    for name, order in _cells(entry, ORDER_TYPE):
        total = _order_total(order)
        if order.total != total:
            report.aggregate_mismatches.append((entry.version, name, "total", total, order.total))
        state = order.state if order.state is not None else "open"
        if state != CANCELLED and order.customer is not None:
            customer = order.customer.name
            committed[customer] = committed.get(customer, Decimal(0)) + total
    for name, customer in _cells(entry, CUSTOMER_TYPE):
        amount = committed.get(name, Decimal(0))
        limit = customer.credit_limit
        if limit is None or amount > limit:
            report.violations.append((entry.version, name, amount, limit))
        if customer.committed is None or customer.committed != amount:
            report.aggregate_mismatches.append((entry.version, name, "committed", amount, customer.committed))


def verify_history(db) -> VerifierReport:
    """Walk ``db.history()`` (every committed version) and check the credit rule and the maintained aggregates
    of each version against a recomputation from the order lines (see the module docstring)."""
    report = VerifierReport()
    for entry in db.history():
        report.versions += 1
        with db.snapshot_at(entry):     # records resolve with this version's module list
            _check_version(entry, report)
    return report
