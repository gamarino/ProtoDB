"""Semantic orders committed concurrently on the cell path (spec section 9; R3, R53).

8 threads create 200 orders of one company, each for a customer of its own, through ``Database.run``.  Every order
joins the company's ``orders`` end, a collection all the commits change, so they conflict on the company cell (the
unit of conflict is the cell, R3) and ``run`` retries each on the state current then.

Catches: a retry that loses an order (a last-writer-wins company record), adds one twice, or gives up;
maintained aggregates moved twice or not at all; a deadlock between group-commit leaders and the threads they
build.  No sleep decides anything (R82): the threads are released by a barrier and every ``run`` is joined.
"""
from datetime import date
from decimal import Decimal

import pytest

from proto_db.semantic import semantic
from proto_db.storage.file import FileStorage
from proto_db.transactions import ObjectSpace
from tests.concurrency.harness import report, run_threads
from tests.unit.semantic_modules import MAPPING

THREADS = 8
ORDERS = 200
RETRIES = 100_000                  # never the limit: each conflict means another thread committed

pytestmark = [pytest.mark.freethreaded, pytest.mark.timeout(240)]


def test_orders_of_one_company_all_commit_through_run(tmp_path):
    space = ObjectSpace(FileStorage(str(tmp_path / "space")), modules_mapping=MAPPING)
    try:
        db = space.new_database("db")
        db.run(lambda tr: tr.install_module("acme_sales"))

        def world(tr):
            s = semantic(tr)
            company = s.create("acme.core.Company", name="ACME")
            product = s.create("acme.catalog.Product", name="widget", list_price=Decimal("10"), company=company)
            customers = [s.create("acme.sales.Customer", name=f"customer {i}", credit_limit=Decimal("100000"),
                                  company=company) for i in range(THREADS)]
            return company, product, customers
        company, product, customers = db.run(world)
        created = [[] for _ in range(THREADS)]

        def worker(i):
            def body():
                for k in range(ORDERS // THREADS):
                    number = i * (ORDERS // THREADS) + k + 1
                    def place(tr):
                        s = semantic(tr)
                        order = s.create("acme.sales.Order", number=number, date=date(2026, 6, 1 + number % 28),
                                         company=company, customer=customers[i])
                        s.get(order).add_line(product, Decimal("1"), Decimal(number))
                        return order
                    order = db.run(place, retries=RETRIES, backoff=(0.0, 0.002))     # a conflict is retried
                    created[i].append((order, number))
            return body

        elapsed = run_threads([worker(i) for i in range(THREADS)])
        everything = [pair for orders in created for pair in orders]
        assert len(everything) == ORDERS
        tr = db.new_transaction()
        try:
            s = semantic(tr)
            members = [v.entity for v in s.get(company).orders]
            assert len(members) == ORDERS and set(members) == {order for order, _ in everything}
            for i, customer in enumerate(customers):
                bob = s.get(customer)
                assert sorted(v.entity.name for v in bob.orders) == sorted(o.name for o, _ in created[i])
                assert bob.committed == sum(Decimal(n) for _, n in created[i])
            assert len(s.get(product).order_lines) == ORDERS
            assert all(s.get(order).total == Decimal(n) for order, n in everything)
        finally:
            tr.abort()
        assert len(db.history()) == ORDERS + 3                  # one version per commit
        report("cell-path orders", elapsed, f"{THREADS} threads", f"{ORDERS} commits")
    finally:
        space.close()
    # What the commits built was saved with them: a reopened space reads it from the storage.
    reopened = ObjectSpace(FileStorage(str(tmp_path / "space")), modules_mapping=MAPPING)
    try:
        tr = reopened.open_database("db").new_transaction()
        s = semantic(tr)
        assert len(s.get(company).orders) == ORDERS and len(s.get(product).order_lines) == ORDERS
        assert sum(s.get(customer).committed for customer in customers) == sum(range(1, ORDERS + 1))
        tr.abort()
    finally:
        reopened.close()
