"""
LINQ-like API: group_by with aggregates.
Run: python examples/linq_groupby.py
"""

from proto_db.linq import from_collection, F


def main():
    orders = [
        {"id": 1, "customer_id": 1, "status": "completed", "total_amount": 10.0},
        {"id": 2, "customer_id": 1, "status": "completed", "total_amount": 15.0},
        {"id": 3, "customer_id": 2, "status": "completed", "total_amount": 8.0},
        {"id": 4, "customer_id": 2, "status": "cancelled", "total_amount": 9.0},
    ]

    q = (
        from_collection(orders)
        .where(F.status == "completed")
        .group_by(F.customer_id, element_selector=F.total_amount)
        .select({"customer_id": F.key(), "orders": F.count(), "sum": F.sum(), "avg": F.average()})
        .order_by(F.sum(), ascending=False)
        .take(10)
    )

    top = q.to_list()
    print("Top customers by order sum (completed only):")
    for row in top:
        print(row)


if __name__ == "__main__":
    main()
