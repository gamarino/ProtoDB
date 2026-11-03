LINQ-like API
=============

ProtoBase provides a lazy, composable, LINQ-like query API (Phase 1) that works over plain Python iterables and ProtoBase collections/query plans.
It allows filtering, projection, ordering, grouping and aggregation with an introspectable DSL and policy-driven fallbacks.

Overview
--------

- Queryable[T]: lazy pipeline over items of type T.
- F: field/expressions DSL (e.g., F.age >= 18, F.name.startswith("A"), F.value.between(10, 20)).
- Policy: controls behavior for unsupported expressions (error|warn|fallback) and local execution limits.
- Grouping[K, E]: result element for group_by.
- from_collection: entrypoint to wrap Python lists, ProtoBase collections, or QueryPlan.

Quick examples
--------------

.. code-block:: python

    from proto_db.linq import from_collection, F

    users = [
        {"id": 1, "first_name": "Alice", "last_name": "Zeus", "age": 30, "country": "ES", "status": "active", "last_login": 5},
        {"id": 2, "first_name": "Bob", "last_name": "Young", "age": 17, "country": "AR", "status": "inactive", "last_login": 10},
        {"id": 3, "first_name": "Carol", "last_name": "Xavier", "age": 25, "country": "US", "status": "active", "last_login": 2},
        {"id": 4, "first_name": "Dan", "last_name": "White", "age": 22, "country": "AR", "status": "active", "last_login": 7},
    ]

    q = (from_collection(users)
         .where((F.age >= 18) & F.country.in_(["ES", "AR"]))
         .order_by(F.last_login, ascending=False)
         .select({"id": F["id"], "name": F.first_name + " " + F.last_name})
         .take(20))

    res = q.to_list()

Distinct and count:

.. code-block:: python

    emails = from_collection(users).select(F.email).distinct().to_list()
    active = from_collection(users).where(F.status == "active").count()

Grouping with aggregates:

.. code-block:: python

    q = (from_collection(orders)
         .where(F.status == "completed")
         .group_by(F.customer_id, element_selector=F.total_amount)
         .select({"customer_id": F.key(), "orders": F.count(), "sum": F.sum(), "avg": F.average()})
         .order_by(F.sum(), ascending=False)
         .take(10))
    top = q.to_list()

Hierarchical traversal with traverse()
--------------------------------------

Use traverse() to recursively walk object graphs either following an upward reference or expanding a collection downward. Parameters:

- relation_attr: attribute name to follow
- direction: 'up' (single reference) or 'down' (iterate collection)
- strategy: 'dfs' or 'bfs'
- max_depth: limit depth (-1 = unlimited)
- include_start_node: include starting nodes in output

Example:

.. code-block:: python

    chain = (from_collection(employees)
             .where(F.name == 'Employee1')
             .traverse('manager', direction='up', strategy='dfs'))
    names = [e.name for e in chain]

Between and lambda chained comparisons
--------------------------------------

The DSL supports ranges via between/ range bounds and also detects simple chained comparisons in lambdas:

.. code-block:: python

    from datetime import datetime

    # Inclusive by default
    q1 = from_collection(rows).where(F.value.between(10, 20))

    # Exclusive upper bound
    q2 = from_collection(rows).where(F.date.between(start, end, inclusive=(True, False)))

    # Lambda translated to between
    q3 = from_collection(rows).where(lambda x: 10 <= x.value <= 20)

Execution, optimization and explain
-----------------------------------

Pipelines are lazy. Terminal operators materialize results (to_list, count, first, any, sum, min, max, average).
When the source is a ProtoBase collection or a QueryPlan, supported filters and projections are translated to
ProtoBase WherePlan/SelectPlan. Index-aware filtering is leveraged by the underlying WherePlan when applicable.

Use explain() to inspect:

.. code-block:: python

    exp = q.explain()              # text summary
    exp_json = q.explain("json")   # dict structure with plan prefix and local ops

Policies and safety limits
--------------------------

.. code-block:: python

    from proto_db.linq import Policy

    # Error on unsupported predicates
    q = from_collection(items).with_policy(Policy(on_unsupported="error"))

    # Warn and fallback to local evaluation up to limits
    q = (from_collection(items)
         .on_unsupported("warn")
         .where(lambda x: custom_python_check(x))
         .take(100))

Policy fields:

- on_unsupported: "error" | "warn" | "fallback"
- max_rows_local: int (default 100k)
- max_memory_mb: int (default 256) [soft guideline]
- timeout_ms: int (0 means no timeout)

API surface
-----------

- Queryable[T]
  - where, select, select_many, order_by, then_by, distinct, take, skip, group_by, traverse
  - to_list, to_set, to_dict, first, first_or_default, any, all, count, sum, min, max, average
  - with_policy, on_unsupported, explain
- F: field/expressions with operators ==, !=, >, >=, <, <=, in_(), contains(), startswith(), endswith(),
     between(), between_closed/open/left_open/right_open, range(bounds)
- Grouping[K, E]
- Policy
- from_collection(source)
