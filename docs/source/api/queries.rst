Query System
=============

.. module:: proto_db.queries

This module provides the query system of ProtoBase, which allows for complex data manipulation and retrieval.

New in this release
---------------------

- Range operators between[]/()/[)/(] with proper bound inclusivity via the Between operator.
- Collection-oriented plans: UnnestPlan and CollectionFieldPlan.
- Index-aware filtering optimization in WherePlan for AND conjunctions using progressive intersection ordered by selectivity.
- Range pushdown over indexes via IndexedRangeSearchPlan.

Query Plans
-----------

QueryPlan
~~~~~~~~

.. autoclass:: proto_db.common.QueryPlan
   :members:
   :special-members: __init__

The ``QueryPlan`` class is the base class for all query plans in ProtoBase. It provides methods for query execution and chaining.

FromPlan
~~~~~~~

.. autoclass:: FromPlan
   :members:
   :special-members: __init__

``FromPlan`` is the starting point for queries. It takes a collection as input, provides an iterator over the collection, and can be used as the basis for other query plans.

WherePlan
~~~~~~~~

.. autoclass:: WherePlan
   :members:
   :special-members: __init__

``WherePlan`` filters records based on a condition. It takes a filter function and a base plan, returns only records that satisfy the condition, and can be chained with other query plans.

JoinPlan
~~~~~~~

.. autoclass:: JoinPlan
   :members:
   :special-members: __init__

``JoinPlan`` joins multiple data sources. It takes two plans and a join condition, returns records that satisfy the join condition, and supports inner, left, right, and full joins.

GroupByPlan
~~~~~~~~~~

.. autoclass:: GroupByPlan
   :members:
   :special-members: __init__

``GroupByPlan`` groups records by a key. It takes a key function and a base plan, returns groups of records with the same key, and can be used for aggregation.

OrderByPlan
~~~~~~~~~~

.. autoclass:: OrderByPlan
   :members:
   :special-members: __init__

``OrderByPlan`` sorts records. It takes a key function and a base plan, returns records sorted by the key, and supports ascending and descending order.

SelectPlan
~~~~~~~~~

.. autoclass:: SelectPlan
   :members:
   :special-members: __init__

``SelectPlan`` projects specific fields. It takes a projection function and a base plan, returns transformed records, and can be used to extract specific fields.

CountPlan
~~~~~~~~

.. autoclass:: CountPlan
   :members:
   :special-members: __init__

``CountPlan`` counts the results from a sub-plan. It takes a base plan and returns a single record with the count. It is optimized to use index counts whenever possible, avoiding full data iteration.

CountResultPlan
~~~~~~~~~~~~~

.. autoclass:: CountResultPlan
   :members:
   :special-members: __init__

``CountResultPlan`` is a terminal plan that holds and returns a pre-calculated count. It is the result of an optimized CountPlan.

LimitPlan
~~~~~~~~

.. autoclass:: LimitPlan
   :members:
   :special-members: __init__

``LimitPlan`` limits the number of records returned. It takes a limit and a base plan, and returns at most the specified number of records.

OffsetPlan
~~~~~~~~~

.. autoclass:: OffsetPlan
   :members:
   :special-members: __init__

``OffsetPlan`` skips a number of records. It takes an offset and a base plan, and returns records starting from the specified offset.

ListPlan
~~~~~~~

.. autoclass:: ListPlan
   :members:
   :special-members: __init__

``ListPlan`` is a simple plan that wraps a list. It provides an iterator over the list and can be used as the basis for other query plans.

UnnestPlan
~~~~~~~~~~

.. autoclass:: UnnestPlan
   :members:
   :special-members: __init__

``UnnestPlan`` flattens an iterable found in each input record (by dotted path or callable). It emits one row per element. If an element_alias is provided, the element is attached to the original record under that key; otherwise the element replaces the record.

CollectionFieldPlan
~~~~~~~~~~~~~~~~~~~

.. autoclass:: CollectionFieldPlan
   :members:
   :special-members: __init__

``CollectionFieldPlan`` evaluates a per-record subplan and assigns the collected results (as a list) to the given field name, preserving the original record.

IndexedRangeSearchPlan
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: IndexedRangeSearchPlan
   :members:
   :special-members: __init__

This plan iterates only the index buckets whose keys fall within a specified range, respecting inclusive/exclusive bounds. It is produced by WherePlan.optimize when a Between operator applies to an indexed field.

Optimized Counting
-------------------

Several query plan classes implement optimized counting methods to improve performance when only the count of results is needed, not the actual data. These optimizations avoid iterating through all records when possible.

IndexedSearchPlan.count()
~~~~~~~~~~~~~~~~~~~~~~~

The ``IndexedSearchPlan`` class provides a fast count method that leverages the underlying index count. This avoids iterating over the actual data.

.. code-block:: python

    # Using an indexed field for counting is much faster
    indexed_search = proto_db.IndexedSearchPlan(
        field_to_scan="city",
        operator="==",
        value="New York",
        based_on=indexed_from_plan,
        transaction=transaction
    )

    # Get the count directly without iterating
    count = indexed_search.count()

AndMerge.count()
~~~~~~~~~~~~~

The ``AndMerge`` class optimizes counting for the intersection of sub-queries. It iterates the smaller result set and checks for existence in the larger one, without materializing full objects.

OrMerge.count()
~~~~~~~~~~~~

The ``OrMerge`` class optimizes counting for the union of sub-queries. It uses a set to efficiently combine IDs and get the final count of unique results.

Range Operators (Between)
-------------------------

.. autoclass:: Between
   :members:
   :special-members: __init__

The ``Between`` operator supports configurable bound inclusivity. Four canonical spellings are supported by the operator factory:

- ``between[]``: inclusive lower and upper bounds
- ``between()``: exclusive lower and upper bounds
- ``between(]``: exclusive lower, inclusive upper
- ``between[)``: inclusive lower, exclusive upper

Each ``Term`` using ``Between`` expects a value tuple ``(lo, hi)``. Example compiled expression: ``['age', 'between()', 10, 20]``.

Index-aware AND optimization
--------------------------------

When the filter is an ``AndExpression`` and the underlying plan exposes secondary indexes for the referenced fields, ``WherePlan.execute`` builds per-term candidate sets from the indexes, sorts them by selectivity (size) ascending, and performs a progressive intersection with early exit. Non-indexable residual predicates are applied only to the reduced set. This can drastically reduce the work on large datasets.

Usage Examples
----------------

Basic Query
~~~~~~~~~~

.. code-block:: python

    import proto_db

    # Create a list of dictionaries
    users = proto_db.List()

    # Add some users
    user1 = proto_db.Dictionary()
    user1["name"] = "John"
    user1["age"] = 30
    user1["city"] = "New York"
    users.append(user1)

    user2 = proto_db.Dictionary()
    user2["name"] = "Jane"
    user2["age"] = 25
    user2["city"] = "Boston"
    users.append(user2)

    user3 = proto_db.Dictionary()
    user3["name"] = "Bob"
    user3["age"] = 35
    user3["city"] = "New York"
    users.append(user3)

    # Create a query plan
    from_plan = proto_db.FromPlan(users)

    # Execute the query
    for user in from_plan.execute():
        print(user["name"])  # Output: John, Jane, Bob

Filtering
~~~~~~~~

.. code-block:: python

    # Filter users from New York
    where_plan = proto_db.WherePlan(
        filter=lambda user: user["city"] == "New York",
        based_on=from_plan
    )

    # Execute the query
    for user in where_plan.execute():
        print(user["name"])  # Output: John, Bob

Projection
~~~~~~~~~

.. code-block:: python

    # Project only the name and age fields
    select_plan = proto_db.SelectPlan(
        projection=lambda user: {"name": user["name"], "age": user["age"]},
        based_on=from_plan
    )

    # Execute the query
    for user in select_plan.execute():
        print(f"{user['name']}: {user['age']}")  # Output: John: 30, Jane: 25, Bob: 35

Sorting
~~~~~~

.. code-block:: python

    # Sort users by age
    order_plan = proto_db.OrderByPlan(
        key=lambda user: user["age"],
        based_on=from_plan
    )

    # Execute the query
    for user in order_plan.execute():
        print(f"{user['name']}: {user['age']}")  # Output: Jane: 25, John: 30, Bob: 35

Grouping
~~~~~~~

.. code-block:: python

    # Group users by city
    group_plan = proto_db.GroupByPlan(
        key=lambda user: user["city"],
        based_on=from_plan
    )

    # Execute the query
    for city, users_in_city in group_plan.execute():
        print(f"{city}: {len(users_in_city)} users")
        for user in users_in_city:
            print(f"  {user['name']}")

    # Output:
    # New York: 2 users
    #   John
    #   Bob
    # Boston: 1 user
    #   Jane

Joining
~~~~~~

.. code-block:: python

    # Create a list of cities
    cities = proto_db.List()

    # Add some cities
    city1 = proto_db.Dictionary()
    city1["name"] = "New York"
    city1["country"] = "USA"
    cities.append(city1)

    city2 = proto_db.Dictionary()
    city2["name"] = "Boston"
    city2["country"] = "USA"
    cities.append(city2)

    # Create a query plan for cities
    cities_plan = proto_db.FromPlan(cities)

    # Join users and cities
    join_plan = proto_db.JoinPlan(
        left=from_plan,
        right=cities_plan,
        condition=lambda user, city: user["city"] == city["name"]
    )

    # Execute the query
    for user, city in join_plan.execute():
        print(f"{user['name']} lives in {city['name']}, {city['country']}")

    # Output:
    # John lives in New York, USA
    # Jane lives in Boston, USA
    # Bob lives in New York, USA

Pagination
~~~~~~~~~

.. code-block:: python

    # Limit to 2 users
    limit_plan = proto_db.LimitPlan(
        limit=2,
        based_on=from_plan
    )

    # Execute the query
    for user in limit_plan.execute():
        print(user["name"])  # Output: John, Jane

    # Skip the first user
    offset_plan = proto_db.OffsetPlan(
        offset=1,
        based_on=from_plan
    )

    # Execute the query
    for user in offset_plan.execute():
        print(user["name"])  # Output: Jane, Bob

    # Combine limit and offset for pagination
    page_plan = proto_db.LimitPlan(
        limit=1,
        based_on=proto_db.OffsetPlan(
            offset=1,
            based_on=from_plan
        )
    )

    # Execute the query
    for user in page_plan.execute():
        print(user["name"])  # Output: Jane

Counting
~~~~~~~

.. code-block:: python

    # Count all users
    count_plan = proto_db.CountPlan(
        based_on=from_plan,
        transaction=from_plan.transaction
    )

    # Execute the query
    result = list(count_plan.execute())
    print(f"Total users: {result[0]['count']}")  # Output: Total users: 3

    # Count users from New York
    filtered_count_plan = proto_db.CountPlan(
        based_on=proto_db.WherePlan(
            filter=lambda user: user["city"] == "New York",
            based_on=from_plan,
            transaction=from_plan.transaction
        ),
        transaction=from_plan.transaction
    )

    # Execute the query
    result = list(filtered_count_plan.execute())
    print(f"Users from New York: {result[0]['count']}")  # Output: Users from New York: 2

Chaining
~~~~~~~

.. code-block:: python

    # Chain multiple operations
    chain_plan = proto_db.SelectPlan(
        projection=lambda user: {"name": user["name"]},
        based_on=proto_db.WherePlan(
            filter=lambda user: user["age"] > 25,
            based_on=proto_db.OrderByPlan(
                key=lambda user: user["name"],
                based_on=from_plan
            )
        )
    )

    # Execute the query
    for user in chain_plan.execute():
        print(user["name"])  # Output: Bob, John
