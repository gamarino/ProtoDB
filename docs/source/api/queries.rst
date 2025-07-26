Query System
===========

.. module:: proto_db.queries

This module provides the query system of ProtoBase, which allows for complex data manipulation and retrieval.

Query Plans
----------

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

Usage Examples
-------------

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