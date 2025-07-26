Quickstart
==========

This guide will help you get started with ProtoBase by walking through some basic examples.

Creating a Database
------------------

The first step is to create a storage instance, an object space, and a database:

.. code-block:: python

    import proto_db
    
    # Create a memory storage (for testing and development)
    storage = proto_db.MemoryStorage()
    
    # Create an object space
    space = proto_db.ObjectSpace(storage)
    
    # Get a database (creates it if it doesn't exist)
    db = space.get_database("my_database")

For production use, you might want to use a file-based storage instead:

.. code-block:: python

    import proto_db
    import os
    
    # Create a directory for the database files
    os.makedirs("my_db_files", exist_ok=True)
    
    # Create a file block provider
    block_provider = proto_db.FileBlockProvider("my_db_files")
    
    # Create a file storage
    storage = proto_db.StandaloneFileStorage(block_provider)
    
    # Create an object space and database as before
    space = proto_db.ObjectSpace(storage)
    db = space.get_database("my_database")

Working with Transactions
------------------------

All operations in ProtoBase are performed within transactions:

.. code-block:: python

    # Create a transaction
    tr = db.new_transaction()
    
    # Perform operations...
    
    # Commit the transaction
    tr.commit()

If you need to abort a transaction, you can simply let it go out of scope without committing it.

Working with Dictionaries
------------------------

Dictionaries are one of the basic data structures in ProtoBase:

.. code-block:: python

    # Create a transaction
    tr = db.new_transaction()
    
    # Create a dictionary
    d = proto_db.Dictionary()
    
    # Add some key-value pairs
    d["name"] = "John Doe"
    d["age"] = 30
    d["email"] = "john.doe@example.com"
    
    # Store the dictionary as a root object
    tr.set_root_object("user", d)
    
    # Commit the transaction
    tr.commit()
    
    # Create a new transaction
    tr2 = db.new_transaction()
    
    # Retrieve the dictionary
    user = tr2.get_root_object("user")
    
    # Access values
    print(user["name"])  # Output: John Doe
    print(user["age"])   # Output: 30
    
    # Modify values
    user["age"] = 31
    
    # Commit the changes
    tr2.commit()

Working with Lists
-----------------

Lists are another basic data structure:

.. code-block:: python

    # Create a transaction
    tr = db.new_transaction()
    
    # Create a list
    l = proto_db.List()
    
    # Add some items
    l.append("apple")
    l.append("banana")
    l.append("cherry")
    
    # Store the list as a root object
    tr.set_root_object("fruits", l)
    
    # Commit the transaction
    tr.commit()
    
    # Create a new transaction
    tr2 = db.new_transaction()
    
    # Retrieve the list
    fruits = tr2.get_root_object("fruits")
    
    # Access items
    print(fruits[0])  # Output: apple
    print(fruits[1])  # Output: banana
    
    # Modify the list
    fruits.append("date")
    fruits[0] = "apricot"
    
    # Commit the changes
    tr2.commit()

Working with Sets
----------------

Sets are useful for storing unique items:

.. code-block:: python

    # Create a transaction
    tr = db.new_transaction()
    
    # Create a set
    s = proto_db.Set()
    
    # Add some items
    s.add("red")
    s.add("green")
    s.add("blue")
    
    # Store the set as a root object
    tr.set_root_object("colors", s)
    
    # Commit the transaction
    tr.commit()
    
    # Create a new transaction
    tr2 = db.new_transaction()
    
    # Retrieve the set
    colors = tr2.get_root_object("colors")
    
    # Check membership
    print("red" in colors)  # Output: True
    print("yellow" in colors)  # Output: False
    
    # Add and remove items
    colors.add("yellow")
    colors.remove("red")
    
    # Commit the changes
    tr2.commit()

Using Queries
------------

ProtoBase provides a powerful query system:

.. code-block:: python

    # Create a transaction
    tr = db.new_transaction()
    
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
    
    # Store the list as a root object
    tr.set_root_object("users", users)
    
    # Commit the transaction
    tr.commit()
    
    # Create a new transaction
    tr2 = db.new_transaction()
    
    # Retrieve the list
    users = tr2.get_root_object("users")
    
    # Create a query plan
    from_plan = proto_db.FromPlan(users)
    
    # Filter users from New York
    where_plan = proto_db.WherePlan(
        filter=lambda user: user["city"] == "New York",
        based_on=from_plan
    )
    
    # Execute the query
    for user in where_plan.execute():
        print(user["name"])  # Output: John, Bob
    
    # Group users by city
    group_plan = proto_db.GroupByPlan(
        key=lambda user: user["city"],
        based_on=from_plan
    )
    
    # Execute the group query
    for city, users_in_city in group_plan.execute():
        print(f"{city}: {len(users_in_city)} users")
        # Output: New York: 2 users, Boston: 1 user

Next Steps
---------

This quickstart guide covered the basics of using ProtoBase. For more detailed information, see the :doc:`api/index` documentation.