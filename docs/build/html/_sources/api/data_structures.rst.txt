Data Structures
==============

This module provides the data structures of ProtoBase, which are built on top of the core components.

Dictionary
----------

.. module:: proto_db.dictionaries

.. autoclass:: Dictionary
   :members:
   :special-members: __init__

A ``Dictionary`` is a key-value mapping with string keys. It supports adding, removing, and updating key-value pairs, provides efficient lookup by key, and can store any type of value.

.. autoclass:: RepeatedKeysDictionary
   :members:
   :special-members: __init__

A ``RepeatedKeysDictionary`` is a dictionary that allows multiple values for the same key. It is useful for representing one-to-many relationships. Each key in the dictionary is associated with a ``Set`` of values, allowing efficient storage and retrieval of multiple values per key.

The class provides methods for:

* Adding values to a key's set with ``set_at``
* Retrieving all values for a key as a ``Set`` with ``get_at``
* Removing all values for a key with ``remove_at``
* Removing a specific value from a key's set with ``remove_record_at``
* Handling concurrent modifications with ``_rebase_on_concurrent_update``

Hash Dictionary
---------------

.. module:: proto_db.hash_dictionaries

.. autoclass:: HashDictionary
   :members:
   :special-members: __init__

A ``HashDictionary`` is a dictionary with hash-based lookups. It supports non-string keys, provides efficient lookup by key, and can store any type of value.

List
----

.. module:: proto_db.lists

.. autoclass:: List
   :members:
   :special-members: __init__

A ``List`` is an ordered collection of items. It supports adding, removing, and updating items, provides efficient access by index, and can store any type of value.

Set
---

.. module:: proto_db.sets

.. autoclass:: Set
   :members:
   :special-members: __init__

A ``Set`` is an unordered collection of unique items. It supports adding and removing items, provides efficient membership testing, and ensures uniqueness of items.

Usage Examples
--------------

Dictionary Example
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import proto_db

    # Create a dictionary
    d = proto_db.Dictionary()

    # Add some key-value pairs
    d["name"] = "John Doe"
    d["age"] = 30
    d["email"] = "john.doe@example.com"

    # Access values
    print(d["name"])  # Output: John Doe

    # Check if a key exists
    if "age" in d:
        print(f"Age: {d['age']}")

    # Iterate over keys
    for key in d.keys():
        print(key)

    # Iterate over values
    for value in d.values():
        print(value)

    # Iterate over key-value pairs
    for key, value in d.items():
        print(f"{key}: {value}")

RepeatedKeysDictionary Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import proto_db

    # Create a transaction and database
    db = proto_db.Database()
    tr = db.new_transaction()

    # Create a repeated keys dictionary
    rkd = proto_db.RepeatedKeysDictionary(transaction=tr)

    # Add multiple values for the same key
    rkd = rkd.set_at("category", "Electronics")
    rkd = rkd.set_at("category", "Computers")
    rkd = rkd.set_at("category", "Laptops")

    # Add values for another key
    rkd = rkd.set_at("tags", "Sale")
    rkd = rkd.set_at("tags", "New")

    # Get all values for a key as a Set
    categories = rkd.get_at("category")
    print("Categories:", [cat for cat in categories.as_iterable()])
    # Output: Categories: ['Electronics', 'Computers', 'Laptops']

    # Check if a key exists
    if rkd.has("tags"):
        tags = rkd.get_at("tags")
        print("Tags:", [tag for tag in tags.as_iterable()])
        # Output: Tags: ['Sale', 'New']

    # Remove a specific value from a key's set
    rkd = rkd.remove_record_at("category", "Computers")

    # Get the updated set of values
    updated_categories = rkd.get_at("category")
    print("Updated categories:", [cat for cat in updated_categories.as_iterable()])
    # Output: Updated categories: ['Electronics', 'Laptops']

    # Remove all values for a key
    rkd = rkd.remove_at("tags")
    print("Has tags:", rkd.has("tags"))  # Output: Has tags: False

    # Store the dictionary in the database
    tr.set_root_object("product_attributes", rkd)
    tr.commit()

Hash Dictionary Example
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import proto_db

    # Create a hash dictionary
    hd = proto_db.HashDictionary()

    # Add some key-value pairs with non-string keys
    hd[123] = "Value for 123"
    hd[(1, 2, 3)] = "Value for tuple"
    hd[object()] = "Value for object"

    # Access values
    print(hd[123])  # Output: Value for 123

    # Check if a key exists
    if (1, 2, 3) in hd:
        print(f"Tuple value: {hd[(1, 2, 3)]}")

    # Iterate over key-value pairs
    for key, value in hd.items():
        print(f"{key}: {value}")

List Example
~~~~~~~~~~~~

.. code-block:: python

    import proto_db

    # Create a list
    l = proto_db.List()

    # Add some items
    l.append("apple")
    l.append("banana")
    l.append("cherry")

    # Access items by index
    print(l[0])  # Output: apple

    # Modify items
    l[1] = "blueberry"

    # Check length
    print(len(l))  # Output: 3

    # Iterate over items
    for item in l:
        print(item)

    # Slice the list
    print(l[1:])  # Output: ["blueberry", "cherry"]

Set Example
~~~~~~~~~~~

.. code-block:: python

    import proto_db

    # Create a set
    s = proto_db.Set()

    # Add some items
    s.add("red")
    s.add("green")
    s.add("blue")

    # Check membership
    print("red" in s)  # Output: True

    # Remove an item
    s.remove("green")

    # Check length
    print(len(s))  # Output: 2

    # Iterate over items
    for item in s:
        print(item)

    # Set operations
    s2 = proto_db.Set()
    s2.add("blue")
    s2.add("yellow")

    # Union
    union = s.union(s2)
    print(union)  # Output: Set containing "red", "blue", "yellow"

    # Intersection
    intersection = s.intersection(s2)
    print(intersection)  # Output: Set containing "blue"

    # Difference
    difference = s.difference(s2)
    print(difference)  # Output: Set containing "red"
