Installation
===========

Requirements
-----------

ProtoBase requires Python 3.11 or higher. It is designed to work with standard Python libraries and has minimal external dependencies.

Installing from PyPI
-------------------

The recommended way to install ProtoBase is from the Python Package Index (PyPI) using pip:

.. code-block:: bash

    pip install proto-db

This will install the latest stable version of ProtoBase and all its dependencies.

Installing from Source
---------------------

If you prefer to install from source, you can clone the repository and install it using pip:

.. code-block:: bash

    git clone https://github.com/yourusername/ProtoBase.git
    cd ProtoBase
    pip install -e .

This will install ProtoBase in development mode, allowing you to make changes to the code and have them immediately reflected in your environment.

Verifying the Installation
-------------------------

To verify that ProtoBase has been installed correctly, you can run the following Python code:

.. code-block:: python

    import proto_db
    
    # Create an in-memory database
    storage = proto_db.MemoryStorage()
    space = proto_db.ObjectSpace(storage)
    db = space.get_database("test_db")
    
    # Create a transaction
    tr = db.new_transaction()
    
    # Create a dictionary and store it as the root object
    d = proto_db.Dictionary()
    d["test"] = "Hello, ProtoBase!"
    tr.set_root_object("test_dict", d)
    
    # Commit the transaction
    tr.commit()
    
    # Create a new transaction and retrieve the dictionary
    tr2 = db.new_transaction()
    d2 = tr2.get_root_object("test_dict")
    
    # Verify the value
    print(d2["test"])  # Should print: Hello, ProtoBase!

If you see the message "Hello, ProtoBase!" printed to the console, then ProtoBase has been installed correctly.

Next Steps
---------

Now that you have ProtoBase installed, you can proceed to the :doc:`quickstart` guide to learn how to use it.