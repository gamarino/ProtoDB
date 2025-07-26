Development
==========

This section provides information for developers who want to contribute to ProtoBase or extend it for their own needs.

Setting Up a Development Environment
----------------------------------

To set up a development environment for ProtoBase:

1. Clone the repository:

   .. code-block:: bash

       git clone https://github.com/yourusername/ProtoBase.git
       cd ProtoBase

2. Create a virtual environment:

   .. code-block:: bash

       python -m venv .venv
       source .venv/bin/activate  # On Windows: .venv\Scripts\activate

3. Install the package in development mode:

   .. code-block:: bash

       pip install -e .

4. Run the tests to verify your setup:

   .. code-block:: bash

       python -m unittest discover proto_db/tests

Project Structure
---------------

The ProtoBase project is organized as follows:

- ``proto_db/``: The main package directory
  - ``__init__.py``: Package initialization and exports
  - ``common.py``: Common classes and utilities
  - ``db_access.py``: Database access layer
  - ``memory_storage.py``: In-memory storage implementation
  - ``standalone_file_storage.py``: File-based storage implementation
  - ``file_block_provider.py``: Block provider for file storage
  - ``cluster_file_storage.py``: Distributed storage implementation
  - ``cloud_file_storage.py``: Cloud storage implementation
  - ``dictionaries.py``: Dictionary implementation
  - ``hash_dictionaries.py``: Hash dictionary implementation
  - ``lists.py``: List implementation
  - ``sets.py``: Set implementation
  - ``queries.py``: Query system implementation
  - ``exceptions.py``: Exception classes
  - ``fsm.py``: Finite State Machine implementation
  - ``tests/``: Test directory
    - ``__init__.py``: Test package initialization
    - ``test_*.py``: Test modules

Coding Guidelines
---------------

When contributing to ProtoBase, please follow these guidelines:

1. **Code Style**: Follow PEP 8 for code style.

2. **Documentation**: Document all classes, methods, and functions using Google-style docstrings.

3. **Testing**: Write tests for all new features and bug fixes. Aim for high test coverage.

4. **Type Hints**: Use type hints to improve code readability and enable static type checking.

5. **Error Handling**: Use appropriate exception classes from ``exceptions.py`` for error handling.

6. **Backward Compatibility**: Maintain backward compatibility when making changes to existing APIs.

7. **Performance**: Consider performance implications of your changes, especially for storage operations.

Adding a New Feature
------------------

To add a new feature to ProtoBase:

1. **Create a Branch**: Create a new branch for your feature:

   .. code-block:: bash

       git checkout -b feature/my-new-feature

2. **Implement the Feature**: Implement your feature, following the coding guidelines.

3. **Write Tests**: Write tests for your feature in the ``tests/`` directory.

4. **Update Documentation**: Update the documentation to reflect your changes.

5. **Submit a Pull Request**: Push your branch and submit a pull request.

Extending ProtoBase
-----------------

ProtoBase is designed to be extensible. Here are some ways you can extend it:

Custom Storage Implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can create a custom storage implementation by implementing the ``SharedStorage`` interface:

.. code-block:: python

    from proto_db.common import SharedStorage, AtomPointer
    import uuid

    class MyCustomStorage(SharedStorage):
        def __init__(self):
            self.state = 'Running'
            # Initialize your storage

        def get_reader(self, wal_id, position):
            # Implement reading from your storage
            pass

        def write_streamer(self, wal_id):
            # Implement writing to your storage
            pass

        def get_new_wal(self):
            # Implement WAL creation
            return uuid.uuid4(), 0

        def get_writer_wal(self):
            # Implement getting the current writer WAL
            pass

        def close_wal(self, transaction_id):
            # Implement closing a WAL
            pass

        def get_current_root_object(self):
            # Implement getting the current root object
            pass

        def update_root_object(self, new_root):
            # Implement updating the root object
            pass

        def close(self):
            # Implement closing the storage
            self.state = 'Closed'

Custom Data Structure
~~~~~~~~~~~~~~~~~~~

You can create a custom data structure by extending the appropriate base class:

.. code-block:: python

    from proto_db.common import DBObject, MutableObject

    class MyCustomStructure(MutableObject):
        def __init__(self):
            super().__init__()
            self.data = {}

        def add_item(self, key, value):
            self.data[key] = value
            self.mark_as_modified()

        def get_item(self, key):
            return self.data.get(key)

        def _serialize(self, serializer):
            # Implement serialization
            pass

        def _deserialize(self, deserializer):
            # Implement deserialization
            pass

Custom Query Plan
~~~~~~~~~~~~~~~

You can create a custom query plan by extending the ``QueryPlan`` class:

.. code-block:: python

    from proto_db.common import QueryPlan

    class MyCustomPlan(QueryPlan):
        def __init__(self, based_on, my_param):
            super().__init__()
            self.based_on = based_on
            self.my_param = my_param

        def execute(self):
            # Implement query execution
            for item in self.based_on.execute():
                # Process item based on my_param
                yield processed_item

Debugging Tips
------------

Here are some tips for debugging ProtoBase:

1. **Enable Logging**: ProtoBase uses the standard Python logging module. You can enable debug logging to see more information:

   .. code-block:: python

       import logging
       logging.basicConfig(level=logging.DEBUG)

2. **Inspect WAL State**: For storage-related issues, inspect the WAL state:

   .. code-block:: python

       print(f"WAL ID: {storage.current_wal_id}")
       print(f"WAL Base: {storage.current_wal_base}")

3. **Use Memory Storage for Testing**: When debugging, use ``MemoryStorage`` to eliminate disk I/O as a potential issue:

   .. code-block:: python

       storage = proto_db.MemoryStorage()
       # Test your code with memory storage

4. **Check Transaction State**: For transaction-related issues, check the transaction state:

   .. code-block:: python

       print(f"Transaction ID: {tr.transaction_id}")
       print(f"Is Committed: {tr.is_committed}")

5. **Verify Object Persistence**: To verify that objects are properly persisted:

   .. code-block:: python

       tr1 = db.new_transaction()
       tr1.set_root_object('key', value)
       tr1.commit()
       
       tr2 = db.new_transaction()
       retrieved_value = tr2.get_root_object('key')
       assert retrieved_value == value

Contributing to Documentation
---------------------------

Documentation is an important part of ProtoBase. To contribute to the documentation:

1. **Update RST Files**: The documentation is written in reStructuredText (RST) format. Update the relevant RST files in the ``docs/source/`` directory.

2. **Build Documentation**: Build the documentation to verify your changes:

   .. code-block:: bash

       cd docs
       make html

3. **Preview Documentation**: Open ``docs/build/html/index.html`` in a web browser to preview the documentation.

4. **Submit a Pull Request**: Push your changes and submit a pull request.

Releasing a New Version
---------------------

To release a new version of ProtoBase:

1. **Update Version**: Update the version number in ``pyproject.toml``.

2. **Update Changelog**: Update the changelog with the changes in the new version.

3. **Build Package**: Build the package:

   .. code-block:: bash

       python -m build

4. **Upload to PyPI**: Upload the package to PyPI:

   .. code-block:: bash

       python -m twine upload dist/*

5. **Tag Release**: Tag the release in Git:

   .. code-block:: bash

       git tag -a v0.1.0 -m "Release v0.1.0"
       git push origin v0.1.0