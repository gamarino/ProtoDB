Testing
=======

ProtoBase uses Python's standard unittest framework. This page summarizes common workflows
and patterns used throughout the repository's test suite.

Running tests
-------------

Run the entire test suite from the project root:

.. code-block:: bash

    python -m unittest discover -s proto_db/tests -t . -v

Run a single test module:

.. code-block:: bash

    python -m unittest proto_db.tests.test_db_access -v

Run a single test case or method:

.. code-block:: bash

    python -m unittest proto_db.tests.test_memory_storage.TestMemoryStorage.test_set_and_read_root -v

Patterns and tips
-----------------

- Transactions pattern: many tests create a transaction, perform operations, commit, open a new transaction
  to verify persistence, and then assert expectations.
- Mocking: use unittest.mock for isolated components. Example:

  .. code-block:: python

      from unittest.mock import Mock, MagicMock
      mock_block_provider = Mock()
      mock_block_provider.get_new_wal = MagicMock(return_value=(uuid4(), 0))

- Cloud/page cache: cluster/cloud storage tests patch network managers and cloud clients to avoid external
  dependencies while asserting cache‑hit behavior.

Environment
-----------

- Python 3.11+ is required.
- A virtual environment is recommended:

  .. code-block:: bash

      python -m venv .venv
      source .venv/bin/activate  # On Windows: .venv\Scripts\activate

Contributing tests
------------------

- Use descriptive method names like test_001_feature_name where ordering or grouping helps reading.
- Add short docstrings to explain the test's intent.
- Keep tests deterministic; seed random where applicable.
