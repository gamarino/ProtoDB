ProtoDB documentation
=====================

ProtoDB is an embedded, transactional, object-oriented database for Python, built on immutable, persistent,
structurally shared collections. Every collection can carry equality, range and vector indexes; one planner
serves every query front end; a commit publishes a new version and every past version stays readable; and
several processes can write one space through a peer cluster that never lets the storage arbitrate between
writers.

This site documents the 2.0 release (``2.0.0``, unreleased). The code is the source of truth: every tutorial
page is executed by the test suite, and every figure comes from a block that ran or from a cited benchmark
result.

Where to start
--------------

* New to ProtoDB: :doc:`tutorials/getting-started`, then :doc:`concepts`.
* Modelling an application: :doc:`tutorials/modelling-data` and the :doc:`ERP example <tutorials/erp>`.
* Running it: :doc:`tutorials/operations`, :doc:`durability` and :doc:`performance`.
* Why it is built this way: :doc:`design-principles`.

.. toctree::
   :maxdepth: 2
   :caption: Concepts and design

   concepts
   design-principles
   architecture
   concurrency-model
   indexing-framework
   query-planner
   durability
   objects-across-transactions
   format-spec
   coordinator-protocol
   design-guide

.. toctree::
   :maxdepth: 2
   :caption: Tutorials

   tutorials/index

.. toctree::
   :maxdepth: 2
   :caption: Reference

   api
   performance

Beyond these pages: ``docs/decisions.md`` records every design decision with the alternatives considered,
``docs/design-principles.md`` holds the principles, ``CHANGELOG.md`` the changes, and ``docs/legacy/`` the 0.1
documentation, kept as history.

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
