API reference
=============

The public API is the set of names re-exported by the top-level package ``proto_db`` (decision D47): every
name in ``proto_db.__all__`` is resolved lazily, on first access, from the subpackage that defines it, so
``import proto_db`` imports no submodule, no optional dependency and starts no thread. The sections below
document each defining module; the narrative is in :doc:`concepts`.

.. automodule:: proto_db

proto_db.transactions
---------------------

.. automodule:: proto_db.transactions
   :members:

proto_db.core.atom
------------------

.. automodule:: proto_db.core.atom
   :members:

proto_db.core.dbobject
----------------------

Parents are passed in two forms, side by side:

.. list-table::
   :header-rows: 1

   * - Call
     - Form of the parents
     - Example
   * - ``DBObject(parents=...)``, ``tr.new_object(parents=...)``
     - one tuple (or list) of ``DBObject``\ s and registry names
     - ``DBObject(parents=(catalog, "crm.Lead"), name="Ada")``
   * - ``o.with_parents(...)``
     - the parents as separate arguments (none: no parents)
     - ``o.with_parents(catalog, "crm.Lead")``, or ``o.with_parents(*parents)`` for a tuple

``o.with_parents((a, b))`` passes one tuple as a single parent and is refused (``ProtoValidationError``,
"A parent must be ... not tuple"), so the two forms are never confused silently.

.. automodule:: proto_db.core.dbobject
   :members:
   :exclude-members: MutableObject

``proto_db.core.dbobject`` re-exports :class:`~proto_db.values.MutableObject`, documented with
:mod:`proto_db.values`.

proto_db.registry
-----------------

The runtime registry: modules, extensions and the registry roots (tutorial:
:doc:`tutorials/extensible-models`; design: :doc:`design-guide/registry`).

.. automodule:: proto_db.registry
   :members:

proto_db.resolution
-------------------

.. automodule:: proto_db.resolution
   :members:

proto_db.modules.base
---------------------

.. automodule:: proto_db.modules.base
   :members:

proto_db.collections.base
-------------------------

.. automodule:: proto_db.collections.base
   :members:

proto_db.collections.lists
--------------------------

.. automodule:: proto_db.collections.lists
   :members:

proto_db.collections.dictionaries
---------------------------------

.. automodule:: proto_db.collections.dictionaries
   :members:

proto_db.collections.hash_dictionaries
--------------------------------------

.. automodule:: proto_db.collections.hash_dictionaries
   :members:

proto_db.collections.sets
-------------------------

.. automodule:: proto_db.collections.sets
   :members:

proto_db.indexing.keyspec
-------------------------

.. automodule:: proto_db.indexing.keyspec
   :members:

proto_db.indexing.base
----------------------

.. automodule:: proto_db.indexing.base
   :members:

proto_db.indexing.equality
--------------------------

.. automodule:: proto_db.indexing.equality
   :members:

proto_db.indexing.range
-----------------------

.. automodule:: proto_db.indexing.range
   :members:

proto_db.indexing.vector
------------------------

.. automodule:: proto_db.indexing.vector
   :members:

proto_db.query.expressions
--------------------------

.. automodule:: proto_db.query.expressions
   :members:
   :exclude-members: Term

proto_db.query.terms
--------------------

.. automodule:: proto_db.query.terms
   :members:

proto_db.query.plans
--------------------

.. automodule:: proto_db.query.plans
   :members:

proto_db.query.planner
----------------------

.. automodule:: proto_db.query.planner
   :members:

proto_db.query.linq
-------------------

.. automodule:: proto_db.query.linq
   :members:

proto_db.query.cache
--------------------

.. automodule:: proto_db.query.cache
   :members:

proto_db.query.pager
--------------------

.. automodule:: proto_db.query.pager
   :members:

proto_db.storage.base
---------------------

.. automodule:: proto_db.storage.base
   :members:

proto_db.storage.memory
-----------------------

.. automodule:: proto_db.storage.memory
   :members:

proto_db.storage.file
---------------------

.. automodule:: proto_db.storage.file
   :members:

proto_db.storage.cloud
----------------------

.. automodule:: proto_db.storage.cloud
   :members:

proto_db.storage.tiered
-----------------------

.. automodule:: proto_db.storage.tiered
   :members:

proto_db.cluster.coordinator
----------------------------

.. automodule:: proto_db.cluster.coordinator
   :members:

proto_db.cluster.transport
--------------------------

.. automodule:: proto_db.cluster.transport
   :members:

proto_db.runtime.parallel
-------------------------

.. automodule:: proto_db.runtime.parallel
   :members:

proto_db.runtime.fsm
--------------------

.. automodule:: proto_db.runtime.fsm
   :members:

proto_db.interop.arrow
----------------------

.. automodule:: proto_db.interop.arrow
   :members:

proto_db.semantic
-----------------

The semantic model (decisions D63 to D72; format spec, "Semantic catalog (format 6)"; demo
``docs/kit/semantic/README.md``). The package re-exports the declarations and the session; the modules below
document them. A semantic object resolves an attribute it does not hold through the registry of the database it
was read from: with no registry current (an object built in plain Python, for one), ``getattr(o, "x", None)``
raises :class:`~proto_db.errors.ProtoNotSupportedError` instead of returning ``None``, while its own fields read
normally. :meth:`SemanticSession.query <proto_db.semantic.session.SemanticSession.query>` takes
``members=True`` to also answer ``"members"``, the :class:`~proto_db.semantic.entities.Version` each row reads
(a query without an aggregate).

.. automodule:: proto_db.semantic

proto_db.semantic.session
~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: proto_db.semantic.session
   :members:

proto_db.semantic.descriptors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: proto_db.semantic.descriptors
   :members:

proto_db.semantic.classes
~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: proto_db.semantic.classes
   :members:

proto_db.semantic.entities
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: proto_db.semantic.entities
   :members: Version, Members

proto_db.agent
--------------

The agent interface (spec section 11). The operations need nothing beyond the core; the MCP server needs the
optional extra ``agent`` (``pip install proto_db[agent]``, the MCP SDK pinned ``mcp>=1.30.0,<2``), imported only
when the server starts. In raw mode (``--raw``, the agent experiment's control group) an agent still sees the
stored type names and attribute names, but none of their meaning.

.. automodule:: proto_db.agent

proto_db.agent.operations
~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: proto_db.agent.operations
   :members:

proto_db.agent.mcp_server
~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: proto_db.agent.mcp_server
   :members:

proto_db.agent.step6
~~~~~~~~~~~~~~~~~~~~

.. automodule:: proto_db.agent.step6
   :members: expected, write_expected, canonical_bytes, differences, canonical_differences

proto_db.values
---------------

.. automodule:: proto_db.values
   :members:

proto_db.errors
---------------

.. automodule:: proto_db.errors
   :members:

