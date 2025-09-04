Core Components
==============

.. module:: proto_db.common

This module provides the core components of ProtoBase, including the base classes and interfaces that form the foundation of the system.

Atom
----

.. autoclass:: Atom
   :members:
   :special-members: __init__

The ``Atom`` class is the fundamental building block of ProtoBase. All database objects are derived from this class. An Atom represents a piece of data that can be stored in the database.

AtomPointer
-----------

.. autoclass:: AtomPointer
   :members:
   :special-members: __init__

An ``AtomPointer`` is a reference to a stored Atom. It contains a transaction ID (the WAL ID where the Atom is stored) and an offset (the position within the WAL).

Literal
-------

.. autoclass:: Literal
   :members:
   :special-members: __init__

The ``Literal`` class represents a string literal in the database. It is a simple wrapper around a string value.

DBObject
--------

.. autoclass:: DBObject
   :members:
   :special-members: __init__

The ``DBObject`` class is the base class for all database objects. It provides methods for serialization and deserialization.

MutableObject
--------------

.. autoclass:: MutableObject
   :members:
   :special-members: __init__

The ``MutableObject`` class is the base class for all mutable database objects. It provides methods for tracking modifications.

DBCollections
------------

.. autoclass:: DBCollections
   :members:
   :special-members: __init__

The ``DBCollections`` class is the base class for all collection types in ProtoBase.

QueryPlan
---------

.. autoclass:: QueryPlan
   :members:
   :special-members: __init__

The ``QueryPlan`` class is the base class for all query plans in ProtoBase. It provides methods for query execution and chaining.

BlockProvider
------------

.. autoclass:: BlockProvider
   :members:
   :special-members: __init__

The ``BlockProvider`` interface defines the contract for block providers, which are responsible for providing storage blocks for the storage layer.

SharedStorage
------------

.. autoclass:: SharedStorage
   :members:
   :special-members: __init__

The ``SharedStorage`` interface defines the contract for storage implementations. It provides methods for reading and writing Atoms, managing transactions, and handling the root object.

.. module:: proto_db.db_access

This module provides the database access layer of ProtoBase.

ObjectSpace
-----------

.. autoclass:: ObjectSpace
   :members:
   :special-members: __init__

An ``ObjectSpace`` is a container for multiple databases. It manages the lifecycle of databases, provides access to databases by name, and ensures proper isolation between databases.

Database
--------

.. autoclass:: Database
   :members:
   :special-members: __init__

A ``Database`` is a container for a single database. It manages the lifecycle of transactions, provides access to the root object, and ensures proper isolation between transactions.

ObjectTransaction
------------------

.. autoclass:: ObjectTransaction
   :members:
   :special-members: __init__

An ``ObjectTransaction`` is a context for database operations. It provides methods for reading and writing objects, ensures atomicity of operations, and manages the commit process.

BytesAtom
---------

.. autoclass:: BytesAtom
   :members:
   :special-members: __init__

The ``BytesAtom`` class represents a binary data atom in the database. It is a wrapper around a bytes value.