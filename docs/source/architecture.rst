Architecture
===========

ProtoBase is designed with a layered architecture that provides flexibility, extensibility, and robustness. This document explains the key architectural components and how they interact.

Core Components
--------------

Atom
~~~~

The ``Atom`` class is the fundamental building block of ProtoBase. All database objects are derived from this class. An Atom represents a piece of data that can be stored in the database. It has the following key characteristics:

- It has a unique identifier
- It can be serialized and deserialized
- It can be stored in and retrieved from the storage layer

AtomPointer
~~~~~~~~~~~

An ``AtomPointer`` is a reference to a stored Atom. It contains:

- A transaction ID (the WAL ID where the Atom is stored)
- An offset (the position within the WAL)

This allows ProtoBase to efficiently locate and retrieve Atoms from storage.

SharedStorage
~~~~~~~~~~~~

The ``SharedStorage`` interface defines the contract for storage implementations. It provides methods for:

- Reading and writing Atoms
- Managing transactions
- Handling the root object

ObjectSpace
~~~~~~~~~~

An ``ObjectSpace`` is a container for multiple databases. It:

- Manages the lifecycle of databases
- Provides access to databases by name
- Ensures proper isolation between databases

Database
~~~~~~~

A ``Database`` is a container for a single database. It:

- Manages the lifecycle of transactions
- Provides access to the root object
- Ensures proper isolation between transactions

Transaction
~~~~~~~~~~

A ``Transaction`` is a context for database operations. It:

- Provides methods for reading and writing objects
- Ensures atomicity of operations
- Manages the commit process

Storage Layer
------------

The storage layer is responsible for persisting Atoms to disk or memory. ProtoBase provides several storage implementations:

MemoryStorage
~~~~~~~~~~~~

``MemoryStorage`` is an in-memory storage implementation. It:

- Stores Atoms in memory
- Does not persist data across process restarts
- Is useful for testing and development

StandaloneFileStorage
~~~~~~~~~~~~~~~~~~~

``StandaloneFileStorage`` is a file-based storage implementation. It:

- Stores Atoms in files on disk
- Uses Write-Ahead Logging (WAL) for durability
- Provides persistence across process restarts

ClusterFileStorage
~~~~~~~~~~~~~~~~

``ClusterFileStorage`` extends ``StandaloneFileStorage`` to provide distributed storage capabilities. It:

- Supports multiple nodes in a cluster
- Uses a vote-based locking mechanism for coordination
- Ensures consistency across nodes
- Allows for horizontal scaling

CloudFileStorage
~~~~~~~~~~~~~~

``CloudFileStorage`` extends ``ClusterFileStorage`` to add support for cloud storage. It:

- Stores data in cloud object storage (Amazon S3 or Google Cloud Storage)
- Provides local caching for performance
- Supports background uploading of data
- Is suitable for cloud-native applications

Data Structures
--------------

ProtoBase provides several data structures built on top of Atoms:

Dictionary
~~~~~~~~~

A ``Dictionary`` is a key-value mapping with string keys. It:

- Supports adding, removing, and updating key-value pairs
- Provides efficient lookup by key
- Can store any type of value

List
~~~~

A ``List`` is an ordered collection of items. It:

- Supports adding, removing, and updating items
- Provides efficient access by index
- Can store any type of value

Set
~~~

A ``Set`` is an unordered collection of unique items. It:

- Supports adding and removing items
- Provides efficient membership testing
- Ensures uniqueness of items

HashDictionary
~~~~~~~~~~~~~

A ``HashDictionary`` is a dictionary with hash-based lookups. It:

- Supports non-string keys
- Provides efficient lookup by key
- Can store any type of value

Query System
-----------

The query system allows for complex data manipulation:

QueryPlan
~~~~~~~~

``QueryPlan`` is the base class for all query plans. It:

- Defines the interface for query execution
- Provides methods for chaining query operations
- Supports lazy evaluation

FromPlan
~~~~~~~

``FromPlan`` is the starting point for queries. It:

- Takes a collection as input
- Provides an iterator over the collection
- Can be used as the basis for other query plans

WherePlan
~~~~~~~~

``WherePlan`` filters records based on a condition. It:

- Takes a filter function and a base plan
- Returns only records that satisfy the condition
- Can be chained with other query plans

JoinPlan
~~~~~~~

``JoinPlan`` joins multiple data sources. It:

- Takes two plans and a join condition
- Returns records that satisfy the join condition
- Supports inner, left, right, and full joins

GroupByPlan
~~~~~~~~~~

``GroupByPlan`` groups records by a key. It:

- Takes a key function and a base plan
- Returns groups of records with the same key
- Can be used for aggregation

OrderByPlan
~~~~~~~~~~

``OrderByPlan`` sorts records. It:

- Takes a key function and a base plan
- Returns records sorted by the key
- Supports ascending and descending order

SelectPlan
~~~~~~~~~

``SelectPlan`` projects specific fields. It:

- Takes a projection function and a base plan
- Returns transformed records
- Can be used to extract specific fields

LimitPlan and OffsetPlan
~~~~~~~~~~~~~~~~~~~~~~~

``LimitPlan`` and ``OffsetPlan`` provide pagination. They:

- Take a limit/offset and a base plan
- Return a subset of records
- Can be combined for pagination

Interaction Flow
--------------

The typical flow of operations in ProtoBase is as follows:

1. Create a storage instance (e.g., ``MemoryStorage``, ``StandaloneFileStorage``)
2. Create an ``ObjectSpace`` with the storage
3. Get a ``Database`` from the object space
4. Create a ``Transaction`` from the database
5. Perform operations within the transaction (create, read, update, delete)
6. Commit the transaction

During this process:

- The transaction creates and modifies Atoms
- The storage layer persists the Atoms
- The query system can be used to retrieve and manipulate data

This architecture provides a flexible and powerful foundation for building database applications.



Atom-level Cache Layer
----------------------

ProtoBase integrates an optional, in-memory cache layer keyed by ``AtomPointer`` to accelerate reads across
transactions:

- ``AtomBytesCache`` stores the raw payload bytes (post WAL headers) as memoryviews.
- ``AtomObjectCache`` stores fully deserialized atom objects (e.g., Python dicts).
- Keys: ``(transaction_id, offset)`` for bytes; ``(transaction_id, offset, schema_epoch)`` for objects.
- Replacement: 2Q (probation + protected LRU) to reduce scan pollution, with limits by bytes and entries.
- Concurrency: single-flight ensures at most one deserialize per key.
- Observability: hit/miss, evictions, and latency buckets exposed via ``AtomCacheBundle``.

The read path first checks the object cache, then the bytes cache, and finally falls back to reading from the
underlying block provider if needed. Caches leverage the immutability of atoms, so they require no invalidation on
writes; bump ``schema_epoch`` to isolate entries across decoder changes.
