Storage Layer
============

This module provides the storage layer of ProtoBase, which is responsible for persisting data to disk, memory, or cloud storage.

Memory Storage
-------------

.. module:: proto_db.memory_storage

.. autoclass:: MemoryStorage
   :members:
   :special-members: __init__

``MemoryStorage`` is an in-memory storage implementation. It stores Atoms in memory, does not persist data across process restarts, and is useful for testing and development.

Standalone File Storage
----------------------

.. module:: proto_db.standalone_file_storage

.. autoclass:: StandaloneFileStorage
   :members:
   :special-members: __init__

``StandaloneFileStorage`` is a file-based storage implementation. It stores Atoms in files on disk, uses Write-Ahead Logging (WAL) for durability, and provides persistence across process restarts.

.. autoclass:: WALState
   :members:
   :special-members: __init__

The ``WALState`` class represents the state of a Write-Ahead Log (WAL). It tracks the current position, base, and other metadata.

.. autoclass:: WALWriteOperation
   :members:
   :special-members: __init__

The ``WALWriteOperation`` class represents a write operation in the WAL. It contains the transaction ID, offset, and segments to be written.

File Block Provider
------------------

.. module:: proto_db.file_block_provider

.. autoclass:: FileBlockProvider
   :members:
   :special-members: __init__

The ``FileBlockProvider`` class is a block provider implementation that uses files on disk for storage. It is used by ``StandaloneFileStorage`` to read and write blocks.

Cluster File Storage
-------------------

.. module:: proto_db.cluster_file_storage

.. autoclass:: ClusterFileStorage
   :members:
   :special-members: __init__

``ClusterFileStorage`` extends ``StandaloneFileStorage`` to provide distributed storage capabilities. It supports multiple nodes in a cluster, uses a vote-based locking mechanism for coordination, ensures consistency across nodes, and allows for horizontal scaling.

.. autoclass:: ClusterNetworkManager
   :members:
   :special-members: __init__

The ``ClusterNetworkManager`` class manages network communication between nodes in a cluster. It handles vote requests, page requests, and root object broadcasts.

Cloud File Storage
-----------------

.. module:: proto_db.cloud_file_storage

.. autoclass:: CloudFileStorage
   :members:
   :special-members: __init__

``CloudFileStorage`` extends ``ClusterFileStorage`` to add support for cloud storage. It stores data in S3-compatible object storage, provides local caching for performance, supports background uploading of data, and is suitable for cloud-native applications.

.. autoclass:: CloudBlockProvider
   :members:
   :special-members: __init__

The ``CloudBlockProvider`` class is a block provider implementation that uses S3-compatible object storage for storage. It is used by ``CloudFileStorage`` to read and write blocks.

.. autoclass:: S3Client
   :members:
   :special-members: __init__

The ``S3Client`` interface defines the contract for S3 client implementations. It provides methods for getting, putting, listing, and deleting objects in S3-compatible storage.

.. autoclass:: MockS3Client
   :members:
   :special-members: __init__

The ``MockS3Client`` class is a mock implementation of the ``S3Client`` interface for testing and development. It simulates S3 behavior without requiring an actual S3 service.

.. autoclass:: S3ObjectMetadata
   :members:
   :special-members: __init__

The ``S3ObjectMetadata`` class represents metadata for an S3 object. It includes the key, size, ETag, last modified timestamp, and caching information.

.. autoclass:: CloudStorageError
   :members:
   :special-members: __init__

The ``CloudStorageError`` class is an exception raised for cloud storage specific errors.