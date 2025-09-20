Storage Layer
============

This module provides the storage layer of ProtoBase, which is responsible for persisting data to disk, memory, or cloud storage.

Memory Storage
--------------

.. module:: proto_db.memory_storage

.. autoclass:: MemoryStorage
   :members:
   :special-members: __init__

``MemoryStorage`` is an in-memory storage implementation. It stores Atoms in memory, does not persist data across process restarts, and is useful for testing and development.

Standalone File Storage
------------------------

.. module:: proto_db.standalone_file_storage

.. autoclass:: StandaloneFileStorage
   :members:
   :special-members: __init__

``StandaloneFileStorage`` is a file-based storage implementation. It stores Atoms in files on disk, uses Write-Ahead Logging (WAL) for durability, and provides persistence across process restarts.

Atom-level caches
^^^^^^^^^^^^^^^^^

``StandaloneFileStorage`` exposes optional, in-memory atom caches configurable via its constructor:

- ``enable_atom_object_cache``, ``enable_atom_bytes_cache``
- ``object_cache_max_entries``, ``object_cache_max_bytes``
- ``bytes_cache_max_entries``, ``bytes_cache_max_bytes``
- ``cache_stripes``, ``cache_probation_ratio``, ``schema_epoch``

When enabled, reads consult the object cache first, then the bytes cache, before touching the page/block provider.
The caches are keyed by ``AtomPointer`` and leverage atom immutability for coherent reuse across transactions.

Data Serialization Formats
^^^^^^^^^^^^^^^^^^^^^^^^^^

``StandaloneFileStorage`` supports multiple data serialization formats:

* **JSON UTF-8** (``FORMAT_JSON_UTF8``): The default format, used for backward compatibility.
* **MessagePack** (``FORMAT_MSGPACK``): A more efficient binary serialization format.
* **Raw Binary** (``FORMAT_RAW_BINARY``): For storing raw binary data without additional serialization.

Format indicators are used to identify the serialization format of stored data, allowing for seamless reading of data regardless of the format it was stored in. This provides flexibility in choosing the most appropriate serialization format for different types of data.

Methods for specific formats:

* ``push_atom_msgpack(atom)``: Pushes an atom using MessagePack serialization.
* ``push_bytes_msgpack(data)``: Pushes a Python object using MessagePack serialization.

.. autoclass:: WALState
   :members:
   :special-members: __init__

The ``WALState`` class represents the state of a Write-Ahead Log (WAL). It tracks the current position, base, and other metadata.

.. autoclass:: WALWriteOperation
   :members:
   :special-members: __init__

The ``WALWriteOperation`` class represents a write operation in the WAL. It contains the transaction ID, offset, and segments to be written.

File Block Provider
-------------------

.. module:: proto_db.file_block_provider

.. autoclass:: FileBlockProvider
   :members:
   :special-members: __init__

The ``FileBlockProvider`` class is a block provider implementation that uses files on disk for storage. It is used by ``StandaloneFileStorage`` to read and write blocks.

Cluster File Storage
--------------------

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
------------------

.. module:: proto_db.cloud_file_storage

.. autoclass:: CloudFileStorage
   :members:
   :special-members: __init__

``CloudFileStorage`` extends ``ClusterFileStorage`` to add support for cloud storage. It stores data in cloud object storage (Amazon S3 or Google Cloud Storage), provides local caching for performance, supports background uploading of data, and is suitable for cloud-native applications.

.. autoclass:: CloudBlockProvider
   :members:
   :special-members: __init__

The ``CloudBlockProvider`` class is a block provider implementation that uses cloud object storage for storage. It is used by ``CloudFileStorage`` to read and write blocks and works with any implementation of the ``CloudStorageClient`` interface.

.. autoclass:: CloudStorageClient
   :members:
   :special-members: __init__

The ``CloudStorageClient`` is an abstract base class that defines the interface for cloud storage clients. Concrete implementations are provided for specific cloud providers like Amazon S3 and Google Cloud Storage.

.. autoclass:: S3Client
   :members:
   :special-members: __init__

The ``S3Client`` class is an implementation of the ``CloudStorageClient`` interface for Amazon S3. It provides methods for getting, putting, listing, and deleting objects in S3-compatible storage.

.. autoclass:: GoogleCloudClient
   :members:
   :special-members: __init__

The ``GoogleCloudClient`` class is an implementation of the ``CloudStorageClient`` interface for Google Cloud Storage. It provides methods for getting, putting, listing, and deleting objects in Google Cloud Storage.

.. autoclass:: MockS3Client
   :members:
   :special-members: __init__

The ``MockS3Client`` class is a mock implementation of the ``S3Client`` interface for testing and development. It simulates S3 behavior without requiring an actual S3 service.

.. autoclass:: MockGoogleCloudClient
   :members:
   :special-members: __init__

The ``MockGoogleCloudClient`` class is a mock implementation of the ``GoogleCloudClient`` interface for testing and development. It simulates Google Cloud Storage behavior without requiring an actual Google Cloud Storage service.

.. autoclass:: CloudObjectMetadata
   :members:
   :special-members: __init__

The ``CloudObjectMetadata`` class represents metadata for a cloud storage object. It includes the key, size, ETag, last modified timestamp, and caching information.

.. autoclass:: S3ObjectMetadata
   :members:
   :special-members: __init__

The ``S3ObjectMetadata`` class is an alias for ``CloudObjectMetadata`` for backward compatibility.

.. autoclass:: CloudStorageError
   :members:
   :special-members: __init__

The ``CloudStorageError`` class is an exception raised for cloud storage specific errors.

Cloud Cluster File Storage
--------------------------

.. module:: proto_db.cloud_cluster_file_storage

.. autoclass:: CloudClusterFileStorage
   :members:
   :special-members: __init__

``CloudClusterFileStorage`` combines the functionality of ``CloudFileStorage`` and ``ClusterFileStorage`` to provide a comprehensive solution for multi-server environments operating in a cluster and using cloud object storage (Amazon S3 or Google Cloud Storage) as the final storage for data. It supports distributed operations like vote-based exclusive locking, root synchronization, and cached page retrieval between servers, as well as cloud storage features like background uploading to cloud storage and local caching.

.. autoclass:: CloudClusterStorageError
   :members:
   :special-members: __init__

The ``CloudClusterStorageError`` class is an exception raised for cloud cluster storage specific errors.
