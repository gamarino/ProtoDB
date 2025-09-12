Advanced Usage
=============

This section covers advanced usage scenarios for ProtoBase, including distributed storage, cloud storage, and performance optimization.

Distributed Storage with ClusterFileStorage
------------------------------------------

ProtoBase provides distributed storage capabilities through the ``ClusterFileStorage`` class. This allows you to deploy ProtoBase across multiple nodes in a cluster for high availability and horizontal scaling.

Setting Up a Cluster
~~~~~~~~~~~~~~~~~~~

To set up a cluster, you need to:

1. Define a list of servers in the cluster
2. Create a ``ClusterFileStorage`` instance on each node
3. Configure each node with a unique server ID

Here's an example:

.. code-block:: python

    import proto_db
    import os

    # Create a directory for the database files
    os.makedirs("node1_files", exist_ok=True)

    # Define the servers in the cluster
    servers = [
        ("node1.example.com", 12345),
        ("node2.example.com", 12345),
        ("node3.example.com", 12345)
    ]

    # Create a file block provider
    block_provider = proto_db.FileBlockProvider("node1_files")

    # Create a cluster file storage
    storage = proto_db.ClusterFileStorage(
        block_provider=block_provider,
        server_id="node1",
        host="node1.example.com",
        port=12345,
        servers=servers
    )

    # Create an object space and database as before
    space = proto_db.ObjectSpace(storage)
    db = space.get_database("my_database")

Distributed Coordination
~~~~~~~~~~~~~~~~~~~~~~~

``ClusterFileStorage`` uses a vote-based locking mechanism for distributed coordination. When a node wants to perform a write operation, it:

1. Requests votes from other nodes in the cluster
2. If it receives a majority of votes, it proceeds with the write
3. After the write, it broadcasts the new root object to other nodes

This ensures that only one node can write to the database at a time, preventing conflicts.

Cloud Storage with CloudFileStorage
----------------------------------

ProtoBase provides cloud storage capabilities through the ``CloudFileStorage`` class. This allows you to store data in cloud object storage services like Amazon S3 or Google Cloud Storage.

Setting Up Cloud Storage
~~~~~~~~~~~~~~~~~~~~~~

To set up cloud storage, you need to:

1. Create a cloud storage client (S3Client or GoogleCloudClient)
2. Create a ``CloudBlockProvider`` with the cloud storage client
3. Create a ``CloudFileStorage`` with the block provider

Here's an example using Amazon S3:

.. code-block:: python

    import proto_db

    # Create an S3 client
    s3_client = proto_db.S3Client(
        bucket="my-bucket",
        prefix="my-prefix",
        endpoint_url="https://s3.amazonaws.com",
        access_key="my-access-key",
        secret_key="my-secret-key",
        region="us-west-2"
    )

    # Create a cloud block provider
    block_provider = proto_db.CloudBlockProvider(
        cloud_client=s3_client,
        cache_dir="s3_cache",
        cache_size=500 * 1024 * 1024,  # 500 MB cache
        object_size=5 * 1024 * 1024     # 5 MB objects
    )

    # Create a cloud file storage
    storage = proto_db.CloudFileStorage(
        block_provider=block_provider,
        upload_interval_ms=5000  # Upload every 5 seconds
    )

    # Create an object space and database as before
    space = proto_db.ObjectSpace(storage)
    db = space.get_database("my_database")

Here's an example using Google Cloud Storage:

.. code-block:: python

    import proto_db

    # Create a Google Cloud Storage client
    gcs_client = proto_db.GoogleCloudClient(
        bucket="my-bucket",
        prefix="my-prefix",
        project_id="my-project",
        credentials_path="/path/to/credentials.json"
    )

    # Create a cloud block provider
    block_provider = proto_db.CloudBlockProvider(
        cloud_client=gcs_client,
        cache_dir="gcs_cache",
        cache_size=500 * 1024 * 1024,  # 500 MB cache
        object_size=5 * 1024 * 1024     # 5 MB objects
    )

    # Create a cloud file storage
    storage = proto_db.CloudFileStorage(
        block_provider=block_provider,
        upload_interval_ms=5000  # Upload every 5 seconds
    )

    # Create an object space and database as before
    space = proto_db.ObjectSpace(storage)
    db = space.get_database("my_database")

For testing purposes, you can use the built-in mock clients:

.. code-block:: python

    # Mock S3 client
    s3_client = proto_db.MockS3Client(
        bucket="my-bucket",
        prefix="my-prefix"
    )

    # Mock Google Cloud Storage client
    gcs_client = proto_db.MockGoogleCloudClient(
        bucket="my-bucket",
        prefix="my-prefix"
    )

Local Caching
~~~~~~~~~~~~

``CloudFileStorage`` uses local caching to improve performance. When an object is read from cloud storage (Amazon S3 or Google Cloud Storage), it is cached locally. Subsequent reads of the same object will use the local cache, avoiding the need to fetch the object from cloud storage again.

The cache is managed automatically, with least recently used objects being evicted when the cache size limit is reached.

Background Uploading
~~~~~~~~~~~~~~~~~~

``CloudFileStorage`` also supports background uploading of data to cloud storage. When data is written to the database, it is first stored locally and then uploaded to cloud storage (Amazon S3 or Google Cloud Storage) in the background. This allows the application to continue working without waiting for the upload to complete.

The upload interval can be configured to balance between performance and durability.

Combined Cluster and Cloud Storage with CloudClusterFileStorage
-------------------------------------------------------------

ProtoBase provides a comprehensive solution for multi-server environments with the ``CloudClusterFileStorage`` class. This class combines the functionality of ``ClusterFileStorage`` and ``CloudFileStorage`` to provide a storage solution that works in a cluster environment while using cloud object storage (Amazon S3 or Google Cloud Storage) as the final storage for data.

Setting Up Cloud Cluster Storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To set up cloud cluster storage, you need to:

1. Create a cloud storage client (S3Client or GoogleCloudClient)
2. Create a ``CloudBlockProvider`` with the cloud storage client
3. Define a list of servers in the cluster
4. Create a ``CloudClusterFileStorage`` instance on each node

Here's an example using Amazon S3:

.. code-block:: python

    import proto_db

    # Create an S3 client
    s3_client = proto_db.S3Client(
        bucket="my-bucket",
        prefix="my-prefix",
        endpoint_url="https://s3.amazonaws.com",
        access_key="my-access-key",
        secret_key="my-secret-key",
        region="us-west-2"
    )

    # Create a cloud block provider
    block_provider = proto_db.CloudBlockProvider(
        cloud_client=s3_client,
        cache_dir="cloud_cluster_cache",
        cache_size=500 * 1024 * 1024,  # 500 MB cache
        object_size=5 * 1024 * 1024     # 5 MB objects
    )

Here's an example using Google Cloud Storage:

.. code-block:: python

    import proto_db

    # Create a Google Cloud Storage client
    gcs_client = proto_db.GoogleCloudClient(
        bucket="my-bucket",
        prefix="my-prefix",
        project_id="my-project",
        credentials_path="/path/to/credentials.json"
    )

    # Create a cloud block provider
    block_provider = proto_db.CloudBlockProvider(
        cloud_client=gcs_client,
        cache_dir="cloud_cluster_cache",
        cache_size=500 * 1024 * 1024,  # 500 MB cache
        object_size=5 * 1024 * 1024     # 5 MB objects
    )

    # Define the servers in the cluster
    servers = [
        ("node1.example.com", 12345),
        ("node2.example.com", 12345),
        ("node3.example.com", 12345)
    ]

    # Create a cloud cluster file storage
    storage = proto_db.CloudClusterFileStorage(
        block_provider=block_provider,
        server_id="node1",
        host="node1.example.com",
        port=12345,
        servers=servers,
        upload_interval_ms=5000,  # Upload every 5 seconds
        page_cache_dir="cloud_page_cache"  # Directory for cloud page cache
    )

    # Create an object space and database as before
    space = proto_db.ObjectSpace(storage)
    db = space.get_database("my_database")

Key Features
~~~~~~~~~~~

``CloudClusterFileStorage`` provides the following key features:

1. **Distributed Coordination**: Uses a vote-based locking mechanism for distributed coordination, ensuring that only one node can write to the database at a time.

2. **Cloud Storage**: Stores data in cloud object storage (Amazon S3 or Google Cloud Storage), providing durability and scalability.

3. **Local Caching**: Uses local caching to improve performance, with least recently used objects being evicted when the cache size limit is reached.

4. **Background Uploading**: Supports background uploading of data to cloud storage, allowing the application to continue working without waiting for the upload to complete.

5. **Fault Tolerance**: Provides fault tolerance through redundancy, with data being available from multiple sources (local cache, other nodes, cloud storage).

Use Cases
~~~~~~~~

``CloudClusterFileStorage`` is ideal for:

- Multi-server applications that need high availability and horizontal scaling
- Cloud-native applications that need to store data in cloud object storage (Amazon S3 or Google Cloud Storage)
- Applications that need both the distributed coordination of a cluster and the durability of cloud storage

Performance Optimization
----------------------

Atom-level Caching
~~~~~~~~~~~~~~~~~~

For read-heavy workloads with object reuse across transactions, enable the atom caches in your storage:

.. code-block:: python

    from proto_db.file_block_provider import FileBlockProvider
    from proto_db.standalone_file_storage import StandaloneFileStorage

    provider = FileBlockProvider('data_dir')
    storage = StandaloneFileStorage(
        provider,
        enable_atom_object_cache=True,
        enable_atom_bytes_cache=True,
        object_cache_max_entries=50_000,
        object_cache_max_bytes=256*1024*1024,
        bytes_cache_max_entries=20_000,
        bytes_cache_max_bytes=128*1024*1024,
        cache_stripes=64,
        cache_probation_ratio=0.5,
        schema_epoch=None,
    )

This layer checks an object cache, then a bytes cache, before reading from the page/block provider. Because atoms are
immutable, cached entries remain valid without invalidation. Use ``schema_epoch`` to quickly isolate entries after
serializer changes.

Here are some tips for optimizing the performance of ProtoBase:

Batch Operations
~~~~~~~~~~~~~~

When performing multiple operations, it's more efficient to batch them within a single transaction:

.. code-block:: python

    # Create a transaction
    tr = db.new_transaction()

    # Perform multiple operations
    for i in range(1000):
        d = proto_db.Dictionary()
        d["id"] = i
        d["name"] = f"Item {i}"
        tr.set_root_object(f"item_{i}", d)

    # Commit the transaction
    tr.commit()

This is more efficient than creating a separate transaction for each operation.

Use Appropriate Storage
~~~~~~~~~~~~~~~~~~~~~

Choose the appropriate storage implementation based on your needs:

- ``MemoryStorage``: For testing and development, or for temporary data that doesn't need to be persisted.
- ``StandaloneFileStorage``: For single-node applications that need persistence.
- ``ClusterFileStorage``: For distributed applications that need high availability and horizontal scaling.
- ``CloudFileStorage``: For cloud-native applications that need to store data in cloud object storage (Amazon S3 or Google Cloud Storage).
- ``CloudClusterFileStorage``: For multi-server applications that need both distributed coordination and cloud storage.

Optimize Queries
~~~~~~~~~~~~~~

When working with large datasets, use the query system to filter, project, and aggregate data efficiently:

.. code-block:: python

    # Create a query plan
    from_plan = proto_db.FromPlan(large_list)

    # Filter records
    where_plan = proto_db.WherePlan(
        filter=lambda item: item["category"] == "electronics",
        based_on=from_plan
    )

    # Project only the fields we need
    select_plan = proto_db.SelectPlan(
        projection=lambda item: {"id": item["id"], "name": item["name"]},
        based_on=where_plan
    )

    # Execute the query
    for item in select_plan.execute():
        # Process only the filtered and projected items
        print(item)

This is more efficient than retrieving all records and filtering them in application code.

Use HashDictionary for Non-String Keys
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you need to use non-string keys, use ``HashDictionary`` instead of ``Dictionary``:

.. code-block:: python

    # Create a hash dictionary
    hd = proto_db.HashDictionary()

    # Add some key-value pairs with non-string keys
    hd[123] = "Value for 123"
    hd[(1, 2, 3)] = "Value for tuple"
    hd[object()] = "Value for object"

    # Store the hash dictionary as a root object
    tr.set_root_object("hash_dict", hd)

``HashDictionary`` uses hash-based lookups, which can be more efficient for certain types of keys.

Close Storage When Done
~~~~~~~~~~~~~~~~~~~~~

Always close the storage when you're done with it to release resources:

.. code-block:: python

    try:
        # Use the storage
        space = proto_db.ObjectSpace(storage)
        db = space.get_database("my_database")
        # ...
    finally:
        # Close the storage
        storage.close()

This is especially important for ``ClusterFileStorage``, ``CloudFileStorage``, and ``CloudClusterFileStorage``, which may have background threads and network connections that need to be properly closed.
