Cloud and Cluster Storage
=========================

ProtoBase provides distributed and cloud‑backed storage options built on top of its file storage engine.
This page summarizes the practical aspects of ClusterFileStorage, CloudFileStorage, and CloudClusterFileStorage
as implemented today, including a cloud page cache used to serve pages to peers.

ClusterFileStorage
------------------

- Extends the standalone file storage with a network manager for peer discovery and coordination.
- Uses a vote‑based mechanism to guard exclusive operations (e.g., root updates).
- Broadcasts root updates so peers can observe new WAL positions promptly.

CloudFileStorage
----------------

- Adds a CloudBlockProvider that writes fixed‑size objects to cloud storage (e.g., S3 or compatible API).
- Maintains a local filesystem cache directory for downloaded objects and uses background uploading.
- Provides get_reader that prefers the local cache, then cloud object fetch, caching on success.

CloudClusterFileStorage
-----------------------

- Combines cluster coordination with cloud storage and a dedicated cloud page cache.
- On page requests from peers, a cache‑aware handler serves data directly from the cloud page cache when available,
  falling back to the default handler otherwise.
- The cache directory is namespaced by server_id by default (e.g., cloud_page_cache/server_<id>), preventing
  cross‑test or multi‑instance contamination.

Example (mocked cloud client / single node)
-------------------------------------------

.. code-block:: python

    from proto_db.cloud_file_storage import CloudBlockProvider, MockS3Client
    from proto_db.cloud_cluster_file_storage import CloudClusterFileStorage

    s3_client = MockS3Client(bucket="bucket", prefix="prefix")
    provider = CloudBlockProvider(s3_client, cache_dir=".cache")

    storage = CloudClusterFileStorage(
        block_provider=provider,
        server_id="server-1",
        host="localhost",
        port=12345,
        servers=[("localhost", 12345)],
        upload_interval_ms=100,
    )

    # Read a page (will check in‑memory cache, local FS cache, then cloud; and cache result)
    # reader = storage.get_reader(wal_id, position)

Notes
-----

- Tests patch the network manager and cloud client so no external systems are required.
- The cloud page cache persists mappings to cache_mappings.json under the cache directory and will reuse
  existing cached objects when possible.
