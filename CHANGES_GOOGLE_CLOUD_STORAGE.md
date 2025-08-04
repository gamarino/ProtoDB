# Google Cloud Storage Support in ProtoBase

This document describes the implementation of Google Cloud Storage support in ProtoBase as an alternative to Amazon S3.

## Overview

ProtoBase now supports both Amazon S3 and Google Cloud Storage as backend storage options for `CloudFileStorage` and
`CloudClusterFileStorage`. This allows you to choose the cloud storage provider that best fits your needs.

## Implementation

The cloud storage support is implemented through the following classes:

- `CloudStorageClient`: An abstract base class that defines the interface for cloud storage clients.
- `S3Client`: An implementation of `CloudStorageClient` for Amazon S3.
- `GoogleCloudClient`: An implementation of `CloudStorageClient` for Google Cloud Storage.
- `CloudBlockProvider`: A block provider that works with any `CloudStorageClient` implementation.
- `CloudFileStorage`: A file storage implementation that uses a `CloudBlockProvider`.
- `CloudClusterFileStorage`: A cluster file storage implementation that uses a `CloudBlockProvider`.

## Changes Made

1. Completed the implementation of `S3Client` by adding the missing abstract methods:
    - `_init_client`: Initializes the S3 client using boto3 or falls back to a mock implementation.
    - `get_object`: Gets an object from S3.
    - `put_object`: Puts an object to S3.
    - `list_objects`: Lists objects in S3.
    - `delete_object`: Deletes an object from S3.

2. Ensured that `CloudBlockProvider`, `CloudFileStorage`, and `CloudClusterFileStorage` work with both `S3Client` and
   `GoogleCloudClient`.

## Usage

### Using Amazon S3

```python
from proto_db.cloud_file_storage import S3Client, CloudBlockProvider, CloudFileStorage

# Create an S3 client
s3_client = S3Client(
    bucket="my-bucket",
    prefix="my-prefix",
    endpoint_url="https://s3.amazonaws.com",
    access_key="my-access-key",
    secret_key="my-secret-key",
    region="us-west-2"
)

# Create a CloudBlockProvider with the S3 client
block_provider = CloudBlockProvider(
    cloud_client=s3_client,
    cache_dir="s3_cache",
    cache_size=500 * 1024 * 1024,  # 500MB cache
    object_size=5 * 1024 * 1024  # 5MB objects
)

# Create a CloudFileStorage with the block provider
storage = CloudFileStorage(
    block_provider=block_provider,
    server_id="my-server",
    host="localhost",
    port=12345,
    servers=[("localhost", 12345)]
)

# Use the storage as usual
```

### Using Google Cloud Storage

```python
from proto_db.cloud_file_storage import GoogleCloudClient, CloudBlockProvider, CloudFileStorage

# Create a Google Cloud Storage client
gcs_client = GoogleCloudClient(
    bucket="my-bucket",
    prefix="my-prefix",
    project_id="my-project",
    credentials_path="/path/to/credentials.json"
)

# Create a CloudBlockProvider with the Google Cloud Storage client
block_provider = CloudBlockProvider(
    cloud_client=gcs_client,
    cache_dir="gcs_cache",
    cache_size=500 * 1024 * 1024,  # 500MB cache
    object_size=5 * 1024 * 1024  # 5MB objects
)

# Create a CloudFileStorage with the block provider
storage = CloudFileStorage(
    block_provider=block_provider,
    server_id="my-server",
    host="localhost",
    port=12345,
    servers=[("localhost", 12345)]
)

# Use the storage as usual
```

### Using CloudClusterFileStorage

The usage for `CloudClusterFileStorage` is similar, just replace `CloudFileStorage` with `CloudClusterFileStorage` in
the examples above.

```python
from proto_db.cloud_cluster_file_storage import CloudClusterFileStorage

# Create a CloudClusterFileStorage with the block provider
storage = CloudClusterFileStorage(
    block_provider=block_provider,
    server_id="my-server",
    host="localhost",
    port=12345,
    servers=[("localhost", 12345)],
    page_cache_dir="cloud_page_cache"
)

# Use the storage as usual
```

## Dependencies

- For Amazon S3: `boto3`
- For Google Cloud Storage: `google-cloud-storage`

Make sure to install the appropriate dependency for the cloud storage provider you want to use:

```bash
# For Amazon S3
pip install boto3

# For Google Cloud Storage
pip install google-cloud-storage
```

## Testing

Both implementations have comprehensive test suites:

- `test_cloud_file_storage.py`: Tests for `CloudFileStorage` with S3.
- `test_cloud_file_storage_with_gcs.py`: Tests for `CloudFileStorage` with Google Cloud Storage.
- `test_cloud_cluster_file_storage.py`: Tests for `CloudClusterFileStorage` with S3.
- `test_cloud_cluster_file_storage_with_gcs.py`: Tests for `CloudClusterFileStorage` with Google Cloud Storage.
- `test_google_cloud_client.py`: Tests specifically for `GoogleCloudClient`.

You can run the tests with:

```bash
python -m unittest proto_db.tests.test_cloud_file_storage
python -m unittest proto_db.tests.test_cloud_file_storage_with_gcs
python -m unittest proto_db.tests.test_cloud_cluster_file_storage
python -m unittest proto_db.tests.test_cloud_cluster_file_storage_with_gcs
python -m unittest proto_db.tests.test_google_cloud_client
```