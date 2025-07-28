from __future__ import annotations
import concurrent.futures
import io
import json
import os
import socket
import threading
import time
import uuid
import logging
import base64
import hashlib
from typing import Dict, List, Optional, Tuple, Any, Set, BinaryIO
from abc import ABC, abstractmethod

from . import common
from .common import MB, GB, Future, BlockProvider, AtomPointer
from .exceptions import ProtoUnexpectedException, ProtoValidationException
from .fsm import FSM
from .standalone_file_storage import StandaloneFileStorage, WALState, WALWriteOperation
from .cluster_file_storage import ClusterFileStorage, ClusterNetworkManager

_logger = logging.getLogger(__name__)

# Default cloud storage settings
DEFAULT_OBJECT_SIZE = 5 * MB  # Default size for cloud storage objects
DEFAULT_LOCAL_CACHE_SIZE = 500 * MB  # Default size for local cache
DEFAULT_CACHE_DIR = "cloud_cache"  # Default directory for local cache
DEFAULT_UPLOAD_INTERVAL_MS = 5000  # 5 seconds between cloud uploads
DEFAULT_CLEANUP_INTERVAL_MS = 60000  # 1 minute between cache cleanups

# For backward compatibility
DEFAULT_S3_OBJECT_SIZE = DEFAULT_OBJECT_SIZE


class CloudStorageError(ProtoUnexpectedException):
    """
    Exception raised for cloud storage specific errors.
    """
    pass


class CloudObjectMetadata:
    """
    Metadata for a cloud storage object.
    """
    def __init__(self, 
                 key: str, 
                 size: int, 
                 etag: str = None, 
                 last_modified: float = None,
                 is_cached: bool = False,
                 cache_path: str = None):
        """
        Initialize cloud object metadata.

        Args:
            key: The object key
            size: The size of the object in bytes
            etag: The ETag of the object (usually MD5 hash)
            last_modified: The last modified timestamp
            is_cached: Whether the object is cached locally
            cache_path: Path to the local cache file
        """
        self.key = key
        self.size = size
        self.etag = etag
        self.last_modified = last_modified or time.time()
        self.is_cached = is_cached
        self.cache_path = cache_path

# For backward compatibility
S3ObjectMetadata = CloudObjectMetadata


class CloudStorageClient(ABC):
    """
    Abstract cloud storage client interface.

    This class defines the interface for interacting with cloud storage providers.
    Concrete implementations should be provided for specific cloud providers.
    """

    def __init__(self, 
                 bucket: str, 
                 prefix: str = ""):
        """
        Initialize the cloud storage client.

        Args:
            bucket: The storage bucket name
            prefix: The prefix for all objects in the bucket
        """
        self.bucket = bucket
        self.prefix = prefix

        # Initialize the client
        self._init_client()

    @abstractmethod
    def _init_client(self):
        """
        Initialize the cloud storage client.

        This method should be implemented by concrete subclasses to initialize
        the specific cloud storage client implementation.
        """
        pass

    @abstractmethod
    def get_object(self, key: str) -> Tuple[bytes, Dict[str, Any]]:
        """
        Get an object from cloud storage.

        Args:
            key: The object key

        Returns:
            Tuple containing the object data and metadata
        """
        pass

    @abstractmethod
    def put_object(self, key: str, data: bytes) -> Dict[str, Any]:
        """
        Put an object to cloud storage.

        Args:
            key: The object key
            data: The object data

        Returns:
            Object metadata
        """
        pass

    @abstractmethod
    def list_objects(self, prefix: str = None) -> List[Dict[str, Any]]:
        """
        List objects in cloud storage.

        Args:
            prefix: The prefix to filter objects

        Returns:
            List of object metadata
        """
        pass

    @abstractmethod
    def delete_object(self, key: str) -> bool:
        """
        Delete an object from cloud storage.

        Args:
            key: The object key

        Returns:
            True if the object was deleted, False otherwise
        """
        pass


class S3Client(CloudStorageClient):
    """
    S3 client implementation.

    This class implements the CloudStorageClient interface for S3-compatible storage.
    """

    def __init__(self, 
                 bucket: str, 
                 prefix: str = "",
                 endpoint_url: str = None,
                 access_key: str = None,
                 secret_key: str = None,
                 region: str = None):
        """
        Initialize the S3 client.

        Args:
            bucket: The S3 bucket name
            prefix: The prefix for all objects in the bucket
            endpoint_url: The S3 endpoint URL
            access_key: The S3 access key
            secret_key: The S3 secret key
            region: The S3 region
        """
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region

        # Call parent constructor
        super().__init__(bucket, prefix)

    def _init_client(self):
        """
        Initialize the S3 client.

        This method initializes the S3 client using the boto3 library if available,
        or falls back to a mock implementation for testing.
        """
        try:
            import boto3
            from botocore.client import Config

            # Create session with credentials if provided
            session_kwargs = {}
            if self.access_key and self.secret_key:
                session_kwargs['aws_access_key_id'] = self.access_key
                session_kwargs['aws_secret_access_key'] = self.secret_key

            if self.region:
                session_kwargs['region_name'] = self.region

            session = boto3.session.Session(**session_kwargs)

            # Create S3 client
            client_kwargs = {}
            if self.endpoint_url:
                client_kwargs['endpoint_url'] = self.endpoint_url

            # Use a config that allows for large files
            client_kwargs['config'] = Config(
                signature_version='s3v4',
                s3={'addressing_style': 'path'}
            )

            self.client = session.client('s3', **client_kwargs)
            self.resource = session.resource('s3', **client_kwargs)
            self.bucket_obj = self.resource.Bucket(self.bucket)

            _logger.info(f"Initialized S3 client for bucket '{self.bucket}'")
        except ImportError:
            _logger.warning("boto3 not installed, using mock implementation")
            self.client = None
            self.resource = None
            self.bucket_obj = None
            self.objects = {}

    def get_object(self, key: str) -> Tuple[bytes, Dict[str, Any]]:
        """
        Get an object from S3.

        Args:
            key: The object key

        Returns:
            Tuple containing the object data and metadata
        """
        if self.client is None:
            # Mock implementation
            full_key = f"{self.prefix}/{key}" if self.prefix else key
            if full_key not in self.objects:
                raise CloudStorageError(message=f"Object '{full_key}' not found in mock S3")

            obj = self.objects[full_key]
            metadata = {
                "ETag": obj["etag"],
                "ContentLength": len(obj["data"]),
                "LastModified": obj["last_modified"]
            }

            return obj["data"], metadata
        else:
            # Real implementation
            full_key = f"{self.prefix}/{key}" if self.prefix else key

            try:
                response = self.client.get_object(Bucket=self.bucket, Key=full_key)
                data = response['Body'].read()

                metadata = {
                    "ETag": response.get('ETag', ''),
                    "ContentLength": response.get('ContentLength', len(data)),
                    "LastModified": response.get('LastModified', time.time())
                }

                return data, metadata
            except Exception as e:
                raise CloudStorageError(message=f"Failed to get object '{full_key}' from S3: {e}")

    def put_object(self, key: str, data: bytes) -> Dict[str, Any]:
        """
        Put an object to S3.

        Args:
            key: The object key
            data: The object data

        Returns:
            Object metadata
        """
        if self.client is None:
            # Mock implementation
            full_key = f"{self.prefix}/{key}" if self.prefix else key
            etag = hashlib.md5(data).hexdigest()
            last_modified = time.time()

            self.objects[full_key] = {
                "data": data,
                "etag": etag,
                "last_modified": last_modified
            }

            metadata = {
                "ETag": etag,
                "ContentLength": len(data),
                "LastModified": last_modified
            }

            _logger.debug(f"Put object '{full_key}' to mock S3 (size: {len(data)} bytes)")
            return metadata
        else:
            # Real implementation
            full_key = f"{self.prefix}/{key}" if self.prefix else key

            try:
                response = self.client.put_object(
                    Bucket=self.bucket,
                    Key=full_key,
                    Body=data
                )

                metadata = {
                    "ETag": response.get('ETag', ''),
                    "ContentLength": len(data),
                    "LastModified": time.time()
                }

                return metadata
            except Exception as e:
                raise CloudStorageError(message=f"Failed to put object '{full_key}' to S3: {e}")

    def list_objects(self, prefix: str = None) -> List[Dict[str, Any]]:
        """
        List objects in S3.

        Args:
            prefix: The prefix to filter objects

        Returns:
            List of object metadata
        """
        if self.client is None:
            # Mock implementation
            list_prefix = f"{self.prefix}/{prefix}" if self.prefix and prefix else (self.prefix or prefix or "")

            result = []
            for key, obj in self.objects.items():
                if not list_prefix or key.startswith(list_prefix):
                    result.append({
                        "Key": key,
                        "ETag": obj["etag"],
                        "Size": len(obj["data"]),
                        "LastModified": obj["last_modified"]
                    })

            return result
        else:
            # Real implementation
            list_prefix = f"{self.prefix}/{prefix}" if self.prefix and prefix else (self.prefix or prefix or "")

            try:
                paginator = self.client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=self.bucket, Prefix=list_prefix)

                result = []
                for page in pages:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            result.append({
                                "Key": obj['Key'],
                                "ETag": obj.get('ETag', ''),
                                "Size": obj.get('Size', 0),
                                "LastModified": obj.get('LastModified', time.time())
                            })

                return result
            except Exception as e:
                _logger.warning(f"Failed to list objects with prefix '{list_prefix}' in S3: {e}")
                return []

    def delete_object(self, key: str) -> bool:
        """
        Delete an object from S3.

        Args:
            key: The object key

        Returns:
            True if the object was deleted, False otherwise
        """
        if self.client is None:
            # Mock implementation
            full_key = f"{self.prefix}/{key}" if self.prefix else key
            if full_key in self.objects:
                del self.objects[full_key]
                _logger.debug(f"Deleted object '{full_key}' from mock S3")
                return True
            return False
        else:
            # Real implementation
            full_key = f"{self.prefix}/{key}" if self.prefix else key

            try:
                self.client.delete_object(Bucket=self.bucket, Key=full_key)
                _logger.debug(f"Deleted object '{full_key}' from S3")
                return True
            except Exception as e:
                _logger.warning(f"Failed to delete object '{full_key}' from S3: {e}")
                return False


class MockCloudStorageClient(CloudStorageClient):
    """
    Mock implementation of CloudStorageClient for testing and development.

    This implementation stores objects in memory and simulates cloud storage behavior.
    """

    def _init_client(self):
        """
        Initialize the mock cloud storage client.
        """
        self.objects = {}
        _logger.info(f"Initialized MockCloudStorageClient with bucket '{self.bucket}' and prefix '{self.prefix}'")

    def get_object(self, key: str) -> Tuple[bytes, Dict[str, Any]]:
        """
        Get an object from the mock cloud storage.

        Args:
            key: The object key

        Returns:
            Tuple containing the object data and metadata
        """
        full_key = f"{self.prefix}/{key}" if self.prefix else key
        if full_key not in self.objects:
            raise CloudStorageError(message=f"Object '{full_key}' not found in mock cloud storage")

        obj = self.objects[full_key]
        metadata = {
            "ETag": obj["etag"],
            "ContentLength": len(obj["data"]),
            "LastModified": obj["last_modified"]
        }

        return obj["data"], metadata

    def put_object(self, key: str, data: bytes) -> Dict[str, Any]:
        """
        Put an object to the mock cloud storage.

        Args:
            key: The object key
            data: The object data

        Returns:
            Object metadata
        """
        full_key = f"{self.prefix}/{key}" if self.prefix else key
        etag = hashlib.md5(data).hexdigest()
        last_modified = time.time()

        self.objects[full_key] = {
            "data": data,
            "etag": etag,
            "last_modified": last_modified
        }

        metadata = {
            "ETag": etag,
            "ContentLength": len(data),
            "LastModified": last_modified
        }

        _logger.debug(f"Put object '{full_key}' to mock cloud storage (size: {len(data)} bytes)")
        return metadata

    def list_objects(self, prefix: str = None) -> List[Dict[str, Any]]:
        """
        List objects in the mock cloud storage.

        Args:
            prefix: The prefix to filter objects

        Returns:
            List of object metadata
        """
        list_prefix = f"{self.prefix}/{prefix}" if self.prefix and prefix else (self.prefix or prefix or "")

        result = []
        for key, obj in self.objects.items():
            if not list_prefix or key.startswith(list_prefix):
                result.append({
                    "Key": key,
                    "ETag": obj["etag"],
                    "Size": len(obj["data"]),
                    "LastModified": obj["last_modified"]
                })

        return result

    def delete_object(self, key: str) -> bool:
        """
        Delete an object from the mock cloud storage.

        Args:
            key: The object key

        Returns:
            True if the object was deleted, False otherwise
        """
        full_key = f"{self.prefix}/{key}" if self.prefix else key
        if full_key in self.objects:
            del self.objects[full_key]
            _logger.debug(f"Deleted object '{full_key}' from mock cloud storage")
            return True
        return False


class MockS3Client(MockCloudStorageClient):
    """
    Mock implementation of S3Client for testing and development.

    This implementation inherits from MockCloudStorageClient and adds S3-specific behavior.
    """

    def __init__(self, 
                 bucket: str, 
                 prefix: str = "",
                 endpoint_url: str = None,
                 access_key: str = None,
                 secret_key: str = None,
                 region: str = None):
        """
        Initialize the mock S3 client.

        Args:
            bucket: The S3 bucket name
            prefix: The prefix for all objects in the bucket
            endpoint_url: The S3 endpoint URL
            access_key: The S3 access key
            secret_key: The S3 secret key
            region: The S3 region
        """
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region

        # Call parent constructor
        super().__init__(bucket, prefix)


class CloudBlockProvider(BlockProvider):
    """
    Block provider implementation for cloud storage.

    This class implements the BlockProvider interface for cloud storage providers,
    with local caching to improve performance.
    """

    def __init__(self, 
                 cloud_client: CloudStorageClient,
                 cache_dir: str = DEFAULT_CACHE_DIR,
                 cache_size: int = DEFAULT_LOCAL_CACHE_SIZE,
                 object_size: int = DEFAULT_OBJECT_SIZE):
        """
        Initialize the cloud block provider.

        Args:
            cloud_client: The cloud storage client to use
            cache_dir: Directory for local cache
            cache_size: Maximum size of local cache in bytes
            object_size: Size of cloud storage objects in bytes
        """
        self.cloud_client = cloud_client
        self.cache_dir = cache_dir
        self.cache_size = cache_size
        self.object_size = object_size

        # For backward compatibility
        self.s3_client = cloud_client

        # Create cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)

        # Initialize cache metadata
        self.cache_metadata = {}
        self.cache_lock = threading.Lock()
        self.current_cache_size = 0

        # Initialize WAL metadata
        self.wal_metadata = {}
        self.wal_lock = threading.Lock()

        # Root object metadata
        self.root_lock = threading.Lock()

        # Load existing cache metadata
        self._load_cache_metadata()

        # Load configuration
        self.config_data = self._load_config()

        _logger.info(f"Initialized CloudBlockProvider with cache_dir='{self.cache_dir}', "
                    f"cache_size={self.cache_size}, object_size={self.object_size}")

    def _load_cache_metadata(self):
        """
        Load cache metadata from disk.
        """
        cache_metadata_path = os.path.join(self.cache_dir, "cache_metadata.json")
        if os.path.exists(cache_metadata_path):
            try:
                with open(cache_metadata_path, 'r') as f:
                    metadata = json.load(f)

                # Convert metadata to CloudObjectMetadata objects
                for key, meta in metadata.items():
                    self.cache_metadata[key] = CloudObjectMetadata(
                        key=meta["key"],
                        size=meta["size"],
                        etag=meta["etag"],
                        last_modified=meta["last_modified"],
                        is_cached=meta["is_cached"],
                        cache_path=meta["cache_path"]
                    )
                    if meta["is_cached"] and os.path.exists(meta["cache_path"]):
                        self.current_cache_size += meta["size"]

                _logger.info(f"Loaded {len(self.cache_metadata)} cache metadata entries, "
                            f"current cache size: {self.current_cache_size} bytes")
            except Exception as e:
                _logger.warning(f"Failed to load cache metadata: {e}")

    def _save_cache_metadata(self):
        """
        Save cache metadata to disk.
        """
        cache_metadata_path = os.path.join(self.cache_dir, "cache_metadata.json")
        try:
            # Convert S3ObjectMetadata objects to dictionaries
            metadata = {}
            for key, meta in self.cache_metadata.items():
                metadata[key] = {
                    "key": meta.key,
                    "size": meta.size,
                    "etag": meta.etag,
                    "last_modified": meta.last_modified,
                    "is_cached": meta.is_cached,
                    "cache_path": meta.cache_path
                }

            with open(cache_metadata_path, 'w') as f:
                json.dump(metadata, f)

            _logger.debug(f"Saved {len(self.cache_metadata)} cache metadata entries")
        except Exception as e:
            _logger.warning(f"Failed to save cache metadata: {e}")

    def _load_config(self):
        """
        Load configuration from S3 or create default configuration.
        """
        import configparser
        config = configparser.ConfigParser()

        try:
            # Try to get config from cloud storage
            data, _ = self.cloud_client.get_object("space.config")
            config_str = data.decode('utf-8')
            config.read_string(config_str)
            _logger.info("Loaded configuration from cloud storage")
        except Exception as e:
            _logger.warning(f"Failed to load configuration from S3: {e}")
            _logger.info("Creating default configuration")

            # Create default configuration
            config["DEFAULT"] = {
                "object_size": str(self.object_size),
                "cache_size": str(self.cache_size)
            }

            # Save configuration to cloud storage
            config_str = io.StringIO()
            config.write(config_str)
            self.cloud_client.put_object("space.config", config_str.getvalue().encode('utf-8'))

        return config

    def _get_object_key(self, wal_id: uuid.UUID, position: int) -> str:
        """
        Get the cloud storage object key for a WAL position.

        Args:
            wal_id: The WAL ID
            position: The position in the WAL

        Returns:
            The cloud storage object key
        """
        # Calculate the object index based on position and object size
        object_index = position // self.object_size
        return f"wal/{wal_id}/{object_index}"

    def _get_object_offset(self, position: int) -> int:
        """
        Get the offset within a cloud storage object for a WAL position.

        Args:
            position: The position in the WAL

        Returns:
            The offset within the cloud storage object
        """
        return position % self.object_size

    def _cache_object(self, key: str, data: bytes, metadata: Dict[str, Any]) -> S3ObjectMetadata:
        """
        Cache a cloud storage object locally.

        Args:
            key: The cloud storage object key
            data: The object data
            metadata: The object metadata

        Returns:
            The cache metadata
        """
        with self.cache_lock:
            # Check if we need to make room in the cache
            if self.current_cache_size + len(data) > self.cache_size:
                self._evict_cache_entries(len(data))

            # Create cache file path
            cache_path = os.path.join(self.cache_dir, hashlib.md5(key.encode()).hexdigest())

            # Write data to cache file
            with open(cache_path, 'wb') as f:
                f.write(data)

            # Update cache metadata
            cache_meta = CloudObjectMetadata(
                key=key,
                size=len(data),
                etag=metadata.get("ETag"),
                last_modified=metadata.get("LastModified", time.time()),
                is_cached=True,
                cache_path=cache_path
            )

            self.cache_metadata[key] = cache_meta
            self.current_cache_size += len(data)

            _logger.debug(f"Cached object '{key}' (size: {len(data)} bytes)")
            return cache_meta

    def _evict_cache_entries(self, required_space: int):
        """
        Evict cache entries to make room for new data.

        Args:
            required_space: The amount of space required in bytes
        """
        # Sort cache entries by last modified time (oldest first)
        entries = sorted(
            [(k, v) for k, v in self.cache_metadata.items() if v.is_cached],
            key=lambda x: x[1].last_modified
        )

        space_freed = 0
        for key, meta in entries:
            if self.current_cache_size - space_freed + required_space <= self.cache_size:
                break

            # Remove cache file
            if os.path.exists(meta.cache_path):
                try:
                    os.remove(meta.cache_path)
                    _logger.debug(f"Evicted cache entry '{key}' (size: {meta.size} bytes)")
                except Exception as e:
                    _logger.warning(f"Failed to remove cache file '{meta.cache_path}': {e}")

            # Update metadata
            meta.is_cached = False
            meta.cache_path = None
            self.cache_metadata[key] = meta

            space_freed += meta.size

        self.current_cache_size -= space_freed
        _logger.debug(f"Evicted {space_freed} bytes from cache")

    def get_config_data(self):
        """
        Get configuration data.

        Returns:
            The configuration data
        """
        return self.config_data

    def get_new_wal(self) -> tuple[uuid.UUID, int]:
        """
        Get a WAL to use.

        Returns:
            A tuple with the ID of the WAL and the next offset to use
        """
        with self.wal_lock:
            # Create a new WAL ID
            wal_id = uuid.uuid4()

            # Store WAL metadata
            self.wal_metadata[str(wal_id)] = {
                "created_at": time.time(),
                "last_position": 0
            }

            _logger.info(f"Created new WAL with ID {wal_id}")
            return wal_id, 0

    def get_reader(self, wal_id: uuid.UUID, position: int) -> BinaryIO:
        """
        Get a reader for a WAL position.

        Args:
            wal_id: The WAL ID
            position: The position in the WAL

        Returns:
            A binary reader
        """
        # Get the S3 object key and offset
        key = self._get_object_key(wal_id, position)
        offset = self._get_object_offset(position)

        # Check if the object is cached
        with self.cache_lock:
            if key in self.cache_metadata and self.cache_metadata[key].is_cached:
                cache_meta = self.cache_metadata[key]
                try:
                    # Open the cache file
                    f = open(cache_meta.cache_path, 'rb')
                    f.seek(offset)
                    return f
                except Exception as e:
                    _logger.warning(f"Failed to open cached object '{key}': {e}")

        try:
            # Get the object from cloud storage
            data, metadata = self.cloud_client.get_object(key)

            # Cache the object
            cache_meta = self._cache_object(key, data, metadata)

            # Open the cache file
            f = open(cache_meta.cache_path, 'rb')
            f.seek(offset)
            return f
        except Exception as e:
            _logger.error(f"Failed to get object '{key}' from cloud storage: {e}")
            raise CloudStorageError(message=f"Failed to read from WAL {wal_id} at position {position}: {e}")

    def get_writer_wal(self) -> uuid.UUID:
        """
        Get the current writer WAL ID.

        Returns:
            The current writer WAL ID
        """
        with self.wal_lock:
            if not self.wal_metadata:
                # No WALs yet, create one
                wal_id, _ = self.get_new_wal()
                return wal_id

            # Get the most recent WAL
            latest_wal = max(self.wal_metadata.items(), key=lambda x: x[1]["created_at"])
            return uuid.UUID(latest_wal[0])

    def write_streamer(self, wal_id: uuid.UUID) -> io.FileIO:
        """
        Get a writer for a WAL.

        Args:
            wal_id: The WAL ID

        Returns:
            A file-like object for writing
        """
        # Create a temporary file for writing
        temp_file = io.BytesIO()

        # Create a wrapper that tracks the position and flushes to cloud storage
        class CloudWriter:
            def __init__(self, provider: CloudBlockProvider, wal_id: uuid.UUID, buffer: io.BytesIO):
                self.provider = provider
                self.wal_id = wal_id
                self.buffer = buffer
                self.position = 0
                self.closed = False

            def write(self, data: bytes) -> int:
                if self.closed:
                    raise ValueError("I/O operation on closed file")

                # Write to the buffer
                bytes_written = self.buffer.write(data)
                self.position += bytes_written
                return bytes_written

            def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
                if self.closed:
                    raise ValueError("I/O operation on closed file")

                # Seek in the buffer
                self.position = self.buffer.seek(offset, whence)
                return self.position

            def tell(self) -> int:
                if self.closed:
                    raise ValueError("I/O operation on closed file")

                return self.position

            def flush(self) -> None:
                if self.closed:
                    raise ValueError("I/O operation on closed file")

                # No need to do anything here, we'll flush on close
                pass

            def close(self) -> None:
                if self.closed:
                    return

                # Get the buffer data
                self.buffer.seek(0)
                data = self.buffer.read()

                if data:
                    # Update WAL metadata
                    with self.provider.wal_lock:
                        wal_meta = self.provider.wal_metadata.get(str(self.wal_id), {})
                        wal_meta["last_position"] = max(wal_meta.get("last_position", 0), self.position)
                        self.provider.wal_metadata[str(self.wal_id)] = wal_meta

                    # Split the data into cloud storage objects
                    for i in range(0, len(data), self.provider.object_size):
                        # Calculate the object key and data
                        start_position = i
                        end_position = min(i + self.provider.object_size, len(data))
                        object_data = data[start_position:end_position]

                        key = self.provider._get_object_key(self.wal_id, start_position)

                        try:
                            # Upload to cloud storage
                            metadata = self.provider.cloud_client.put_object(key, object_data)

                            # Cache the object
                            self.provider._cache_object(key, object_data, metadata)
                        except Exception as e:
                            _logger.error(f"Failed to upload object '{key}' to cloud storage: {e}")
                            raise CloudStorageError(message=f"Failed to write to WAL {self.wal_id}: {e}")

                self.closed = True
                self.buffer.close()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.close()

        return CloudWriter(self, wal_id, temp_file)

    def get_current_root_object(self) -> AtomPointer:
        """
        Read current root object from storage.

        Returns:
            The current root object pointer
        """
        with self.root_lock:
            try:
                # Try to get the root object from cloud storage
                data, _ = self.cloud_client.get_object("space_root")
                root_dict = json.loads(data.decode('utf-8'))

                if not isinstance(root_dict, dict):
                    raise CloudStorageError(
                        message=f"Reading root object, a dictionary was expected, got {type(root_dict)} instead"
                    )

                if "className" not in root_dict or \
                   "transaction_id" not in root_dict or \
                   "offset" not in root_dict:
                    raise CloudStorageError(
                        message="Invalid format for root object!"
                    )

                root_pointer = AtomPointer(
                    transaction_id=uuid.UUID(root_dict["transaction_id"]),
                    offset=root_dict["offset"]
                )

                return root_pointer
            except Exception as e:
                _logger.warning(f"Failed to get root object from cloud storage: {e}")
                return None

    def update_root_object(self, new_root: AtomPointer):
        """
        Updates or create the root object in storage.

        Args:
            new_root: The new root object pointer
        """
        with self.root_lock:
            try:
                # Create root object dictionary
                root_dict = {
                    "className": "RootObject",
                    "transaction_id": str(new_root.transaction_id),
                    "offset": new_root.offset
                }

                # Convert to JSON and upload to cloud storage
                data = json.dumps(root_dict).encode('utf-8')
                self.cloud_client.put_object("space_root", data)

                _logger.info(f"Updated root object: {root_dict}")
            except Exception as e:
                _logger.error(f"Failed to update root object: {e}")
                raise CloudStorageError(message=f"Failed to update root object: {e}")

    def close_wal(self, transaction_id: uuid.UUID):
        """
        Close a WAL.

        Args:
            transaction_id: The WAL ID
        """
        # Nothing special to do here, WALs are automatically closed
        pass

    def close(self):
        """
        Close the block provider.
        """
        # Save cache metadata
        self._save_cache_metadata()
        _logger.info("Closed CloudBlockProvider")


class CloudFileStorage(ClusterFileStorage):
    """
    An implementation of cloud file storage with support for cloud object storage.

    This class extends ClusterFileStorage to add support for storing data in a cloud
    object storage service, with local caching to improve performance.
    """

    def __init__(self,
                 block_provider: CloudBlockProvider,
                 server_id: str = None,
                 host: str = None,
                 port: int = None,
                 servers: List[Tuple[str, int]] = None,
                 vote_timeout_ms: int = None,
                 read_timeout_ms: int = None,
                 retry_interval_ms: int = None,
                 max_retries: int = None,
                 buffer_size: int = common.MB,
                 blob_max_size: int = common.GB * 2,
                 max_workers: int = (os.cpu_count() or 1) * 5,
                 upload_interval_ms: int = DEFAULT_UPLOAD_INTERVAL_MS):
        """
        Constructor for the CloudFileStorage class.

        Args:
            block_provider: The underlying cloud block provider
            server_id: Unique identifier for this server
            host: Host address to bind to
            port: Port to listen on
            servers: List of (host, port) tuples for all servers in the cluster
            vote_timeout_ms: Timeout for vote requests in milliseconds
            read_timeout_ms: Timeout for page requests in milliseconds
            retry_interval_ms: Interval between retries in milliseconds
            max_retries: Maximum number of retry attempts
            buffer_size: Size of the WAL buffer in bytes
            blob_max_size: Maximum size of a blob in bytes
            max_workers: Number of worker threads for asynchronous operations
            upload_interval_ms: Interval between S3 uploads in milliseconds
        """
        super().__init__(
            block_provider=block_provider,
            server_id=server_id,
            host=host,
            port=port,
            servers=servers,
            vote_timeout_ms=vote_timeout_ms,
            read_timeout_ms=read_timeout_ms,
            retry_interval_ms=retry_interval_ms,
            max_retries=max_retries,
            buffer_size=buffer_size,
            blob_max_size=blob_max_size,
            max_workers=max_workers
        )

        # Ensure block_provider is a CloudBlockProvider
        if not isinstance(block_provider, CloudBlockProvider):
            raise ProtoValidationException(
                message="block_provider must be an instance of CloudBlockProvider"
            )

        # Cloud-specific attributes
        self.cloud_block_provider = block_provider
        self.upload_interval_ms = upload_interval_ms

        # Pending uploads
        self.pending_uploads = []
        self.upload_lock = threading.Lock()

        # Start background uploader thread
        self.uploader_running = True
        self.uploader_thread = threading.Thread(target=self._background_uploader)
        self.uploader_thread.daemon = True
        self.uploader_thread.start()

        _logger.info(f"Initialized CloudFileStorage with upload_interval_ms={self.upload_interval_ms}")

    def _background_uploader(self):
        """
        Background thread for uploading pending writes to cloud storage.
        """
        while self.state == 'Running' and self.uploader_running:
            try:
                # Sleep for the upload interval
                time.sleep(self.upload_interval_ms / 1000)

                # Process pending uploads
                self._process_pending_uploads()
            except Exception as e:
                _logger.error(f"Error in background uploader: {e}")
                # Add a small sleep to avoid tight loop in case of repeated errors
                time.sleep(0.1)

    def _process_pending_uploads(self):
        """
        Process pending uploads to cloud storage.
        """
        operations = []

        # Use a timeout when acquiring the lock to avoid potential deadlocks
        if not self.upload_lock.acquire(timeout=5):  # 5 seconds timeout
            _logger.warning("Could not acquire upload_lock in _process_pending_uploads, skipping this cycle")
            return

        try:
            if not self.pending_uploads:
                return

            # Get all pending uploads
            operations = self.pending_uploads
            self.pending_uploads = []
        finally:
            self.upload_lock.release()

        if not operations:
            return

        _logger.debug(f"Processing {len(operations)} pending uploads")

        # Group operations by WAL ID
        wal_operations = {}
        for operation in operations:
            wal_id = operation.transaction_id
            if wal_id not in wal_operations:
                wal_operations[wal_id] = []
            wal_operations[wal_id].append(operation)

        # Process each WAL's operations
        for wal_id, ops in wal_operations.items():
            try:
                # Sort operations by offset
                ops.sort(key=lambda op: op.offset)

                # Open a writer for this WAL
                with self.cloud_block_provider.write_streamer(wal_id) as stream:
                    for operation in ops:
                        # Seek to the operation's offset
                        stream.seek(operation.offset)

                        # Write all segments
                        for segment in operation.segments:
                            if not segment:
                                continue

                            stream.write(segment)

                # Remove the corresponding in-memory segments
                with self._lock:
                    for operation in ops:
                        for i, segment in enumerate(operation.segments):
                            if not segment:
                                continue

                            segment_offset = operation.offset
                            for j in range(i):
                                segment_offset += len(operation.segments[j])

                            pointer = (operation.transaction_id, segment_offset)
                            if pointer in self.in_memory_segments:
                                del self.in_memory_segments[pointer]

                _logger.debug(f"Uploaded {len(ops)} operations for WAL {wal_id}")
            except Exception as e:
                _logger.error(f"Failed to upload operations for WAL {wal_id}: {e}")

                # Put operations back in the pending list
                if not self.upload_lock.acquire(timeout=5):  # 5 seconds timeout
                    _logger.warning("Could not acquire upload_lock to put operations back in pending_uploads, operations will be lost")
                else:
                    try:
                        self.pending_uploads.extend(ops)
                    finally:
                        self.upload_lock.release()

    def _flush_wal(self):
        """
        Flushes the current WAL buffer to the pending writes list.

        This method overrides the base implementation to also add the operation
        to the pending uploads list.

        Returns:
            int: The number of bytes flushed, or 0 if nothing was flushed
        """
        # Call the parent implementation
        written_size = super()._flush_wal()

        if written_size > 0:
            # Get the operation that was just added to pending_writes
            if not self._lock.acquire(timeout=5):  # 5 seconds timeout
                _logger.warning("Could not acquire _lock in _flush_wal, skipping adding to pending_uploads")
                return written_size

            try:
                if self.pending_writes:
                    operation = self.pending_writes[-1]

                    # Add to pending uploads
                    if not self.upload_lock.acquire(timeout=5):  # 5 seconds timeout
                        _logger.warning("Could not acquire upload_lock in _flush_wal, skipping adding to pending_uploads")
                        return written_size

                    try:
                        self.pending_uploads.append(operation)
                    finally:
                        self.upload_lock.release()
            finally:
                self._lock.release()

        return written_size

    def flush_wal(self) -> tuple[int, int]:
        """
        Public method to flush WAL buffer and process pending writes.

        This method overrides the base implementation to also process pending uploads.

        Returns:
            tuple: A tuple containing (bytes_flushed, operations_processed)
        """
        # Call the parent implementation
        result = super().flush_wal()

        # Process pending uploads
        self._process_pending_uploads()

        return result

    def close(self):
        """
        Closes the storage, flushing any pending writes and releasing resources.

        This method overrides the base implementation to also process pending uploads
        to cloud storage before closing.
        """
        # First stop the background uploader thread to avoid potential deadlocks
        self.uploader_running = False

        # Wait for the uploader thread to finish
        if hasattr(self, 'uploader_thread') and self.uploader_thread.is_alive():
            self.uploader_thread.join(timeout=2.0)  # Wait up to 2 seconds for the thread to finish

        # Now process any remaining pending uploads
        try:
            self._process_pending_uploads()
        except Exception as e:
            _logger.error(f"Error processing pending uploads during close: {e}")
            # Continue with closing even if processing fails

        # Call the parent implementation
        super().close()

        _logger.info("Closed CloudFileStorage")


class GoogleCloudClient(CloudStorageClient):
    """
    Google Cloud Storage client implementation.

    This class implements the CloudStorageClient interface for Google Cloud Storage.
    """

    def __init__(self, 
                 bucket: str, 
                 prefix: str = "",
                 project_id: str = None,
                 credentials_path: str = None):
        """
        Initialize the Google Cloud Storage client.

        Args:
            bucket: The GCS bucket name
            prefix: The prefix for all objects in the bucket
            project_id: The Google Cloud project ID
            credentials_path: Path to the Google Cloud credentials file
        """
        self.project_id = project_id
        self.credentials_path = credentials_path

        # Call parent constructor
        super().__init__(bucket, prefix)

    def _init_client(self):
        """
        Initialize the Google Cloud Storage client.

        This method initializes the Google Cloud Storage client using the
        google-cloud-storage library if available, or falls back to a mock
        implementation for testing.
        """
        try:
            from google.cloud import storage
            from google.oauth2 import service_account

            if self.credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path
                )
                self.client = storage.Client(
                    project=self.project_id,
                    credentials=credentials
                )
            else:
                self.client = storage.Client(project=self.project_id)

            self.bucket_obj = self.client.bucket(self.bucket)
            _logger.info(f"Initialized Google Cloud Storage client for bucket '{self.bucket}'")
        except ImportError:
            _logger.warning("google-cloud-storage not installed, using mock implementation")
            self.client = None
            self.bucket_obj = None
            self.objects = {}

    def get_object(self, key: str) -> Tuple[bytes, Dict[str, Any]]:
        """
        Get an object from Google Cloud Storage.

        Args:
            key: The object key

        Returns:
            Tuple containing the object data and metadata
        """
        if self.client is None:
            # Mock implementation
            full_key = f"{self.prefix}/{key}" if self.prefix else key
            if full_key not in self.objects:
                raise CloudStorageError(message=f"Object '{full_key}' not found in mock GCS")

            obj = self.objects[full_key]
            metadata = {
                "ETag": obj["etag"],
                "ContentLength": len(obj["data"]),
                "LastModified": obj["last_modified"]
            }

            return obj["data"], metadata
        else:
            # Real implementation
            full_key = f"{self.prefix}/{key}" if self.prefix else key
            blob = self.bucket_obj.blob(full_key)

            try:
                data = blob.download_as_bytes()
                metadata = {
                    "ETag": blob.etag,
                    "ContentLength": blob.size,
                    "LastModified": blob.updated
                }
                return data, metadata
            except Exception as e:
                raise CloudStorageError(message=f"Failed to get object '{full_key}' from GCS: {e}")

    def put_object(self, key: str, data: bytes) -> Dict[str, Any]:
        """
        Put an object to Google Cloud Storage.

        Args:
            key: The object key
            data: The object data

        Returns:
            Object metadata
        """
        if self.client is None:
            # Mock implementation
            full_key = f"{self.prefix}/{key}" if self.prefix else key
            etag = hashlib.md5(data).hexdigest()
            last_modified = time.time()

            self.objects[full_key] = {
                "data": data,
                "etag": etag,
                "last_modified": last_modified
            }

            metadata = {
                "ETag": etag,
                "ContentLength": len(data),
                "LastModified": last_modified
            }

            _logger.debug(f"Put object '{full_key}' to mock GCS (size: {len(data)} bytes)")
            return metadata
        else:
            # Real implementation
            full_key = f"{self.prefix}/{key}" if self.prefix else key
            blob = self.bucket_obj.blob(full_key)

            try:
                blob.upload_from_string(data)
                metadata = {
                    "ETag": blob.etag,
                    "ContentLength": blob.size,
                    "LastModified": blob.updated
                }
                return metadata
            except Exception as e:
                raise CloudStorageError(message=f"Failed to put object '{full_key}' to GCS: {e}")

    def list_objects(self, prefix: str = None) -> List[Dict[str, Any]]:
        """
        List objects in Google Cloud Storage.

        Args:
            prefix: The prefix to filter objects

        Returns:
            List of object metadata
        """
        if self.client is None:
            # Mock implementation
            list_prefix = f"{self.prefix}/{prefix}" if self.prefix and prefix else (self.prefix or prefix or "")

            result = []
            for key, obj in self.objects.items():
                if not list_prefix or key.startswith(list_prefix):
                    result.append({
                        "Key": key,
                        "ETag": obj["etag"],
                        "Size": len(obj["data"]),
                        "LastModified": obj["last_modified"]
                    })

            return result
        else:
            # Real implementation
            list_prefix = f"{self.prefix}/{prefix}" if self.prefix and prefix else (self.prefix or prefix or "")
            blobs = self.bucket_obj.list_blobs(prefix=list_prefix)

            result = []
            for blob in blobs:
                result.append({
                    "Key": blob.name,
                    "ETag": blob.etag,
                    "Size": blob.size,
                    "LastModified": blob.updated
                })

            return result

    def delete_object(self, key: str) -> bool:
        """
        Delete an object from Google Cloud Storage.

        Args:
            key: The object key

        Returns:
            True if the object was deleted, False otherwise
        """
        if self.client is None:
            # Mock implementation
            full_key = f"{self.prefix}/{key}" if self.prefix else key
            if full_key in self.objects:
                del self.objects[full_key]
                _logger.debug(f"Deleted object '{full_key}' from mock GCS")
                return True
            return False
        else:
            # Real implementation
            full_key = f"{self.prefix}/{key}" if self.prefix else key
            blob = self.bucket_obj.blob(full_key)

            try:
                blob.delete()
                _logger.debug(f"Deleted object '{full_key}' from GCS")
                return True
            except Exception as e:
                _logger.warning(f"Failed to delete object '{full_key}' from GCS: {e}")
                return False


class MockGoogleCloudClient(MockCloudStorageClient):
    """
    Mock implementation of GoogleCloudClient for testing and development.

    This implementation inherits from MockCloudStorageClient and adds GCS-specific behavior.
    """

    def __init__(self, 
                 bucket: str, 
                 prefix: str = "",
                 project_id: str = None,
                 credentials_path: str = None):
        """
        Initialize the mock Google Cloud Storage client.

        Args:
            bucket: The GCS bucket name
            prefix: The prefix for all objects in the bucket
            project_id: The Google Cloud project ID
            credentials_path: Path to the Google Cloud credentials file
        """
        self.project_id = project_id
        self.credentials_path = credentials_path

        # Call parent constructor
        super().__init__(bucket, prefix)
