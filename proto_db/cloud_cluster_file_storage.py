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

from . import common
from .common import MB, GB, Future, BlockProvider, AtomPointer
from .exceptions import ProtoUnexpectedException, ProtoValidationException
from .fsm import FSM
from .standalone_file_storage import StandaloneFileStorage, WALState, WALWriteOperation
from .cluster_file_storage import ClusterFileStorage, ClusterNetworkManager
from .cloud_file_storage import CloudFileStorage, CloudBlockProvider, S3Client

_logger = logging.getLogger(__name__)

# Default cloud cluster storage settings
DEFAULT_S3_OBJECT_SIZE = 5 * MB  # Default size for S3 objects
DEFAULT_LOCAL_CACHE_SIZE = 500 * MB  # Default size for local cache
DEFAULT_CACHE_DIR = "cloud_cluster_cache"  # Default directory for local cache
DEFAULT_UPLOAD_INTERVAL_MS = 5000  # 5 seconds between S3 uploads
DEFAULT_CLEANUP_INTERVAL_MS = 60000  # 1 minute between cache cleanups


class CloudClusterStorageError(ProtoUnexpectedException):
    """
    Exception raised for cloud cluster storage specific errors.
    """
    pass


class CloudClusterFileStorage(CloudFileStorage):
    """
    An implementation of cloud cluster file storage with support for distributed operations and S3 storage.

    This class combines the functionality of CloudFileStorage and ClusterFileStorage to provide
    a storage solution that works in a multi-server cluster environment while using S3 as the
    final storage for data. It supports distributed operations like vote-based exclusive locking,
    root synchronization, and cached page retrieval between servers, as well as cloud storage
    features like background uploading to S3 and local caching.
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
        Constructor for the CloudClusterFileStorage class.

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
        # Initialize CloudFileStorage
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
            max_workers=max_workers,
            upload_interval_ms=upload_interval_ms
        )

        _logger.info(f"Initialized CloudClusterFileStorage for server {self.server_id}")

    def read_lock_current_root(self) -> AtomPointer:
        """
        Read and lock the current root object.

        This method acquires a distributed lock on the root object before reading it.
        It ensures that the root object is synchronized across all servers in the cluster
        and that the latest version is retrieved from S3 if necessary.

        Returns:
            AtomPointer: The current root object pointer
        """
        # Acquire distributed lock through voting
        success, votes = self.network_manager.request_vote()

        if not success:
            _logger.warning(f"Failed to acquire distributed lock for root update (received {votes} votes)")
            raise CloudClusterStorageError(message=f"Failed to acquire distributed lock for root update")

        # Process any pending uploads to ensure we have the latest data
        self._process_pending_uploads()

        # Read the root object
        with self.root_lock:
            return self.read_current_root()

    def set_current_root(self, root_pointer: AtomPointer):
        """
        Set the current root object.

        This method updates the root object, broadcasts the update to all servers,
        and ensures the update is persisted to S3.

        Args:
            root_pointer: The new root object pointer
        """
        # Update the root object locally
        self.block_provider.update_root_object(root_pointer)

        # Broadcast the update to all servers
        servers_updated = self.network_manager.broadcast_root_update(
            root_pointer.transaction_id, 
            root_pointer.offset
        )

        # Process pending uploads to ensure the update is persisted to S3
        self._process_pending_uploads()

        _logger.info(f"Updated root object, notified {servers_updated} servers, and persisted to S3")

    def get_reader(self, wal_id: uuid.UUID, position: int) -> io.BytesIO:
        """
        Get a reader for the specified WAL at the given position.

        This method first tries to get the data from the local cache, then from the
        local file system, then from other servers in the cluster, and finally from S3.

        Args:
            wal_id: WAL ID
            position: Position in the WAL

        Returns:
            io.BytesIO: A reader for the WAL data
        """
        # First check if the data is in the in-memory cache
        with self._lock:
            if (wal_id, position) in self.in_memory_segments:
                return io.BytesIO(self.in_memory_segments[(wal_id, position)])

        try:
            # Try to get the data from the local file system or S3 via CloudBlockProvider
            return self.block_provider.get_reader(wal_id, position)
        except Exception as e:
            _logger.debug(f"Failed to read from local file system or S3: {e}")

            # Try to get the data from other servers
            for retry in range(self.max_retries):
                data = self.network_manager.request_page(wal_id, position)
                if data:
                    return io.BytesIO(data)

                if retry < self.max_retries - 1:
                    time.sleep(self.retry_interval_ms / 1000)

            # If all retries fail, raise an exception
            raise CloudClusterStorageError(
                message=f"Failed to read WAL {wal_id} at position {position} from any server or S3"
            )

    def flush_wal(self) -> tuple[int, int]:
        """
        Public method to flush WAL buffer and process pending writes.

        This method ensures that WAL data is flushed to disk, processed for pending writes,
        and uploaded to S3. It also broadcasts the updates to other servers in the cluster.

        Returns:
            tuple: A tuple containing (bytes_flushed, operations_processed)
        """
        # Call the parent implementation (CloudFileStorage.flush_wal)
        result = super().flush_wal()

        # Ensure all servers in the cluster are notified of the latest WAL state
        if self.current_wal_id and self.current_wal_position > 0:
            self.network_manager.broadcast_root_update(
                self.current_wal_id,
                self.current_wal_position
            )

        return result

    def close(self):
        """
        Closes the storage, flushing any pending writes and releasing resources.

        This method ensures that all pending uploads are processed, the background uploader
        thread is stopped, and the network manager is stopped before closing the storage.
        """
        # Process pending uploads
        self._process_pending_uploads()

        # Stop the background uploader thread
        self.uploader_running = False

        # Wait for the uploader thread to finish
        if hasattr(self, 'uploader_thread') and self.uploader_thread.is_alive():
            self.uploader_thread.join(timeout=2.0)  # Wait up to 2 seconds for the thread to finish

        # Call the parent implementation (CloudFileStorage.close)
        super().close()

        _logger.info("Closed CloudClusterFileStorage")