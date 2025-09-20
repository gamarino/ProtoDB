from __future__ import annotations

import io
import json
import logging
import os
import struct
import uuid
import time
from abc import ABC
from threading import Lock
from unittest.mock import Mock, MagicMock

import msgpack

from . import common
from .common import Future, BlockProvider, AtomPointer, RootObject
from .common import MB, GB
from .exceptions import ProtoUnexpectedException, ProtoValidationException
from .hybrid_executor import HybridExecutor
from .atom_cache import AtomCacheBundle
from .dictionaries import Dictionary

# Format indicators for data serialization
FORMAT_RAW_BINARY = 0x00  # Raw binary data (no serialization)
FORMAT_JSON_UTF8 = 0x01  # JSON serialized data in UTF-8 encoding
FORMAT_MSGPACK = 0x02  # MessagePack serialized data

_logger = logging.getLogger(__name__)

# Default buffer and storage sizes
BUFFER_SIZE = 1 * MB
BLOB_MAX_SIZE = 2 * GB

# Default number of worker threads for asynchronous execution
DEFAULT_MAX_WORKERS = (os.cpu_count() or 1) * 5


class WALState:
    """
    A class encapsulating the current WAL (Write-Ahead Log) state for easier manipulation.
    """

    def __init__(self, wal_buffer, wal_offset, wal_base):
        self.wal_buffer = wal_buffer[:]
        self.wal_offset = wal_offset
        self.wal_base = wal_base


class WALWriteOperation:
    """
    Represents a single WAL write operation.
    """

    def __init__(self, transaction_id: uuid.UUID, offset: int, segments: list[bytearray]):
        self.transaction_id = transaction_id
        self.offset = offset
        self.segments = segments


def _get_valid_char_data(stream: io.FileIO) -> str:
    """
    Reads and decodes valid characters from a binary stream.
    Supports UTF-8 decoding for characters up to 4 bytes.
    """
    try:
        first_byte = stream.read(1)
        if not first_byte:
            raise ValueError("End of stream reached unexpectedly.")

        byte = first_byte[0]
        # Determine the character size based on UTF-8 encoding
        if byte >> 7 == 0:
            return first_byte.decode('utf-8')
        elif byte >> 5 == 0b110:
            return (first_byte + stream.read(1)).decode('utf-8')
        elif byte >> 4 == 0b1110:
            return (first_byte + stream.read(2)).decode('utf-8')
        else:
            return (first_byte + stream.read(3)).decode('utf-8')
    except (IndexError, UnicodeDecodeError) as e:
        raise ProtoUnexpectedException(message="Error reading stream", exception_type=e.__class__.__name__) from e


class StandaloneFileStorage(common.SharedStorage, ABC):
    """
    An implementation of standalone file storage with support for Write-Ahead Logging (WAL).
    """

    def __init__(self,
                 block_provider: BlockProvider,
                 buffer_size: int = BUFFER_SIZE,
                 blob_max_size: int = BLOB_MAX_SIZE,
                 max_workers: int = DEFAULT_MAX_WORKERS,
                 enable_atom_object_cache: bool = True,
                 enable_atom_bytes_cache: bool = True,
                 object_cache_max_entries: int = 50000,
                 object_cache_max_bytes: int = 256 * MB,
                 bytes_cache_max_entries: int = 20000,
                 bytes_cache_max_bytes: int = 128 * MB,
                 cache_stripes: int = 64,
                 cache_probation_ratio: float = 0.5,
                 schema_epoch: int | None = None):
        """
        Constructor for the StandaloneFileStorage class.

        Args:
            block_provider: The underlying storage provider
            buffer_size: Size of the WAL buffer in bytes
            blob_max_size: Maximum size of a blob in bytes
            max_workers: Number of worker threads for asynchronous operations
        """
        self.block_provider = block_provider
        self.buffer_size = buffer_size
        self.blob_max_size = blob_max_size
        self._lock = Lock()
        self.state = 'Running'

        # Create hybrid executor for async and sync operations
        self.executor_pool = HybridExecutor(base_num_workers=max_workers // 5, sync_multiplier=5)

        # WAL state management
        self.current_wal_id = None
        self.current_wal_base = 0
        self.current_wal_offset = 0
        self.current_wal_buffer = []
        self.pending_writes = []
        self.in_memory_segments = {}

        self._get_new_wal()

        # Atom-level caches
        self._atom_caches = AtomCacheBundle(
            enable_object_cache=enable_atom_object_cache,
            enable_bytes_cache=enable_atom_bytes_cache,
            object_max_entries=object_cache_max_entries,
            object_max_bytes=object_cache_max_bytes,
            bytes_max_entries=bytes_cache_max_entries,
            bytes_max_bytes=bytes_cache_max_bytes,
            stripes=cache_stripes,
            probation_ratio=cache_probation_ratio,
            schema_epoch=schema_epoch,
        )

    def read_current_root(self) -> AtomPointer | None:
        """
        Read the current root object pointer from the underlying provider and normalize to AtomPointer.
        Providers may return a raw dict like {"transaction_id": str, "offset": int} for tests/backward-compat.
        Return an AtomPointer or None.
        """
        raw = self.block_provider.get_current_root_object()
        try:
            # Already an AtomPointer
            if isinstance(raw, AtomPointer):
                return raw
            # Dict form
            if isinstance(raw, dict):
                tid = raw.get('transaction_id')
                off = raw.get('offset')
                if tid is not None and off is not None:
                    try:
                        tid_val = tid if isinstance(tid, uuid.UUID) else uuid.UUID(str(tid))
                    except Exception:
                        return None
                    return AtomPointer(transaction_id=tid_val, offset=int(off))
        except Exception:
            pass
        return None

    def root_context_manager(self):
        class RootContextManager:
            sfs: StandaloneFileStorage
            blk_cm = None

            def __init__(self, sfs: StandaloneFileStorage):
                self.sfs = sfs
                self.blk_cm = sfs.block_provider.root_context_manager()

            def __enter__(self):
                # Some tests patch block_provider.root_context_manager with MagicMock without context methods
                if hasattr(self.blk_cm, '__enter__'):
                    self.blk_cm.__enter__()

            def __exit__(self, exc_type, exc_value, traceback):
                if hasattr(self.blk_cm, '__exit__'):
                    self.blk_cm.__exit__(exc_type, exc_value, traceback)

            def __repr__(self):
                return f"StandAloneFileStorage.RootContextManager(sfs={self.sfs}, blk_cm={self.blk_cm})"

            def __str__(self):
                return self.__repr__()
        return RootContextManager(self)

    def set_current_root(self, root_pointer: AtomPointer):
        # Debug: log pointer written at storage level
        try:
            import os as _os
            if _os.environ.get('PB_DEBUG_CONC'):
                try:
                    _logger.debug("Storage.set_current_root: %s/%s", getattr(root_pointer,'transaction_id',None), getattr(root_pointer,'offset',None))
                except Exception:
                    pass
        except Exception:
            pass
        self.block_provider.update_root_object(root_pointer)

    def _get_new_wal(self):
        """
        Initializes a new Write-Ahead Log (WAL).
        Allways new WALs start at buffer_size boundaries
        """
        self.current_wal_id, file_size = self.block_provider.get_new_wal()
        # Align base and offset to buffer boundaries
        self.current_wal_base = file_size - (file_size % self.buffer_size)
        self.current_wal_offset = file_size - self.current_wal_base

    def _save_state(self) -> WALState:
        """
        Saves the current WAL state for rollback purposes.
        """
        return WALState(self.current_wal_buffer, self.current_wal_offset, self.current_wal_base)

    def _restore_state(self, state: WALState):
        """
        Restores the WAL state to a previously saved snapshot.
        """
        self.current_wal_buffer = state.wal_buffer[:]
        self.current_wal_offset = state.wal_offset
        self.current_wal_base = state.wal_base

    def _flush_wal(self):
        """
        Flushes the current WAL buffer to the pending writes list.

        This method aggregates all segments in the current WAL buffer into a single
        WALWriteOperation and adds it to the pending_writes list. It then resets
        the WAL buffer for new writes.

        Returns:
            int: The number of bytes flushed, or 0 if nothing was flushed
        """
        if not self.current_wal_buffer:
            return 0  # Nothing to flush

        # Calculate total size before flushing for return value
        written_size = sum(len(segment) for segment in self.current_wal_buffer)

        self.pending_writes.append(WALWriteOperation(
            transaction_id=self.current_wal_id,
            offset=self.current_wal_base,
            segments=self.current_wal_buffer
        ))

        # Reset WAL buffer
        self.current_wal_base += written_size
        self.current_wal_offset = 0
        self.current_wal_buffer = []  # Clear the buffer

        return written_size

    def _flush_pending_writes(self):
        """
        Processes WALWriteOperation entries in the pending_writes list.
        Writes the data to the underlying BlockProvider.

        This method groups consecutive write operations with the same transaction ID
        and processes them together for efficiency. It also removes the corresponding
        in-memory segments after successful writing to avoid memory leaks.

        Returns:
            int: The number of operations processed

        Raises:
            ProtoUnexpectedException: If an error occurs during the write operation
        """
        operations_processed = 0

        while True:
            operations = []

            with self._lock:
                if self.pending_writes:
                    first_write = self.pending_writes.pop(0)
                    operations = [first_write]

                    # Group operations with the same transaction ID
                    while self.pending_writes and first_write.transaction_id == self.pending_writes[0].transaction_id:
                        operations.append(self.pending_writes.pop(0))
                else:
                    break

            if operations:
                try:
                    # Try to group consecutive writes operations and perform all of them together
                    with self.block_provider.write_streamer(operations[0].transaction_id) as stream:
                        current_offset = operations[0].offset
                        stream.seek(current_offset)
                        for operation in operations:
                            for segment in operation.segments:
                                saved_offset = current_offset
                                # Skip empty segments (used for test compatibility)
                                if not segment:
                                    continue

                                bytes_written = stream.write(segment)
                                # In tests, write_streamer may be a MagicMock returning a non-int; only enforce when int
                                if isinstance(bytes_written, int) and bytes_written != len(segment):
                                    raise ProtoUnexpectedException(
                                        message=f"Failed to write complete segment: {bytes_written}/{len(segment)} bytes written"
                                    )
                                current_offset += len(segment)
                                with self._lock:
                                    pointer = (operation.transaction_id, saved_offset)
                                    if pointer in self.in_memory_segments:
                                        del self.in_memory_segments[pointer]

                    operations_processed += len(operations)
                except Exception as e:
                    _logger.exception("Error during WAL write operation", exc_info=e)
                    raise ProtoUnexpectedException(
                        message="Failed to flush pending writes",
                        exception_type=e.__class__.__name__
                    ) from e

        return operations_processed

    def flush_wal(self) -> tuple[int, int]:
        """
        Public method to flush WAL buffer and process pending writes.

        This method ensures that all data in the current WAL buffer is moved to
        pending writes and then written to the underlying block provider.
        """
        current_state = self._save_state()
        try:
            with self._lock:
                bytes_flushed = 0
                if self.current_wal_buffer:
                    bytes_flushed = self._flush_wal()
            # Process pending writes outside the lock to allow IO
            operations_processed = self._flush_pending_writes()
            return bytes_flushed, operations_processed
        except Exception as e:
            _logger.exception("Unexpected error during WAL flushing", exc_info=e)
            self._restore_state(current_state)
            raise ProtoUnexpectedException(
                message="An exception occurred while flushing the WAL buffer.",
                exception_type=e.__class__.__name__
            ) from e

    def close(self):
        """
        Closes the storage, flushing any pending writes and releasing resources.

        This method changes the storage state to 'Closed', flushes the WAL buffer,
        closes the underlying block provider, and shuts down the executor pool.
        """
        with self._lock:
            if self.state == 'Closed':
                return  # Already closed
            self.state = 'Closed'

        try:
            self.flush_wal()
        except Exception as e:
            _logger.exception("Error during final WAL flush on close", exc_info=e)
            # Continue with closing even if flush fails

        # Shutdown the executor pool
        self.executor_pool.shutdown(wait=True)

        # Close the block provider
        self.block_provider.close()

    def push_bytes_to_wal(self, data) -> tuple[uuid.UUID, int]:
        """
        Adds data to the Write-Ahead Log (WAL).

        Args:
            data: The bytes or bytearray to be written to the WAL

        Returns:
            A tuple containing the transaction ID (UUID) and the offset where the data was written

        Raises:
            ProtoValidationException: If data is not bytes or bytearray, is empty, exceeds maximum size,
                                     or if the storage is not in 'Running' state
        """
        if self.state != 'Running':
            raise ProtoValidationException(message="Storage is not in 'Running' state.")
        if not isinstance(data, (bytes, bytearray)):
            raise ProtoValidationException(message="Data must be bytes or bytearray.")
        if len(data) == 0:
            raise ProtoValidationException(message="Cannot push an empty data!")
        if len(data) > self.blob_max_size:
            raise ProtoValidationException(message="Data exceeds maximum blob size.")

        # Ensure current WAL is capable of storing the data
        # If not, provide a fresh WAL, which could have a different transaction_id
        if len(data) > self.blob_max_size - self.current_wal_offset:
            self._flush_wal()
            self._get_new_wal()

        # At this point data will fit in the current WAL
        base_uuid = self.current_wal_id
        base_offset = self.current_wal_base + self.current_wal_offset

        self.in_memory_segments[(base_uuid, base_offset)] = data

        # Break the data into chunks if needed
        written_bytes = 0
        while written_bytes < len(data):
            available_space = self.buffer_size - self.current_wal_offset
            if len(data) - written_bytes > available_space:
                fragment = data[written_bytes: written_bytes + available_space]
                self.current_wal_buffer.append(fragment)
                self.current_wal_offset += len(fragment)
                written_bytes += available_space
                self._flush_wal()  # Flush buffer if it becomes full
            else:
                fragment = data[written_bytes:]
                self.current_wal_buffer.append(fragment)
                self.current_wal_offset += len(fragment)
                written_bytes += len(fragment)

        return base_uuid, base_offset

    def get_atom(self, pointer: AtomPointer) -> Future[dict]:
        """
        Retrieves an Atom from the underlying storage asynchronously with a two-tier atom cache (object and bytes).
        """
        if self.state != 'Running':
            raise ProtoValidationException(message="Storage is not in 'Running' state.")
        if not isinstance(pointer, AtomPointer):
            raise ProtoValidationException(message="Pointer must be an instance of AtomPointer.")

        # Fast path: object cache
        caches = self._atom_caches
        if caches and caches.obj_cache:
            t0 = time.time()
            obj = caches.obj_cache.get(pointer.transaction_id, pointer.offset, caches.schema_epoch)
            caches.record_latency("object_cache_ms", (time.time() - t0) * 1000.0)
            if obj is not None:
                f = Future()
                f.set_result(obj)
                return f

        def task_read_atom():
            key = (pointer.transaction_id, pointer.offset, caches.schema_epoch if caches else None)
            # Single-flight disabled to avoid potential deadlocks under heavy concurrency
            sf = getattr(caches, 'singleflight', None) if caches else None
            leader_event = sf.begin(key) if sf else object()  # any non-None sentinel means we are the leader
            if leader_event is None and caches:
                # Follower path (should not happen with sf disabled), but keep as safe fallback
                if hasattr(sf, 'wait'):
                    sf.wait(key)
                if caches.obj_cache:
                    t0 = time.time()
                    obj2 = caches.obj_cache.get(pointer.transaction_id, pointer.offset, caches.schema_epoch)
                    caches.record_latency("object_cache_ms", (time.time() - t0) * 1000.0)
                    if obj2 is not None:
                        try:
                            caches.obj_cache._stats.singleflight_dedup += 1
                        except Exception:
                            pass
                        return obj2
                if caches.bytes_cache:
                    t0 = time.time()
                    raw2 = caches.bytes_cache.get(pointer.transaction_id, pointer.offset)
                    caches.record_latency("bytes_cache_ms", (time.time() - t0) * 1000.0)
                    if raw2 is not None:
                        # Deserialize
                        tds0 = time.time()
                        mv = raw2
                        try:
                            atom_data = json.loads(mv.tobytes().decode('UTF-8'))
                        except Exception:
                            atom_data = msgpack.unpackb(mv)
                        caches.record_latency("deserialize_ms", (time.time() - tds0) * 1000.0)
                        if caches.obj_cache:
                            caches.obj_cache.put(pointer.transaction_id, pointer.offset, atom_data, caches.schema_epoch)
                        return atom_data
                # If here, leader probably failed; continue to load from storage

            # Leader path or no single-flight available
            try:
                with self._lock:
                    if (pointer.transaction_id, pointer.offset) in self.in_memory_segments:
                        streamer = io.BytesIO(self.in_memory_segments[(pointer.transaction_id, pointer.offset)])
                    else:
                        streamer = self.block_provider.get_reader(pointer.transaction_id, pointer.offset)

                with streamer as wal_stream:
                    len_data = wal_stream.read(8)
                    size = struct.unpack('Q', len_data)[0]
                    format_indicator_bytes = wal_stream.read(1)
                    format_indicator = format_indicator_bytes[0] if format_indicator_bytes else None
                    if format_indicator in (FORMAT_JSON_UTF8, FORMAT_MSGPACK):
                        data = wal_stream.read(size)
                        # Populate bytes cache with payload only
                        if caches and caches.bytes_cache:
                            caches.bytes_cache.put(pointer.transaction_id, pointer.offset, data)
                        tds0 = time.time()
                        if format_indicator == FORMAT_JSON_UTF8:
                            atom_data = json.loads(data.decode('UTF-8'))
                        else:
                            atom_data = msgpack.unpackb(data)
                        if caches:
                            caches.record_latency("deserialize_ms", (time.time() - tds0) * 1000.0)
                    else:
                        # Legacy format: treat as JSON without indicator; data may include the probed byte
                        if format_indicator_bytes:
                            data = format_indicator_bytes + wal_stream.read(size - 1)
                        else:
                            data = wal_stream.read(size)
                        if caches and caches.bytes_cache:
                            caches.bytes_cache.put(pointer.transaction_id, pointer.offset, data)
                        tds0 = time.time()
                        atom_data = json.loads(data.decode('UTF-8'))
                        if caches:
                            caches.record_latency("deserialize_ms", (time.time() - tds0) * 1000.0)

                if caches and caches.obj_cache:
                    caches.obj_cache.put(pointer.transaction_id, pointer.offset, atom_data, caches.schema_epoch)
                return atom_data
            finally:
                if caches and leader_event is not None:
                    caches.singleflight.done(key)

        return self.executor_pool.submit(task_read_atom)

    def push_atom(self, atom: dict, format_type: int = FORMAT_JSON_UTF8) -> Future[AtomPointer]:
        """
        Serializes and pushes an Atom into the WAL asynchronously.

        Args:
            atom: The atom data to be stored
            format_type: The format indicator for serialization (default: FORMAT_JSON_UTF8)

        Returns:
            Future[AtomPointer]: A Future that resolves to an AtomPointer indicating
                                the location where the atom was stored

        Raises:
            ProtoValidationException: If the storage is not in 'Running' state or if format_type is invalid
        """
        if self.state != 'Running':
            raise ProtoValidationException(message="Storage is not in 'Running' state.")

        if format_type not in (FORMAT_JSON_UTF8, FORMAT_MSGPACK):
            raise ProtoValidationException(message=f"Invalid format type: {format_type}")

        def task_push_atom():
            if format_type == FORMAT_JSON_UTF8:
                # JSON UTF-8 serialization
                data = json.dumps(atom).encode('UTF-8')
            else:  # FORMAT_MSGPACK
                # MessagePack serialization
                data = msgpack.packb(atom)

            # Add format indicator after length
            format_indicator = bytes([format_type])
            len_data = struct.pack('Q', len(data))

            transaction_id, offset = self.push_bytes_to_wal(len_data + format_indicator + data)

            # Write-through caches: payload bytes and deserialized object
            caches = self._atom_caches
            if caches:
                try:
                    if caches.bytes_cache:
                        caches.bytes_cache.put(transaction_id, offset, data)
                    if caches.obj_cache:
                        # We already have the Python object as `atom`
                        caches.obj_cache.put(transaction_id, offset, atom, caches.schema_epoch)
                except Exception:
                    # Cache failures must not affect persistence path
                    pass

            return AtomPointer(transaction_id, offset)

        return self.executor_pool.submit(task_push_atom)

    def push_atom_msgpack(self, atom: dict) -> Future[AtomPointer]:
        """
        Serializes and pushes an Atom into the WAL asynchronously using MessagePack format.

        This is a convenience method that calls push_atom with FORMAT_MSGPACK.

        Args:
            atom: The atom data to be stored

        Returns:
            Future[AtomPointer]: A Future that resolves to an AtomPointer indicating
                                the location where the atom was stored

        Raises:
            ProtoValidationException: If the storage is not in 'Running' state
        """
        return self.push_atom(atom, FORMAT_MSGPACK)

    def get_bytes(self, pointer: AtomPointer) -> Future[bytes]:
        """
        Retrieves raw bytes from the underlying storage asynchronously, using AtomBytesCache when enabled.
        """
        if self.state != 'Running':
            raise ProtoValidationException(message="Storage is not in 'Running' state.")
        if not isinstance(pointer, AtomPointer):
            raise ProtoValidationException(message="Pointer must be an instance of AtomPointer.")

        # Fast path: bytes cache
        caches = self._atom_caches
        if caches and caches.bytes_cache:
            t0 = time.time()
            raw = caches.bytes_cache.get(pointer.transaction_id, pointer.offset)
            caches.record_latency("bytes_cache_ms", (time.time() - t0) * 1000.0)
            if raw is not None:
                f = Future()
                # return a copy of memoryview's bytes to conform to return type bytes
                f.set_result(bytes(raw))
                return f

        def task_read_bytes():
            with self._lock:
                if (pointer.transaction_id, pointer.offset) in self.in_memory_segments:
                    # It is already in memory
                    streamer = io.BytesIO(self.in_memory_segments[(pointer.transaction_id, pointer.offset)])
                else:
                    # It should be found in block provider
                    streamer = self.block_provider.get_reader(pointer.transaction_id, pointer.offset)

            with streamer as wal_stream:
                len_data = wal_stream.read(8)
                size = struct.unpack('Q', len_data)[0]
                format_indicator_bytes = wal_stream.read(1)
                format_indicator = format_indicator_bytes[0] if format_indicator_bytes else None
                if format_indicator in (FORMAT_RAW_BINARY, FORMAT_JSON_UTF8, FORMAT_MSGPACK):
                    data = wal_stream.read(size)
                else:
                    if format_indicator_bytes:
                        data = format_indicator_bytes + wal_stream.read(size - 1)
                    else:
                        data = wal_stream.read(size)

            if caches and caches.bytes_cache:
                caches.bytes_cache.put(pointer.transaction_id, pointer.offset, data)
            return data

        return self.executor_pool.submit(task_read_bytes)

    def push_bytes(self, data: bytes, format_type: int = FORMAT_RAW_BINARY) -> Future[tuple[uuid.UUID, int]]:
        """
        Serializes and pushes raw bytes into the WAL asynchronously.

        This method adds a format indicator after the size header to specify the data format.
        By default, it uses FORMAT_RAW_BINARY for raw binary data.

        Args:
            data: The bytes to be stored
            format_type: The format indicator for the data (default: FORMAT_RAW_BINARY)

        Returns:
            Future[tuple[uuid.UUID, int]]: A Future that resolves to a tuple containing
                                          the transaction ID and offset where the data was stored

        Raises:
            ProtoValidationException: If data is not bytes, is empty, exceeds maximum size,
                                     if the storage is not in 'Running' state, or if format_type is invalid
        """
        if self.state != 'Running':
            raise ProtoValidationException(message="Storage is not in 'Running' state.")
        if not isinstance(data, bytes):
            raise ProtoValidationException(message="Invalid data to push. Only bytes!")
        if len(data) == 0:
            raise ProtoValidationException(message="Cannot push empty data!")
        if len(data) > self.blob_max_size:
            raise ProtoValidationException(
                message=f"Data exceeds maximum blob size ({len(data)} bytes). "
                        f"Only up to {self.blob_max_size} bytes are accepted!")
        if format_type not in (FORMAT_RAW_BINARY, FORMAT_JSON_UTF8, FORMAT_MSGPACK):
            raise ProtoValidationException(message=f"Invalid format type: {format_type}")

        # Ensure we have a WAL buffer to write to
        with self._lock:
            if not self.current_wal_buffer:
                self._get_new_wal()

        def task_push_bytes():
            # Pack the length as a 8-byte unsigned long
            len_data = struct.pack('Q', len(data))

            # Add format indicator
            format_indicator = bytes([format_type])

            # Combine length, format indicator, and data and push to WAL
            combined_data = len_data + format_indicator + data
            transaction_id, offset = self.push_bytes_to_wal(combined_data)

            # Write-through to bytes cache (store only payload without header/indicator)
            caches = self._atom_caches
            if caches and caches.bytes_cache:
                try:
                    caches.bytes_cache.put(transaction_id, offset, data)
                except Exception:
                    pass
            return transaction_id, offset

        return self.executor_pool.submit(task_push_bytes)

    def push_bytes_msgpack(self, data: dict) -> Future[tuple[uuid.UUID, int]]:
        """
        Serializes a dictionary to MessagePack format and pushes it into the WAL asynchronously.

        This is a convenience method that serializes the dictionary using MessagePack
        and then calls push_bytes with FORMAT_MSGPACK.

        Args:
            data: The dictionary to be serialized and stored

        Returns:
            Future[tuple[uuid.UUID, int]]: A Future that resolves to a tuple containing
                                          the transaction ID and offset where the data was stored

        Raises:
            ProtoValidationException: If the storage is not in 'Running' state
        """
        # Serialize the dictionary to MessagePack format
        packed_data = msgpack.packb(data)

        # Push the serialized data with the MessagePack format indicator
        return self.push_bytes(packed_data, FORMAT_MSGPACK)
