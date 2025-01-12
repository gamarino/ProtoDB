from __future__ import annotations
import concurrent.futures
import io
import json
import os

from abc import ABC
from threading import Lock

from . import common
from .common import MB, GB
from .exceptions import ProtoUnexpectedException, ProtoValidationException
from .common import Future, BlockProvider, AtomPointer
import uuid
import logging
import struct


_logger = logging.getLogger(__name__)

# Default buffer and storage sizes
BUFFER_SIZE = 1 * MB
BLOB_MAX_SIZE = 2 * GB

# Executor threads for async operations
# Determines the number of worker threads for asynchronous execution
max_workers = (os.cpu_count() or 1) * 5
executor_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)


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
                 blob_max_size: int = BLOB_MAX_SIZE):
        """
        Constructor for the StandaloneFileStorage class.
        """
        self.block_provider = block_provider
        self.buffer_size = buffer_size
        self.blob_max_size = blob_max_size
        self._lock = Lock()
        self.state = 'Running'

        # WAL state management
        self.current_wal_id = None
        self.current_wal_base = 0
        self.current_wal_offset = 0
        self.current_wal_buffer = []
        self.pending_writes = []
        self.in_memory_segments = {}

        self._get_new_wal()

    def read_current_root(self) -> AtomPointer:
        """
        Read the current root object
        :return:
        """
        return self.block_provider.get_current_root_object()

    def read_lock_current_root(self) -> AtomPointer:
        """
        Read the current root object
        In this provider, there is no difference with
        a simple root reading
        :return:
        """
        return self.read_current_root()

    def set_current_root(self, root_pointer: AtomPointer):
        """
        Set the current root object
        :return:
        """
        self.block_provider.update_root_object(root_pointer)

    def unlock_current_root(self):
        """
        Abort root update process
        Nothing to do for this provider
        :return:
        """
        pass

    def _get_new_wal(self):
        """
        Initializes a new Write-Ahead Log (WAL).
        Allways new WALs start at buffer_size boundaries
        """
        self.current_wal_id, self.current_wal_offset = self.block_provider.get_new_wal()
        self.current_wal_offset = (self.current_wal_offset + 1) // self.buffer_size

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
        """
        if not self.current_wal_buffer:
            return  # Nothing to flush

        self.pending_writes.append(WALWriteOperation(
            transaction_id=self.current_wal_id,
            offset=self.current_wal_base,
            segments=self.current_wal_buffer
        ))

        # Reset WAL buffer
        written_size = sum(len(segment) for segment in self.current_wal_buffer)
        self.current_wal_base += written_size
        self.current_wal_offset = 0
        self.current_wal_buffer = []  # Clear the buffer

    def _flush_pending_writes(self):
        """
        Processes WALWriteOperation entries in the pending_writes list.
        Writes the data to the underlying BlockProvider.
        """
        while True:
            with self._lock:
                if self.pending_writes:
                    first_write = self.pending_writes.pop(0)
                    operations = [first_write]

                    while self.pending_writes and first_write.transaction_id == self.pending_writes[0].transaction_id:
                        operations.append(self.pending_writes.pop(0))
                else:
                    break

            if operations:
                # Try to group consecutive writes operations and perform all of them together
                with self.block_provider.write_streamer(operations[0].transaction_id) as stream:
                    current_offset = operations[0].offset
                    stream.seek(current_offset)
                    for operation in operations:
                        for i, segment in enumerate(operation.segments):
                            saved_offset = current_offset
                            stream.write(segment)
                            current_offset += len(segment)
                            with self._lock:
                                pointer = (operation.transaction_id, saved_offset)
                                if pointer in self.in_memory_segments:
                                    del self.in_memory_segments[pointer]

    def flush_wal(self):
        """
        Public method to flush WAL buffer and process pending writes.
        """
        current_state = self._save_state()
        try:
            with self._lock:
                self._flush_wal()
            self._flush_pending_writes()
        except Exception as e:
            _logger.exception("Unexpected error during WAL flushing", exc_info=e)
            self._restore_state(current_state)
            raise ProtoUnexpectedException(
                message="An exception occurred while flushing the WAL buffer.",
                exception_type=e.__class__.__name__
            )

    def close(self):
        with self._lock:
            self.state = 'Closed'

        self.flush_wal()
        self.block_provider.close()

    def push_bytes_to_wal(self, data: bytes) -> tuple[uuid.UUID, int]:
        """
        Adds data to the Write-Ahead Log (WAL).
        """
        if not isinstance(data, bytes):
            raise ProtoValidationException(message="Data must be a bytes.")
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
        Retrieves an Atom from the underlying storage asynchronously.
        """
        if not isinstance(pointer, AtomPointer):
            raise ProtoValidationException(message="Pointer must be an instance of AtomPointer.")

        def task_read_atom():
            with self._lock:
                if (pointer.transaction_id, pointer.offset) in self.in_memory_segments:
                    # It is already in memory
                    streamer = io.BytesIO(self.in_memory_segments[(pointer.transaction_id, pointer.offset)])
                else:
                    # It should be find in block provider
                    streamer = self.block_provider.get_reader(pointer.transaction_id, pointer.offset)

            # Read the atom from the storage getting just the needed amount of data. No extra reads
            with streamer as wal_stream:
                len_data = wal_stream.read(8)
                size = struct.unpack('Q', len_data)[0]

                data = wal_stream.read(size).decode('UTF-8')
                atom_data = json.loads(data)
                return atom_data

        return executor_pool.submit(task_read_atom)

    def push_atom(self, atom: dict) -> Future[AtomPointer]:
        """
        Serializes and pushes an Atom into the WAL asynchronously.
        """
        def task_push_atom():
            data = json.dumps(atom).encode('UTF-8')
            len_data = struct.pack('Q', len(data))

            transaction_id, offset = self.push_bytes_to_wal(len_data + data)
            return AtomPointer(transaction_id, offset)

        return executor_pool.submit(task_push_atom)

    def get_bytes(self, pointer: AtomPointer) -> Future[bytes]:
        """
        Retrieves an Atom from the underlying storage asynchronously.
        """
        if not isinstance(pointer, AtomPointer):
            raise ProtoValidationException(message="Pointer must be an instance of AtomPointer.")

        def task_read_bytes():
            with self._lock:
                if (pointer.transaction_id, pointer.offset) in self.in_memory_segments:
                    # It is already in memory
                    streamer = io.BytesIO(self.in_memory_segments[(pointer.transaction_id, pointer.offset)])
                else:
                    # It should be find in block provider
                    streamer = self.block_provider.get_reader(pointer.transaction_id, pointer.offset)

            # Read the atom from the storage getting just the needed amount of data. No extra reads
            with streamer as wal_stream:
                data = wal_stream.read(8)
                size = struct.unpack('Q', data)[0]

                data = wal_stream.read(size)

            return data

        return executor_pool.submit(task_read_bytes)

    def push_bytes(self, data: bytes) -> Future[tuple[uuid.UUID, int]]:
        """
        Serializes and pushes an Atom into the WAL asynchronously.
        """
        if not isinstance(data, bytes):
            raise ProtoValidationException(message="Invalid data to push. Only bytes!")

        if len(data) > self.blob_max_size:
            raise ProtoValidationException(
                message=f"Data exceeds maximum blob size ({len(data)} bytes). "
                        f"Only up to {self.blob_max_size} bytes are accepted!")

        def task_push_bytes():
            len_data = bytearray(struct.pack('Q', len(data)))

            transaction_id, offset = self.push_bytes_to_wal(bytearray(len_data + data))
            return transaction_id, offset

        return executor_pool.submit(task_push_bytes)
