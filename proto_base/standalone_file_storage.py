import concurrent.futures
import io
import json
import os
from threading import Lock

from . import common

from .common import ProtoUnexpectedException
from .common import Future, BlockProvider, Atom, AtomPointer, RootObject, atom_class_registry
import uuid

import logging
_logger = logging.getLogger(__name__)

KB = 1024
MB = KB * KB
GB = KB * MB
PB = KB * GB

BUFFER_SIZE = 1 * MB
BLOB_MAX_SIZE = 2 * GB


class WALState:
    """Encapsula el estado de WAL para manipularlo fácilmente."""

    def __init__(self, wal_buffer, wal_offset, wal_base):
        self.wal_buffer = wal_buffer[:]
        self.wal_offset = wal_offset
        self.wal_base = wal_base


def _get_valid_char_data(stream: io.FileIO) -> str:
    bytes = stream.read(1)
    byte = bytes[0]
    if byte >> 7 == 0:
        return bytes.decode('utf-8')
    elif byte >> 5 == 0b110:
        # It is a two byte char
        return (bytes + stream.read(1)).decode('utf-8')
    elif byte >> 4 == 0b1110:
        # It is a three byte char
        return (bytes + stream.read(2)).decode('utf-8')
    else:
        # It should be a four byte char
        return (bytes + stream.read(3)).decode('utf-8')


class StandaloneFileStorage(common.SharedStorage):
    """

    """
    block_provider: BlockProvider
    buffer_size: int
    blob_max_size: int

    _lock: Lock
    current_wal_id: uuid.UUID
    current_wal_base: int = 0
    current_wal_offset: int = 0
    current_wal_buffer: list[bytearray] = []

    executor_pool: concurrent.futures.ThreadPoolExecutor

    def __init__(self,
                 block_provider: BlockProvider = None,
                 buffer_size: int = BUFFER_SIZE,
                 blob_max_size: int = BLOB_MAX_SIZE,
                 max_workers: int = 0):
        self.block_provider = block_provider
        self._lock = Lock()
        self._get_new_wal()

        if not max_workers:
            max_workers = (os.cpu_count() or 1) * 5
        self.executor_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    def _get_new_wal(self):
        self.current_wal_id, self.current_wal_offset = self.block_provider.get_new_wal()

    def _save_state(self) -> WALState:
        """Guarda el estado actual del WAL."""
        return WALState(self.current_wal_buffer, self.current_wal_offset, self.current_wal_base)

    def _restore_state(self, state: WALState):
        """Restaura el estado del WAL a partir de un WALState."""
        self.current_wal_buffer = state.wal_buffer
        self.current_wal_offset = state.wal_offset
        self.current_wal_base = state.wal_base

    def push_bytes(self, data: bytearray) -> tuple(uuid.UUID, int):
        """
        Añade datos al Write-Ahead Log (WAL).

        :param data: Datos como un objeto `bytearray`
        :return: Offset inicial donde se guardaron los datos en el WAL
        :raises: ProtoUnexpectedException en caso de errores inesperados
        """
        if not isinstance(data, bytearray):
            raise ValueError("El parámetro `data` debe ser de tipo bytearray.")
        if len(data) == 0:
            raise ValueError("El parámetro `data` no puede estar vacío.")
        if len(data) > self.blob_max_size:
            raise ValueError(f"El parámetro `data` no puede ser mas grande que {self.blob_max_size} bytes!")

        current_state = self._save_state()

        try:
            with self._lock:
                return self._push_bytes(data)
        except Exception as e:
            _logger.exception(e)

            self._restore_state(current_state)

            raise ProtoUnexpectedException(
                message=f'Pushing to WAL, unexpected exception',
                exception_type=e.__class__.__name__
            )

    def _push_bytes(self, data: bytearray) -> tuple(uuid.UUID, int):
        if len(data) > self.blob_max_size - self.current_wal_offset:
            self._flush_wal()
            self._get_new_wal()

        # Here we are sure that data will fit in the current WAL

        base_uuid = self.current_wal_id
        base_offset = self.current_wal_base + self.current_wal_offset

        written_bytes = 0
        while written_bytes < len(data):
            remaining_space = BUFFER_SIZE - self.current_wal_offset

            if len(data) - written_bytes > remaining_space:
                fragment = data[written_bytes: written_bytes + remaining_space]
                self.current_wal_buffer.append(fragment)
                self.current_wal_offset += remaining_space
                written_bytes += remaining_space
                self._flush_wal()
            else:
                fragment = data[written_bytes:]
                self.current_wal_buffer.append(fragment)
                self.current_wal_offset += len(fragment)
                written_bytes += len(fragment)

        return base_uuid, base_offset

    def flush_wal(self):
        """
        Fuerza el vaciado del Write-Ahead Log (WAL) hacia el proveedor de bloques.

        Este método escribe todos los datos pendientes en el buffer al proveedor de bloques
        y limpia el estado interno del WAL para permitir nuevas operaciones.

        :raises ProtoUnexpectedException: Si ocurre un error inesperado al escribir en el proveedor.
        """
        current_state = self._save_state()

        try:
            with self._lock:
                self._flush_wal()
        except Exception as e:
            _logger.exception(e)

            self._restore_state(current_state)

            raise ProtoUnexpectedException(message=f'Flushing WAL, unexpected exception',
                                           exception_type=e.__class__.__name__)

    def _flush_wal(self):
        if not self.current_wal_buffer:
            return

        segments_to_write = []
        written_base = self.current_wal_base

        # Bloquear solo la parte crítica
        with self._lock:
            segments_to_write = self.current_wal_buffer[:]
            self.current_wal_base += sum(len(segment) for segment in self.current_wal_buffer)
            self.current_wal_offset = 0
            self.current_wal_buffer.clear()

        # No bloquea la escritura en disco
        with self.block_provider as write_streamer:
            write_streamer.seek(written_base)
            for segment in segments_to_write:
                write_streamer.write(segment)

    def read_current_root(self) -> RootObject:
        """
        Read the current root object
        :return:
        """
        return self.block_provider.read_current_root()

    def set_current_root(self, new_root: RootObject):
        """
        Set the current root object
        :return:
        """
        self.block_provider.update_root_object(new_root)

    def _push_atom_worker(self, raw_data: bytearray) -> AtomPointer:
        """

        :param atom:
        :return:
        """
        wal_id, wal_offset = self.push_bytes(raw_data)
        return AtomPointer(wal_id, wal_offset)

    def push_atom(self, atom: Atom) -> Future[AtomPointer]:
        """

        :param atom:
        :return:
        """

        data = io.BytesIO()
        storage_structure = {'AtomClass': atom.__name__}
        for attribute, value in atom.__dict__.items():
            if not attribute.startswith('_'):
                if isinstance(value, Atom):
                    # it is reference to other atom
                    storage_structure[attribute] = value.atom_pointer
                else:
                    storage_structure[attribute] = value

        storage_as_str = json.dumps(storage_structure, ensure_ascii=False)
        data.write(storage_as_str.encode('utf-8'))

        return self.executor_pool.submit(self._push_atom_worker, bytearray(data.getvalue()))

    def _get_atom_worker(self, atom_pointer: AtomPointer) -> Atom:
        """
        Perform the posibily slow read operation
        :param atom_pointer:
        :return:
        """
        data_stream = self.block_provider.get_reader(atom_pointer.transaction_id, atom_pointer.offset)

        chars = []
        escape_next = False
        inside_string = False
        brace_level = 0
        bracket_level = 0

        while True:
            char = _get_valid_char_data(data_stream)
            chars.append(char)
            if escape_next:
                escape_next = False
                continue

            if char == '"':
                inside_string = not inside_string
                continue

            if inside_string:
                continue

            if char == '{':
                brace_level += 1
            elif char == '[':
                bracket_level += 1
            elif char in ('}', ']'):
                if char == '}':
                    brace_level -= 1
                elif char == ']':
                    bracket_level -= 1

                if brace_level == 0 and bracket_level == 0:
                    try:
                        json_data = json.loads(''.join(chars))
                        class_to_create = atom_class_registry[json_data['AtomClass']]
                        return class_to_create(**json_data)
                    except Exception as e:
                        _logger.exception(e)
                        raise ProtoUnexpectedException(message='Unexpected exception')

    def get_atom(self, atom_pointer: AtomPointer) -> Future[Atom]:
        """
        Read an atom from underlining storage
        :param atom_pointer: Pointer to storage
        :return: the materialized atom
        """
        return self.executor_pool.submit(self._get_atom_worker, atom_pointer)
