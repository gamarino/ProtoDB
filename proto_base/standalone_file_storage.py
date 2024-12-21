import io
from os import write
from threading import Lock

from . import common
from . import file_common

from .common import ProtoUnexpectedException
from .common import Future, Atom, AtomPointer, RootObject, StorageWriteTransaction, StorageReadTransaction
import uuid

import logging
_logger = logging.getLogger(__name__)

KB = 1024
MB = KB * KB
GB = KB * MB
PB = KB * GB

BUFFER_SIZE = 1 * MB


class StandaloneFileTransaction(common.StorageReadTransaction):
    """

    """
    storage: common.SharedStorage

    def __init__(self, storage: common.SharedStorage):
        self.storage = storage

    def get_atom(self, atom: Atom) -> Future[Atom]:
        if atom.atom_pointer.transaction_id in self.storage.stored_transactions:
            transaction = self.storage.stored_transactions[atom.atom_pointer.transaction_id]
            if atom.atom_pointer.offset in transaction:
                return transaction[atom.atom_pointer.offset]

        raise common.ProtoCorruptionException(message=f'Atom {atom} does not exist')


class StandaloneFileWriteTransaction(common.StorageWriteTransaction, StandaloneFileTransaction):
    """

    """
    transaction_id: uuid.UUID

    def __init__(self, storage: common.SharedStorage):
        super().__init__()
        self.storage = storage
        while self.transaction_id and self.transaction_id in self.storage.stored_transactions:
            self.transaction_id = uuid.uuid4()
        self.atoms = {}
        self.state = 'open'

    def get_own_id(self) -> uuid.UUID:
        """
        Return a globaly unique identifier for the transaction
        :return:
        """
        return self.transaction_id

    def push_atom(self, atom: Atom) -> Future[int]:
        """

        :param atom:
        :return:
        """
        atom.atom_pointer.transaction_id = self.transaction_id
        offset = uuid.uuid4()
        atom.atom_pointer.offset = offset
        self.atoms[offset] = atom
        result = Future()
        result.set_result(offset)
        return result

    def commit(self):
        self.storage.stored_transactions[self.transaction_id] = self.atoms
        self.state = 'committed'


class WALState:
    """Encapsula el estado de WAL para manipularlo fácilmente."""

    def __init__(self, wal_buffer, wal_offset, wal_base):
        self.wal_buffer = wal_buffer[:]
        self.wal_offset = wal_offset
        self.wal_base = wal_base

class StandaloneFileStorage(common.SharedStorage):
    """

    """
    block_provider: file_common.BlockProvider
    _lock: Lock
    current_wal_base: int = 0
    current_wal_offset: int = 0
    current_wal_buffer: list[bytearray] = []

    current_root: RootObject
    class_register: dict[str, type[Atom]]

    def __init__(self, block_provider: file_common.BlockProvider):
        self.block_provider = block_provider
        self._lock = Lock()

    def _save_state(self) -> WALState:
        """Guarda el estado actual del WAL."""
        return WALState(self.current_wal_buffer, self.current_wal_offset, self.current_wal_base)

    def _restore_state(self, state: WALState):
        """Restaura el estado del WAL a partir de un WALState."""
        self.current_wal_buffer = state.wal_buffer
        self.current_wal_offset = state.wal_offset
        self.current_wal_base = state.wal_base

    def push_bytes(self, data: bytearray) -> int:
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

    def _push_bytes(self, data: bytearray) -> int:
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

        return base_offset

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

    def new_write_transaction(self) -> Future[StorageWriteTransaction]:
        """
        Inicia una nueva transacción de escritura
        :return: Objeto Future que contiene la transacción de escritura.
        """
        with self._lock:
            result = Future()
            result.set_result(StandaloneFileWriteTransaction(self))
            return result

    def new_read_transaction(self) -> Future[StorageReadTransaction]:
        """
        Inicia una nueva transacción de lectura
        :return: Objeto Future que contiene la transacción de lectura.
        """
        with self._lock:
            result = Future()
            result.set_result(StandaloneFileTransaction(self))
            return result

    def read_current_root(self) -> RootObject:
        """
        Read the current root object
        :return:
        """
        if self.current_root:
            return self.current_root

        raise common.ProtoValidationException(message='You are trying to read an empty DB!')

    def set_current_root(self, new_root: RootObject):
        """
        Set the current root object
        :return:
        """
        self.current_root = new_root

