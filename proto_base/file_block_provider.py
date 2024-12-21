from . import common
from . import file_common

from .common import Future, Atom, AtomPointer, StorageWriteTransaction, StorageReadTransaction
import uuid
import io

class FileBlockProvider(file_common.BlockProvider):

    base_directory: str
    wal: io.FileIO

    def __init__(self, base_directory: str):
        self.base_directory = base_directory

        self.wal = self._select_writer_wal()


    def _select_writer_wal(self) -> io.FileIO:
        """
        Open base_directory, scan existing WALs, try to
        exclusively open one of the existing ones, on failure
        create a new WAL.
        In any case return an open io.FileIO on the existing or
        new WAL, with rights to write to it
        The writer WAL is just one per process

        :return:
        """

    def get_reader(self, wal_id: uuid.UUID, position: int) -> io.FileIO:
        """
        Get read only access to any of the existing WALs
        You can also select the writer WAL
        There can be multiple open readers at a time
        Multiple readers could be reading simoultaneosly
        from the same WAL. The base file will remain open
        as long there is an open reader to it
        Once used the reader should be closed in order to
        allow to close the WAL file when all readers are
        no longer in use
        Each reader keeps a current position, independent of
        other readers

        :return: a reader streamer
        """

    def get_writer_wal(self) -> uuid.UUID:
        """

        :return:
        """

    def write_streamer(self) -> io.FileIO:
        """
        Get write only access to the only writing WAL.
        There can be just one streamer open in a process
        Once used the should be closed. The actual file
        will be closed only on processs shutdown
        On close it is assured that the data is flushed to
        disk and the WAL will be available in future runs
        of the system.
        In a future use of the system it is not assured that
        the process will choose the same write WAL.

        :return: a reader streamer
        """


