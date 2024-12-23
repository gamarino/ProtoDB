from . import common

from .common import Future, KB, MB, GB, RootObject
import uuid
import io
import psutil
import os
import uuid


class FileBlockProvider(common.BlockProvider):
    """
    Shared File Block Provider.

    This class defines a block storage provider that uses a shared file space, represented by a directory
    accessible for read/write operations by multiple processes. These processes can run either on the same
    machine or on different machines sharing the same space.

    ### Functionality Description

    - **Shared Space:**
      The storage block is defined within a directory (space directory), which must be accessible for all
      storage operations. Synchronization between processes is exclusively managed through common file
      operations provided by the underlying operating system.

    - **Root Object:**
      The root object of the system will be managed outside the scope of this class. However, within the
      space directory, two fixed-name files are expected to be present at all times:
        - `space.config`: A configuration file containing global information for all processes.
        - `space_root`: A JSON file containing the last saved root object. This file can be updated by one
          process through exclusive write access. The new root value will be written in JSON format,
          UTF-8 encoded, and the file will then be closed.

    - **WAL Files (Write-Ahead Logs):**
      Within the space, there will also be 1 to N WAL files. Each process will take exclusive write access
      to one of these WAL files (the WAL assigned to the process), while other processes may access it for
      simultaneous read operations. WALs that are not assigned to any process can be accessed freely for
      concurrent reads by any process.

    - **Buffered Writes:**
      In certain cases, a recently written operation may have been committed to a process's own WAL but may
      not yet be accessible to other processes due to buffered writes. It is the responsibility of the
      storage system to handle such cases, serving read requests from the same process via buffered data
      or exposing the data to other processes, potentially via a network connection. This is beyond the
      responsibility of the block provider.

    - **WAL Maximum Size:**
      WALs can grow up to a predefined maximum size. When this limit is reached, any pending operations
      must be flushed, and the old WAL should be closed through the `close_wal` method. Afterward, a new
      WAL can be created or selected using `get_new_wal`. This ensures that each process will sequentially
      use multiple WALs. Additionally, this mechanism aids in backup management, as individual WAL files
      will remain within manageable size limits.

    - **Space Growth:**
      The space directory will grow dynamically with as many WAL files as needed to store all recorded
      data. Each WAL will be named using a unique identifier (a `uuid4` hex identifier).

    ### Common Operations

    - **Reading:**
      A single WAL file can support multiple simultaneous readers, each with its own independent cursor
      position. Seek operations are allowed for readers.

    - **Writing:**
      Write operations will primarily be sequential. However, in cases of retries following exceptions,
      a seek operation may be performed to retry the failed operation.

    ### Thread Safety
    All code implemented within this class is thread-safe.
    """

    current_wal: io.BytesIO
    current_wal_id: uuid.UUID

    def __init__(self, space_path: str = None, maximun_cache_size: int = 0):
        """
        Constructor for the FileBlockProvider class.

        :param space_path: shared directory to use for the space. if not given, current directory is used
        :param maximun_cache_size: requested cache size. If no value is provided a default value
                                   of 50% of physical memory will be used
        """
        self.space_path = space_path or '.'
        if not maximun_cache_size:
            total_memory = psutil.virtual_memory().total
            maximun_cache_size = 50 * total_memory / 100

        self.maximun_cache_size = maximun_cache_size


    def get_new_wal(self) -> tuple[uuid.UUID, int]:
        """
        Get a WAL to use.
        It could be an old one, or a new one.

        :return: a tuple with the id of the WAL and the next offset to use
        """
        available_wals = [file for file in os.listdir(self.space_path)
                          if os.path.isfile(os.path.join(self.space_path, file)) and \
                             len(file) == 32]

        wals_with_size = [(file, os.path.getsize(os.path.join(self.space_path, file))) for file in available_wals]

        for file, size in wals_with_size.sort(key=lambda x: x[1]):
            self.current_wal_id = uuid.UUID(file)
            try:
                self.current_wal = open(os.path.join(self.space_path, file),'wb+')
                return self.current_wal_id, size
            except PermissionError:
                continue
            except BlockingIOError:
                continue

        self.current_wal_id = uuid.uuid4()
        self.current_wal = open(os.path.join(self.space_path, self.current_wal_id.hex, 'wb+'))

        return self.current_wal_id, 0

    def get_reader(self, wal_id: uuid.UUID, position: int) -> io.FileIO:
        """
        Get a streamer initialized at position in WAL file
        wal_id

        :param wal_id:
        :param position:
        :return:
        """

    def get_writer_wal(self) -> uuid.UUID:
        """

        :return:
        """

    def write_streamer(self, wal_id: uuid.UUID) -> io.FileIO:
        """

        :return:
        """

    def get_current_root_object(self) -> RootObject:
        """
        Read current root object from storage
        :return: the current root object
        """

    def update_root_object(self, new_root: RootObject):
        """
        Updates or create the root object in storage
        On newly created databases, this is the first
        operation to perform

        :param new_root:
        :return:
        """

    def close_wal(self, transaction_id: uuid.UUID):
        """
        Close a previous WAL. Flush any pending data. Make all changes durable
        :return:
        """

    def close(self):
        """
        Close the operation of the block provider. Flush any pending data to WAL. Make all changes durable
        No further operations are allowed
        :return:
        """

