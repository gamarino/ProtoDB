import collections
import configparser
import json
import logging
import os
import uuid
from io import BytesIO, SEEK_SET, SEEK_CUR, SEEK_END
from threading import Lock, get_ident
from typing import BinaryIO

import psutil

from . import common, ProtoValidationException
from .common import MB, AtomPointer
from .exceptions import ProtoUnexpectedException

_logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 1 * MB


class FileReaderFactory:
    """
    A factory to manage reusable file readers (io.BinaryIO).

    Readers are reused to minimize resource usage and optimize performance.
    Thread-safe due to the use of a Lock.
    """
    available_readers: dict[str, list[BinaryIO]]
    path: str
    _lock: Lock

    def __init__(self, path: str):
        """
        Initialize the factory with the directory path containing the files to read.

        :param path: Path to the directory containing target files.
        """
        self.path = path
        self.available_readers = {}
        self._lock = Lock()

    def get_reader(self, file_name: str) -> BinaryIO:
        """
        Fetch a file reader for the given file name. Reuse a reader if available.

        :param file_name: Name of the file to read.
        :return: An open BinaryIO object in binary reading mode.
        """
        with self._lock:
            # Reuse an available reader if possible
            if file_name in self.available_readers and self.available_readers[file_name]:
                reader = self.available_readers[file_name].pop(0)
                reader.seek(0)  # Ensure the reader's position is reset
                return reader

        try:
            # Create a new reader if no reusable ones exist
            reader = open(os.path.join(self.path, file_name), 'rb')
            return reader
        except FileNotFoundError:
            _logger.error(f"File not found: {file_name}")
            raise ProtoUnexpectedException(message=f"WAL File not found: {file_name}")
        except PermissionError:
            _logger.error(f"Permission denied when accessing: {file_name}")
            raise ProtoUnexpectedException(message=f"Permission denied reading WAL File: {file_name}")

    def return_reader(self, reader: BinaryIO, file_name: str):
        """
        Return a file reader to the pool for reuse.

        :param reader: The BinaryIO object to be returned.
        :param file_name: File name associated with the reader.
        """
        try:
            with self._lock:
                if file_name in self.available_readers:
                    self.available_readers[file_name].append(reader)
                else:
                    self.available_readers[file_name] = [reader]
        except Exception as e:
            _logger.exception(e)
            raise ProtoUnexpectedException(message=f'Unexpected exception returning reader for {file_name}')

    def close(self):
        with self._lock:
            for readers in self.available_readers.values():
                for reader in readers:
                    reader.close()
            self.available_readers = {}


class PageCache:
    """
    Implements a thread-safe, LRU-based binary page cache.

    Each page is identified by a unique key (e.g., a combination of WAL ID and page number).
    Stores a limited number of pages in memory, evicting the least recently used (LRU) page
    when the cache is full.
    """

    def __init__(self, capacity: int, page_size: int, reader_factory: FileReaderFactory):
        """
        Initialize the cache with a given capacity and page size.

        :param capacity: Maximum number of pages the cache can hold.
        :param page_size: The size of each page in bytes.
        :param reader_factory: An instance of FileReaderFactory for disk reads.
        """
        self.capacity = max(1, capacity)  # Ensure a valid capacity
        self.cache = collections.OrderedDict()  # Preserves insertion order for LRU eviction
        self.page_size = page_size
        self.reader_factory = reader_factory
        self._lock = Lock()

    def read_page(self, wal_id: uuid.UUID, page_number: int) -> bytes:
        """
        Read a page from memory or disk if not cached.

        :param wal_id: UUID of the WAL file.
        :param page_number: Page number (starting at 0).
        :return: Raw binary content of the requested page.
        """
        page_key = (wal_id, page_number)

        with self._lock:
            # Check if the page is in cache
            if page_key in self.cache:
                _logger.debug(f"Cache hit for page: {page_key}")
                self.cache.move_to_end(page_key)  # Make it the most recently used
                return self.cache[page_key]

        # Cache miss: Read from disk
        page_content = self._read_page_from_disk(str(wal_id), page_number)

        with self._lock:
            # Remove the LRU if the cache is full
            if len(self.cache) >= self.capacity:
                evicted_key, _ = self.cache.popitem(last=False)
                _logger.info(f"Evicting page: {evicted_key}")

            # Cache the new page
            self.cache[page_key] = page_content

        return page_content

    def _read_page_from_disk(self, file: str, page_number: int) -> bytes:
        """
        Read a specific page from disk using the file reader factory.

        :param file: File name (WAL UUID as string).
        :param page_number: Page number to read.
        :return: Raw binary page data.
        """
        reader = None
        try:
            reader = self.reader_factory.get_reader(file)
            reader.seek(page_number * self.page_size)
            data = reader.read(self.page_size)
            return data
        except Exception as e:
            _logger.error(f"Failed to read page {page_number} from file {file}: {e}")
            raise ProtoUnexpectedException(
                message=f"Unexpected error reading WAL {file}, page {page_number}"
            )
        finally:
            if reader:
                self.reader_factory.return_reader(reader, file)  # Return the reader for reuse


class ReadStreamer(BytesIO):
    def __init__(self, wal_id: uuid.UUID, offset: int, page_size: int, page_cache: PageCache):
        super().__init__()
        self.wal_id = wal_id
        self.page_size = page_size
        self.page_cache = page_cache
        self.initial_offset = offset
        self.current_offset = offset
        self.current_page = None

    def tell(self):
        return self.current_offset

    def seek(self, offset: int, whence: int = SEEK_SET):
        if whence == SEEK_CUR:
            offset += self.current_offset
        elif whence == SEEK_END:
            raise ProtoValidationException(
                message=f'In readers, seek method end relative is not supported!'
            )

        if offset < 0:
            self.current_offset = self.initial_offset
        else:
            self.current_offset = self.initial_offset + offset

    def read(self, count: int | None = None):
        count = count or 0
        page_number = self.tell() // self.page_size
        page_offset = self.tell() % self.page_size

        if not self.current_page:
            self.current_page = self.page_cache.read_page(self.wal_id, page_number)

        if page_offset + count < self.page_size:
            value = self.current_page[page_offset:page_offset + count]
            self.current_offset += count
        else:
            value: bytes = bytes()

            already_read = 0
            while already_read < count:
                if page_offset >= self.page_size:
                    page_number += 1
                    page_offset = 0
                    self.current_page = self.page_cache.read_page(self.wal_id, page_number)

                fragment_size = min(self.page_size - page_offset, count - already_read)
                fragment = self.current_page[page_offset:page_offset + fragment_size]
                value += fragment

                page_offset += fragment_size
                self.current_offset += fragment_size
                already_read += fragment_size

        return value

    def close(self):
        # At closing, nothing to do, it will be
        # collected for further usage
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


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

    current_wal: BinaryIO | None
    current_wal_id: uuid.UUID
    page_size: int
    page_cache: PageCache
    streamer_factory: FileReaderFactory

    def __init__(self, space_path: str = None, maximun_cache_size: int = 0, page_size: int = DEFAULT_PAGE_SIZE):
        """
        Constructor for the FileBlockProvider class.

        :param space_path: shared directory to use for the space. if not given, current directory is used
        :param maximun_cache_size: requested cache size. If no value is provided a default value
                                   of 50% of physical memory will be used
        :param page_size: page_size to use. if not given, then DEFAULT_PAGE_SIZE
        """
        self.space_path = space_path or '.'
        if not maximun_cache_size:
            total_memory = psutil.virtual_memory().total
            maximun_cache_size = 50 * total_memory / 100

        self.maximun_cache_size = maximun_cache_size
        self.page_size = page_size

        if not os.path.exists(self.space_path):
            os.mkdir(self.space_path)

        self.config_data = configparser.ConfigParser()

        try:
            self.config_data.read(os.path.join(self.space_path, 'space.config'))
        except:
            pass

        self.reader_factory = FileReaderFactory(space_path)
        self.page_cache = PageCache(self.maximun_cache_size / self.page_size, self.page_size, self.reader_factory)
        # Root lock state (for OS-level exclusive locks with reentrancy awareness)
        self._root_lock_fd = None
        self._root_lock_owner = None
        self._root_lock_count = 0
        self._root_lock_guard = Lock()

    def get_config_data(self):
        return self.config_data

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

        for file, size in sorted(wals_with_size, key=lambda x: x[1]):
            self.current_wal_id = uuid.UUID(file)
            try:
                self.current_wal = open(os.path.join(self.space_path, file), 'ab+')
                return self.current_wal_id, size
            except PermissionError:
                continue
            except BlockingIOError:
                continue

        self.current_wal_id = uuid.uuid4()
        self.current_wal = open(os.path.join(self.space_path, str(self.current_wal_id)), 'ab+')

        return self.current_wal_id, 0

    def get_reader(self, wal_id: uuid.UUID, position: int) -> BinaryIO:
        """
        Get a streamer initialized at position in WAL file

        :param wal_id:
        :param position:
        :return:
        """
        return ReadStreamer(wal_id, position, self.page_size, self.page_cache)

    def get_writer_wal(self) -> uuid.UUID:
        """

        :return:
        """
        return self.current_wal_id

    def write_streamer(self, wal_id: uuid.UUID) -> BinaryIO:
        """

        :return:
        """
        return self.current_wal

    def root_context_manager(self):
        class ContextManager:
            bp: FileBlockProvider
            def __init__(self, bp: FileBlockProvider):
                self.bp = bp

            def __enter__(self):
                self.bp._acquire_root_lock()

            def __exit__(self, exc_type, exc_value, traceback):
                self.bp._release_root_lock()

            def __repr__(self):
                return f"FileBlockProvider.RootContextManager(bp={self.bp})"

        return ContextManager(self)

    def get_current_root_object(self):
        """
        Read current root object from storage with robustness under concurrent writers.
        Returns dict or None if not available/valid yet.
        Lowest-level path: uses os.open/os.read and os.fstat to avoid higher-level buffering
        and to instrument exact bytes, inode and timestamps read from disk.
        """
        path = os.path.join(self.space_path, 'space_root')
        # Retry a few times to avoid transient races while another thread/process is replacing the file
        for _ in range(10):
            try:
                # Low-level open and read
                fd = os.open(path, os.O_RDONLY)
                try:
                    st = os.fstat(fd)
                    chunks = []
                    while True:
                        b = os.read(fd, 8192)
                        if not b:
                            break
                        chunks.append(b)
                    raw = b"".join(chunks)
                    # Decode and parse
                    try:
                        text = raw.decode('utf-8')
                    except Exception:
                        # If decoding fails, fall back to BytesIO+json for diagnostics
                        text = ''
                    try:
                        data = json.loads(text) if text else json.loads(raw.decode('utf-8', 'ignore'))
                    except json.JSONDecodeError:
                        # Empty or partially written file: try again shortly
                        import time as _time
                        _time.sleep(0.001)
                        continue
                    # Debug: log pointer and file stats at lowest level
                    try:
                        import os as _os
                        if _os.environ.get('PB_DEBUG_IO') or _os.environ.get('PB_DEBUG_CONC'):
                            prefix = text[:200] if text else ''
                            print(
                                f"[DEBUG][IO] READ space_root fd={fd} inode={st.st_ino} size={st.st_size} "
                                f"mtime_ns={st.st_mtime_ns} ctime_ns={st.st_ctime_ns} bytes_prefix={prefix!r}")
                            try:
                                print(f"[DEBUG] Provider.read space_root -> {data.get('transaction_id')}/{data.get('offset')}")
                            except Exception:
                                pass
                    except Exception:
                        pass
                    return data
                finally:
                    try:
                        os.close(fd)
                    except Exception:
                        pass
            except FileNotFoundError:
                return None
            except OSError:
                # Bad file descriptor or similar transient errors; retry briefly
                import time as _time
                _time.sleep(0.001)
                continue
            except Exception:
                # Unknown error: break and return None
                break
        return None

    def _acquire_root_lock(self, timeout_sec: float = 5.0):
        """
        Acquire an OS-level exclusive lock on the root lock file to serialize root updates
        across processes/threads. Best-effort on platforms without fcntl.
        Re-entrant for the same thread: nested calls will be counted and not re-lock.
        A small timeout prevents indefinite blocking; on timeout, a ProtoValidationException is raised.
        """
        try:
            tid = get_ident()
            with self._root_lock_guard:
                if self._root_lock_owner == tid and self._root_lock_fd is not None:
                    # Re-entrant acquisition by the same thread
                    self._root_lock_count += 1
                    return
                lock_path = os.path.join(self.space_path, 'space_root.lock')
                # Ensure lock file exists
                fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
                self._root_lock_fd = fd
            # Try non-blocking flock with retries to avoid deadlock
            start = None
            try:
                import fcntl  # type: ignore
                import errno, time as _time
                if start is None:
                    start = _time.time()
                while True:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except OSError as e:
                        if e.errno in (errno.EAGAIN, errno.EACCES):
                            if (_time.time() - start) >= timeout_sec:
                                raise ProtoValidationException(message="Timeout acquiring root lock")
                            _time.sleep(0.01)
                        else:
                            raise
                with self._root_lock_guard:
                    self._root_lock_owner = tid
                    self._root_lock_count = 1
            except Exception:
                # Windows fallback
                try:
                    import msvcrt  # type: ignore
                    import time as _time
                    if start is None:
                        start = _time.time()
                    while True:
                        try:
                            msvcrt.locking(fd, msvcrt.LK_NBRLCK, 1)
                            break
                        except OSError:
                            if (_time.time() - start) >= timeout_sec:
                                raise ProtoValidationException(message="Timeout acquiring root lock (msvcrt)")
                            _time.sleep(0.01)
                    with self._root_lock_guard:
                        self._root_lock_owner = tid
                        self._root_lock_count = 1
                except Exception:
                    # If locking is unavailable, proceed without it
                    with self._root_lock_guard:
                        self._root_lock_owner = tid
                        self._root_lock_count = 1
        except Exception:
            # Do not fail the operation due to lock acquisition problems (but we attempted)
            with self._root_lock_guard:
                if self._root_lock_owner is None:
                    self._root_lock_fd = None

    def _release_root_lock(self):
        try:
            tid = get_ident()
            with self._root_lock_guard:
                if self._root_lock_owner == tid and self._root_lock_count > 1:
                    # Nested lock: just decrement
                    self._root_lock_count -= 1
                    return
                fd = self._root_lock_fd
            if fd is None:
                return
            try:
                try:
                    import fcntl  # type: ignore
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except Exception:
                    try:
                        import msvcrt  # type: ignore
                        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                    except Exception:
                        pass
            finally:
                try:
                    os.close(fd)
                except Exception:
                    pass
            with self._root_lock_guard:
                self._root_lock_fd = None
                self._root_lock_owner = None
                self._root_lock_count = 0
        except Exception:
            with self._root_lock_guard:
                self._root_lock_fd = None
                self._root_lock_owner = None
                self._root_lock_count = 0

    def update_root_object(self, new_root):
        """
        Updates or creates the root object in storage in an atomic and durable way.
        Accepts either a plain dict (tests) or an AtomPointer (runtime code).

        This method assumes the caller has already acquired the provider's root lock via
        root_context_manager(). It performs an atomic replace of the root file without
        attempting to acquire/release the lock itself to avoid re-entrant or cross-thread
        deadlocks under high concurrency.

        Includes low-level instrumentation (PB_DEBUG_IO) that logs tmp/final file inodes,
        sizes, and timestamps; and verifies read-back after rename using os.open/os.read.

        :param new_root: dict or AtomPointer
        :return: None
        """
        try:
            # Prepare JSON payload
            if isinstance(new_root, AtomPointer):
                new_root_dict = {
                    'transaction_id': str(new_root.transaction_id),
                    'offset': new_root.offset,
                }
            else:
                new_root_dict = new_root

            import uuid as _uuid
            from threading import get_ident as _get_ident
            tid = _get_ident()
            tmp_path = os.path.join(self.space_path, f'space_root.{tid}.{_uuid.uuid4().hex}.tmp')
            final_path = os.path.join(self.space_path, 'space_root')

            # Write tmp with explicit fsync
            with open(tmp_path, 'w') as f:
                json.dump(new_root_dict, f)
                f.flush()
                os.fsync(f.fileno())
                # Instrument tmp stats
                try:
                    import os as _os
                    if _os.environ.get('PB_DEBUG_IO') or _os.environ.get('PB_DEBUG_CONC'):
                        st = os.fstat(f.fileno())
                        print(f"[DEBUG][IO] WRITE tmp fd={f.fileno()} inode={st.st_ino} size={st.st_size} mtime_ns={st.st_mtime_ns} ctime_ns={st.st_ctime_ns} payload={new_root_dict}")
                except Exception:
                    pass

            # Atomic replace
            os.replace(tmp_path, final_path)

            # Fsync directory to persist the rename on crash-prone filesystems
            try:
                dir_fd = os.open(self.space_path, os.O_DIRECTORY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except Exception:
                # Not all platforms/filesystems support directory fsync
                pass

            # Instrument final file stats and verify contents via low-level read
            try:
                import os as _os
                if _os.environ.get('PB_DEBUG_IO') or _os.environ.get('PB_DEBUG_CONC'):
                    fd = os.open(final_path, os.O_RDONLY)
                    try:
                        st = os.fstat(fd)
                        chunks = []
                        while True:
                            b = os.read(fd, 8192)
                            if not b:
                                break
                            chunks.append(b)
                        raw = b"".join(chunks)
                        txt = ''
                        try:
                            txt = raw.decode('utf-8')
                        except Exception:
                            pass
                        print(f"[DEBUG][IO] POST-RENAME READ space_root fd={fd} inode={st.st_ino} size={st.st_size} mtime_ns={st.st_mtime_ns} ctime_ns={st.st_ctime_ns} bytes_prefix={txt[:200]!r}")
                    finally:
                        try:
                            os.close(fd)
                        except Exception:
                            pass
            except Exception:
                pass
        except Exception as e:
            _logger.exception(e)
            raise ProtoUnexpectedException(message=f'Unexpected exception {e} writing root')

    def close_wal(self, transaction_id: uuid.UUID):
        """
        Close a previous WAL. Flush any pending data. Make all changes durable
        :return:
        """
        self.current_wal.close()
        self.current_wal = None

    def close(self):
        """
        Close the operation of the block provider. Flush any pending data to WAL. Make all changes durable
        No further operations are allowed
        :return:
        """
        self.current_wal.close()
        self.current_wal = None
        self.reader_factory.close()
