import uuid
from threading import RLock as Lock  # Use re-entrant lock to avoid deadlocks under nested root operations

from . import common
from .common import Future, AtomPointer
from .db_access import BytesAtom
from .atom_cache import AtomCacheBundle
from .exceptions import ProtoCorruptionException


class MemoryStorage(common.SharedStorage):
    """
    A simple in-memory implementation of a storage system.
    This acts as a lightweight and temporary alternative to persistent storage,
    ideal for testing and simulation purposes.
    """

    def __init__(self,
                 enable_atom_object_cache: bool = True,
                 enable_atom_bytes_cache: bool = True,
                 object_cache_max_entries: int = 50000,
                 object_cache_max_bytes: int = 256 * 1024 * 1024,
                 bytes_cache_max_entries: int = 20000,
                 bytes_cache_max_bytes: int = 128 * 1024 * 1024,
                 cache_stripes: int = 64,
                 cache_probation_ratio: float = 0.5,
                 schema_epoch: int | None = None):
        """
        Initializes the in-memory storage. It sets up:
        - A unique transaction ID for all operations during this session.
        - An empty atom dictionary for storing atoms against their offsets.
        - A lock to make the memory storage thread-safe.
        """
        self.transaction_id = uuid.uuid4()  # A unique transaction ID for this storage session.
        self.atoms = dict()  # Dictionary to store atoms in memory.
        self.current_root_history_pointer = None  # A container for the current root object.
        self.lock = Lock()  # Thread lock to ensure safe concurrent access.
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

    def read_current_root(self) -> AtomPointer:
        """
        Retrieve the current root object of the storage.
        :return: The `RootObject`, if it exists.
        :raises:
            ProtoValidationException: If no root object has been set yet.
        """
        with self.lock:  # Ensure thread-safety when accessing `current_root`.
            ptr = self.current_root_history_pointer
            # Debug instrumentation
            try:
                import os as _os
                if _os.environ.get('PB_DEBUG_CONC'):
                    tid = getattr(ptr, 'transaction_id', None)
                    off = getattr(ptr, 'offset', None)
                    print(f"[DEBUG][MEM] read_current_root -> {tid}/{off}")
            except Exception:
                pass
            return ptr

    def read_lock_current_root(self) -> AtomPointer:
        return self.read_current_root()

    def set_current_root(self, new_root_history_pointer: AtomPointer):
        """
        Set a new root history object for the storage, replacing any existing one.
        :param new_root_history_pointer: The pointer to the new `RootObject` to be set.
        """
        with self.lock:  # Ensure thread-safety when modifying `current_root`.
            # Debug instrumentation
            try:
                import os as _os
                if _os.environ.get('PB_DEBUG_CONC'):
                    tid = getattr(new_root_history_pointer, 'transaction_id', None)
                    off = getattr(new_root_history_pointer, 'offset', None)
                    print(f"[DEBUG][MEM] set_current_root <- {tid}/{off}")
            except Exception:
                pass
            self.current_root_history_pointer = new_root_history_pointer

    def unlock_current_root(self):
        pass

    def root_context_manager(self):
        class ContextManager:
            ms: "MemoryStorage"
            def __init__(self, ms: "MemoryStorage"):
                self.ms = ms
                self._acquired = False
            def __enter__(self):
                # Debug
                try:
                    import os as _os
                    if _os.environ.get('PB_DEBUG_CONC'):
                        print("[DEBUG][MEM] RootContextManager enter: acquiring lock")
                except Exception:
                    pass
                self.ms.lock.acquire()
                self._acquired = True
                try:
                    import os as _os
                    if _os.environ.get('PB_DEBUG_CONC'):
                        print("[DEBUG][MEM] RootContextManager entered: lock acquired")
                except Exception:
                    pass
            def __exit__(self, exc_type, exc_value, traceback):
                if self._acquired:
                    self.ms.lock.release()
                self._acquired = False
                try:
                    import os as _os
                    if _os.environ.get('PB_DEBUG_CONC'):
                        print("[DEBUG][MEM] RootContextManager exit: lock released")
                except Exception:
                    pass
            def __repr__(self):
                return f"MemoryStorage.RootContextManager(ms={self.ms})"
        return ContextManager(self)

    def flush_wal(self):
        """
        No data to be flushed for memory storage
        :return:
        """
        pass

    def push_atom(self, atom: dict) -> Future[AtomPointer]:
        """
        Save an atom payload (JSON-serializable dict) in the in-memory storage.
        Always assigns a fresh offset and returns an AtomPointer. The input is not mutated.
        :param atom: The atom payload to be stored (dict produced by Atom._save()).
        :return: A Future containing the AtomPointer of the stored atom.
        :raises ProtoCorruptionException: If a randomly chosen offset collides (extremely unlikely).
        """
        with self.lock:
            # Allocate a fresh unique offset
            offset = uuid.uuid4().int
            while offset in self.atoms:
                offset = uuid.uuid4().int
            atom_pointer = AtomPointer(transaction_id=self.transaction_id, offset=offset)

            # Store the payload dict at the offset
            self.atoms[offset] = atom

            # Return the pointer
            result = Future()
            result.set_result(atom_pointer)
            return result

    def get_atom(self, atom_pointer: AtomPointer) -> Future[dict]:
        """
        Retrieve an atom from the storage using its atom pointer.
        :param atom_pointer: The `AtomPointer` associated with the atom.
        :return: A `Future` object containing the retrieved `Atom` as a dict.
        :raises:
            ProtoCorruptionException: If the atom does not exist in the storage.
        """
        with self.lock:  # Ensure thread-safety for operations on `atoms`.
            # Check if the atom exists in the dictionary.
            if atom_pointer.offset in self.atoms:
                atom = self.atoms[atom_pointer.offset]

                # Create and return a Future with the retrieved atom.
                result = Future()
                result.set_result(atom)
                return result

            # Raise an error if the atom does not exist.
            raise ProtoCorruptionException(
                message=f'Atom at {atom_pointer} does not exist'
            )

    def get_bytes(self, atom_pointer: AtomPointer) -> Future[bytes]:
        """
        Retrieves the byte data associated with the given atom pointer.

        This method is used to asynchronously fetch and return the byte data
        corresponding to the `AtomPointer` provided. It must be implemented
        by any subclass as it is declared abstract.

        :param atom_pointer: Pointer to the atom whose byte data is to be
                             retrieved.
        :type atom_pointer: AtomPointer
        :return: A future holding the byte data corresponding to
                 the atom pointer.
        :rtype: Future[bytes]
        """
        with self.lock:  # Ensure thread-safety for operations on `atoms`.
            # Check if the atom exists in the dictionary.
            if atom_pointer.offset in self.atoms:
                data: bytes = self.atoms[atom_pointer.offset]

                # Create and return a Future with the retrieved atom.
                result = Future()
                result.set_result(data)
                return result

            # Raise an error if the atom does not exist.
            raise ProtoCorruptionException(
                message=f'Atom at {atom_pointer} does not exist'
            )

    def push_bytes(self, data: bytes) -> Future[AtomPointer]:
        """
        Pushes a sequence of bytes to the underlying data structure or processing unit.

        This method is abstract and must be implemented by subclasses. The concrete
        implementation should handle the provided byte sequence according to its
        specific requirements and behavior.

        :param data: A sequence of bytes to be processed or stored.
        :type data: bytes
        :return: None
        """
        atom = BytesAtom(content=data)

        with self.lock:  # Ensure thread-safety for operations on `atoms`.
            atom.atom_pointer.transaction_id = self.transaction_id  # Associate atom with the current transaction ID.

            atom.atom_pointer.offset = uuid.uuid4()
            # Check if the offset already exists in the atoms dictionary.
            while atom.atom_pointer.offset in self.atoms:
                atom.atom_pointer.offset = uuid.uuid4()

            # Add the atom to the storage.
            self.atoms[atom.atom_pointer.offset] = atom

            # Create and return a Future with the atom's pointer.
            result = Future()
            result.set_result(atom.atom_pointer)
            return result

    def close(self):
        """
        Close the storage operation. This flushes any pending data and marks
        the storage as closed. Further operations should not be allowed.
        For the in-memory storage, this method does not perform any operations.
        """
