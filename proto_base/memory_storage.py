from . import common
from .common import Future, Atom, AtomPointer, RootObject
import uuid
from threading import Lock  # Import threading lock to ensure thread safety


class MemoryStorage(common.SharedStorage):
    """
    A simple in-memory implementation of a storage system.
    This acts as a lightweight and temporary alternative to persistent storage,
    ideal for testing and simulation purposes.
    """

    def __init__(self):
        """
        Initializes the in-memory storage. It sets up:
        - A unique transaction ID for all operations during this session.
        - An empty atom dictionary for storing atoms against their offsets.
        - A lock to make the memory storage thread-safe.
        """
        self.transaction_id = uuid.uuid4()  # A unique transaction ID for this storage session.
        self.atoms = dict()  # Dictionary to store atoms in memory.
        self.current_root = None  # A container for the current root object.
        self.lock = Lock()  # Thread lock to ensure safe concurrent access.

    def read_current_root(self) -> RootObject:
        """
        Retrieve the current root object of the storage.
        :return: The `RootObject`, if it exists.
        :raises:
            ProtoValidationException: If no root object has been set yet.
        """
        with self.lock:  # Ensure thread-safety when accessing `current_root`.
            if self.current_root:
                return self.current_root

            # Raise an error if the root is not set.
            raise common.ProtoValidationException(message='You are trying to read an empty DB!')

    def set_current_root(self, new_root: RootObject):
        """
        Set a new root object for the storage, replacing any existing root.
        :param new_root: The new `RootObject` to be set.
        """
        with self.lock:  # Ensure thread-safety when modifying `current_root`.
            self.current_root = new_root

    def push_atom(self, atom: Atom) -> Future[AtomPointer]:
        """
        Save an atom in the in-memory storage. Each atom gets a unique offset and is tied
        to the current transaction ID.
        :param atom: The `Atom` object to be stored.
        :return: A `Future` containing the corresponding `AtomPointer` of the stored atom.
        :raises:
            ProtoCorruptionException: If an atom with the same offset already exists.
        """
        with self.lock:  # Ensure thread-safety for operations on `atoms`.
            atom.atom_pointer.transaction_id = self.transaction_id  # Associate atom with the current transaction ID.
            offset = uuid.uuid4()  # Generate a unique offset for the atom.
            atom.atom_pointer.offset = offset

            # Check if the offset already exists in the atoms dictionary.
            if offset in self.atoms:
                raise common.ProtoCorruptionException(
                    message=f'You are trying to push an already existing atom: {atom}'
                )

            # Add the atom to the storage.
            self.atoms[offset] = atom

            # Create and return a Future with the atom's pointer.
            result = Future()
            result.set_result(atom.atom_pointer)
            return result

    def get_atom(self, atom_pointer: AtomPointer) -> Future[Atom]:
        """
        Retrieve an atom from the storage using its atom pointer.
        :param atom_pointer: The `AtomPointer` associated with the atom.
        :return: A `Future` object containing the retrieved `Atom`.
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
            raise common.ProtoCorruptionException(
                message=f'Atom at {atom_pointer} does not exist'
            )

    def close(self):
        """
        Close the storage operation. This flushes any pending data and marks
        the storage as closed. Further operations should not be allowed.
        For the in-memory storage, this method does not perform any operations.
        """
        with self.lock:  # Ensure thread-safety during any final operations.
            pass  # No specific close logic required for in-memory storage.
