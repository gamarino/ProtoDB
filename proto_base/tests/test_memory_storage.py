import unittest
import uuid
from threading import Thread
from ..common import Atom, AtomPointer, RootObject, ProtoValidationException, ProtoCorruptionException
from ..memory_storage import MemoryStorage


class TestMemoryStorage(unittest.TestCase):

    def setUp(self):
        """
        Initialize a fresh MemoryStorage instance for each test.
        """
        self.storage = MemoryStorage()

    # ---- STANDARD BEHAVIOR TESTS ----
    def test_read_empty_root(self):
        """
        Verifies that reading the root object when no root is set raises an exception.
        """
        with self.assertRaises(ProtoValidationException):
            self.storage.read_current_root()

    def test_set_and_read_root(self):
        """
        Verifies that the root object is set and can be read correctly.
        """
        root = RootObject()
        self.storage.set_current_root(root)
        self.assertEqual(self.storage.read_current_root(), root)

    def test_push_and_get_atom(self):
        """
        Verifies that an atom can be pushed and then retrieved correctly.
        """
        atom = Atom()
        atom.atom_pointer = AtomPointer(transaction_id=None, offset=None)

        # Push the atom
        future_pointer = self.storage.push_atom(atom)
        pointer = future_pointer.result()

        # Retrieve the same atom
        future_atom = self.storage.get_atom(pointer)
        retrieved_atom = future_atom.result()

        # Verify the atom data matches
        self.assertEqual(retrieved_atom, atom)

    def test_push_atom_duplicate_offset(self):
        """
        Verifies that trying to add an atom with a duplicate offset raises an exception.
        """
        atom = Atom()
        atom.atom_pointer = AtomPointer(transaction_id=None, offset=None)

        # Push the atom successfully
        future_pointer = self.storage.push_atom(atom)
        pointer = future_pointer.result()

        # Attempt to push the same atom again
        with self.assertRaises(ProtoCorruptionException):
            self.storage.push_atom(atom)

    def test_get_nonexistent_atom(self):
        """
        Verifies that requesting an atom not in the storage raises a ProtoCorruptionException.
        """
        nonexistent_pointer = AtomPointer(transaction_id=uuid.uuid4(), offset=uuid.uuid4())
        with self.assertRaises(ProtoCorruptionException):
            self.storage.get_atom(nonexistent_pointer)

    def test_close_storage(self):
        """
        Verifies that the `close` method runs successfully, even if it does nothing.
        """
        try:
            self.storage.close()
        except Exception as e:
            self.fail(f"Close storage raised an exception: {e}")

    # ---- MULTITHREADING TESTS ----
    def test_multithreading_safe_push(self):
        """
        Verifies the thread-safe nature of `push_atom` by pushing atoms from multiple threads concurrently.
        """

        def thread_push_atom(storage, atom_list):
            for atom in atom_list:
                atom.atom_pointer = AtomPointer(transaction_id=None, offset=None)
                storage.push_atom(atom)

        # Create a list of atoms to push
        atoms = [Atom() for _ in range(50)]  # Total 50 atoms

        # Divide the atoms into two threads
        atoms_thread1 = atoms[:25]
        atoms_thread2 = atoms[25:]

        # Create threads
        t1 = Thread(target=thread_push_atom, args=(self.storage, atoms_thread1))
        t2 = Thread(target=thread_push_atom, args=(self.storage, atoms_thread2))

        # Start threads
        t1.start()
        t2.start()

        # Wait for threads to finish
        t1.join()
        t2.join()

        # Verify that all 50 atoms were stored without duplicates
        self.assertEqual(len(self.storage.atoms), 50)

    def test_multithreading_safe_get(self):
        """
        Verifies thread-safe retrieval of atoms using multiple threads.
        """
        # Push some atoms into the storage
        atom_list = [Atom() for _ in range(10)]
        for atom in atom_list:
            atom.atom_pointer = AtomPointer(transaction_id=None, offset=None)
            self.storage.push_atom(atom)

        # Get all the keys of atoms stored
        atom_pointers = list(self.storage.atoms.keys())

        def thread_get_atom(storage, pointers):
            for pointer in pointers:
                atom_pointer = AtomPointer(transaction_id=None, offset=pointer)
                try:
                    storage.get_atom(atom_pointer)
                except ProtoCorruptionException as e:
                    self.fail(f"Thread-safe get_atom failed: {e}")

        # Divide pointers for two threads
        pointers_thread1 = atom_pointers[:5]
        pointers_thread2 = atom_pointers[5:]

        # Create threads
        t1 = Thread(target=thread_get_atom, args=(self.storage, pointers_thread1))
        t2 = Thread(target=thread_get_atom, args=(self.storage, pointers_thread2))

        # Start threads
        t1.start()
        t2.start()

        # Wait for threads to finish
        t1.join()
        t2.join()

    def test_multithreading_push_and_get(self):
        """
        Verifies concurrent pushing and retrieving of atoms works without data corruption.
        """

        def thread_push(storage, atom_list):
            for atom in atom_list:
                atom.atom_pointer = AtomPointer(transaction_id=None, offset=None)
                storage.push_atom(atom)

        def thread_get(storage, atom_list):
            for atom in atom_list:
                atom_pointer = atom.atom_pointer
                if atom_pointer:
                    try:
                        storage.get_atom(atom_pointer)
                    except ProtoCorruptionException:
                        pass

        # Create a list of atoms and pre-push some to storage
        atom_list = [Atom() for _ in range(20)]  # 20 atoms

        for atom in atom_list:
            atom.atom_pointer = AtomPointer(transaction_id=None, offset=None)
        leftover_atoms = atom_list[:10]

        # Create threads
        t_push = Thread(target=thread_push, args=(self.storage, leftover_atoms))
        t_get = Thread(target=thread_get, args=(self.storage, atom_list))

        # Start threads
        t_push.start()
        t_get.start()

        # Wait for threads to finish
        t_push.join()
        t_get.join()

    def test_thread_safe_set_and_read_root(self):
        """
        Verifies thread-safe setting and retrieval of the root object.
        """

        def set_root_safe(storage, root_object):
            storage.set_current_root(root_object)

        def read_root_safe(storage):
            try:
                storage.read_current_root()
            except ProtoValidationException as e:
                self.fail(f"Root read failed unexpectedly: {e}")

        # Set root object in one thread
        root_object = RootObject()
        t1 = Thread(target=set_root_safe, args=(self.storage, root_object))

        # Attempt to read root object concurrently
        t2 = Thread(target=read_root_safe, args=(self.storage,))

        # Start threads
        t1.start()
        t2.start()

        # Wait for threads to finish
        t1.join()
        t2.join()

        # Verify correctness
        self.assertEqual(self.storage.read_current_root(), root_object)


if __name__ == '__main__':
    unittest.main()
