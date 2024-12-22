from . import common
from .common import Future, Atom, AtomPointer, RootObject
import uuid


class MemoryStorage(common.SharedStorage):
    """

    """
    atoms: dict = {}
    current_root: RootObject = None
    transaction_id: uuid.UUID = None

    def __init__(self):
        self.transaction_id = uuid.uuid4()
        self.atoms = dict()

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

    def push_atom(self, atom: Atom) -> Future[AtomPointer]:
        """

        :param atom:
        :return:
        """
        atom.atom_pointer.transaction_id = self.transaction_id
        offset = uuid.uuid4()
        atom.atom_pointer.offset = offset

        if offset in self.atoms:
            raise common.ProtoCorruptionException(message=f'You are trying to push an atom an already existing {atom}')

        self.atoms[offset] = atom

        result = Future()
        result.set_result(atom.atom_pointer)
        return result

    def get_atom(self, atom_pointer: AtomPointer) -> Future[Atom]:
        if atom_pointer.offset in self.atoms:
            atom = self.atoms[atom_pointer.offset]

            result = Future()
            result.set_result(atom)
            return result

        raise common.ProtoCorruptionException(message=f'Atom at {atom_pointer} does not exist')

    def close(self):
        """
        Close the operation. Flush any pending data. Make all changes durable
        No further operations are allowed
        :return:
        """
        pass
