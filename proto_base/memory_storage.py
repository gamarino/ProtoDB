from . import common
from .common import Future, Atom, AtomPointer, StorageWriteTransaction, StorageReadTransaction, RootObject
import uuid


class MemoryTransaction(common.StorageReadTransaction):
    """

    """
    def __init__(self, storage: common.SharedStorage):
        self.storage = storage

    def get_atom(self, atom: Atom) -> Future[Atom]:
        if atom.atom_pointer.transaction_id in self.storage.stored_transactions:
            transaction = self.storage.stored_transactions[atom.atom_pointer.transaction_id]
            if atom.atom_pointer.offset in transaction:
                return transaction[atom.atom_pointer.offset]

        raise common.ProtoCorruptionException(message=f'Atom {atom} does not exist')


class MemoryWriteTransaction(common.StorageWriteTransaction, MemoryTransaction):
    """

    """
    transaction_id: uuid.UUID

    def __init__(self, storage: common.SharedStorage):
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


class MemoryStorage(common.SharedStorage):
    """

    """
    stored_transactions = {}
    current_root: AtomPointer = None

    def new_read_transaction(self) -> Future[StorageReadTransaction]:
        """
        Start a new write transaction
        :return:
        """
        result = Future()
        result.set_result(MemoryTransaction(self))
        return result

    def new_write_transaction(self) -> Future[StorageWriteTransaction]:
        """
        Start a new write transaction
        :return:
        """
        result = Future()
        result.set_result(MemoryWriteTransaction(self))
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
