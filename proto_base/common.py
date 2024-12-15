"""
Common objects


"""
from _ast import Tuple
from concurrent.futures import Future
import uuid
from abc import ABC, abstractmethod
from xml.dom import NOT_SUPPORTED_ERR

VALIDATION_ERROR = 10_000
USER_ERROR = 20_000
CORRUPTION_ERROR = 30_000
NOT_SUPPORTED_ERROR = 40_000
NOT_AUTHORIZED_ERROR = 50_000


class ProtoBaseException(Exception):
    """
    Base exception for ProtoBase exceptions
    """

    def __init__(self, code: int=1, exception_type: str=None, message: str=None):
        self.code = code
        self.exception_type = exception_type
        self.message = message


class ProtoValidationException(ProtoBaseException):
    def __init__(self, code: int=VALIDATION_ERROR, exception_type: str=None, message: str=None):
        super().__init__(
            code if code else VALIDATION_ERROR,
            exception_type if exception_type else 'ValidationException',
            message
        )

class ProtoUserException(ProtoBaseException):
    def __init__(self, code: int=USER_ERROR, exception_type: str=None, message: str=None):
        super().__init__(
            code if code else USER_ERROR,
            exception_type if exception_type else 'ValidationException',
            message
        )


class ProtoCorruptionException(ProtoBaseException):
    def __init__(self, code: int=CORRUPTION_ERROR, exception_type: str=None, message: str=None):
        super().__init__(
            code if code else CORRUPTION_ERROR,
            exception_type if exception_type else 'CorruptionException',
            message
        )


class ProtoNotSupportedException(ProtoBaseException):
    def __init__(self, code: int=NOT_SUPPORTED_ERROR, exception_type: str=None, message: str=None):
        super().__init__(
            code if code else NOT_SUPPORTED_ERROR,
            exception_type if exception_type else 'NotSupportedException',
            message
        )


class ProtoNotAuthorizedException(ProtoBaseException):
    def __init__(self, code: int=NOT_AUTHORIZED_ERROR, exception_type: str=None, message: str=None):
        super().__init__(
            code if code else 6,
            exception_type if exception_type else 'AuthorizationException',
            message
        )


class AtomPointer(object):
    def __init__(self, transaction_id: uuid.UUID, offset: int):
        self.transaction_id = transaction_id
        self.offset = offset


class Atom(ABC):
    atom_pointer: AtomPointer

    def __init__(self, atom_pointer: AtomPointer = None):
        self.atom_pointer = atom_pointer


class StorageReadTransaction(ABC):
    @abstractmethod
    def get_atom(self, atom: Atom) -> Future[Atom]:
        """

        :param atom:
        :return:
        """


class StorageWriteTransaction(StorageReadTransaction):
    @abstractmethod
    def get_own_id(self) -> uuid.UUID:
        """
        Return a globaly unique identifier for the transaction
        :return:
        """

    @abstractmethod
    def push_atom(self, atom: Atom) -> Future[int]:
        """

        :param atom:
        :return:
        """

    @abstractmethod
    def commit(self):
        """

        :return:
        """


class SharedStorage(ABC):
    """
    A SharedStorage defines the minimun set of functionality required to implement a storage interface
    A SharedStorage object represents the current use instance of a permanent storage.
    A permanent storage is a set of transactions that represent the full story of the database. If you want
    to use that database, you will use an AtomStorage object to open, update or expand the database
    All methods should return concurret.futures.Future objects, and thus, depending on the actual implementation
    provides a level of paralellism to the system
    SharedStorage object should support multithreaded and multiprocessed uses, and can be safe in a multiserver
    environment, depending on the implementation
    """

    @abstractmethod
    def new_read_transaction(self) -> Future[StorageReadTransaction]:
        """
        Start a new write transaction
        :return:
        """


    @abstractmethod
    def new_write_transaction(self) -> Future[StorageWriteTransaction]:
        """
        Start a new write transaction
        :return:
        """

    @abstractmethod
    def read_current_root(self) -> Future[AtomPointer]:
        """
        Read the current root object
        :return:
        """

    @abstractmethod
    def set_current_root(self, root_pointer: AtomPointer) -> Future[AtomPointer]:
        """
        Set the current root object
        :return:
        """

class AtomTransaction(ABC):
    @abstractmethod
    def get_atom(self, atom_pointer: AtomPointer) -> Future[Atom]:
        raise NotImplemented()


class AtomWriteTransaction(AtomTransaction):
    @abstractmethod
    def push_atom(self, atom: Atom) -> Future[StorageTransaction]:
        """
        Adds a new atom to the storage.
        Atom's atom_pointer is update to reflect the assigned position
        :param atom:
        :return:
        """

    @abstractmethod
    def commit(self):
        """
        Close the transaction and make it persistent
        :return:
        """

    def abort(self):
        """
        Discard any changes made. Data base is not modified.
        :return:
        """

class AtomStorage(ABC):
    shared_storage: SharedStorage

    def __init__(self, shared_storage: SharedStorage):
        self.shared_storage = shared_storage

    @abstractmethod
    def new_read_transaction(self) -> Future[AtomTransaction]:
        """
        Start a new read transaction
        :return: the transaction to use
        """

    @abstractmethod
    def new_write_transaction(self) -> Future[AtomTransaction]:
        """
        Start a new write transaction
        :return: the transaction to use
        """

class ObjectId:
    id : int


class Object:
    object_id: ObjectId

    def __init__(self, object_id: ObjectId=None):
        self.object_id = object_id


class ObjectTransaction(ABC):
    @abstractmethod
    def get_object(self, object_id: ObjectId) -> Future[Object]:
        """
        Get an object into memory
        :param object_id:
        :return:
        """


class ObjectWriteTransaction(ObjectTransaction):
    @abstractmethod
    def get_checked_object(self, object_id: ObjectId) -> Future[Object]:
        """
        Get an object into memory. At commit time, it will be checked if this
        object was not modified by another transaction, even if you don't modify it.
        :param object_id:
        :return:
        """

    @abstractmethod
    def commit(self) -> Future:
        """
        Close the transaction and make it persistent. All changes recorded
        Before commit all checked and modified objects will be tested if modified
        by another transaction. Transaction will proceed only if no change in
        used objects is verified.'
        :return:
        """

    @abstractmethod
    def abort(self) -> Future:
        """
        Discard any changes made. Database is not modified.
        :return:
        """


class ObjectDatabase(ABC):
    def __init__(self, atom_storage: AtomStorage):
        self.atom_storage = atom_storage

    @abstractmethod
    def new_read_transaction(self) -> Future[ObjectTransaction]:
        """
        Start a new read transaction
        :return:
        """

    @abstractmethod
    def new_write_transaction(self) -> Future[ObjectWriteTransaction]:
        """
        Start a new write transaction
        :return:
        """

    @abstractmethod
    def new_branch_databse(self) -> Future:
        """
        Gets a new database, derived from the current state of the origin database.
        The derived database could be modified in an idependant history.
        Transactions in the derived database will not impact in the origin database
        :return:
        """


class Database:
    """
    Opens up a database using an URI
    URI:
        <storage_type>://<database_name>

        where:
            <storage_type> could be memory, file, s3

    """
    def __init__(self, uri: str):
        # TODO
        pass






