"""
Common objects


"""
from _ast import Tuple
from concurrent.futures import Future
import uuid
from abc import ABC, abstractmethod
import io

VALIDATION_ERROR = 10_000
USER_ERROR = 20_000
CORRUPTION_ERROR = 30_000
NOT_SUPPORTED_ERROR = 40_000
NOT_AUTHORIZED_ERROR = 50_000
UNEXPECTED_ERROR = 60_000


class ProtoBaseException(Exception):
    """
    Base exception for ProtoBase exceptions
    """

    def __init__(self, code: int=1, exception_type: str=None, message: str=None):
        self.code = code
        self.exception_type = exception_type
        self.message = message


class ProtoUnexpectedException(ProtoBaseException):
    def __init__(self, code: int=UNEXPECTED_ERROR, exception_type: str=None, message: str=None):
        super().__init__(
            code if code else UNEXPECTED_ERROR,
            exception_type if exception_type else 'UnexpectedException',
            message
        )


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


atom_class_registry = dict()

class AtomMetaclass:
    def __init__(self):
        class_name = self.__class__
        if class_name != 'Atom':
            if class_name in atom_class_registry:
                raise ProtoValidationException(
                    message=f'Class repeated in atom class registry ({class_name}). Please check it')
            atom_class_registry[class_name] = self


class Atom(ABC, metaclass=AtomMetaclass):
    atom_pointer: AtomPointer
    transaction: object

    def __init__(self, transaction: object=None, atom_pointer: AtomPointer = None, **kwargs):
        self.transaction = transaction
        self.atom_pointer = atom_pointer
        for name, value in kwargs:
            setattr(self, name, value)


class RootObject(Atom):
    object_root: Atom
    literal_root: Atom


class BlockProvider(ABC):

    @abstractmethod
    def get_new_wal(self) -> tuple(uuid.UUID, int):
        """
        Get a WAL to use.
        It could be an old one, or a new one.

        :return: a tuple with the id of the WAL and the next offset to use
        """

    @abstractmethod
    def get_reader(self, wal_id: uuid.UUID, position: int) -> io.FileIO:
        """
        Get a streamer initialized at position in WAL file
        wal_id

        :param wal_id:
        :param position:
        :return:
        """

    @abstractmethod
    def get_writer_wal(self) -> uuid.UUID:
        """

        :return:
        """

    @abstractmethod
    def write_streamer(self) -> io.FileIO:
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
    def read_current_root(self) -> RootObject:
        """
        Read the current root object
        :return:
        """

    @abstractmethod
    def set_current_root(self, root_pointer: RootObject):
        """
        Set the current root object
        :return:
        """

    @abstractmethod
    def push_atom(self, atom: Atom) -> Future[AtomPointer]:
        """

        :param atom:
        :return:
        """

    @abstractmethod
    def get_atom(self, atom_pointer: AtomPointer) -> Future:
        """

        :param atom:
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






