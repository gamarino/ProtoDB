"""
Basic definitions
"""
from __future__ import annotations
from typing import cast

from concurrent.futures import Future
import uuid
from abc import ABC, abstractmethod, ABCMeta
import io
import configparser
import datetime
from .exceptions import ProtoValidationException

# Constants for storage size units
KB: int  = 1024
MB: int  = KB * KB
GB: int  = KB * MB
PB: int  = KB * GB


class AtomPointer(object):
    def __init__(self, transaction_id: uuid.UUID, offset: int):
        self.transaction_id = transaction_id
        self.offset = offset


atom_class_registry = dict()


class AtomMetaclass(type):
    def __init__(cls, name, bases, class_dict):
        class_name = name
        if class_name != 'Atom':
            if class_name in atom_class_registry:
                raise ProtoValidationException(
                    message=f'Class repeated in atom class registry ({class_name}). Please check it')
            atom_class_registry[class_name] = cls

        # Llamar al __init__ de la metaclase base
        super().__init__(name, bases, class_dict)


class AbstractSharedStorage(ABC):
    """
    ABC to solve forward type definitions
    """

    @abstractmethod
    def push_atom(self, atom: dict) -> Future[AtomPointer]:
        """
        Pushes an atom to the underlying system.

        This method is an abstract method that must be implemented by subclasses.
        The method takes an atom dictionary as input and returns a future representing
        an `AtomPointer`. The implementation details are deferred to the derived classes.

        :param atom: A dictionary representing the atom data to be pushed.
        :return: A future object that resolves to an `AtomPointer`.
        """

    @abstractmethod
    def get_atom(self, atom_pointer: AtomPointer) -> Future[Atom]:
        """
        Fetches an atom using the given AtomPointer and returns a Future that resolves
        to the atom. This method provides an interface for retrieving atoms, which can
        be implemented asynchronously or synchronously depending on the underlying
        implementation.

        This is an abstract method and must be implemented by subclasses, enforcing
        a contract that ensures a consistent pattern for atom retrieval.

        :param atom_pointer: A pointer object that identifies the atom to be retrieved.
        :type atom_pointer: AtomPointer
        :return: A Future object that resolves to the retrieved Atom instance.
        :rtype: Future[Atom]
        """

    @abstractmethod
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

    @abstractmethod
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


class AbstractObjectSpace(ABC):
    """
    ABC to solve forward type definitions
    """
    storage_provider: AbstractSharedStorage

    def __init__(self, storage_provider: AbstractSharedStorage):
        self.storage_provider = storage_provider


class AbstractDatabase(ABC):
    """
    ABC to solve forward type definitions
    """
    object_space: AbstractObjectSpace

    def __init__(self, object_space: AbstractObjectSpace):
        self.object_space = object_space

    @abstractmethod
    def get_literal(self, literal: str):
        """

        :param literal:
        :return:
        """


class AbstractTransaction(ABC):
    """
    ABC to solve forward type definition
    """
    database: AbstractDatabase

    def __init__(self, database: AbstractDatabase):
        self.database = database

    @abstractmethod
    def get_literal(self, string: str) -> Literal:
        """

        :param string:
        :return:
        """

    @abstractmethod
    def set_literal(self, string: str, value: Atom):
        """

        :param string:
        :param value:
        :return:
        """

    @abstractmethod
    def get_mutable(self, key:int) -> Atom:
        """
        Retrieve a mutable object based on the provided key.

        This method retrieves a mutable object associated with the given key. The key
        is an integer identifier for the object. Implementations of this abstract
        method should provide the mutable object corresponding to the key.

        :param key: Identifier for the object to retrieve.
        :type key: int
        :return: The mutable object associated with the provided key.
        """

    @abstractmethod
    def set_mutable(self, key:int, value:Atom):
        """
        Sets the mutable value for the specified key.

        This abstract method is intended to be implemented by a subclass, allowing
        the setting of a mutable value represented by an Atom to the given key.
        The provided implementation should define the behavior for storing or
        associating the value with the key dynamically. Ensure that the input key
        and value comply with the required types.

        :param key: The integer key for which the value will be set.
        :type key: int
        :param value: The Atom instance to be associated with the specified key.
        :type value: Atom
        :return: None
        :rtype: None
        """

# Metaclase combinada: Combina AtomMetaclass y ABCMeta
class CombinedMeta(ABCMeta, AtomMetaclass):
    pass


class Atom(metaclass=CombinedMeta):
    """
    Represents a self-contained unit of data (Atom) that interacts with a database
    through an associated transaction mechanism. Atoms can be saved, loaded, and
    interact with storage providers to persist data in a structured format.

    This class implements a mechanism to serialize and deserialize its attributes
    to and from JSON for storage, and uses the notion of a 'pointer' (AtomPointer)
    to uniquely identify its position in the storage.

    :ivar atom_pointer: Points to the storage location of the atom. Contains
        information like transaction ID and offset.
    :type atom_pointer: AtomPointer
    :ivar _transaction: References the transaction context for operations
        involving this atom. If absent, certain operations like saving
        are not permitted.
    :type _transaction: AbstractTransaction
    :ivar _loaded: Whether the atom's attributes have been loaded from storage.
        False by default.
    :type _loaded: bool
    :ivar _saving: Indicates whether the atom is in the process of being
        saved to prevent recursion loops. Defaults to False.
    :type _saving: bool
    """
    atom_pointer: AtomPointer
    _transaction: AbstractTransaction
    _loaded: bool
    _saving: bool = False

    def __init__(self, transaction: AbstractTransaction = None, atom_pointer: AtomPointer = None, **kwargs):
        self._transaction = transaction
        self.atom_pointer = atom_pointer
        self._loaded = False
        for name, value in self._json_to_dict(kwargs).items():
            setattr(self, name, value)

    def _load(self):
        if not self._loaded:
            if self._transaction:
                if self.atom_pointer.transaction_id and \
                   self.atom_pointer.offset:
                    loaded_atom: Atom = self._transaction.database.object_space.storage_provider.get_atom(
                        self.atom_pointer).result()
                    loaded_dict = self._json_to_dict(loaded_atom.__dict__)
                    for attribute_name, attribute_value in loaded_dict.items():
                        setattr(self, attribute_name, attribute_value)
            self._loaded = True

    def __getattr__(self, name: str):
        if name.startswith('_') or name == 'atom_pointer':
            return super().__getattribute__(name)
        self._load()
        return super().__getattribute__(name)

    def _push_to_storage(self, json_value: dict) -> AtomPointer:
        return self._transaction.database.object_space.storage_provider.push_atom(json_value).result()

    def _json_to_dict(self, json_data: dict) -> dict:
        data = {}

        for name, value in json_data.items():
            if isinstance(value, dict):
                if 'className' == 'datetime.datetime':
                    value = datetime.datetime.fromisoformat(value['iso'])
                elif 'className' == 'datetime.date':
                    value = datetime.date.fromisoformat(value['iso'])
                elif 'className' == 'datetime.timedelta':
                    value = datetime.timedelta(microseconds=value['microseconds'])
                elif 'className' == 'int':
                    value = int(value['value'])
                elif 'className' == 'float':
                    value = float(value['value'])
                elif 'className' == 'bool':
                    value = bool(value['value'])
                elif 'className' == 'None':
                    value = None
            data[name] = value

        return data

    def _dict_to_json(self, data: dict) -> dict:
        json_value = {}

        for name, value in data.items():
            if isinstance(value, Atom):
                if not value._transaction:
                    value._transaction = self._transaction
                value._save()
                json_value[name] = {
                    'className': value.__name__,
                    'transaction_id': value.atom_pointer.transaction_id,
                    'offset': value.atom_pointer.offset,
                }
            elif isinstance(value, str):
                new_literal = self._transaction.get_literal(value)
                setattr(self, name, new_literal)
                new_literal._save()
            elif isinstance(value, datetime.datetime):
                json_value[name] = {
                    'className': 'datetime.datetime',
                    'iso': value.isoformat(),
                }
            elif isinstance(value, datetime.date):
                json_value[name] = {
                    'className': 'datetime.date',
                    'iso': value.isoformat,
                }
            elif isinstance(value, datetime.timedelta):
                json_value[name] = {
                    'className': 'datetime.timedelta',
                    'microseconds': value.microseconds,
                }
            elif isinstance(value, int):
                json_value[name] = {
                    'className': 'int',
                    'value': value,
                }
            elif isinstance(value, float):
                json_value[name] = {
                    'className': 'float',
                    'value': value,
                }
            elif isinstance(value, bool):
                json_value[name] = {
                    'className': 'bool',
                    'value': value,
                }
            elif isinstance(value, bytes):
                bytes_atom = BytesAtom(content=value)
                bytes_atom._save()
                json_value[name] = {
                    'className': 'BytesAtom',
                    'transaction_id': bytes_atom.atom_pointer.transaction_id,
                    'offset': bytes_atom.atom_pointer.offset,
                }
            elif value == None:
                json_value[name] = {
                    'className': 'None',
                }
        return json_value

    def _save(self):
        if not self.atom_pointer and not self._saving:
            # It's a new object

            if self._transaction:
                # Push the object tree downhill, avoiding recursion loops
                # converting attributes strs to Literals
                self._saving = True

                for name, value in self.__dict__.items():
                    if isinstance(value, Atom):
                        if not value._transaction:
                            value._transaction = self._transaction
                        value._save()

                json_value = {'AtomClass': self.__class__.__name__}

                for name, value in self.__dict__.items():
                    if name.startswith('_'):
                        continue
                    json_value[name] = value

                json_value = self._dict_to_json(json_value)

                # At this point all attributes has been flushed to storage if they are newly created
                # All attributes has valid AtomPointer values (either old or new)
                pointer = self._push_to_storage(json_value)
                self.atom_pointer = AtomPointer(pointer.transaction_id, pointer.offset)
            else:
                raise ProtoValidationException(
                    message=f'An DBObject can only be saved within a given transaction!'
                )

    def hash(self) -> int:
        return self.atom_pointer.transaction_id.int ^ \
            self.atom_pointer.offset


class RootObject(Atom):
    """
    Represents the root object in a data structure.

    This class serves as the root element for a hierarchical or structured data
    representation. It provides access to basic components and operations
    to manage and utilize the data structure effectively. `RootObject` inherits
    from `Atom`, enabling integration with its core functionalities and properties.

    :ivar object_root: The primary root object representing structured data.
    :type object_root: Atom
    :ivar literal_root: An auxiliary root object used for managing literals
        within the data structure.
    :type literal_root: Atom
    """
    object_root: Atom
    literal_root: Atom


class BlockProvider(ABC):
    """
    An abstract base class that defines the interface for a block-based storage provider.

    This class serves as a blueprint for managing Write-Ahead Logs (WALs) and root objects in a
    block-related storage system. It provides abstract methods for obtaining and writing data to WALs,
    retrieving the root object, and managing data durability by closing WALs and the provider. Concrete
    implementations of this class must provide functionality for these operations as outlined in the
    specifications of the abstract methods.

    """
    @abstractmethod
    def get_config_data(self) -> configparser.ConfigParser:
        """
        Get config data
        :return:
        """

    @abstractmethod
    def get_new_wal(self) -> tuple[uuid.UUID, int]:
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
        Provides an abstract method that should be implemented by subclasses to retrieve the
        unique identifier (UUID) of the writer's Write-Ahead Log (WAL). This UUID is used to
        identify the WAL instance associated with the writer for consistency and tracking purposes.

        :raises NotImplementedError: This method must be implemented in a subclass.
        :return: The UUID of the writer's WAL.
        :rtype: uuid.UUID
        """

    @abstractmethod
    def write_streamer(self, wal_id: uuid.UUID) -> io.FileIO:
        """
        This abstract method must be implemented to handle the writing of a streaming
        process for a given WAL (Write-Ahead Log) identifier. It is responsible for
        generating and returning a writable file-like object, intended for downstream
        operations that require data persistence or streaming output based on the
        specified WAL ID.

        :param wal_id: The unique identifier (UUID) of the Write-Ahead Log (WAL) to be
                       streamed.
        :type wal_id: uuid.UUID
        :return: A writable file-like object for handling the streaming operations
                 associated with the given WAL ID.
        :rtype: io.FileIO
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
    def close_wal(self, transaction_id: uuid.UUID):
        """
        Close a previous WAL. Flush any pending data. Make all changes durable
        :return:
        """

    @abstractmethod
    def close(self):
        """
        Close the operation of the block provider. Flush any pending data to WAL. Make all changes durable
        No further operations are allowed
        :return:
        """


class SharedStorage(AbstractSharedStorage):
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
    def read_current_root(self) -> RootObject:
        """
        Read the current root object
        :return:
        """

    @abstractmethod
    def read_lock_current_root(self) -> RootObject:
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
    def flush_wal(self):
        """
        Function to be called periodically (eg 2 minutes) to ensure no pending writes to WAL
        Additionally it is assumed that previously set_current_root, so new objects created
        before that all are included in flushed data
        This will not add any delay to operations performed after the root update, that could
        or could not be part of the flushed data.
        :return:
        """


class DBObject(Atom):
    """
    Represents a database object that provides dynamic attribute loading and immutability.

    DBObject is designed to interact with protobase-based database systems. It features
    dynamic attribute loading, where unknown attributes are resolved during runtime, and
    enforces immutability by restricting direct attribute modifications. Instead, any modifications
    result in the creation of a new instance with updated attributes.

    If you try not access a not existing attribute, DBObjects will thow no error, instead a None
    value will be returned.

    :ivar _loaded: Indicates whether the object's attributes have been fully loaded.
    :type _loaded: bool
    """

    def __init__(self,
                 transaction: AbstractTransaction = None,
                 atom_pointer: AtomPointer = None,
                 offset: int = 0,
                 **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self._loaded = False

    def __getattr__(self, name: str):
        self._load()
        if hasattr(self, name):
            return getattr(super(), name)
        return None

    def __setattr__(self, key, value):
        self._load()
        if hasattr(self, key):
            super().__setattr__(key, value)
        else:
            raise ProtoValidationException(
                message=f'ProtoBase DBObjects are inmutable! Your are trying to set attribute {key}'
            )

    def _setattr(self, name:str, value: object) ->DBObject:
        new_object = DBObject()
        for name, value in self.__dict__:
            setattr(new_object, name, value)
        setattr(new_object, name, value)
        return new_object


class MutableObject(DBObject):
    """
    Represents a mutable object that inherits from DBObject and is used within the context
    of a transaction. The purpose of this class is to provide a means for interacting with
    mutable states in a database-like system, ensuring that operations are performed within
    a valid transaction scope. The class supports attribute access, modification, and
    validation while maintaining a unique hash key for identity.

    This class enforces the rule that the mutable object must always work within the scope
    of a transaction, throwing exceptions otherwise. It includes mechanisms to retrieve
    and modify attributes dynamically and methods for serialization and deserialization
    (_load and _save). It also assigns either a user-defined hash key or a newly generated
    unique identifier.

    :ivar hash_key: Unique key identifying the mutable object. This key can either be
        provided during initialization or generated if not supplied.
    :type hash_key: int
    """
    hash_key: int = 0

    def __init__(self,
                 hash_key: int = 0,
                 transaction: AbstractTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        if hash_key:
            self.hash_key = hash_key
        else:
            self.hash_key = uuid.uuid4().int

    def __getattr__(self, name: str):
        if not self._transaction:
            raise ProtoValidationException(
                message=f"You can't access a mutable object out of the scope of a transaction!"
            )

        current_object = self.transaction.get_mutable(self.hash_key)
        return current_object.__getattr__(name)

    def __setattr__(self, key, value):
        if not self._transaction:
            raise ProtoValidationException(
                message=f"You can't access a mutable object out of the scope of a transaction!"
            )

        current_object = cast(DBObject, self._transaction.get_mutable(self.hash_key))
        new_object = current_object._setattr(key, value)
        self._transaction.set_mutable(self.hash_key, new_object)

    def __hasattr__(self, name: str):
        if not self._transaction:
            raise ProtoValidationException(
                message=f"You can't access a mutable object out of the scope of a transaction!"
            )

        current_object = self.transaction.get_mutable(self.hash_key)
        return hasattr(current_object, name)

    def _load(self):
        pass

    def _save(self):
        pass

    def hash(self) -> int:
        return self.hash()


class DBCollections(Atom):
    """
    DBCollections provides an abstraction layer for database collections.

    This class serves as a base class for specific database collections, containing common
    functionality such as managing indexes and abstract methods for data representation and
    query planning.

    :ivar indexes: A dictionary mapping index names (str) to Atom objects, representing
        the indexes associated with this collection.
    :type indexes: dict[str, Atom] | None
    :ivar count: The total number of items in the collection.
    :type count: int
    """
    indexes: dict[str, Atom] | None
    count: int = 0

    def __init__(self,
                 indexes: dict[str, Atom] | None = None,
                 transaction: AbstractTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.indexes = indexes if indexes else {}

    @abstractmethod
    def as_iterable(self) -> list[object]:
        """

        :return:
        """

    @abstractmethod
    def as_query_plan(self) -> QueryPlan:
        """
        Get a query plan based on this collection
        :return:
        """


class QueryPlan(Atom):
    """
    Maintains the structure and logic for a query execution plan.

    This class serves as a blueprint for creating and managing query execution
    plans. It is designed to abstract the process of execution and optimization
    of queries in systems, enabling extension for specific use cases or query types.
    Being an abstract class, it defines the required methods that subclasses must
    implement for their respective functionality.

    :ivar based_on: The base query plan that this instance derives from or is built upon.
    :type based_on: QueryPlan
    """
    based_on: QueryPlan

    def __init__(self,
                 based_on: QueryPlan = None,
                 transaction: AbstractTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.based_on = based_on

    @abstractmethod
    def execute(self) -> list:
        """

        :return:
        """

    @abstractmethod
    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        """

        :return:
        """


class Literal(Atom):
    """
    Represents a Literal, which is an extension of the Atom type.

    This class is designed to store and manage a literal value. It provides
    methods for equality comparison, string representation, and concatenation.
    The class is initialized with a literal string, and provides additional
    support for managing this string through overloaded operators. The primary
    use of this class is for handling and encapsulating a literal string value
    that can be utilized in various string operations and comparisons.

    :ivar string: The underlying literal string value.
    :type string: str
    """
    string: str

    def __init__(self,
                 literal: str = None,
                 transaction: AbstractTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.string = literal or kwargs.pop('literal')

    def __eq__(self, other: str | Literal) -> bool:
        if isinstance(other, Literal):
            return self.string == other.string
        else:
            return self.string == other

    def __str__(self) -> str:
        return self.string

    def __add__(self, other: str | Literal) -> Literal:
        if isinstance(other, Literal):
            return Literal(literal=self.string + other.string)
        else:
            return Literal(literal=self.string + other)


class BytesAtom(Atom):
    """
    Represents a specialized type of Atom that holds content in a bytes-like or string format, along with
    associated metadata like filename and MIME type.

    This class encapsulates data in a manner that allows for content manipulation and provides
    support for operability such as addition of byte-based content. The content is stored in a base64
    encoded format for consistency.

    :ivar filename: Specifies the name of the file associated with the atom.
    :type filename: str
    :ivar mimetype: The MIME type associated with the file content (e.g., "text/plain").
    :type mimetype: str
    :ivar content: Encoded string representation of the content held by this instance.
    :type content: str
    """
    filename: str
    mimetype: str
    content: bytes

    def __init__(self,
                 filename: str = None,
                 mimetype: str = None,
                 content: bytes = None,
                 transaction: AbstractTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.filename = filename
        self.mimetype = mimetype

        if not isinstance(content, bytes):
            raise ProtoValidationException(
                message=f"It's not possible to create a BytesAtom with {type(content)}!"
            )
        self.content = content

    def __str__(self) -> str:
        return f'BytesAtom with {len(self.content) if self.content else 0 } byte(s)'

    def __eq__(self, other: BytesAtom) -> bool:
        if isinstance(other, BytesAtom):
            if self.atom_pointer and other.atom_pointer:
                return self.atom_pointer == other.atom_pointer
            elif self.atom_pointer and isinstance(other, bytes):
                self._load()
                if self.content == other:
                    return True
        return False

    def __add__(self, other: bytes | BytesAtom) -> BytesAtom:
        raise ProtoValidationException(
            message=f'It is not possible to extend BytesAtom using "+"!'
        )

    def _add(self, other: bytes | BytesAtom) -> BytesAtom:
        if isinstance(other, BytesAtom):
            self._load()
            other._load()
            return BytesAtom(content=self.content + other.content)
        elif isinstance(other, bytes):
            self._load()
            return BytesAtom(content=self.content + other)
        else:
            raise ProtoValidationException(
                message=f"It's not possible to extend BytesAtom with {type(other)}!"
            )

    def _load(self):
        if not self._loaded:
            if self._transaction:
                if self.atom_pointer.transaction_id and \
                   self.atom_pointer.offset:
                    loaded_content = self._transaction.database.object_space.storage_provider.get_bytes(
                        self.atom_pointer).result()
                    self.content = loaded_content
            self._loaded = True

    def _save(self):
        if not self.atom_pointer and not self._saving:
            # It's a new object

            if self._transaction:
                # Push the object tree downhill, avoiding recursion loops
                # converting attributes strs to Literals
                self._saving = True

                # At this point all attributes has been flushed to storage if they are newly created
                # All attributes has valid AtomPointer values (either old or new)
                pointer = self._push_bytes(self.content)
                self.atom_pointer = AtomPointer(pointer.transaction_id, pointer.offset)
            else:
                raise ProtoValidationException(
                    message=f'An DBObject can only be saved within a given transaction!'
                )
