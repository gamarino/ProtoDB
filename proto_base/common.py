"""
Common objects


"""
from __future__ import annotations
from typing import cast

from concurrent.futures import Future
import uuid
from abc import ABC, abstractmethod
import io
import configparser
import datetime
import base64
from .exceptions import ProtoValidationException, ProtoCorruptionException


# Constants for storage size units
KB = 1024
MB = KB * KB
GB = KB * MB
PB = KB * GB



class AtomPointer(object):
    def __init__(self, transaction_id: uuid.UUID, offset: int):
        self.transaction_id = transaction_id
        self.offset = offset


atom_class_registry = dict()

class AtomMetaclass:
    def __init__(cls, name, bases, class_dict):
        class_name = name
        if class_name != 'Atom':
            if class_name in atom_class_registry:
                raise ProtoValidationException(
                    message=f'Class repeated in atom class registry ({class_name}). Please check it')
            atom_class_registry[class_name] = cls


class AbstractSharedStorage(ABC):
    """
    ABC to solve forward type definitions
    """

    @abstractmethod
    def push_atom(self, atom: dict) -> Future[AtomPointer]:
        """

        :param atom:
        :return:
        """

    @abstractmethod
    def get_atom(self, atom_pointer: AtomPointer) -> Future[Atom]:
        """

        :param atom_pointer:
        :return:
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
    def get_literal(self, string: str):
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


class Atom(metaclass=AtomMetaclass):
    atom_pointer: AtomPointer
    _transaction: AbstractTransaction
    _loaded: bool
    _saving: bool = False

    def __init__(self, transaction: AbstractTransaction=None, atom_pointer: AtomPointer = None, **kwargs):
        self._transaction = transaction
        self.atom_pointer = atom_pointer
        self._loaded = False
        for name, value in self._json_to_dict(kwargs).items():
            setattr(self, name, value)

    def _load(self):
        if not self._loaded:
            if not self._transaction:
                raise ProtoValidationException(
                    message=f'An DBObject can only be instanciated within a given transaction!'
                )

            if self._transaction and \
               self.atom_pointer.transaction_id and \
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
        return getattr(self, name)

    def __setattr__(self, key, value):
        if key.startswith('_'):
            super().__setattr__(key, value)
        raise ProtoValidationException(
            message=f'Atoms are inmutable objects! Your are trying to set attribute {key}'
        )

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
                    'className': 'AtomPointer',
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
    object_root: Atom
    literal_root: Atom


class BlockProvider(ABC):
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

        :return:
        """

    @abstractmethod
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


class AbstractDBObject(Atom):
    """
    ABC to solve forward definition
    """
    _attributes: dict[str, Atom]

    def __init__(self, transaction_id: uuid.UUID = None, offset: int = 0, attributes: dict[str, Atom] = None):
        super().__init__(transaction_id=transaction_id, offset=offset)
        self._attributes = attributes


class ParentLink(Atom):
    parent_link: AbstractDBObject | None
    cls: AbstractDBObject | None


class DBObject(Atom, AbstractDBObject):
    parent_link: ParentLink | None

    def __init__(self,
                 transaction_id: uuid.UUID=None,
                 offset:int = 0,
                 parent_link: ParentLink=None,
                 attributes: dict[str, Atom | int | float | None | datetime.datetime | datetime.date |
                                       datetime.timedelta | bool | bytes | str]=None,
                 **kwargs):
        if attributes:
            self._attributes = attributes
        super().__init__(transaction_id=transaction_id, offset=offset, attributes=attributes)
        self._parent_link = parent_link or kwargs.pop('parent_link')
        self._loaded = False

        self._attributes = {}
        if '_attributes' in kwargs:
            for attribute_name, attribute_value in kwargs['_attributes'].items():
                if attribute_name.startswith('_'):
                    raise ProtoCorruptionException(
                        message=f'DBObject attribute names could not start with "_" ({attribute_name}')

                if isinstance(attribute_value, dict) and 'AtomClass' in attribute_value:
                    if not attribute_value['AtomClass'] in atom_class_registry:
                        raise ProtoCorruptionException(
                            message=f"AtomClass {attribute_value['AtomClass']} unknown!")

                    self._attributes[attribute_name] = atom_class_registry[attribute_value['AtomClass']](
                        transaction_id=attribute_value['transaction_id'],
                        offset=attribute_value['offset'],
                    )
                else:
                    self._attributes[attribute_name] = attribute_value

    def _push_to_storage(self, json_value: dict):
        json_value['_attributes'] = {}
        for attribute_name, attribute_value in self._attributes.items():
            json_value['_attributes'][attribute_name] = attribute_value

        return super()._push_to_storage(json_value)

    def _json_to_dict(self, json_value:dict) -> dict:
        data = super()._json_to_dict(json_value)
        if '_attributes' in data:
            data['_attributes'] = super()._json_to_dict(data['_attributes'])
        return data

    def __getattr__(self, name: str):
        self._load()

        if name.startswith('_'):
            return getattr(super(), name)

        if name in self._attributes:
            return self._attributes[name]

        pl = self._parent_link
        while pl:
            if name in pl.cls._attributes:
                return pl.cls._attributes[name]
            pl = pl.parent_link

        if hasattr(self, name):
            return getattr(super(), name)

        return None

    def __setattr__(self, key, value):
        if hasattr(self, key):
            super().__setattr__(key, value)
        else:
            raise ProtoValidationException(
                message=f'ProtoBase DBObjects are inmutable! Your are trying to set attribute {key}'
            )

    def _hasattr(self, name: str):
        self._load()

        if name.startswith('_'):
            return hasattr(self, name)

        if name in self._attributes:
            return True

        pl = self.parent_link
        while pl:
            if name in pl.cls._attributes:
                return True
            pl = pl.parent_link

        return False

    def _setattr(self, name: str, value):
        self._load()

        if name.startswith('_'):
            super().__setattr__(name, value)
            return self
        else:
            attr =  self.attributes
            attr[name] = value
            return DBObject(
                object_id=self.object_id,
                transaction_id=self.transaction_id,
                parent_link=attr,
                offset=self.offset,
            )

    def _add_parent(self, new_parent: Atom):
        self._load()

        new_parent_link = ParentLink(parent_link=self._parent_link, cls=new_parent)
        return DBObject(attributes=self._attributes, parent_link=new_parent_link)


class MutableObject(DBObject):
    """

    """
    hash_key: int = 0

    def __init__(self,
                 transaction: AbstractTransaction=None,
                 atom_pointer: AtomPointer = None,
                 **kwargs: dict[str, Atom]):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        if kwargs and 'hash_key' in kwargs:
            self.hash_key = cast(int, kwargs['hash_key'])
        else:
            self.hash_key = uuid.uuid4().int

    def __getattr__(self, name: str):
        if not self.transaction:
            raise ProtoValidationException(
                message=f"You can't access a mutable object out of the scope of a transaction!"
            )

        current_object = self.transaction.get_mutable(self.hash_key)
        return current_object.__getattr__(name)

    def __setattr__(self, key, value):
        if hasattr(self, key):
            super().__setattr__(key, value)
        else:
            if not self._transaction:
                raise ProtoValidationException(
                    message=f'Proto MutableObjects can only be modified within the context of a transaction!'
                )
            current_object = cast(DBObject, self._transaction.get_mutable(self.hash_key))
            new_object = current_object._setattr(key, value)
            self._transaction.set_mutable(self.hash_key, new_object)

    def _hasattr(self, name: str):
        if not self.transaction:
            raise ProtoValidationException(
                message=f"You can't access a mutable object out of the scope of a transaction!"
            )

        current_object = self.transaction.get_mutable(self.hash_key)
        return current_object.hasattr(name)

    def _setattr(self, name: str, value):
        if not self.transaction:
            raise ProtoValidationException(
                message=f"You can't access a mutable object out of the scope of a transaction!"
            )

        current_object = self.transaction.get_mutable(self.hash_key)
        new_object = current_object._setattr(name, value)
        self.transaction.set_mutable(self.hash_key, new_object)
        return self

    def _add_parent(self, new_parent: Atom):
        if not self.transaction:
            raise ProtoValidationException(
                message=f"You can't access a mutable object out of the scope of a transaction!"
            )

        current_object = self.transaction.get_mutable(self.hash_key)
        new_object = current_object._add_parent(new_parent)
        self._transaction.set_mutable(self.hash_key, new_object)
        return self

    def _load(self):
        pass

    def _save(self):
        pass

    def hash(self) -> int:
        return self.hash_key


class DBCollections(Atom):
    indexes: dict[str, Atom] | None
    count: int = 0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.indexes = {}

    @abstractmethod
    def as_iterable(self) -> list[Atom]:
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
    based_on: QueryPlan

    @abstractmethod
    def execute(self) -> DBCollections:
        """

        :return:
        """

    @abstractmethod
    def optimize(self, full_plan: QueryPlan) -> QueryPlan:
        """

        :return:
        """


class Literal(Atom):
    string:str

    def __init__(self,
                 transaction_id: uuid.UUID=None,
                 offset:int = 0,
                 literal: str = None,
                 **kwargs):
        super().__init__(transaction_id=transaction_id, offset=offset)
        self.string = literal or kwargs.pop('literal')

    def __eq__(self, other:str | Literal) -> bool:
        if isinstance(other, Literal):
            return self.string == other.string
        else:
            return self.string == other

    def __str__(self) -> str:
        return self.string

    def __add__(self, other:str | Literal) -> Literal :
        if isinstance(other, Literal):
            return Literal(literal=self.string + other.string)
        else:
            return Literal(literal=self.string + other)


class BytesAtom(Atom):
    filename: str
    mimetype: str
    content: str

    def __init__(self,
                 transaction_id: uuid.UUID = None,
                 offset: int = 0,
                 filename: str = None,
                 mimetype: str = None,
                 content: str | bytes = None,
                 **kwargs):
        super().__init__(transaction_id=transaction_id, offset=offset)

        self.filename = filename
        self.mimetype = mimetype

        if isinstance(content, bytes):
            self.content = base64.b64encode(content).decode('UTF-8')
        elif isinstance(content, str):
            self.content = content or kwargs.pop('content')
        else:
            raise ProtoValidationException(
                message=f'It is not possible to create a BytesAtom with {type(content)}!'
            )

    def __str__(self) -> str:
        return self.content

    def __eq__(self, other: BytesAtom) -> bool:
        if isinstance(other, BytesAtom):
            return self.transaction_id == other.transaction_id and \
                   self.offset == other.offset
        else:
            return False

    def __add__(self, other: bytes | BytesAtom) -> BytesAtom:
        if isinstance(other, BytesAtom):
            return BytesAtom(
                content=base64.b64encode(
                    base64.b64decode(self.content) + base64.b64decode(other.content)).decode('UTF-8'),
            )
        elif isinstance(other, bytes):
            return BytesAtom(
                content=base64.b64encode(base64.b64decode(self.content) + other).decode('UTF-8'),
            )
        raise ProtoValidationException(
            message=f'It is not possible to extend BytesAtom with {type(other)}!'
        )
