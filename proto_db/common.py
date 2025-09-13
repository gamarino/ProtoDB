"""
Basic definitions
"""
from __future__ import annotations

import configparser
import datetime
import io
import uuid
from abc import ABC, abstractmethod, ABCMeta
from concurrent.futures import Future
from typing import cast, BinaryIO, TYPE_CHECKING

from .exceptions import ProtoValidationException, ProtoCorruptionException

if TYPE_CHECKING:
    # Only for type checking to avoid circular imports at runtime
    from .dictionaries import Dictionary

# Constants for storage size units
KB: int = 1024
MB: int = KB * KB
GB: int = KB * MB
PB: int = KB * GB


class AtomPointer(object):
    def __init__(self, transaction_id: uuid.UUID = None, offset: int = 0):
        # Allow default construction for tests and convenience
        if transaction_id is None:
            transaction_id = uuid.uuid4()
        self.transaction_id = transaction_id
        self.offset = offset

    def hash(self):
        return self.transaction_id.int ^ self.offset


atom_class_registry = dict()


class AtomMetaclass(type):
    def __init__(cls, name, bases, class_dict):
        class_name = name
        if class_name != 'Atom':
            if class_name in atom_class_registry:
                raise ProtoValidationException(
                    message=f'Class repeated in atom class registry ({class_name}). Please check it')
            atom_class_registry[class_name] = cls

        # Call the __init__ method of the base metaclass
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
    database_name: str

    def __init__(self, object_space: AbstractObjectSpace):
        self.object_space = object_space


class AbstractTransaction(ABC):
    """
    ABC to solve forward type definition
    """

    def __init__(self):
        self.storage_provider = None

    @abstractmethod
    def read_object(self, class_name: str, atom_pointer: AtomPointer) -> Atom:
        """
        Get an unloaded Atom based on atom_pointer, of a given Atom class.

        This method creates an Atom instance with the specified class and pointer,
        but does not load data from the database. If the same Atom was already read 
        in this transaction, it returns the same object instance to ensure all references 
        within this transaction receive exactly the same object. With this strategy,
        any comparison can use object memory addresses to check for identity.

        :param class_name: The name of the Atom class to instantiate.
        :param atom_pointer: The pointer to the atom's location in storage.
        :return: An unloaded Atom instance of the specified class.
        """

    @abstractmethod
    def get_literal(self, string: str) -> Literal:
        """
        Retrieves or creates a Literal object for the given string.

        This method ensures that string literals are properly managed within the
        transaction context. It may return an existing Literal if the string has
        already been processed in this transaction, or create a new one if needed.

        :param string: The string value to be represented as a Literal.
        :return: A Literal object representing the provided string.
        """

    @abstractmethod
    def get_mutable(self, key: int) -> Atom:
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
    def set_mutable(self, key: int, value: Atom):
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
        """

    @abstractmethod
    def set_locked_object(self, mutable_index: int, current_atom: Atom):
        """
        Sets the locked object in the system. This method is abstract and should
        be implemented by subclasses to provide specific behavior for locking
        functionality. By invoking this method, the system expects the specified
        atom to become associated with the given mutable index.

        :param mutable_index: The index in the mutable collection to lock.
        :type mutable_index: int
        :param current_atom: The Atom object to associate with the locked
            mutable index.
        :type current_atom: Atom
        :return: None.
        :rtype: None
        """

    @abstractmethod
    def get_mutable(self, obj_id, obj_type):
        pass

    @abstractmethod
    def set_mutable(self, obj, obj_type):
        pass


class ConcurrentOptimized:
    """
    A mixin class for objects that can handle concurrent modifications
    by rebasing their changes on top of a more recent version of the object.
    """

    def _rebase_on_concurrent_update(self, current_db_object: 'Atom') -> 'Atom':
        """
        This method is called on the new object (from the current transaction)
        when a concurrent modification is detected on the original object.

        It should try to apply the changes made in the current transaction
        on top of the 'current_db_object'.

        :param current_db_object: The object state as it is in the database at commit time.
        :return: A new object with the changes from this transaction merged
                 into the current_db_object.
        :raises: ProtoNotSupportedException if the merge is not possible.
        """
        raise NotImplementedError("This object does not implement concurrent merge logic.")


# Combined metaclass: Combines AtomMetaclass and ABCMeta
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
    :ivar transaction: References the transaction context for operations
        involving this atom. If absent, certain operations like saving
        are not permitted.
    :type transaction: AbstractTransaction
    :ivar _loaded: Whether the atom's attributes have been loaded from storage.
        False by default.
    :type _loaded: bool
    :ivar _saved: Indicates whether the atom is in the process of being
        saved to prevent recursion loops. Defaults to False.
    :type _saving: bool
    """
    atom_pointer: AtomPointer
    transaction: AbstractTransaction
    _loaded: bool = False
    _saved: bool = False

    def __init__(self, transaction: AbstractTransaction = None, atom_pointer: AtomPointer = None, **kwargs):
        super().__init__()
        self.transaction = transaction
        self.atom_pointer = atom_pointer
        # Accept arbitrary attributes for simple Atom usage in tests (e.g., Atom(value="..."))
        for k, v in kwargs.items():
            try:
                object.__setattr__(self, k, v)
            except Exception:
                pass

    def _load(self):
        # Use direct attribute access to avoid recursion through __getattr__
        if not getattr(self, '_loaded', False):
            # Use direct dictionary access to avoid triggering __getattr__
            if 'transaction' in self.__dict__ and self.__dict__['transaction']:
                transaction = self.__dict__['transaction']
                # Normalize atom_pointer to AtomPointer if it is present as a raw dict (backward compatibility)
                ap = self.__dict__.get('atom_pointer')
                if isinstance(ap, dict):
                    try:
                        tid_val = ap.get('transaction_id')
                        txid = None
                        if isinstance(tid_val, uuid.UUID):
                            txid = tid_val
                        elif isinstance(tid_val, str):
                            try:
                                txid = uuid.UUID(tid_val)
                            except Exception:
                                txid = uuid.UUID(hex=tid_val)
                        if txid is not None:
                            ap = AtomPointer(transaction_id=txid, offset=ap.get('offset', 0))
                            self.__dict__['atom_pointer'] = ap
                    except Exception:
                        # If normalization fails, leave as-is and skip loading
                        ap = None
                if ap and getattr(ap, 'transaction_id', None):
                    atom_pointer = ap
                    loaded_atom = transaction.storage.get_atom(atom_pointer).result()
                    loaded_dict = self._json_to_dict(loaded_atom)
                    for attribute_name, attribute_value in loaded_dict.items():
                        # Use object.__setattr__ to bypass potential recursion in __setattr__
                        object.__setattr__(self, attribute_name, attribute_value)
                        if isinstance(attribute_value, Atom):
                            object.__setattr__(attribute_value, 'transaction', transaction)

            # Use object.__setattr__ to bypass potential recursion in __setattr__
            object.__setattr__(self, '_loaded', True)
            self.after_load()

    def after_load(self):
        """
        Perform any additional operations after the object is loaded in memory from storage.
        Loading will restore all fields whose name do not start with '_', or are callable.
        If for a particular Atom some internal state should be restored, this is the right
        place to do that.

        This method will be called just once per transaction.

        This method is intended to be a hook for executing custom logic once an
        object instance has been fully loaded or initialized. Any specific routines,
        validations, or updates required post-loading should be implemented here.

        :return: None
        :rtype: None
        """
        pass

    def __eq__(self, other):
        if isinstance(other, Atom):
            if self.atom_pointer and \
                    other.atom_pointer and \
                    self.atom_pointer == other.atom_pointer:
                return True
            elif self.atom_pointer and other.atom_pointer:
                return self.atom_pointer.transaction_id == other.atom_pointer.transaction_id and \
                    self.atom_pointer.offset == other.atom_pointer.offset
            else:
                return self is other
        else:
            return False

    def _push_to_storage(self, json_value: dict) -> AtomPointer:
        return self.transaction.storage.push_atom(json_value).result()

    def _json_to_dict(self, json_data: dict) -> dict:
        data = {}

        for name, value in json_data.items():
            if isinstance(value, dict) and 'className' in value:
                class_name = value['className']
                if class_name == 'datetime.datetime':
                    value = datetime.datetime.fromisoformat(value['iso'])
                elif class_name == 'datetime.date':
                    value = datetime.date.fromisoformat(value['iso'])
                elif class_name == 'datetime.timedelta':
                    value = datetime.timedelta(microseconds=value['microseconds'])
                elif class_name == 'int':
                    value = int(value['value'])
                elif class_name == 'float':
                    value = float(value['value'])
                elif class_name == 'bool':
                    value = bool(value['value'])
                elif class_name == 'None':
                    value = None
                elif class_name == 'Literal':
                    if 'transaction_id' in value:
                        value = Literal(
                            atom_pointer=AtomPointer(
                                transaction_id=uuid.UUID(value['transaction_id']),
                                offset=value['offset']
                            ),
                            transaction=self.transaction
                        )
                        value._load()
                    else:
                        value = self.transaction.get_literal(value['string'])
                elif class_name in atom_class_registry:
                    # Accept hyphenated UUID strings and raw hex
                    tx_str = value['transaction_id']
                    try:
                        txid = uuid.UUID(tx_str)
                    except Exception:
                        txid = uuid.UUID(hex=tx_str)
                    atom_pointer = AtomPointer(
                        txid,
                        value['offset']
                    )
                    value = self.transaction.read_object(class_name, atom_pointer)
                    # value._load()
                else:
                    raise ProtoValidationException(
                        message=f'It is not possible to load Atom of class {class_name}!'
                    )
            data[name] = value

        return data

    def _dict_to_json(self, data: dict) -> dict:
        json_value = {}
        from . import db_access

        for name, value in data.items():
            if isinstance(value, Atom):
                if not value.transaction:
                    value.transaction = self.transaction
                value._save()
                json_value[name] = {
                    'className': type(value).__name__,
                    'transaction_id': str(value.atom_pointer.transaction_id),
                    'offset': value.atom_pointer.offset,
                }
            elif isinstance(value, str):
                json_value[name] = value
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
                json_value[name] = value
            elif isinstance(value, float):
                json_value[name] = value
            elif isinstance(value, bool):
                json_value[name] = value
            elif isinstance(value, bytes):
                bytes_atom = db_access.BytesAtom(content=value)
                bytes_atom._save()
                json_value[name] = {
                    'className': 'BytesAtom',
                    'transaction_id': bytes_atom.atom_pointer.transaction_id,
                    'offset': bytes_atom.atom_pointer.offset,
                }
            elif value is None:
                # There is no need to store None values, it is the default for newly created atoms
                continue
            else:
                json_value[name] = value

        return json_value

    def _save(self):
        self._load()
        # Use direct attribute access to avoid recursion
        if 'atom_pointer' not in self.__dict__ or not self.__dict__['atom_pointer']:
            if '_saved' not in self.__dict__ or not self.__dict__['_saved']:
                # It's a new object

                if 'transaction' in self.__dict__ and self.__dict__['transaction']:
                    # Push the object tree downhill, avoiding recursion loops
                    # converting attributes strs to Literals
                    object.__setattr__(self, '_saved', True)

                    if isinstance(self, Literal) and not self.atom_pointer:
                        json_value = {
                            'className': 'Literal',
                            'string': self.string
                        }
                    else:
                        json_value = {'className': type(self).__name__}

                        for name, value in self.__dict__.items():
                            if callable(value) or name.startswith('_'):
                                continue

                            if isinstance(self, Atom) and name in ('atom_pointer', 'transaction'):
                                continue

                            if isinstance(value, Atom):
                                if 'transaction' not in value.__dict__ or not value.__dict__['transaction']:
                                    # it is a newly created atom
                                    # it should not happen, but try to solve the misbehaviour
                                    # capture it for this transaction
                                    object.__setattr__(value, 'transaction', self.__dict__['transaction'])
                                    object.__setattr__(value, '_saved', False)
                                if '_saved' not in value.__dict__ or not value.__dict__['_saved']:
                                    object.__setattr__(value, 'transaction', self.__dict__['transaction'])
                                value._save()

                                json_value[name] = {
                                    'className': type(value).__name__,
                                    'transaction_id': str(value.__dict__['atom_pointer'].transaction_id),
                                    'offset': value.__dict__['atom_pointer'].offset
                                }

                            elif isinstance(value, str):
                                literal = self.transaction.get_literal(value)
                                if not literal.atom_pointer:
                                    self.transaction._update_created_literals(
                                        self.transaction,
                                        self.transaction.new_literals
                                    )
                                if not literal.atom_pointer:
                                    raise ProtoCorruptionException(
                                        message="Corruption saving string as literal!"
                                    )

                                json_value[name] = {
                                    'className': type(literal).__name__,
                                    'transaction_id': str(literal.__dict__['atom_pointer'].transaction_id),
                                    'offset': literal.__dict__['atom_pointer'].offset
                                }

                            else:
                                json_value[name] = value

                        json_value = self._dict_to_json(json_value)

                    # At this point all attributes has been flushed to storage if they are newly created
                    # All attributes has valid AtomPointer values (either old or new)
                    pointer = self._push_to_storage(json_value)
                    object.__setattr__(self, 'atom_pointer', AtomPointer(pointer.transaction_id, pointer.offset))

                    # Write-through caching: populate caches immediately after persisting
                    try:
                        tr = getattr(self, 'transaction', None)
                        bundle = getattr(tr, 'atom_cache_bundle', None) if tr else None
                        if bundle:
                            # Serialize to bytes to estimate size and populate bytes cache
                            atom_bytes = None
                            # Prefer a storage-provided serializer if available
                            storage = getattr(tr, 'storage', None) if tr else None
                            serializer = getattr(storage, 'serializer', None)
                            if serializer and hasattr(serializer, 'serialize'):
                                try:
                                    atom_bytes = serializer.serialize(json_value)
                                except Exception:
                                    atom_bytes = None
                            if atom_bytes is None:
                                # Fallback to JSON serialization
                                import json as _json
                                atom_bytes = _json.dumps(json_value, separators=(',', ':')).encode('utf-8')

                            # Object cache (store the serialized object dict, as storage.get_atom returns dicts)
                            if getattr(bundle, 'obj_cache', None):
                                bundle.obj_cache.put(
                                    self.atom_pointer.transaction_id,
                                    self.atom_pointer.offset,
                                    json_value,
                                    schema_epoch=getattr(bundle, 'schema_epoch', None),
                                    size_bytes_est=len(atom_bytes) if atom_bytes is not None else None
                                )
                            # Bytes cache (stores serialized bytes)
                            if getattr(bundle, 'bytes_cache', None) and atom_bytes is not None:
                                bundle.bytes_cache.put(
                                    self.atom_pointer.transaction_id,
                                    self.atom_pointer.offset,
                                    atom_bytes
                                )
                    except Exception:
                        # Caching must never break persistence; ignore cache errors
                        pass
                else:
                    raise ProtoValidationException(
                        message=f'An DBObject can only be saved within a given transaction!'
                    )

    def hash(self) -> int:
        ap = getattr(self, 'atom_pointer', None)
        if ap:
            return ap.hash()
        # Fallback for unsaved/ephemeral atoms: use object identity
        return hash(id(self))

    def __getitem__(self, item: str):
        if hasattr(self, item):
            return super().__getitem__(item)
        else:
            return None


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
    created_at: datetime.datetime

    def __init__(self,
                 object_root: Atom = None,
                 literal_root: Atom = None,
                 transaction: AbstractTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs
                 ):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.object_root = object_root
        self.literal_root = literal_root
        self.created_at = datetime.datetime.now()


class DBObject(Atom):
    """
    Represents a database object that provides dynamic attribute loading and immutability.

    DBObject is designed to interact with protobase-based database systems. It features
    dynamic attribute loading, where unknown attributes are resolved during runtime, and
    enforces immutability by restricting direct attribute modifications. Instead, any modifications
    result in the creation of a new instance with updated attributes.

    If you try to access a not existing attribute, DBObjects will throw no error, instead a None
    value will be returned.

    :ivar _loaded: Indicates whether the object's attributes have been fully loaded.
    :type _loaded: bool
    """

    def __init__(self,
                 transaction: AbstractTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self._loaded = False
        for key, value in kwargs.items():
            object.__setattr__(self, key, value)

    def __getattr__(self, name: str):
        if name == '_loaded':  # Prevent recursion when checking _loaded
            return False
        self._load()
        if name in self.__dict__:
            return self.__dict__[name]
        # Align with Python semantics: signal missing attribute for hasattr()
        raise AttributeError(name)

    def __setattr__(self, key, value):
        # Special case for _loaded to prevent recursion
        if key == '_loaded':
            object.__setattr__(self, key, value)
            return

        # Special case for transaction and atom_pointer during initialization
        if key in ('transaction', 'atom_pointer'):
            object.__setattr__(self, key, value)
            return

        # For other attributes, ensure the object is loaded first
        if not key.startswith('_'):  # Skip internal attributes to prevent recursion
            self._load()

        if key in self.__dict__:
            object.__setattr__(self, key, value)
        else:
            raise ProtoValidationException(
                message=f'ProtoBase DBObjects are immutable! You are trying to set attribute {key}'
            )

    def _setattr(self, name: str, value: object) -> DBObject:
        new_object = DBObject(transaction=self.transaction)
        for attr_name, attr_value in self.__dict__.items():
            if attr_name != '_loaded':  # Skip _loaded flag to avoid recursion
                object.__setattr__(new_object, attr_name, attr_value)
        object.__setattr__(new_object, name, value)
        return new_object

    def _load(self):
        if not self._loaded:
            super()._load()
            self._loaded = True


class MutableObject(Atom):
    """
    Represents a mutable object and is used within the context
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
        if not self.transaction:
            raise ProtoValidationException(
                message=f"You can't access a mutable object out of the scope of a transaction!"
            )

        current_object = self.transaction.get_mutable(self.hash_key)
        return current_object.__getattr__(name)

    def __setitem__(self, name: str, value):
        self.__setattr__(name, value)

    def __delitem__(self, name: str):
        self.__setattr__(name, None)

    def __setattr__(self, key, value):
        if not self.transaction:
            raise ProtoValidationException(
                message=f"You can't access a mutable object out of the scope of a transaction!"
            )

        current_object = cast(DBObject, self.transaction.get_mutable(self.hash_key))
        new_object = current_object._setattr(key, value)
        self.transaction.set_mutable(self.hash_key, new_object)
        if self.atom_pointer and self.atom_pointer.transaction_id:
            # Object is stored in DB and it is going to be modified.
            # It should be added to the set of objects to be checked if were modified
            # by other transaction simultaneously with this transaction
            self.transaction.set_locked_object(self.hash_key, current_object)

    def __hasattr__(self, name: str):
        if not self.transaction:
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
    functionality such as data representation and
    query planning.

    :ivar count: The total number of items in the collection.
    :type count: int
    """
    count: int = 0
    indexes: Dictionary = None

    def __init__(self,
                 indexes: 'Dictionary' = None,
                 transaction: AbstractTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        # Indexes are optional; some collections don't use them. Avoid importing Dictionary here to prevent cycles.
        self.indexes = indexes
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)

    def index_add(self, item) -> DBCollections:
        """
        When the DBCollection represents an index, add an element to the index.
        On other collections, no change is made

        :param item:
        :return: the new modified collection
        """
        return self

    def index_remove(self, item) -> DBCollections:
        """
        When the DBCollection represents an index, remove an element from the index.
        On other collections, no change is made

        :param item:
        :return: the new modified collection
        """
        return self

    def add2indexes(self, item):
        """
            Add an element to the indexes.
        :param item:
        :return:
        """
        if not self.indexes:
            return
        for index_name, index in self.indexes.as_iterable():
            if index_name in item:
                self.indexes[index_name] = self.indexes.get_at(index_name).index_add(item)

    def remove_from_indexes(self, item):
        """
            Remove an element from the indexes.
        :param item:
        :return:
        """
        if not self.indexes:
            return
        for index_name, index in self.indexes.as_iterable():
            if index_name in item:
                self.indexes[index_name] = self.indexes.get_at(index_name).index_remove(item)

    @abstractmethod
    def as_query_plan(self) -> QueryPlan:
        """
        Creates a query plan based on this collection.

        This method generates a QueryPlan object that can be used to execute
        queries against the collection's data. Implementations should define
        how the collection's specific structure is translated into a query plan.

        :return: A QueryPlan object for this collection.
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
        Executes the query plan and returns the results.

        This method processes the query plan and retrieves the data according to
        the plan's specifications. Implementations should define the specific
        execution logic based on the query plan type.

        :return: A list of results from executing the query plan.
        """

    @abstractmethod
    def optimize(self) -> QueryPlan:
        """
        Optimizes the query plan for better performance.

        Implementations should analyze the current node (and recursively its
        children via based_on) to create a more efficient execution strategy.
        No external context parameter is required.

        :return: The optimized query plan.
        """

    def count(self) -> int:
        """
        Default count implementation: iterate through execute() and count the results.
        Subclasses can override for more efficient counting.
        """
        return sum(1 for _ in self.execute())


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
                 string: str = None,
                 transaction: AbstractTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        # Accept legacy alias 'literal' for compatibility with older tests/usages
        if string is None and 'literal' in kwargs:
            string = kwargs.pop('literal')
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.string = string or ''

    def __eq__(self, other: str | Literal) -> bool:
        if isinstance(other, Literal):
            return self.string == other.string
        else:
            return self.string == other

    def __str__(self) -> str:
        return self.string

    def __add__(self, other: str | Literal) -> Literal:
        if isinstance(other, Literal):
            return Literal(string=self.string + other.string)
        else:
            return Literal(string=self.string + other)


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
        Retrieves the configuration data for the block provider.

        This method returns a ConfigParser object containing the configuration
        settings needed for the block provider's operation. Implementations should
        define how configuration data is loaded and structured.

        :return: A ConfigParser object containing the configuration data.
        """

    @abstractmethod
    def get_new_wal(self) -> tuple[uuid.UUID, int]:
        """
        Get a WAL to use.
        It could be an old one, or a new one.

        :return: a tuple with the id of the WAL and the next offset to use
        """

    @abstractmethod
    def get_reader(self, wal_id: uuid.UUID, position: int) -> BinaryIO:
        """
        Gets a binary reader initialized at the specified position in the WAL file.

        This method creates and returns a binary I/O stream positioned at the given
        offset within the specified Write-Ahead Log (WAL) file. This allows for
        reading data from a specific location in the WAL.

        :param wal_id: The unique identifier of the WAL file to read from.
        :param position: The byte offset position within the WAL file to start reading from.
        :return: A binary I/O stream positioned at the specified location in the WAL file.
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

    @abstractmethod
    def get_current_root_object(self) -> AtomPointer:
        """
        Read current root object from storage
        :return: the current root object
        """

    @abstractmethod
    def update_root_object(self, new_root: AtomPointer):
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
    def read_current_root(self) -> AtomPointer:
        """
        Read the current root object
        :return:
        """

    @abstractmethod
    def read_lock_current_root(self) -> AtomPointer:
        """
        Read the current root object
        :return:
        """

    @abstractmethod
    def set_current_root(self, new_root_pointer: AtomPointer):
        """
        Set the current root object
        :return:
        """

    @abstractmethod
    def unlock_current_root(self):
        """
        Unlock the current root by performing necessary operations based
        on the implementation. This method typically interacts with the state or
        systems associated with this object to achieve the unlocking process.

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

    @abstractmethod
    def close(self):
        """
        Ends all operations, make all changes stable
        :return:
        """


# Lazy export to avoid circular import: provide CountedSet via attribute access
def __getattr__(name: str):
    if name == 'CountedSet':
        from .sets import CountedSet as _CS
        return _CS
    raise AttributeError(name)
