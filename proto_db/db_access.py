from __future__ import annotations

from typing import cast

from .exceptions import ProtoValidationException, ProtoLockingException
from .common import Atom, \
    AbstractObjectSpace, AbstractDatabase, AbstractTransaction, \
    SharedStorage, RootObject, Literal, atom_class_registry, AtomPointer

from .dictionaries import HashDictionary, Dictionary, _str_hash
from .lists import List
from .sets import Set
import datetime
from threading import Lock


class ObjectSpace(AbstractObjectSpace):
    storage: SharedStorage
    _lock: Lock

    def __init__(self, storage: SharedStorage):
        super().__init__(storage)
        self.storage = storage
        self._lock = Lock()

    def _read_db_catalog(self) -> dict[str:Dictionary]:
        catalog_db = Database(self, '_db_catalog')
        read_tr = catalog_db.new_transaction()
        root = self.storage.read_current_root()
        space_history: List = List(
            transaction=read_tr,
            atom_pointer=root
        )
        space_history._load()
        current_root = cast(RootObject, space_history.get_at(0))
        if not current_root:
            current_root = RootObject(
                object_root=Dictionary(),
                literal_root=Dictionary()
            )
        databases = {key:value for key, value in current_root.object_root.as_iterable()}
        read_tr.commit()
        return databases

    def open_database(self, database_name: str) -> Database:
        """
        Opens a database
        :return:
        """
        with self._lock:
            databases = self._read_db_catalog()
            if database_name in databases:
                return Database(self, database_name)

            raise ProtoValidationException(
                message=f'Database {database_name} does not exist!'
            )

    def new_database(self, database_name: str) -> Database:
        """
        Opens a database
        :return:
        """
        with self._lock:
            databases = self._read_db_catalog()
            if database_name not in databases:
                return Database(self, database_name)

        raise ProtoValidationException(
            message=f'Database {database_name} already exists!'
        )

    def rename_database(self, old_name: str, new_name: str):
        """
        Rename an existing database. If database is already opened, it will not
        commit anymore!
        :return:
        """
        with self._lock:
            databases = self._read_db_catalog()
            if old_name in databases and new_name not in databases:
                return Database(self, new_name)

            raise ProtoValidationException(
                message=f'Database {old_name} does not exist or {new_name} already exists!'
            )

    def remove_database(self, name: str):
        """
        Remove database from db catalog.
        If database is already opened, it will not commit anymore! Be carefull
        :return:
        """
        # TODO
        pass

    def get_literals(self, literals: list[str]) -> dict[str, Atom]:
        with self._lock:
            root = self.storage.read_current_root()
            literal_catalog: Dictionary = cast(Dictionary, root.literal_root)
            result = {}
            for literal in literals:
                if literal_catalog.has(literal):
                    result[literal] = literal_catalog.get_at(literal)
                else:
                    new_literal = Literal(literal=literal)
                    result[literal] = new_literal
                    literal_catalog.set_at(literal, new_literal)

            root.literal_root = literal_catalog
            self.storage.set_current_root(root)

            return result


class Database(AbstractDatabase):
    database_name: str
    object_space: ObjectSpace
    current_root: RootObject

    def __init__(self, object_space: ObjectSpace, database_name: str):
        super().__init__(object_space)
        self.object_space = object_space
        self.database_name = database_name

    def _read_db_root(self, root_pointer: AtomPointer) -> Dictionary:
        read_tr = ObjectTransaction(self)
        if root_pointer:
            space_history = List(transaction=read_tr, atom_pointer=root_pointer)
            space_history._load()
        else:
            space_history = List(transaction=read_tr)
            initial_root = RootObject(
                object_root=Dictionary(transaction=read_tr),
                literal_root=Dictionary(transaction=read_tr),
                transaction=read_tr
            )
            space_history = space_history.set_at(0, initial_root)

        space_root = cast(RootObject, space_history.get_at(0))
        space_root._load()

        db_catalog = space_root.object_root
        db_catalog._load()
        if db_catalog:
            db_root = cast(Dictionary, db_catalog.get_at(self.database_name))
            if db_root:
                db_root._load()
            else:
                db_root = Dictionary(transaction=read_tr)
        else:
            db_root = Dictionary()

        read_tr.commit()
        return db_root

    def get_current_root(self) -> Dictionary:
        root_pointer = self.object_space.storage.read_current_root()
        return self._read_db_root(root_pointer)

    def get_lock_current_root(self) -> Dictionary:
        root_pointer = self.object_space.storage.read_lock_current_root()
        return self._read_db_root(root_pointer)

    def set_current_root(self, new_db_root: Dictionary):
        update_tr = ObjectTransaction(self)
        root_pointer = self.object_space.storage.read_current_root()
        if root_pointer:
            space_history = List(transaction=update_tr, atom_pointer=root_pointer)
            space_history._load()
            initial_root = space_history.get_at(0)
        else:
            space_history = List(transaction=update_tr)
            initial_root = RootObject(
                object_root=Dictionary(transaction=update_tr),
                literal_root=Dictionary(transaction=update_tr),
                transaction=update_tr
            )
            space_history = space_history.set_at(0, initial_root)

        new_space_root = RootObject(
            object_root=initial_root.object_root.set_at(self.database_name, new_db_root),
            literal_root=initial_root.literal_root
        )
        space_history = space_history.set_at(0, new_space_root)
        space_history._save()
        self.object_space.storage_provider.set_current_root(space_history.atom_pointer)
        update_tr.abort()

        self.object_space.storage.set_current_root(space_history.atom_pointer)

    def unlock_current_root(self):
        return self.object_space.storage.unlock_current_root()

    def new_transaction(self) -> ObjectTransaction:
        """
        Start a new read transaction
        :return:
        """

        current_root = self.get_current_root()
        return ObjectTransaction(self, current_root)

    def new_branch_database(self, new_db_name: str) -> Database:
        """
        Gets a new database, derived from the current state of the origin database.
        The derived database could be modified in an idependant history.
        Transactions in the derived database will not impact in the origin database
        :return:
        """

        new_db = self.object_space.new_database(new_db_name)

        creation_tr = ObjectTransaction(new_db)
        creation_tr.set_root_object(
            '_creation_timestamp',
            Literal(str(datetime.datetime.now())))
        creation_tr.commit()

        return new_db

    def get_state_at(self, when: datetime.datetime, snapshot_name:str) -> Database:
        # TODO
        # First, locate root at the given time, through a binary search on space history
        #        (using RootObject created_at field). Space history is a reverse time ordered list
        # Second, creates a new database with the database root at the time (even an
        #         empty databases if the database didn't exist at the time)
        pass

    def get_literal(self, string: str) -> Literal | None:
        root = self.get_current_root()
        if not root:
            return None
        literal_root: Dictionary = cast(Dictionary, root.literal_root)
        if literal_root.has(string):
            return literal_root.get_at(string)
        else:
            return None


class ObjectTransaction(AbstractTransaction):
    """
    Root object at the time transaction was started
    """
    transaction_root: Dictionary = None

    """
    Current root at commit time
    """
    current_root: Dictionary = None

    read_objects: HashDictionary = HashDictionary()

    """
    Any modified or created roots within this transaction
    """
    new_roots: Dictionary = None

    """
    Mutable indexes to be checked for changes at commit time.
    If at commit time, value read from current root for this mutable is not the same, that means
    another transaction(s) has committed changes during this transaction execution. So commit should
    be aborted
    """
    read_lock_objects: HashDictionary = None

    """
    Mutable indexes modified in this transaction
    """
    modified_mutable_objects: HashDictionary = None

    """
    New mutable indexes modified in this transaction
    """
    new_mutable_objects: HashDictionary = None

    """
    Snapshot of mutable objects at transaction start time
    """
    initial_mutable_objects: HashDictionary = None

    """
    Literals created in this transaction
    """
    new_literals: Dictionary = None

    """
    Transaction state: Running, Committed or Aborted
    """
    state: str = 'Running'

    """
    Lock to ensure smooth operation in multithreading environments
    """

    lock: Lock
    database: Database

    def __init__(self, database: Database, transaction_root: Dictionary = None):
        super().__init__()
        self.lock = Lock()
        self.new_literals = Dictionary(transaction=self)
        self.database = database

        self.transaction_root = transaction_root
        self.initial_transaction_root = transaction_root
        self.new_roots = Dictionary()
        self.read_lock_objects = HashDictionary()
        self.new_mutable_objects = HashDictionary()
        self.modified_mutable_objects = HashDictionary()

        if transaction_root and self.transaction_root.has('_mutable_root'):
            self.initial_mutable_objects = cast(HashDictionary, self.transaction_root.get_at('_mutable_root'))
        self.mutable_objects = HashDictionary()

    def read_object(self, class_name: str, atom_pointer: AtomPointer) -> Atom:
        with self.lock:
            atom_hash = atom_pointer.hash()
            if not self.read_objects.has(atom_hash):
                atom = atom_class_registry[class_name](transaction=self, atom_pointer=atom_pointer)
                self.read_objects = self.read_objects.set_at(
                    atom_hash,
                    atom
                )
            else:
                atom = self.read_objects.get_at(atom_hash)

            return atom

    def get_literal(self, string: str):
        if self.new_literals.has(string):
            return self.new_literals.get_at(string)
        else:
            existing_literal = self.database.get_literal(string)
            if existing_literal:
                return existing_literal
            else:
                new_literal = Literal(transaction=self, string=string)
                self.new_literals = self.new_literals.set_at(string, new_literal)
                return new_literal

    def get_root_object(self, name: str) -> object | None:
        """
        Get a root object from the database root catalog

        :param name:
        :return:
        """
        with self.lock:
            return self.transaction_root.get_at(name)

    def set_root_object(self, name: str, value: object):
        """
        Set a root object into the database root catalog. It is the only way to persist changes

        :param name:
        :param value:
        :return:
        """
        if isinstance(value, Atom):
            value._save()

        with self.lock:
            if self.transaction_root:
                self.new_roots = self.transaction_root.set_at(name, value)
            else:
                self.new_roots = Dictionary(transaction=self)
                self.transaction_root = self.new_roots
                self.new_roots = self.new_roots.set_at(name, value)

    def set_locked_object(self, mutable_index: int, current_atom: Atom):
        with self.lock:
            if not self.read_lock_objects.has(mutable_index):
                self.read_lock_objects.set_at(mutable_index, current_atom)

    def _save_modified_mutables(self):
        if self.modified_mutable_objects.count > 0:
            for key, value in self.modified_mutable_objects.as_iterable():
                if isinstance(value, Atom):
                    value._save()

    def _save_modified_roots(self):
        if self.transaction_root.count > 0:
            for key, value in self.transaction_root.as_iterable():
                if isinstance(value, Atom):
                    value._save()

    def _check_read_locked_objects(self, current_root: Dictionary):
        if self.read_lock_objects.count > 0:
            current_mutable_root: HashDictionary = cast(HashDictionary, current_root.get_at('_mutable_root'))
            for key, value in self.read_lock_objects.as_iterable():
                if current_mutable_root.get_at(key) != value:
                    raise ProtoLockingException(
                        message=f'Another transaction has modified an object modified in this transaction!'
                    )

    def _update_created_literals(self, current_literal_root: Dictionary) -> Dictionary:
        if self.new_literals.count > 0:
            for key, value in self.new_literals.as_iterable():
                if not current_literal_root.has(key):
                    value._save()
                    current_literal_root = current_literal_root.set_at(key, value)

        return current_literal_root

    def _update_mutable_indexes(self, current_db_root: Dictionary) -> Dictionary:
        # It is assumed all updated mutables were previously saved
        current_mutable_root: HashDictionary = cast(HashDictionary, current_db_root.get_at('_mutable_root'))
        if self.modified_mutable_objects.count > 0:
            for key, value in self.modified_mutable_objects.as_iterable():
                current_db_root = current_mutable_root.set_at(key, value)
            current_db_root = current_db_root.set_at('_mutable_root', current_mutable_root)
        return current_db_root

    def _update_database_roots(self, current_root: Dictionary) -> Dictionary:
        # It is assumed all updated roots were previously saved
        current_db_root = current_root
        if self.new_roots.count > 0:
            for key, value in self.new_roots.as_iterable():
                current_db_root = current_db_root.set_at(key, value)
        return current_db_root

    def commit(self):
        """
        Close the transaction and make it persistent. All changes recorded
        Before commit all checked and modified objects will be tested if modified
        by another transaction. Transaction will proceed only if no change in
        used objects is verified.
        If a return object is specified, the full tree of related objects is persisted
        All created objects, not reachable from this return_object or any updated root
        will NOT BE PERSISTED, and they will be not usable after commit!
        :return:
        """
        with self.lock:
            if self.state != 'Running':
                raise ProtoValidationException(
                    message=f'Transaction is not running ({self.state}). It could not be committed!'
                )

            if self.new_roots.count != 0 or self.modified_mutable_objects.count != 0 or self.new_literals.count != 0:
                # Save transaction created objects before locking database root
                self._save_modified_mutables()
                self._save_modified_roots()

                # The folling block will be synchronized among all transactions
                # for this database
                with RootContextManager(object_transaction=self) as space_root:
                    if not space_root:
                        space_root = RootObject(
                            object_root=Dictionary(),
                            literal_root=Dictionary(),
                            transaction=self
                        )

                    if self.new_literals.count > 0:
                        space_tr = ObjectTransaction(self.database, space_root)

                        space_root.transaction = space_tr
                        space_root._load()
                        space_root = RootObject(
                            object_root=space_root.object_root,
                            literal_root=self._update_created_literals(space_root.literal_root),
                            transaction=space_tr
                        )
                        space_root._save()
                        space_tr.commit()

                    db_root = cast(Dictionary, space_root.object_root.get_at(self.database.database_name))
                    if not db_root:
                        db_root = Dictionary(transaction=self)

                    self._check_read_locked_objects(db_root)

                    db_root = self._update_mutable_indexes(db_root)
                    db_root = self._update_database_roots(db_root)
                    db_root.transaction = self
                    db_root._save()
                    self.database.set_current_root(db_root)

            # At this point everything changed has been commited
            self.state = 'Commited'

    def abort(self):
        """
        Discard any changes made. Database is not modified. All created objects are no longer usable
        :return:
        """
        with self.lock:
            if self.state != 'Running':
                raise ProtoValidationException(
                    message=f'Transaction is not running ({self.state}). It could not be aborted!'
                )

            self.state = 'Aborted'

    def _get_string_hash(self, string: str) -> int:
        """

        :param string:
        :return: a hash based in db persisted strings
        """
        return _str_hash(string)

    def get_mutable(self, key:int):
        with self.lock:
            if self.new_mutable_objects.has(key):
                return self.new_mutable_objects.get_at(key)

            if self.initial_mutable_objects.has(key):
                return self.initial_mutable_objects.get_at(key)

            raise ProtoValidationException(
                message=f'Mutable with index {key} not found!'
            )

    def set_mutable(self, key:int, value:Atom):
        with self.lock:
            if self.initial_mutable_objects.has(key):
                self.modified_mutable_objects.set_at(key, value)
            else:
                self.new_mutable_objects.set_at(key, value)

    def new_hash_dictionary(self) -> HashDictionary :
        """
        Return a new HashDictionary conected to this transaction
        :return:
        """
        return HashDictionary(transaction=self)

    def new_dictionary(self) -> Dictionary:
        """
        Return a new Dictionary conected to this transaction

        :return:
        """
        return Dictionary(transaction=self)

    def new_list(self) -> List:
        """
        Return a new List connected to this transaction
        :return:
        """
        return List(transaction=self)

    def new_hash_set(self) -> Set:
        """
        Return a new Set connected to this transaction
        :return:
        """
        return Set(transaction=self)

class RootContextManager:
    def __init__(self, object_transaction: ObjectTransaction):
        self.object_transaction = object_transaction

    def __enter__(self):
        root_pointer = self.object_transaction.database.object_space.storage_provider.read_current_root()
        root_history = RootObject(transaction=self.object_transaction, atom_pointer=root_pointer)
        if not root_pointer:
            root_history = List(transaction=self.object_transaction)
            root_history = root_history.set_at(
                0,
                RootObject(
                    object_root=Dictionary(transaction=self.object_transaction),
                    literal_root=Dictionary(transaction=self.object_transaction),
                    transaction=self.object_transaction
                )
            )
        else:
            root_history._load()
        self.current_root = cast(RootObject, root_history.get_at(0))
        return self.current_root

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.object_transaction.database.unlock_current_root()
        # let the exception follows the try chain
        return False


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
    transaction: ObjectTransaction

    def __init__(self,
                 filename: str = None,
                 mimetype: str = None,
                 content: bytes = None,
                 transaction: ObjectTransaction = None,
                 atom_pointer: AtomPointer = None,
                 **kwargs):
        super().__init__(transaction=transaction, atom_pointer=atom_pointer, **kwargs)
        self.filename = filename
        self.mimetype = mimetype
        self.transaction = cast(ObjectTransaction, transaction)

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
            if self.transaction:
                if self.atom_pointer.transaction_id and \
                   self.atom_pointer.offset:
                    loaded_content = self.transaction.database.object_space.storage_provider.get_bytes(
                        self.atom_pointer).result()
                    self.content = loaded_content
            self._loaded = True

    def _save(self):
        if not self.atom_pointer and not self._saved:
            # It's a new object

            if self.transaction:
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

