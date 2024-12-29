from __future__ import annotations
from typing import cast

import uuid
from .exceptions import ProtoValidationException, ProtoLockingException
from .common import Atom, \
    AbstractObjectSpace, AbstractDatabase, AbstractTransaction, \
    SharedStorage, RootObject, Literal, atom_class_registry, AtomPointer

from .dictionaries import HashDictionary, Dictionary
from .lists import List
from .sets import Set

from threading import Lock


class ObjectSpace(AbstractObjectSpace):
    storage: SharedStorage
    _lock: Lock

    def __init__(self, storage: SharedStorage):
        super().__init__(storage)
        self.storage = storage
        self._lock = Lock()

    def open_database(self, database_name: str) -> Database:
        """
        Opens a database
        :return:
        """
        with self._lock:
            root = self.storage.read_current_root()
            if root:
                db_catalog: Dictionary = cast(Dictionary, root.object_root)
                if db_catalog.has(database_name):
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
            root = self.storage.read_current_root()
            if not root:
                root = RootObject()
                root.object_root = Dictionary()
                root.literal_root = Dictionary()

            db_catalog: Dictionary = cast(Dictionary, root.object_root)
            if not db_catalog.has(database_name):
                new_db = Database(self, database_name)
                new_db_root = Dictionary()
                new_db_catalog = db_catalog.set_at(database_name, new_db_root)
                new_db_catalog._save()
                root.object_root = new_db_catalog
                self.storage.set_current_root(root)
                return new_db

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
            root = self.storage.read_current_root()
            if root:
                db_catalog: Dictionary = cast(Dictionary, root.object_root)
                if db_catalog.has(old_name):
                    database_root: Dictionary = cast(Dictionary, db_catalog.get_at(old_name))
                    new_db_catalog: Dictionary = db_catalog.remove_key(old_name)
                    new_db_catalog = new_db_catalog.set_at(new_name, database_root)
                    new_db_catalog._save()
                    root.object_root = new_db_catalog
                    self.storage.set_current_root(root)

            raise ProtoValidationException(
                message=f'Database {old_name} does not exist!'
            )

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

    def commit_database(self, database_name:str, new_root: Atom):
        with self._lock:
            root = self.storage.read_current_root()
            db_catalog: Dictionary = cast(Dictionary, root.object_root)
            if db_catalog.has(database_name):
                new_db_catalog = db_catalog.set_at(database_name, new_root)
                new_db_catalog._save()
                root.object_root = new_db_catalog
                self.storage.set_current_root(root)
            else:
                raise ProtoValidationException(
                    message=f'Database {database_name} does not exist!'
                )


class Database(AbstractDatabase):
    database_name: str
    object_space: ObjectSpace

    def __init__(self, object_space: ObjectSpace, database_name: str):
        super().__init__(object_space)
        self.object_space = object_space
        self.database_name = database_name

    def get_current_root(self):
        return self.object_space.storage.read_current_root()

    def get_lock_current_root(self):
        return self.object_space.storage.read_lock_current_root()

    def set_current_root(self, new_root: Atom):
        self.object_space.commit_database(self.database_name, new_root)

    def unlock_current_root(self):
        return self.object_space.storage.unlock_current_root()

    def new_transaction(self) -> ObjectTransaction:
        """
        Start a new read transaction
        :return:
        """
        root = self.object_space.storage.read_current_root()
        db_catalog: Dictionary = cast(Dictionary, root.object_root)
        if db_catalog.has(self.database_name):
            current_root: Dictionary = cast(Dictionary, db_catalog.get_at(self.database_name))
            return ObjectTransaction(self, current_root)

    def new_branch_database(self) -> Database:
        """
        Gets a new database, derived from the current state of the origin database.
        The derived database could be modified in an idependant history.
        Transactions in the derived database will not impact in the origin database
        :return:
        """

        new_db_name = uuid.uuid4().hex
        new_db = Database(self.object_space, new_db_name)

        self.object_space.commit_database(self.database_name, Dictionary())
        return new_db

    def get_literal(self, string: str):
        root = self.object_space.storage.read_current_root()
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

    def __init__(self, database: Database, transaction_root: Dictionary):
        super().__init__(database)
        if self.transaction_root:
            self.transaction_root = transaction_root
        else:
            self.transaction_root = Dictionary()
        self.initial_transaction_root = transaction_root
        self.new_roots = Dictionary()
        self.read_lock_objects = HashDictionary()
        if transaction_root.has('_mutable_root'):
            self.initial_mutable_objects = cast(HashDictionary, self.transaction_root.get_at('_mutable_root'))
        else:
            self.mutable_objects = HashDictionary()
        self.new_mutable_objects = HashDictionary()
        self.modified_mutable_objects = HashDictionary()
        self.new_literals = Dictionary(transaction=self)
        self.lock = Lock()

    def read_object(self, class_name: str, atom_pointer: AtomPointer) -> Atom:
        with self.lock:
            atom_hash = atom_pointer.hash()
            if not self.read_objects.has(atom_hash):
                atom = self.read_objects.set_at(
                    atom_hash,
                    atom_class_registry[class_name](transaction=self, atom_pointer=atom_pointer)
                )
            else:
                atom = self.read_objects.get_at(atom_hash)

            return atom

    def get_literal(self, string: str):
        with self.lock:
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

    def get_root_object(self, name: str) -> Atom | None:
        """
        Get a root object from the root catalog

        :param name:
        :return:
        """
        with self.lock:
            return self.transaction_root.get_at(name)

    def set_root_object(self, name: str, value: Atom):
        """
        Set a root object into the root catalog. It is the only way to persist changes

        :param name:
        :param value:
        :return:
        """
        with self.lock:
            self.transaction_root = self.transaction_root.set_at(name, value)

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

    def _update_created_literals(self):
        if self.new_literals.count > 0:
            current_db_root = self.database.get_current_root()
            db_literals: Dictionary = cast(Dictionary, current_db_root.literal_root)

            some_new_literals = False
            try:
                for key, value in self.new_literals.as_iterable():
                    if not db_literals.has(key):
                        some_new_literals = True
                        value._save()
                        db_literals.set_at(key, value)
            finally:
                if some_new_literals:
                    current_db_root.literal_root = db_literals
                    self.database.set_current_root(current_db_root)

    def _update_mutable_indexes(self, current_root: Dictionary) -> Dictionary:
        # It is assumed all updated mutables were previously saved
        current_db_root = current_root
        current_mutable_root: HashDictionary = cast(HashDictionary, current_db_root.get_at('_mutable_root'))
        if self.modified_mutable_objects.count > 0:
            for key, value in self.modified_mutable_objects.as_iterable():
                current_mutable_root.set_at(key, value)
            current_db_root = current_db_root.set_at('_mutable_root', current_mutable_root)
        return current_db_root

    def _update_database_roots(self, current_root: Dictionary) -> Dictionary:
        # It is assumed all updated roots were previously saved
        current_db_root = current_root
        if self.new_roots.count > 0:
            for key, value in self.new_roots.as_iterable():
                current_db_root.set_at(key, value)
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
                with self.database.get_lock_current_root() as current_root:
                    self._update_created_literals()
                    self._check_read_locked_objects(current_root)
                    current_root = self._update_mutable_indexes(current_root)
                    current_root = self._update_database_roots(current_root)
                    current_root._save()
                    self.database.set_current_root(current_root)

            # At this point everything changed was commited
            self.state = 'Committed'

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
        with self.lock:
            return self.database.get_literal(literal=string)

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
    def __init__(self, transaction: ObjectTransaction):
        self.transaction = transaction

    def __enter__(self):
        self.current_root = self.transaction.database.get_lock_current_root()
        return self.current_root

    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_type:
            self.transaction.database.set_current_root(self.current_root)
        else:
            self.transaction.database.unlock_current_root()
        # let the exception follows the try chain
        return False

