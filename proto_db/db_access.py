from __future__ import annotations
from typing import cast

import uuid
from .exceptions import ProtoValidationException
from .common import Atom, \
                    AbstractObjectSpace, AbstractDatabase, AbstractTransaction, \
                    SharedStorage, RootObject, Literal, \
                    DBObject

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
        Rename an existing database. If database is already opened, if will not
        commit any more!
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

        root = self.object_space.storage.read_current_root()
        db_catalog: Dictionary = cast(Dictionary, root.object_root)
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
    initial_transaction_root: Dictionary = None
    transaction_root: Dictionary = None
    new_roots: dict[str, Atom] = None
    read_objects:dict[int, Atom] = None
    read_lock_objects: dict[int, Atom] = None
    mutable_objects: HashDictionary = None
    initial_mutable_root: HashDictionary = None
    new_literals: Dictionary = None

    def __init__(self, database: Database, transaction_root: Dictionary):
        super().__init__(database)
        if self.transaction_root:
            self.transaction_root = transaction_root
        else:
            self.transaction_root = Dictionary()
        self.initial_transaction_root = transaction_root
        self.new_roots = {}
        self.read_objects = {}
        self.read_lock_objects = {}
        if transaction_root.has('_mutable_root'):
            self.mutable_objects = cast(HashDictionary, self.transaction_root.get_at('_mutable_root'))
            self.initial_mutable_root = self.mutable_objects
        else:
            self.mutable_objects = HashDictionary()
        self.new_literals = Dictionary(transaction=self)

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

    def get_root_object(self, name: str) -> Atom | None:
        """
        Get a root object from the root catalog

        :param name:
        :return:
        """
        return self.transaction_root.get_at(name)

    def set_root_object(self, name: str, value: Atom):
        """
        Set a root object into the root catalog. It is the only way to persist changes

        :param name:
        :param value:
        :return:
        """
        self.transaction_root = self.transaction_root.set_at(name, value)

    def set_lock_object(self, object: DBObject):
        self.read_lock_objects[object.hash()] = object

    def commit(self, return_object: Atom = None):
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

    def abort(self):
        """
        Discard any changes made. Database is not modified. All created objects are no longer usable
        :return:
        """

    def _get_string_hash(self, string: str) -> int:
        """

        :param string:
        :return: a hash based in db persisted strings
        """
        return self.database.get_literal(literal=string)

    def get_mutable(self, key:int):
        return self.mutable_objects.get_at(key)

    def set_mutable(self, key:int, value:Atom):
        return self.mutable_objects.set_at(key, value)

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