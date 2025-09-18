from __future__ import annotations

import datetime
import hashlib
from threading import Lock
from threading import RLock
from typing import cast

from . import ProtoCorruptionException
from .common import Atom, \
    AbstractObjectSpace, AbstractDatabase, AbstractTransaction, \
    SharedStorage, RootObject, Literal, atom_class_registry, AtomPointer, ConcurrentOptimized
from .dictionaries import Dictionary
from .exceptions import ProtoValidationException, ProtoLockingException
from .hash_dictionaries import HashDictionary
from .lists import List
from .sets import Set


class ObjectSpace(AbstractObjectSpace):
    storage: SharedStorage
    state: str
    _lock: Lock

    def __init__(self, storage: SharedStorage):
        super().__init__(storage)
        self.storage = storage
        self.state = 'Running'
        self._lock = Lock()

    def _read_db_catalog(self) -> dict[str:Dictionary]:
        """
        Read the current database catalog from the space root in a robust way.
        Falls back to an empty catalog if the space or root is not yet initialized.
        """
        try:
            space_root = self.get_space_root(lock=False)
        except ProtoValidationException:
            return {}

        if not space_root:
            return {}

        # Ensure we load the root to materialize object_root/literal_root attributes
        try:
            space_root._load()
        except Exception:
            pass

        if not getattr(space_root, 'object_root', None):
            return {}

        try:
            # Ensure dictionary is loaded before iterating to materialize its content
            space_root.object_root._load()
            return {key: value for key, value in space_root.object_root.as_iterable()}
        except Exception:
            # If anything goes wrong reading the catalog, treat it as empty to avoid crashes
            return {}

    def open_database(self, database_name: str) -> Database:
        """
        Opens a database
        :return:
        """
        with self._lock:
            if self.state != 'Running':
                raise ProtoValidationException(
                    message=f'Object space is not running!'
                )

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
            if self.state != 'Running':
                raise ProtoValidationException(
                    message=f'Object space is not running!'
                )

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
            if self.state != 'Running':
                raise ProtoValidationException(
                    message=f'Object space is not running!'
                )

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

    def finish_update(self):
        return self.storage.unlock_current_root()

    def get_space_history(self, lock=False) -> List:
        read_tr = ObjectTransaction(None, object_space=self, storage=self.storage)

        try:
            if lock:
                root_pointer = self.storage.read_lock_current_root()
            else:
                root_pointer = self.storage.read_current_root()
        except ProtoValidationException:
            root_pointer = None

        if root_pointer:
            space_history = List(transaction=read_tr, atom_pointer=root_pointer)
            space_history._load()
        else:
            space_history = List(transaction=read_tr)

        return space_history

    def get_space_root(self, lock=False) -> RootObject:
        read_tr = ObjectTransaction(None, object_space=self, storage=self.storage)

        space_history = self.get_space_history(lock)

        if space_history.count == 0:
            space_root = RootObject(
                object_root=Dictionary(),
                literal_root=Dictionary(),
                transaction=read_tr
            )
        else:
            space_root = space_history.get_at(0)

        return space_root

    def set_space_root(self, new_space_root: RootObject):
        update_tr = ObjectTransaction(None, object_space=self, storage=self.storage)

        # CAS check against the expected root pointer installed at lock time
        try:
            current_hist = self.get_space_history(lock=False)
            current_ptr = getattr(current_hist, 'atom_pointer', None)
            expected_ptr = getattr(self.storage, '_locked_expected_root', None)
            if current_ptr is not None and expected_ptr is not None:
                if (current_ptr.transaction_id != expected_ptr.transaction_id) or (current_ptr.offset != expected_ptr.offset):
                    raise ProtoLockingException(message='Concurrent root update detected (space history changed)')
        except Exception:
            # If anything goes wrong, proceed; storage.set_current_root will enforce CAS too
            pass

        space_history = self.get_space_history()

        new_space_root.transaction = update_tr
        space_history = space_history.insert_at(0, new_space_root)
        space_history._save()

        # Prefer explicit CAS when available
        try:
            expected_ptr = getattr(self.storage, '_locked_expected_root', None)
            if hasattr(self.storage, 'set_current_root_cas'):
                self.storage.set_current_root_cas(expected_ptr, space_history.atom_pointer)
            else:
                self.storage.set_current_root(space_history.atom_pointer)
        except Exception:
            # Fallback to legacy setter
            self.storage.set_current_root(space_history.atom_pointer)

    def get_literals(self, literals: Dictionary) -> dict[str, Literal]:
        read_tr = ObjectTransaction(None, storage=self.storage)

        print("Entrando a update de literales\n")

        with self._lock:
            root = self.get_space_root(lock=False)
            literal_catalog: Dictionary = cast(Dictionary, root.literal_root)
            new_literals = list()
            for literal_string, literal in literals.as_iterable():
                if literal.atom_pointer:
                    continue
                if literal_catalog.has(literal_string):
                    existing_literal = literal_catalog.get_at(literal_string)
                    literal.atom_pointer = existing_literal.atom_pointer
                else:
                    new_literals.append(literal)

            if new_literals:
                # There are non resolved literals still

                root = self.get_space_root(lock=True)
                literal_catalog: Dictionary = cast(Dictionary, root.literal_root)
                literal_catalog.transaction = read_tr
                update_catalog = False
                for literal in new_literals:
                    if not literal_catalog.has(literal.string):
                        literal._save()
                        literal_catalog = literal_catalog.set_at(literal.string, literal)
                        update_catalog = True
                    else:
                        existing_literal = literal_catalog.get_at(literal.string)
                        literal.atom_pointer = existing_literal.atom_pointer

                if update_catalog:
                    literal_catalog._save()
                    root = RootObject(
                        object_root=root.object_root,
                        literal_root=literal_catalog,
                        transaction=read_tr
                    )
                    self.set_space_root(root)

                self.storage.unlock_current_root()

        print("Seliendo de update de literales\n")

    def close(self):
        with self._lock:
            if self.state != 'Running':
                raise ProtoValidationException(
                    message=f'Object space is not running!'
                )

            self.storage.close()

            self.state = 'Closed'


class Database(AbstractDatabase):
    database_name: str
    object_space: ObjectSpace
    current_root: RootObject
    state: str

    def __init__(self, object_space: ObjectSpace, database_name: str = None):
        super().__init__(object_space)
        self.object_space = object_space
        self.database_name = database_name
        self.state = 'Running'

    def __enter(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.state = 'Closed'
        return False

    def update_literals(self, new_literals: Dictionary) -> Dictionary:
        return self.object_space.get_literals(new_literals)

    def read_db_root(self, lock=False) -> Dictionary:
        read_tr = ObjectTransaction(self)
        space_root = self.object_space.get_space_root(lock)
        if space_root.object_root:
            db_catalog = space_root.object_root
        else:
            db_catalog = Dictionary(transaction=read_tr)

        if db_catalog:
            db_root = cast(Dictionary, db_catalog.get_at(self.database_name))
            if db_root:
                db_root._load()
            else:
                db_root = Dictionary(transaction=read_tr)
        else:
            db_root = Dictionary(transaction=read_tr)

        return db_root

    def set_db_root(self, new_db_root: Dictionary):
        update_tr = ObjectTransaction(self)

        initial_root = self.object_space.get_space_root()
        if initial_root.atom_pointer:
            initial_root = RootObject(
                atom_pointer=initial_root.atom_pointer,
                transaction=update_tr
            )
            initial_root._load()
        else:
            initial_root = RootObject(
                object_root=Dictionary(transaction=update_tr),
                literal_root=Dictionary(transaction=update_tr),
                transaction=update_tr
            )

        new_space_root = RootObject(
            object_root=initial_root.object_root.set_at(self.database_name, new_db_root),
            literal_root=initial_root.literal_root,
            transaction=update_tr
        )
        new_space_root._save()

        self.object_space.set_space_root(new_space_root)
        update_tr.abort()

    def finish_update(self):
        self.object_space.finish_update()

    def new_transaction(self) -> ObjectTransaction:
        """
        Start a new read transaction
        :return:
        """

        # Capture the current space root pointer for CAS during commit
        try:
            space_hist = self.object_space.get_space_history(lock=False)
            expected_ptr = getattr(space_hist, 'atom_pointer', None)
        except Exception:
            expected_ptr = None
        current_root = self.read_db_root() if self.database_name != '_sysdb' else None
        tx = ObjectTransaction(self, db_root=current_root)
        try:
            # Stash expected root pointer on the transaction for later CAS check
            setattr(tx, '_expected_root_pointer', expected_ptr)
        except Exception:
            pass
        return tx

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

    def get_state_at(self, when: datetime.datetime, snapshot_name: str) -> Database:
        # TODO
        # First, locate root at the given time, through a binary search on space history
        #        (using RootObject created_at field). Space history is a reverse time ordered list
        # Second, creates a new database with the database root at the time (even an
        #         empty databases if the database didn't exist at the time)
        pass


class ObjectTransaction(AbstractTransaction):
    """
    Enclosing transaction
    """
    enclosing_transaction: ObjectTransaction = None

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

    lock: RLock
    database: Database

    def __init__(self,
                 database: Database,
                 object_space=None,
                 db_root: Dictionary = None,
                 storage=None,
                 enclosing_transaction: ObjectTransaction = None):
        super().__init__()
        self.lock = RLock()
        self.new_literals = Dictionary(transaction=self)
        self.object_space = object_space if object_space else database.object_space if database else None
        if not self.object_space:
            raise ProtoCorruptionException(
                message="Invalid ObjectSpace"
            )
        self.database = database
        self.enclosing_transaction = enclosing_transaction

        self.transaction_root = db_root
        self.initial_transaction_root = self.transaction_root
        self.storage = storage if storage else \
            database.object_space.storage if database else None
        # Expose atom cache bundle from the underlying storage (if available)
        self.atom_cache_bundle = getattr(self.storage, '_atom_caches', None)
        self.new_roots = Dictionary()
        self.read_lock_objects = HashDictionary()
        self.new_mutable_objects = HashDictionary()
        self.modified_mutable_objects = HashDictionary()

        if self.transaction_root and self.transaction_root.has('_mutable_root'):
            self.initial_mutable_objects = cast(HashDictionary, self.transaction_root.get_at('_mutable_root'))
        self.mutable_objects = HashDictionary()
        self.literals = self.database.object_space.get_space_root().literal_root if self.database else \
            self.new_dictionary()

    def __enter(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not exc_type:
            self.commit()
        else:
            self.abort()
        return False

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
            existing_literal = self.literals.get_at(string)
            if existing_literal:
                return existing_literal
            else:
                new_literal = Literal(transaction=self, string=string)
                self.new_literals = self.new_literals.set_at(string, new_literal)
                return new_literal

    def get_root_object(self, name: str) -> object | None:
        """
        Get a root object from the database root catalog and record a read lock snapshot
        of its pointer to detect concurrent modifications at commit time.

        :param name:
        :return:
        """
        with self.lock:
            if self.transaction_root:
                # Capture original pointer for CAS-on-object at commit
                try:
                    if self.transaction_root.has(name):
                        obj = self.transaction_root.get_at(name)
                        original_ptr = getattr(obj, 'atom_pointer', None)
                        # Store snapshot if not already present
                        if not self.read_lock_objects.has(name):
                            self.read_lock_objects = self.read_lock_objects.set_at(name, original_ptr)
                except Exception:
                    pass
                return self.transaction_root.get_at(name)
            return None

    def set_root_object(self, name: str, value: object):
        """
        Set a root object into the database root catalog. It is the only way to persist changes

        :param name:
        :param value:
        :return:
        """

        if isinstance(value, Atom):
            value._save()

        # Ensure all new literals are created
        self._update_created_literals(self, self.new_literals)

        with self.lock:
            if self.transaction_root:
                self.new_roots = self.new_roots.set_at(name, value)
            else:
                self.new_roots = Dictionary(transaction=self)
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

    def _check_read_locked_objects(self, current_root: RootObject):
        """
        Check if any of the read-locked objects have been modified by another transaction.

        :param current_root: The current root object of the database at commit time.
        """
        if not self.read_lock_objects:
            return

        for name, original_object_pointer in self.read_lock_objects.as_iterable():
            try:
                current_obj = current_root.object_root.get_at(name)
                current_object_pointer = getattr(current_obj, 'atom_pointer', None)
            except Exception:
                current_object_pointer = None
            if original_object_pointer != current_object_pointer:
                # CONCURRENT MODIFICATION DETECTED
                new_object = self.new_roots.get_at(name)

                # Check if the object supports automatic merging
                if new_object and isinstance(new_object, ConcurrentOptimized):
                    try:
                        # Load the currently committed object from the database
                        current_db_object = current_root.object_root.get_at(name)
                        # Attempt to rebase our changes on top of the concurrent version
                        rebased_object = new_object._rebase_on_concurrent_update(current_db_object)
                        # If successful, replace the object in our transaction with the merged one
                        self.new_roots = self.new_roots.set_at(name, rebased_object)
                        # And continue to the next locked object
                        continue
                    except Exception as e:
                        # If merge fails, raise a specific error
                        raise ProtoLockingException(
                            f"Concurrent transaction detected on '{name}' and automatic merge failed: {e}"
                        ) from e

                raise ProtoLockingException(f"Concurrent transaction detected on object '{name}' "
                                            f"that does not support automatic merging.")

    def _update_created_literals(self, transaction: ObjectTransaction, literal_root: Dictionary) -> Dictionary:
        literal_update_tr = ObjectTransaction(transaction.database, object_space=transaction.object_space,
                                              storage=self.storage)
        space_root = transaction.object_space.get_space_root()
        current_literal_root = space_root.literal_root
        if self.new_literals.count > 0:
            for key, value in self.new_literals.as_iterable():
                if value.atom_pointer:
                    continue
                if not current_literal_root.has(key):
                    value._save()
                    current_literal_root = current_literal_root.set_at(key, value)
                elif not value.atom_pointer:
                    new_literal = current_literal_root.get_at(key)
                    value.atom_pointer = new_literal.atom_pointer

        self.new_literals = Dictionary(transaction=self)
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
        Commit this transaction, making changes durable and visible to others.

        High-level steps:

        1) Save newly created/modified objects in this transaction context.
        2) Acquire a lock on the database root to prevent concurrent root updates.
        3) Check for concurrent modifications of any read-locked objects; abort on conflicts.
        4) Update indexes for modified mutables and merge new/updated roots.
        5) Persist the new root object pointer to storage (WAL write-through).

        .. note::
           Only objects reachable from updated roots (and modified mutables) are persisted.
           Objects created during the transaction but not reachable from the final committed
           graph will not be saved and become unusable after commit.
        """
        with self.lock:
            if self.state != 'Running':
                raise ProtoValidationException(
                    message=f'Transaction is not running ({self.state}). It could not be committed!'
                )

            if not self.enclosing_transaction:
                # It's a base transaction, it should commit changes to db

                if self.new_roots.count != 0 or self.modified_mutable_objects.count != 0 or self.new_literals.count != 0:
                    # Save transaction created objects before locking database root

                    self._save_modified_mutables()
                    self._save_modified_roots()

                    # The following block will be synchronized among all transactions
                    # for this database
                    with RootContextManager(object_transaction=self) as db_root:
                        self._check_read_locked_objects(db_root)

                        db_root = self._update_mutable_indexes(db_root)
                        db_root = self._update_database_roots(db_root)
                        db_root.transaction = self

                        db_root._save()

                        self.database.set_db_root(db_root)

            else:
                # It's a nested transaction
                enclosing_tr = self.enclosing_transaction
                with enclosing_tr.lock:
                    if enclosing_tr.state == 'Running':
                        if self.new_literals.count > 0:
                            enclosing_tr.new_literals = enclosing_tr.new_literals.merge(self.new_literals)
                        if self.modified_mutable_objects.count > 0:
                            enclosing_tr.modified_mutable_objects = enclosing_tr.modified_mutable_objects.merge(
                                self.modified_mutable_objects)
                        if self.new_mutable_objects.count > 0:
                            enclosing_tr.new_mutable_objects = enclosing_tr.new_mutable_objects.merge(
                                self.new_mutable_objects)
                        if self.new_roots.count > 0:
                            enclosing_tr.new_roots = enclosing_tr.new_roots.merge(self.new_roots)

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
        hash_obj = hashlib.sha256(string.encode('utf-8'))
        hash_int = int(hash_obj.hexdigest(), 16)
        return hash_int

    def get_mutable(self, key: int):
        with self.lock:
            if self.new_mutable_objects.has(key):
                return self.new_mutable_objects.get_at(key)

            if self.initial_mutable_objects.has(key):
                return self.initial_mutable_objects.get_at(key)

            raise ProtoValidationException(
                message=f'Mutable with index {key} not found!'
            )

    def set_mutable(self, key: int, value: Atom):
        with self.lock:
            if self.initial_mutable_objects.has(key):
                self.modified_mutable_objects.set_at(key, value)
            else:
                self.new_mutable_objects.set_at(key, value)

    def new_hash_dictionary(self) -> HashDictionary:
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
        # Install expected root pointer (from tx start) into storage for CAS before locking
        try:
            storage = self.object_transaction.database.object_space.storage
            exp = getattr(self.object_transaction, '_expected_root_pointer', None)
            if exp is not None and hasattr(storage, '_locked_expected_root'):
                # Only set if not already set (avoid clobbering in nested contexts)
                if getattr(storage, '_locked_expected_root', None) is None:
                    setattr(storage, '_locked_expected_root', exp)
        except Exception:
            pass
        return self.object_transaction.database.read_db_root(lock=True)

    def __exit__(self, exc_type, exc_value, traceback):
        self.object_transaction.database.finish_update()
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
        return f'BytesAtom with {len(self.content) if self.content else 0} byte(s)'

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
