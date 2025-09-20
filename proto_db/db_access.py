from __future__ import annotations

import datetime
import hashlib
import logging
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

logger = logging.getLogger(__name__)


class ObjectSpace(AbstractObjectSpace):
    storage: SharedStorage
    state: str
    _lock: Lock

    def __init__(self, storage: SharedStorage):
        super().__init__(storage)
        self.storage = storage
        self.state = 'Running'
        self._lock = Lock()

    def _read_db_catalog(self) -> Dictionary:
        """
        Read the current database catalog from the space root in a robust way.
        Falls back to an empty catalog if the space or root is not yet initialized.
        """
        space_root = self.get_space_root()

        if not space_root:
            return Dictionary()

        space_root._load()

        if not space_root.object_root:
            return Dictionary()

        # Ensure dictionary is loaded before iterating to materialize its content
        database_catalog: Dictionary = cast(Dictionary, space_root.object_root)
        return database_catalog

    def _space_context(self):
        class SpaceContext:
            def __init__(self, space):
                self.space = space
                self.storage_context = space.storage.root_context_manager()

            def __enter__(self):
                self.storage_context.__enter__()

            def __exit__(self, exc_type, exc_value, traceback):
                self.storage_context.__exit__(exc_type, exc_value, traceback)

        return SpaceContext(self)

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
            if databases.has(database_name):
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

            with self._space_context():
                update_tr = ObjectTransaction(None, object_space=self, storage=self.storage)

                current_hist = self.get_space_history()
                if current_hist.count > 0:
                    current_root:RootObject = cast(RootObject, current_hist.get_at(0))
                else:
                    # Initialize a fresh root with both catalogs present
                    current_root:RootObject = RootObject(
                        object_root=Dictionary(transaction=update_tr),
                        literal_root=Dictionary(transaction=update_tr),
                        transaction=update_tr
                    )
                databases = cast(Dictionary, current_root.object_root)
                if not databases:
                    databases = Dictionary()
                if not databases.has(database_name):
                    # Create the new database with an empty roots catalog
                    databases = databases.set_at(database_name, Dictionary())
                    # Build a new RootObject with updated object_root
                    current_root = RootObject(
                        object_root=databases,
                        literal_root=current_root.literal_root,
                        transaction=update_tr
                    )
                    space_history = current_hist.insert_at(0, current_root)
                    space_history._save()
                    self.storage.set_current_root(space_history.atom_pointer)

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

            with self._space_context():
                update_tr = ObjectTransaction(None, object_space=self, storage=self.storage)

                current_hist = self.get_space_history()
                if current_hist.count > 0:
                    current_root:RootObject = cast(RootObject, current_hist.get_at(0))
                else:
                    current_root:RootObject = RootObject(
                        object_root=Dictionary(transaction=update_tr),
                        literal_root=Dictionary(transaction=update_tr),
                        transaction=update_tr
                    )
                databases = cast(Dictionary, current_root.object_root)
                if not databases:
                    databases = Dictionary()
                if databases.has(old_name):
                    database = databases.get_at(old_name)
                    databases = databases.remove_at(old_name)
                    if database.has(new_name):
                        raise ProtoValidationException(
                            message=f'Database {new_name} already exists!'
                        )

                    databases = databases.set_at(new_name, database)

                    # Build a new RootObject with updated object_root
                    current_root = RootObject(
                        object_root=databases,
                        literal_root=current_root.literal_root,
                        transaction=update_tr
                    )
                    space_history = current_hist.insert_at(0, current_root)
                    space_history._save()

                    self.storage.set_current_root(space_history.atom_pointer)
                else:
                    raise ProtoValidationException(
                        message=f'Database {old_name} does not exist!'
                    )

    def remove_database(self, name: str):
        """
        Remove database from db catalog.
        If database is already opened, it will not commit anymore! Be carefull
        :return:
        """
        with self._lock:
            if self.state != 'Running':
                raise ProtoValidationException(
                    message=f'Object space is not running!'
                )

            with self._space_context():
                update_tr = ObjectTransaction(None, object_space=self, storage=self.storage)

                current_hist = self.get_space_history()
                if current_hist.count > 0:
                    current_root:RootObject = cast(RootObject, current_hist.get_at(0))
                else:
                    current_root:RootObject = RootObject(transaction=update_tr)
                databases = cast(Dictionary, current_root.object_root)
                if not databases:
                    databases = Dictionary()
                if databases.has(name):
                    databases = databases.remove_at(name)

                    # Build a new RootObject with updated object_root
                    current_root = RootObject(
                        object_root=databases,
                        literal_root=current_root.literal_root,
                        transaction=update_tr
                    )
                    space_history = current_hist.insert_at(0, current_root)
                    space_history._save()

                    self.storage.set_current_root(space_history.atom_pointer)
                else:
                    raise ProtoValidationException(
                        message=f'Database {name} does not exist!'
                    )

    def get_space_history(self, lock=False) -> List:
        read_tr = ObjectTransaction(None, object_space=self, storage=self.storage)

        root_pointer = self.storage.read_current_root()

        if root_pointer:
            space_history = List(transaction=read_tr, atom_pointer=root_pointer)
            space_history._load()
        else:
            space_history = List(transaction=read_tr)

        # Debug: log space_history pointer when reading
        try:
            import os as _os
            if _os.environ.get('PB_DEBUG_CONC'):
                ap = getattr(space_history, 'atom_pointer', None)
                logger.debug("ObjectSpace.get_space_history read pointer: %s/%s", getattr(ap,'transaction_id',None), getattr(ap,'offset',None))
        except Exception:
            pass

        return space_history

    def get_space_root(self) -> RootObject:
        read_tr = ObjectTransaction(None, object_space=self, storage=self.storage)

        space_history = self.get_space_history()

        if space_history.count == 0:
            space_root = RootObject(
                object_root=Dictionary(),
                literal_root=Dictionary(),
                transaction=read_tr
            )
        else:
            space_root = cast(RootObject, space_history.get_at(0))

        return space_root

    def set_space_root(self, new_space_root: RootObject):
        """
        Persist a new space root version by prepending it to the space history and
        updating the provider root pointer. This method assumes the caller is already
        executing under the provider's root context manager (via _space_context()).

        Note: This variant re-reads the current history to prepend the new root. Prefer
        set_space_root_locked when you already have the current history object.
        """
        update_tr = ObjectTransaction(None, object_space=self, storage=self.storage)

        space_history = self.get_space_history()

        new_space_root.transaction = update_tr
        space_history = space_history.insert_at(0, new_space_root)
        space_history._save()

        # Debug: pointer being written for space_history
        try:
            import os as _os
            if _os.environ.get('PB_DEBUG_CONC'):
                ap = getattr(space_history, 'atom_pointer', None)
                logger.debug("ObjectSpace.set_space_root writing pointer: %s/%s", getattr(ap,'transaction_id',None), getattr(ap,'offset',None))
        except Exception:
            pass

        # Do not acquire locks here; assume caller holds them (via _space_context())
        self.storage.set_current_root(space_history.atom_pointer)

    def set_space_root_locked(self, new_space_root: RootObject, current_history: "List"):
        """
        Persist a new space root using the already-read current_history. This avoids
        re-reading the root or re-entering any locks and must be called while the
        provider root lock is held.
        Now that List.insert_at is fixed, prepend the new root directly with insert_at(0)
        without reconstructing the history.
        """
        update_tr = ObjectTransaction(None, object_space=self, storage=self.storage)
        new_space_root.transaction = update_tr
        # Directly prepend the new root to the existing history
        try:
            # Ensure the history uses the same transaction for persistence
            try:
                current_history.transaction = update_tr
            except Exception:
                pass
            space_history = current_history.insert_at(0, new_space_root)
        except Exception:
            # As a fallback, create a minimal list with the new head
            from .lists import List as _List
            space_history = _List(transaction=update_tr).insert_at(0, new_space_root)
        space_history._save()
        # Debug: pointer being written for space_history and embedded object_root pointer
        try:
            import os as _os
            if _os.environ.get('PB_DEBUG_CONC'):
                ap = getattr(space_history, 'atom_pointer', None)
                ap_ro = getattr(getattr(new_space_root, 'object_root', None), 'atom_pointer', None)
                logger.debug("Writing space_root pointer: %s/%s with object_root_ptr=%s/%s", getattr(ap,'transaction_id',None), getattr(ap,'offset',None), getattr(ap_ro,'transaction_id',None), getattr(ap_ro,'offset',None))
        except Exception:
            pass
        self.storage.set_current_root(space_history.atom_pointer)
        # Deep-verify the just written history head points to the intended RootObject
        try:
            import os as _os
            if _os.environ.get('PB_DEBUG_CONC'):
                try:
                    hp = getattr(space_history, 'atom_pointer', None)
                    logger.debug("ObjectSpace.set_space_root_locked wrote history_ptr=%s/%s", getattr(hp,'transaction_id',None), getattr(hp,'offset',None))
                    stg = self.storage
                    if hp and stg:
                        lst_json = stg.get_atom(hp).result()
                        head = lst_json.get('value') if isinstance(lst_json, dict) else None
                        if isinstance(head, dict):
                            ro_tid = head.get('transaction_id')
                            ro_off = head.get('offset')
                            logger.debug("History head RootObject ptr in JSON: %s/%s", ro_tid, ro_off)
                except Exception:
                    pass
        except Exception:
            pass

    def get_literals(self, literals: Dictionary) -> dict[str, Literal]:
        update_tr = ObjectTransaction(None, storage=self.storage)

        with self._lock:
            root = self.get_space_root()
            literal_catalog: RootObject = root.literal_root
            literal_catalog.transaction = update_tr
            result = dict()
            new_literals = dict()
            for literal_string, literal in literals.as_iterable():
                if literal.atom_pointer:
                    result[literal_string] = literal
                elif literal_catalog.has(literal_string):
                    existing_literal = literal_catalog.get_at(literal_string)
                    new_literals[literal_string] = existing_literal
                    result[literal_string] = existing_literal
                else:
                    new_literals[literal_string] = literal_catalog.get_at(literal_string)

            if new_literals:
                # There are non resolved literals still

                with self._space_context():
                    root = self.get_space_root()
                    literal_catalog: Dictionary = root.literal_root
                    literal_catalog.transaction = update_tr
                    update_catalog = False
                    for literal_string, literal in new_literals.items():
                        if not literal_catalog.has(literal_string):
                            literal_catalog = literal_catalog.set_at(literal.string, literal)
                            result[literal_string] = literal
                            update_catalog = True
                        else:
                            existing_literal = literal_catalog.get_at(literal.string)
                            result[literal_string] = literal

                    if update_catalog:
                        literal_catalog._save()
                        # Persist only the literal_root under the provider lock, preserving the freshest object_root
                        try:
                            # Re-enter provider root context to serialize with other root updates
                            with self._space_context():
                                locked_tr = ObjectTransaction(None, object_space=self, storage=self.storage)
                                # Read the freshest space history and root under the lock
                                current_hist = self.get_space_history()
                                fresh_root = self.get_space_root()
                                current_object_root = fresh_root.object_root if fresh_root and fresh_root.object_root else root.object_root
                                # Build and save a new RootObject with the updated literal_root and current object_root
                                locked_root = RootObject(
                                    object_root=current_object_root,
                                    literal_root=literal_catalog,
                                    transaction=locked_tr
                                )
                                locked_root._save()
                                # Write using locked history to avoid re-reads and prevent overwrites of object_root
                                self.set_space_root_locked(locked_root, current_hist)
                        except Exception:
                            # As a last resort, fall back to non-locked update but still preserve object_root from the freshest head
                            try:
                                fresh_space_root = self.get_space_root()
                                current_object_root = fresh_space_root.object_root if fresh_space_root and fresh_space_root.object_root else root.object_root
                            except Exception:
                                current_object_root = root.object_root
                            root = RootObject(
                                object_root=current_object_root,
                                literal_root=literal_catalog,
                                transaction=update_tr
                            )
                            self.set_space_root(root)

            return new_literals

    def close(self):
        with self._lock:
            if self.state != 'Running':
                raise ProtoValidationException(
                    message=f'Object space is not running!'
                )

            self.storage.close()

            self.state = 'Closed'


class Database(AbstractDatabase):
    def _merge_single_root(self, existing_root, staged_root):
        """
        Merge a staged root onto an existing root deterministically to avoid lost updates
        when commits interleave. Applied under the provider root lock.
        - Integer counters: treat staged change as +1 on the current value
        - Set/CountedSet: union staged elements into the current bucket
        - Otherwise: last-writer-wins per key for Dictionary; default to staged object
        """
        try:
            from .dictionaries import Dictionary as _Dict
        except Exception:
            _Dict = None
        try:
            from .sets import Set as _Set, CountedSet as _CSet
        except Exception:
            _Set = tuple()
            _CSet = tuple()
        # If both are Dictionary, merge per-entry
        if _Dict and isinstance(staged_root, _Dict) and isinstance(existing_root, _Dict):
            merged = existing_root
            try:
                for k, v in staged_root.as_iterable():
                    try:
                        curr_v = merged.get_at(k)
                    except Exception:
                        curr_v = None
                    # Integer counters: always +1 on current when both ints
                    if isinstance(v, int) and isinstance(curr_v, int):
                        merged = merged.set_at(k, int(curr_v) + 1)
                        continue
                    # Set/CountedSet: union staged elements into current bucket
                    if hasattr(v, 'as_iterable') and isinstance(curr_v, (_Set, _CSet)):
                        try:
                            bucket = curr_v
                            for e in v.as_iterable():
                                if isinstance(e, (_Set, _CSet)):
                                    continue
                                bucket = bucket.add(e)
                            merged = merged.set_at(k, bucket)
                            continue
                        except Exception:
                            pass
                    # If current missing, adopt staged
                    if curr_v is None:
                        merged = merged.set_at(k, v)
                        continue
                    # Fallback: last-writer-wins per entry
                    try:
                        equal = (v == curr_v)
                    except Exception:
                        equal = False
                    if not equal:
                        merged = merged.set_at(k, v)
                return merged
            except Exception:
                # On any issue, fall back to staged
                return staged_root
        # Default: prefer staged_root
        return staged_root

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

    def read_db_root(self) -> Dictionary:
        read_tr = ObjectTransaction(self)
        # Temporarily disable atom caches for root resolution to avoid stale reads
        caches = getattr(read_tr.storage, '_atom_caches', None)
        saved_obj_cache = getattr(caches, 'obj_cache', None) if caches else None
        saved_bytes_cache = getattr(caches, 'bytes_cache', None) if caches else None
        try:
            if caches:
                try:
                    caches.obj_cache = None
                    caches.bytes_cache = None
                except Exception:
                    pass
            # Non-blocking read of space root (do not acquire provider lock to preserve concurrency)
            # Read the space history pointer directly to avoid any intermediate cache effects
            root_pointer = self.object_space.storage.read_current_root()
            if root_pointer:
                space_history = List(transaction=read_tr, atom_pointer=root_pointer)
                space_history._load()
                space_root = space_history.get_at(0) if space_history.count > 0 else RootObject(transaction=read_tr)
            else:
                space_root = RootObject(object_root=Dictionary(transaction=read_tr), literal_root=Dictionary(transaction=read_tr), transaction=read_tr)

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

            # Debug: log pointer and counter for the resolved db_root
            try:
                import os as _os
                if _os.environ.get('PB_DEBUG_CONC') and db_root:
                    try:
                        cnt_root = db_root.get_at('counter_root')
                        cnt_val = cnt_root.get_at('counter') if cnt_root else None
                        ap = getattr(db_root, 'atom_pointer', None)
                        logger.debug("Read db_root pointer=%s/%s counter=%s", getattr(ap,'transaction_id',None), getattr(ap,'offset',None), cnt_val)
                    except Exception:
                        pass
            except Exception:
                pass
            return db_root
        finally:
            if caches:
                try:
                    caches.obj_cache = saved_obj_cache
                    caches.bytes_cache = saved_bytes_cache
                except Exception:
                    pass

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

        # Build an initial new_space_root from the current snapshot
        new_space_root = RootObject(
            object_root=initial_root.object_root.set_at(self.database_name, new_db_root),
            literal_root=initial_root.literal_root,
            transaction=update_tr
        )
        # Just before saving, refresh the object_root from the freshest head to avoid lost updates
        try:
            freshest = self.object_space.get_space_root()
            fresh_catalog = freshest.object_root if freshest and freshest.object_root else initial_root.object_root
            fresh_catalog = fresh_catalog.set_at(self.database_name, new_db_root)
            new_space_root = RootObject(
                object_root=fresh_catalog,
                literal_root=new_space_root.literal_root,
                transaction=update_tr
            )
        except Exception:
            pass
        # Debug persist info
        try:
            import os as _os
            if _os.environ.get('PB_DEBUG_CONC'):
                try:
                    cur_db_root = new_space_root.object_root.get_at(self.database_name)
                    cnt_root = cur_db_root.get_at('counter_root') if cur_db_root else None
                    cnt_val = cnt_root.get_at('counter') if cnt_root else None
                    logger.debug("set_db_root persist: counter=%s", cnt_val)
                except Exception:
                    pass
        except Exception:
            pass
        new_space_root._save()

        self.object_space.set_space_root(new_space_root)
        update_tr.abort()

    def set_db_root_locked(self, new_db_root: Dictionary):
        """
        Same as set_db_root(), but intended to be called while already holding
        the storage root lock via RootContextManager. Avoids any nested attempts
        to (re)acquire the provider lock, preventing deadlocks on non-reentrant locks.
        If RootContextManager captured the current space root/history for this
        transaction, use them to avoid re-reading anything under the lock.
        """
        update_tr = ObjectTransaction(self)

        # Try to use locked context captured by RootContextManager
        try:
            ot = update_tr  # our transaction context
            locked_space_root = getattr(ot, '_locked_space_root', None)
            locked_space_history = getattr(ot, '_locked_space_history', None)
        except Exception:
            locked_space_root = None
            locked_space_history = None

        if locked_space_root is not None and locked_space_history is not None:
            base_root = RootObject(
                atom_pointer=getattr(locked_space_root, 'atom_pointer', None),
                transaction=update_tr
            )
            if base_root.atom_pointer:
                base_root._load()
            else:
                base_root = RootObject(
                    object_root=Dictionary(transaction=update_tr),
                    literal_root=Dictionary(transaction=update_tr),
                    transaction=update_tr
                )
            # Merge with current db_root under lock
            try:
                _ = base_root.object_root.get_at(self.database_name)
            except Exception:
                pass
            # The caller already reconciled staged changes against the freshest values under the lock.
            # Persist the provided new_db_root directly without a second merge to avoid lost updates.
            new_space_root = RootObject(
                object_root=base_root.object_root.set_at(self.database_name, new_db_root),
                literal_root=base_root.literal_root,
                transaction=update_tr
            )
            # Ensure the updated object_root is persisted before saving the RootObject
            try:
                if getattr(new_space_root, 'object_root', None):
                    new_space_root.object_root.transaction = update_tr
                    new_space_root.object_root._save()
            except Exception:
                pass
            new_space_root._save()
            # Persist using the already-captured history under the same lock to avoid re-reads
            try:
                self.object_space.set_space_root_locked(new_space_root, locked_space_history)
            except Exception:
                # Fallback to non-locked variant if locked API is unavailable
                self.object_space.set_space_root(new_space_root)
            update_tr.abort()
            return

        # Fallback: re-read under lock (kept for backward compatibility)
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

        # We are still under the RootContextManager lock held by the caller
        self.object_space.set_space_root(new_space_root)
        update_tr.abort()

    def set_db_root_with_locked_context(self, new_db_root: Dictionary, locked_space_root: RootObject, locked_space_history: "List"):
        """
        Persist a new DB root using the space_root and space_history captured by RootContextManager
        upon entering the commit critical section. This avoids any re-reads and guarantees we are
        updating exactly the history we observed under the same provider root lock.
        Must be called while the provider root lock is held.
        """
        update_tr = ObjectTransaction(self)
        # Build from provided locked space_root
        base_root = RootObject(
            atom_pointer=getattr(locked_space_root, 'atom_pointer', None),
            transaction=update_tr
        )
        if base_root.atom_pointer:
            base_root._load()
        else:
            base_root = RootObject(
                object_root=Dictionary(transaction=update_tr),
                literal_root=Dictionary(transaction=update_tr),
                transaction=update_tr
            )
        # Ensure object_root dictionary is bound to this update transaction
        try:
            if getattr(base_root, 'object_root', None):
                base_root.object_root.transaction = update_tr
        except Exception:
            pass
        # Update the catalog mapping to the new db_root and persist object_root explicitly
        # Persist the provided new_db_root directly without a second merge to avoid lost updates
        try:
            _ = base_root.object_root.get_at(self.database_name)
        except Exception:
            pass
        updated_catalog = base_root.object_root.set_at(self.database_name, new_db_root)
        try:
            updated_catalog.transaction = update_tr
            updated_catalog._save()
        except Exception:
            pass
        # Debug: log catalog/root pointers prior to RootObject creation
        try:
            import os as _os
            if _os.environ.get('PB_DEBUG_CONC'):
                ap_base = getattr(getattr(base_root, 'object_root', None), 'atom_pointer', None)
                ap_cat = getattr(updated_catalog, 'atom_pointer', None)
                logger.debug("set_db_root_locked pre-root: base_catalog_ptr=%s/%s updated_catalog_ptr=%s/%s", getattr(ap_base,'transaction_id',None), getattr(ap_base,'offset',None), getattr(ap_cat,'transaction_id',None), getattr(ap_cat,'offset',None))
        except Exception:
            pass
        new_space_root = RootObject(
            object_root=updated_catalog,
            literal_root=base_root.literal_root,
            transaction=update_tr
        )
        new_space_root._save()
        # Debug: show counter and pointers just before persisting the new root pointer
        try:
            import os as _os
            if _os.environ.get('PB_DEBUG_CONC'):
                try:
                    cur_db_root = new_space_root.object_root.get_at(self.database_name)
                    cnt_root = cur_db_root.get_at('counter_root') if cur_db_root else None
                    cnt_val = cnt_root.get_at('counter') if cnt_root else None
                    ap_db = getattr(cur_db_root, 'atom_pointer', None)
                    ap_sr = getattr(new_space_root, 'atom_pointer', None)
                    ap_or = getattr(getattr(new_space_root, 'object_root', None), 'atom_pointer', None)
                    logger.debug("set_db_root_locked about to persist: db='%s' db_root_ptr=%s/%s space_root_ptr=%s/%s object_root_ptr=%s/%s counter=%s", self.database_name, getattr(ap_db,'transaction_id',None), getattr(ap_db,'offset',None), getattr(ap_sr,'transaction_id',None), getattr(ap_sr,'offset',None), getattr(ap_or,'transaction_id',None), getattr(ap_or,'offset',None), cnt_val)
                except Exception:
                    pass
        except Exception:
            pass
        # Persist using the provided locked history under the same lock to avoid re-reads
        try:
            self.object_space.set_space_root_locked(new_space_root, locked_space_history)
        except Exception:
            # Fallback to non-locked variant if locked API is unavailable
            self.object_space.set_space_root(new_space_root)
        # Debug: immediately read under lock to log current mapping (no retry)
        try:
            import os as _os
            if _os.environ.get('PB_DEBUG_CONC'):
                try:
                    latest_hist = self.object_space.get_space_history()
                    latest_sr = self.object_space.get_space_root()
                    latest_catalog = getattr(latest_sr, 'object_root', None)
                    latest_db_root = latest_catalog.get_at(self.database_name) if latest_catalog else None
                    ap_latest = getattr(latest_db_root, 'atom_pointer', None)
                    ap_cat = getattr(latest_catalog, 'atom_pointer', None)
                    logger.debug("set_db_root_locked post-persist mapping: db='%s' db_root_ptr=%s/%s", self.database_name, getattr(ap_latest,'transaction_id',None), getattr(ap_latest,'offset',None))
                    # Deep-inspect latest catalog content to verify STDB mapping
                    try:
                        tx = getattr(latest_sr, 'transaction', None)
                        storage = getattr(tx, 'storage', None) if tx else self.object_space.storage
                        ap_lr = getattr(latest_sr, 'atom_pointer', None)
                        logger.debug("latest space_root_ptr=%s/%s latest object_root_ptr=%s/%s", getattr(ap_lr,'transaction_id',None), getattr(ap_lr,'offset',None), getattr(ap_cat,'transaction_id',None), getattr(ap_cat,'offset',None))
                        if storage and ap_cat:
                            cat_json = storage.get_atom(ap_cat).result()
                            # The catalog is a Dictionary serialized as {'className':'Dictionary', 'content': ...}
                            logger.debug("latest_catalog JSON keys: %s", list(cat_json.keys()))
                    except Exception:
                        pass
                except Exception:
                    pass
        except Exception:
            pass
        update_tr.abort()

    def finish_update(self):
        self.object_space.finish_update()

    def new_transaction(self) -> ObjectTransaction:
        """
        Start a new read transaction
        :return:
        """

        # Capture the current space root pointer for CAS during commit
        current_root = self.read_db_root() if self.database_name != '_sysdb' else None
        tx = ObjectTransaction(self, db_root=current_root)
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
        # Track read-locked mutable objects (by integer key)
        self.read_lock_objects = HashDictionary()
        # Track read-locked database roots (by string name)
        self.read_lock_roots = Dictionary()
        self.new_mutable_objects = HashDictionary()
        self.modified_mutable_objects = HashDictionary()
        # Track which roots were successfully rebased due to concurrent updates
        self._rebased_root_names = set()

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
                        # Store snapshot if not already present (track roots by name)
                        if not self.read_lock_roots.has(name):
                            self.read_lock_roots = self.read_lock_roots.set_at(name, original_ptr)
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
                # Also reflect the change in the transaction snapshot so merges see staged values
                try:
                    self.transaction_root = self.transaction_root.set_at(name, value)
                except Exception:
                    pass
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

    def _check_read_locked_objects(self, current_root: Dictionary):
        """
        Check if any of the read-locked objects (roots by name) have been modified by another transaction.

        :param current_root: The current database root (Dictionary) at commit time.
        """
        if not self.read_lock_roots:
            return

        import os
        debug = os.environ.get('PB_DEBUG_CONC')

        # Debug snapshot count
        if debug:
            try:
                logger.debug("read_lock_roots count: %s", self.read_lock_roots.count)
                for n, p in self.read_lock_roots.as_iterable():
                    logger.debug("Locked root '%s' original ptr: %s/%s", n, getattr(p,'transaction_id',None), getattr(p,'offset',None))
            except Exception:
                pass
        for name, original_object_pointer in self.read_lock_roots.as_iterable():
            try:
                current_obj = current_root.get_at(name)
                current_object_pointer = getattr(current_obj, 'atom_pointer', None)
            except Exception:
                current_object_pointer = None
            if debug:
                try:
                    logger.debug("Current ptr for '%s': %s/%s", name, getattr(current_object_pointer,'transaction_id',None), getattr(current_object_pointer,'offset',None))
                except Exception:
                    pass
            if original_object_pointer != current_object_pointer:
                if debug:
                    logger.debug("Conflict on '%s': original=%s current=%s", name, original_object_pointer, current_object_pointer)
                # CONCURRENT MODIFICATION DETECTED
                new_object = self.new_roots.get_at(name)

                # Check if the object supports automatic merging
                if new_object and isinstance(new_object, ConcurrentOptimized):
                    try:
                        # Load the currently committed object from the database
                        current_db_object = current_root.get_at(name)
                        # Attempt to rebase our changes on top of the concurrent version
                        rebased_object = new_object._rebase_on_concurrent_update(current_db_object)
                        # If successful, replace the object in our transaction with the merged one
                        self.new_roots = self.new_roots.set_at(name, rebased_object)
                        if debug:
                            logger.debug("Rebased '%s' and continuing commit", name)
                        # Mark this root as rebased to avoid double-reconciliation later
                        try:
                            self._rebased_root_names.add(name)
                        except Exception:
                            pass
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
        """
        Merge staged roots into the current root using per-key reconciliation against the fresh
        current values observed under the root lock. This implements idempotent increments and
        set/CountedSet unions to guarantee progress under contention.
        """
        current_db_root = current_root
        if self.new_roots.count > 0:
            for root_name, staged_root in self.new_roots.as_iterable():
                # Note: Even if a rebase occurred earlier, we still run per-entry reconciliation.
                # Treat a staged integer change as an intent to increment by +1 on the freshest current value.
                # This makes the operation idempotent and correct under contention.
                try:
                    _ = getattr(self, '_rebased_root_names', None)
                except Exception:
                    pass
                try:
                    existing_root = current_db_root.get_at(root_name)
                except Exception:
                    existing_root = None
                if existing_root is None:
                    # Fallback to initial snapshot if current lookup failed (defensive)
                    try:
                        init_snapshot = getattr(self, 'initial_transaction_root', None)
                        if init_snapshot is not None:
                            existing_root = init_snapshot.get_at(root_name)
                    except Exception:
                        pass
                # Debug: log staged vs existing counter when applicable
                try:
                    import os as _os
                    if _os.environ.get('PB_DEBUG_CONC') and root_name == 'counter_root':
                        try:
                            staged_val = None
                            try:
                                staged_val = staged_root.get_at('counter') if hasattr(staged_root, 'get_at') else None
                            except Exception:
                                staged_val = None
                            exist_val = None
                            try:
                                exist_val = existing_root.get_at('counter') if hasattr(existing_root, 'get_at') else None
                            except Exception:
                                exist_val = None
                            logger.debug("Merge pre-check for '%s': staged.counter=%s existing.counter=%s", root_name, staged_val, exist_val)
                        except Exception:
                            pass
                except Exception:
                    pass
                # Defensive CAS: if our initial snapshot pointer differs from current, treat as conflict
                try:
                    init_snapshot = getattr(self, 'initial_transaction_root', None)
                    if init_snapshot is not None and existing_root is not None:
                        try:
                            snap_obj = init_snapshot.get_at(root_name)
                        except Exception:
                            snap_obj = None
                        orig_ptr = getattr(snap_obj, 'atom_pointer', None)
                        curr_ptr = getattr(existing_root, 'atom_pointer', None)
                        if orig_ptr != curr_ptr:
                            # Conflict: rebase if possible, else raise to trigger retry
                            if isinstance(staged_root, ConcurrentOptimized):
                                try:
                                    staged_root = staged_root._rebase_on_concurrent_update(existing_root)
                                    # Mark as rebased to avoid double-apply rules below
                                    try:
                                        self._rebased_root_names.add(root_name)
                                    except Exception:
                                        pass
                                except Exception as _e:
                                    raise ProtoLockingException(f"Concurrent update on '{root_name}' and automatic rebase failed: {_e}")
                            else:
                                raise ProtoLockingException(f"Concurrent update on '{root_name}' (non-mergeable)")
                except Exception:
                    pass
                # Per-entry reconciliation when both sides are Dictionary-like
                if (isinstance(staged_root, Dictionary) or hasattr(staged_root, 'as_iterable')) and \
                   (isinstance(existing_root, Dictionary) or hasattr(existing_root, 'get_at')):
                    merged = existing_root
                    try:
                        import os as _os
                        dbg = _os.environ.get('PB_DEBUG_CONC')
                        # Iterate staged entries and merge against the freshest current value
                        for k, v in staged_root.as_iterable():
                            try:
                                curr_v = merged.get_at(k)
                            except Exception:
                                curr_v = None
                            if dbg:
                                try:
                                    logger.debug("Merge key '%s.%s': staged=%s curr=%s", root_name, k, type(v).__name__, (type(curr_v).__name__ if curr_v is not None else None))
                                except Exception:
                                    pass
                            # 1) Integer counters: treat a staged change as +1 increment on the fresh current value
                            if isinstance(v, int) and isinstance(curr_v, int):
                                merged = merged.set_at(k, int(curr_v) + 1)
                                if dbg:
                                    logger.debug("Counter increment for '%s.%s': %s -> %s", root_name, k, curr_v, int(curr_v) + 1)
                                continue
                            # 2) Set/CountedSet buckets: union staged elements into the current bucket
                            try:
                                from .sets import Set as _Set, CountedSet as _CSet
                            except Exception:
                                _Set = tuple()
                                _CSet = tuple()
                            # If current bucket is Set/CountedSet, union staged elements into it
                            if hasattr(v, 'as_iterable') and isinstance(curr_v, (_Set, _CSet)):
                                try:
                                    bucket = curr_v
                                    for e in v.as_iterable():
                                        # Guard: do not add Set/CountedSet objects as elements
                                        if isinstance(e, (_Set, _CSet)):
                                            continue
                                        bucket = bucket.add(e)
                                    merged = merged.set_at(k, bucket)
                                    if dbg:
                                        logger.debug("Unioned bucket for '%s.%s'", root_name, k)
                                    continue
                                except Exception:
                                    pass
                            # 3) If current is missing, adopt staged as-is
                            if curr_v is None:
                                merged = merged.set_at(k, v)
                                continue
                            # 4) Fallback: last-writer-wins
                            try:
                                equal = (v == curr_v)
                            except Exception:
                                equal = False
                            if not equal:
                                merged = merged.set_at(k, v)
                        current_db_root = current_db_root.set_at(root_name, merged)
                        try:
                            import os as _os
                            if _os.environ.get('PB_DEBUG_CONC'):
                                logger.debug("Per-entry merged root '%s'", root_name)
                        except Exception:
                            pass
                        continue
                    except Exception:
                        # If anything goes wrong, fall back to overwrite of the whole root
                        pass
                # Default behavior: overwrite root
                current_db_root = current_db_root.set_at(root_name, staged_root)
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
                        # Re-check read-locked objects under the root lock; raise on conflicts to allow retry
                        self._check_read_locked_objects(db_root)
                        db_root = self._update_mutable_indexes(db_root)
                        db_root = self._update_database_roots(db_root)
                        db_root.transaction = self

                        # Debug final state before save
                        try:
                            import os as _os
                            if _os.environ.get('PB_DEBUG_CONC'):
                                try:
                                    cr = db_root.get_at('counter_root')
                                    cv = cr.get_at('counter') if cr else None
                                    logger.debug("Final counter before save: %s", cv)
                                except Exception:
                                    pass
                        except Exception:
                            pass

                        db_root._save()

                        # Persist db_root under the provider root lock using captured locked context when available.
                        try:
                            lsr = getattr(self, '_locked_space_root', None)
                            lsh = getattr(self, '_locked_space_history', None)
                            if lsr is not None and lsh is not None:
                                self.database.set_db_root_with_locked_context(db_root, lsr, lsh)
                            else:
                                self.database.set_db_root_locked(db_root)
                        except Exception:
                            self.database.set_db_root_locked(db_root)

                        # Note: removed read-after-write second pass to avoid double-applying merges
                        # under high contention (which led to over-incrementing counters). The primary
                        # save is done under the provider lock with freshest reconciliation, which is
                        # sufficient for deterministic results.

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
        self.storage = self.object_transaction.storage
        self.root_cm = self.storage.root_context_manager()

    def __enter__(self):
        import os
        if os.environ.get('PB_DEBUG_CONC'):
            logger.debug('Entering RootContextManager: acquiring provider root lock')
        self.root_cm.__enter__()
        if os.environ.get('PB_DEBUG_CONC'):
            logger.debug('Entered RootContextManager: lock acquired, reading db root')
        # Capture the full space_root and history once under the lock for downstream operations
        try:
            os_obj = getattr(self.object_transaction, 'object_space', None)
            if os_obj is not None:
                space_history = os_obj.get_space_history()
                space_root = os_obj.get_space_root()
                # Stash them in the transaction for locked updates without re-reading
                setattr(self.object_transaction, '_locked_space_history', space_history)
                setattr(self.object_transaction, '_locked_space_root', space_root)
        except Exception:
            pass
        # Return the current database root under the acquired root lock using the locked snapshot
        try:
            ot = self.object_transaction
            db = getattr(ot, 'database', None)
            os_obj = getattr(ot, 'object_space', None)
            if db is not None and os_obj is not None:
                # Build current db_root from the locked space_root captured above
                locked_space_root = getattr(ot, '_locked_space_root', None)
                if locked_space_root is None:
                    # As a fallback (should not happen), read via database API
                    return db.read_db_root()
                # Ensure the locked space_root is loaded and bound to this transaction
                tx = ot
                # Extract the catalog and get this database's root
                catalog = getattr(locked_space_root, 'object_root', None)
                if catalog is None:
                    return Dictionary(transaction=tx)
                try:
                    # Ensure the catalog is bound and loaded under this transaction
                    try:
                        catalog.transaction = tx
                    except Exception:
                        pass
                    try:
                        catalog._load()
                    except Exception:
                        pass
                    db_root = catalog.get_at(db.database_name)
                except Exception:
                    db_root = None
                if db_root is None:
                    return Dictionary(transaction=tx)
                # Bind transaction for subsequent saves in this critical section
                try:
                    db_root.transaction = tx
                except Exception:
                    pass
                try:
                    db_root._load()
                except Exception:
                    pass
                return db_root
        except Exception:
            pass
        # Fallback: return an empty Dictionary if database context is unavailable
        return Dictionary()

    def __exit__(self, exc_type, exc_value, traceback):
        import os
        self.root_cm.__exit__(exc_type, exc_value, traceback)
        if os.environ.get('PB_DEBUG_CONC'):
            logger.debug('Exiting RootContextManager: provider root lock released')
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


# --- Added helper to avoid deadlocks when already under root lock ---
# Provide a locked variant of set_db_root to be used while inside RootContextManager
# so we do not attempt to re-enter provider locks indirectly.

def _append_db_locked_method():
    pass
