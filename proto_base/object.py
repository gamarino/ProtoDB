"""
Common objects


"""
import datetime
from concurrent.futures import Future, ThreadPoolExecutor
import uuid
import base64
import json
from abc import ABC, abstractmethod

from enum import Enum

class ObjectStorage(object):
    atom_storage: AtomStorage

    def __init__(self, atom_storage: AtomStorage):
        self.atom_storage = atom_storage

    def new_transaction(self, transaction_type: TransactionType = TransactionType.READ_ONLY) -> ObjectTransaction:
        raise NotImplemented()


class BufferedStream(object):
    chunk_size: int = 0
    chunk: bytes
    chunk_offset: int = 0
    ot: ObjectTransaction

    def __init__(self, chunk_size: int, ot: ObjectTransaction, obj_id: int):
        self.chunk_size = chunk_size
        self.ot = ot
        self.chunk = None
        self.transaction_id, self.current_offset = self.ot.root_object['objects_by_id'][obj_id] or (None, None)
        if self.transaction_id is None:
            raise ValueError(
                f"Object {obj_id} not found!"
            )

    def read(self, size: int) -> bytes:
        output_data = bytearray(size)
        current_count = 0
        while current_count < size:
            if not self.chunk or self.chunk_offset >= self.chunk_size:
                self.chunk = self.ot.atom_transaction.storage_transaction.read_data(
                    self.transaction_id,
                    self.current_offset,
                    self.chunk_size
                ).result()
                self.current_offset += self.chunk_size
                self.chunk_offset = 0
            available_count = min(self.chunk_size - self.chunk_offset, size)
            output_index = current_count
            source_index = self.current_offset
            for count in range(available_count):
                output_data[output_index + count] = self.chunk[source_index + count]
            current_count += available_count
            self.current_offset += available_count
        return output_data


class DBObject(object):
    transaction: ObjectTransaction = None
    id: int = None
    attributes: dict = None
    modified_attributes: set[str] = None
    byte_pointers: dict = None
    parent = None
    is_dirty: bool = False
    offset: int = 0
    transaction_id: uuid.UUID = None
    promise: Future = None

    def __init__(self, transaction, new_id: int = None):
        self.transaction = transaction

        self.attributes = {}
        self.modified_attributes = set()
        self.byte_pointers = {}
        self.is_dirty = False
        if not new_id:
            self.id = transaction.new_id()
        else:
            self.id = new_id

    def __getitem__(self, item: str):
        if self.id:
            transaction_id, off = self.transaction.get_pointer_by_id(self.id)


            if transaction_id:
                buffered_stream = BufferedStream(64, self.transaction)
                dict_readed = json.load(buffered_stream)
            else:
                self.attributes = dict()

        obj = self
        while obj and item not in obj.attributes:
            obj = obj.parent

        return obj.attributes.get(item) if obj else None

    def __setitem__(self, key: str, value):
        self.attributes[key] = value
        self.is_dirty = True
        self.modified_attributes.add(key)
        if self.id:
            # It is an existing object, thus it is now a modified object
            self.transaction.modified_objects.add(self.id)

    def dump_value(self, value, at: AtomTransaction) -> tuple:
        if isinstance(value, DBInmutable):
            if not value.transaction_id:
                value.dump_object(at)
            return 0, (self.transaction_id.int, value.offset)
        elif isinstance(value, DBObject):
            if not value.id:
                value.id = at.storage_transaction.new_id()
            return 1, value.id
        elif isinstance(value, int):
            return 2, value
        elif isinstance(value, float):
            return 3, value
        elif isinstance(value, datetime.datetime):
            return 4, str(value.isoformat())
        elif isinstance(value, datetime.date):
            return 5, str(value.isoformat())
        elif isinstance(value, str):
            return 6, value
        elif isinstance(value, bool):
            return 8, value
        elif value is None:
            return 9, None
        else:
            raise ProtoNotSupportedException(
                f"Unsupported attribute value type {type(value)}"
            )

    def dump_object(self, at: AtomTransaction):
        # Instantiate new objects in attributes
        for att_name, att_value in self.attributes.items():
            if isinstance(att_value, DBObject) and not att_value.id:
                att_value.dump_object(at)

        if self.is_dirty:
            dict_to_dump = {}
            for att_name, att_value in self.attributes.items():
                if isinstance(att_value, bytes):
                    if att_name in self.modified_attributes or att_name not in self.byte_pointers:
                        data_offset = at.storage_transaction.dump_buffer(base64.encodebytes(att_value)).result()
                        t_id = self.atom_transaction.storage_transaction.get_id()
                    else:
                        t_id, data_offset = self.byte_pointers[att_name]
                    dict_to_dump[att_name] = (7, (t_id, data_offset))
                else:
                    dict_to_dump[att_name] = self.dump_value(att_value, at)

            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({self.id: dict_to_dump}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()

        return self.transaction_id, self.offset


class DBInmutable(DBObject):
    object_pointer: AtomPointer = None


class DBList(DBInmutable):
    next = None
    prev = None
    order: int = 0
    value = None
    count = 0
    height = 0

    def dump_object(self, at: AtomTransaction):
        if self.transaction_id is None:
            dict_to_dump = dict(
                next=(0, (self.next.transaction_id.int, self.next.offset)) if self.next else None,
                prev=(0, (self.prev.transaction_id.int, self.prev.offset)) if self.prev else None,
                order=self.order,
                count=self.count,
                height=self.height,
            )
            dict_to_dump['value'] = self.dump_value(self.value)

            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({'DBList': dict_to_dump}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()

    def get_at(self, index: int):
        if index < 0:
            index = self.count + index if index < self.count else 0

        if index >= self.count:
            return None

        node = self
        while node is not None:
            if node.order == index:
                return node.value
            if index > node.order:
                node = node.next
            else:
                node = node.prev

        return None

    def set_at(self, item_name, value):
        return DBList(self.transaction)


class DBMap(DBInmutable):
    next = None
    prev = None
    key: int = None
    value = None
    count = 0
    height = 0

    def dump_object(self, at: AtomTransaction):
        if self.transaction_id is None:
            dict_to_dump = dict(
                next=(0, (self.next.transaction_id.int, self.next.offset)) if self.next else None,
                prev=(0, (self.prev.transaction_id.int, self.prev.offset)) if self.prev else None,
                key=self.key or None,
                count=self.count,
                height=self.height,
            )
            dict_to_dump['value'] = self.dump_value(self.value)

            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({'DBMap': dict_to_dump}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()

    def get_at(self, item_name: int):
        node = self
        while node is not None:
            if node.key == item_name:
                return node.value
            if item_name > node.key:
                node = node.next
            else:
                node = node.prev

        return None

    def set_at(self, item_name, value):
        return DBMap(self.transaction)


class DBSet(DBMap):
    next = None
    prev = None
    key: int = None
    value = None
    count = 0
    height = 0

    def dump_object(self, at: AtomTransaction):
        if self.transaction_id is None:
            dict_to_dump = dict(
                next=(0, (self.next.transaction_id.int, self.next.offset)) if self.next else None,
                prev=(0, (self.prev.transaction_id.int, self.prev.offset)) if self.prev else None,
                key=self.key or None,
                count=self.count,
                height=self.height,
            )
            dict_to_dump['value'] = self.dump_value(self.value)

            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({'DBSet': dict_to_dump}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()

    def contains(self, obj):
        key = None
        if isinstance(obj, DBObject):
            key = obj.id
        else:
            key = obj.get_hash()

        node = self
        while node is not None:
            if node.hash == key:
                return True
            if key > node.hash:
                node = node.next
            else:
                node = node.prev

        return False

    def add(self, obj):
        key = None
        if isinstance(obj, DBObject):
            key = obj.id
        else:
            key = obj.get_hash()

        return DBSet(self.transaction)


class DBLiteral(DBInmutable):
    literal_value: str = None

    def dump_object(self, at: AtomTransaction):
        if self.transaction_id is None:
            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({'DBLiteral': (self.id, self.literal_value)}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()


class DBLiteralDict(DBInmutable):
    next = None
    prev = None
    key: DBLiteral = None
    count = 0
    height = 0

    def dump_object(self, at: AtomTransaction):
        if self.is_dirty or self.transaction_id is None:
            dict_to_dump = dict(
                next=(0, (self.next.transaction_id.int, self.next.offset)) if self.next else None,
                prev=(0, (self.prev.transaction_id.int, self.prev.offset)) if self.prev else None,
                key=self.dump_value(self.key, at),
                count=self.count,
                height=self.height,
            )

            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({'DBLiteralDict': dict_to_dump}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()

    def __getattr__(self, key: str):
        node = self
        while node is not None:
            if node.literal_value == key:
                return node.value
            if key > node.key:
                node = node.next
            else:
                node = node.prev

        return None

    def __setitem__(self, key, value):
        self.is_dirty = True
        return DBLiteralDict(self.transaction)


class DBDict(DBInmutable):
    back_map: DBMap = None

    def __init__(self, transaction: ObjectTransaction):
        super().__init__(transaction)
        self.back_map = DBMap(self.transaction)

    def dump_object(self, at: AtomTransaction):
        if self.transaction_id is None or self.is_dirty:
            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({'DBDict': (self.transaction_id.int, self.offset)}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()

    def __getattr__(self, key: str):
        return self.back_map.get_at(self.transaction.get_literal(key).hash())

    def __setitem__(self, key, value):
        self.back_map = self.back_map.set_at(self.transaction.get_literal(key).hash(), value)
        self.is_dirty = True
class TransactionType(Enum):
    READ_ONLY = 0
    READ_WRITE = 1


class ObjectTransaction(object):
    atom_transaction: AtomTransaction
    transaction_type: TransactionType
    readed_objects = {}
    modified_objects = set()
    added_objects = set()
    root_pointer: AtomPointer = None
    root_object = None

    def __init__(self, atom_transaction: AtomTransaction):
        self.atom_transaction = atom_transaction
        self.root_pointer = atom_transaction.atom_storage.shared_storage.read_current_root().result()
        if self.root_pointer is None:
            self.root_object = DBObject(self)
        else:
            self.root_object = atom_transaction.get_atom(
                self.root_pointer
            )

    def read_data(self, transaction_id: uuid.UUID, offset: int, size:int) -> Future[bytes]:
        ## TODO leer del físico por páginas y devolver un bytearray
        return bytearray(size)

    def get_root(self, root_name: str):
        return self.root_object

    def set_root(self, root_name: str, obj):
        self.root_object[root_name] = obj

    def new_id(self):
        return uuid.uuid4().int

    def add_object(self, obj):
        if not obj.id:
            obj.id = self.new_id()
        self.added_objects.add(obj)

    def abort(self):
        raise NotImplemented()

    def commit(self, ret_value):
        if self.transaction_type == TransactionType.READ_WRITE:
            at = self.atom_transaction

            for obj in self.added_objects:
                obj.dump_object(at)

            for obj in self.modified_objects:
                obj.dump_object(at)

            if self.root_object.is_dirty:
                self.root_object.offset = self.root_object.dump_object(at)
                self.root_object.transaction_id = self.atom_transaction.storage_transaction.get_id()

            at.close()

            ###### Start of critical section for commit
            ## At this point all transaction changes are stored in physical storage
            ## All modified objects should have updated pointers
            ## Added objects have IDs to be added with the new pointers
            ## Root object could be dirty, in that case with an updated pointer

            # in the critical section, you should open a new transaction to update the main id mapping to pointers
            # If you fail acquiring the exclusive lock of the main mapping, try again a limited number of retries

            ## TODO


class ObjectStorage(object):
    atom_storage: AtomStorage

    def __init__(self, atom_storage: AtomStorage):
        self.atom_storage = atom_storage

    def new_transaction(self, transaction_type: TransactionType = TransactionType.READ_ONLY) -> ObjectTransaction:
        raise NotImplemented()


class BufferedStream(object):
    chunk_size: int = 0
    chunk: bytes
    chunk_offset: int = 0
    ot: ObjectTransaction

    def __init__(self, chunk_size: int, ot: ObjectTransaction, obj_id: int):
        self.chunk_size = chunk_size
        self.ot = ot
        self.chunk = None
        self.transaction_id, self.current_offset = self.ot.root_object['objects_by_id'][obj_id] or (None, None)
        if self.transaction_id is None:
            raise ValueError(
                f"Object {obj_id} not found!"
            )

    def read(self, size: int) -> bytes:
        output_data = bytearray(size)
        current_count = 0
        while current_count < size:
            if not self.chunk or self.chunk_offset >= self.chunk_size:
                self.chunk = self.ot.atom_transaction.storage_transaction.read_data(
                    self.transaction_id,
                    self.current_offset,
                    self.chunk_size
                ).result()
                self.current_offset += self.chunk_size
                self.chunk_offset = 0
            available_count = min(self.chunk_size - self.chunk_offset, size)
            output_index = current_count
            source_index = self.current_offset
            for count in range(available_count):
                output_data[output_index + count] = self.chunk[source_index + count]
            current_count += available_count
            self.current_offset += available_count
        return output_data


class DBObject(object):
    transaction: ObjectTransaction = None
    id: int = None
    attributes: dict = None
    modified_attributes: set[str] = None
    byte_pointers: dict = None
    parent = None
    is_dirty: bool = False
    offset: int = 0
    transaction_id: uuid.UUID = None
    promise: Future = None

    def __init__(self, transaction, new_id: int = None):
        self.transaction = transaction

        self.attributes = {}
        self.modified_attributes = set()
        self.byte_pointers = {}
        self.is_dirty = False
        if not new_id:
            self.id = transaction.new_id()
        else:
            self.id = new_id

    def __getitem__(self, item: str):
        if self.id:
            transaction_id, off = self.transaction.get_pointer_by_id(self.id)


            if transaction_id:
                buffered_stream = BufferedStream(64, self.transaction)
                dict_readed = json.load(buffered_stream)
            else:
                self.attributes = dict()

        obj = self
        while obj and item not in obj.attributes:
            obj = obj.parent

        return obj.attributes.get(item) if obj else None

    def __setitem__(self, key: str, value):
        self.attributes[key] = value
        self.is_dirty = True
        self.modified_attributes.add(key)
        if self.id:
            # It is an existing object, thus it is now a modified object
            self.transaction.modified_objects.add(self.id)

    def dump_value(self, value, at: AtomTransaction) -> tuple:
        if isinstance(value, DBInmutable):
            if not value.transaction_id:
                value.dump_object(at)
            return 0, (self.transaction_id.int, value.offset)
        elif isinstance(value, DBObject):
            if not value.id:
                value.id = at.storage_transaction.new_id()
            return 1, value.id
        elif isinstance(value, int):
            return 2, value
        elif isinstance(value, float):
            return 3, value
        elif isinstance(value, datetime.datetime):
            return 4, str(value.isoformat())
        elif isinstance(value, datetime.date):
            return 5, str(value.isoformat())
        elif isinstance(value, str):
            return 6, value
        elif isinstance(value, bool):
            return 8, value
        elif value is None:
            return 9, None
        else:
            raise ProtoNotSupportedException(
                f"Unsupported attribute value type {type(value)}"
            )

    def dump_object(self, at: AtomTransaction):
        # Instantiate new objects in attributes
        for att_name, att_value in self.attributes.items():
            if isinstance(att_value, DBObject) and not att_value.id:
                att_value.dump_object(at)

        if self.is_dirty:
            dict_to_dump = {}
            for att_name, att_value in self.attributes.items():
                if isinstance(att_value, bytes):
                    if att_name in self.modified_attributes or att_name not in self.byte_pointers:
                        data_offset = at.storage_transaction.dump_buffer(base64.encodebytes(att_value)).result()
                        t_id = self.atom_transaction.storage_transaction.get_id()
                    else:
                        t_id, data_offset = self.byte_pointers[att_name]
                    dict_to_dump[att_name] = (7, (t_id, data_offset))
                else:
                    dict_to_dump[att_name] = self.dump_value(att_value, at)

            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({self.id: dict_to_dump}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()

        return self.transaction_id, self.offset


class DBInmutable(DBObject):
    object_pointer: AtomPointer = None


class DBList(DBInmutable):
    next = None
    prev = None
    order: int = 0
    value = None
    count = 0
    height = 0

    def dump_object(self, at: AtomTransaction):
        if self.transaction_id is None:
            dict_to_dump = dict(
                next=(0, (self.next.transaction_id.int, self.next.offset)) if self.next else None,
                prev=(0, (self.prev.transaction_id.int, self.prev.offset)) if self.prev else None,
                order=self.order,
                count=self.count,
                height=self.height,
            )
            dict_to_dump['value'] = self.dump_value(self.value)

            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({'DBList': dict_to_dump}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()

    def get_at(self, index: int):
        if index < 0:
            index = self.count + index if index < self.count else 0

        if index >= self.count:
            return None

        node = self
        while node is not None:
            if node.order == index:
                return node.value
            if index > node.order:
                node = node.next
            else:
                node = node.prev

        return None

    def set_at(self, item_name, value):
        return DBList(self.transaction)


class DBMap(DBInmutable):
    next = None
    prev = None
    key: int = None
    value = None
    count = 0
    height = 0

    def dump_object(self, at: AtomTransaction):
        if self.transaction_id is None:
            dict_to_dump = dict(
                next=(0, (self.next.transaction_id.int, self.next.offset)) if self.next else None,
                prev=(0, (self.prev.transaction_id.int, self.prev.offset)) if self.prev else None,
                key=self.key or None,
                count=self.count,
                height=self.height,
            )
            dict_to_dump['value'] = self.dump_value(self.value)

            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({'DBMap': dict_to_dump}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()

    def get_at(self, item_name: int):
        node = self
        while node is not None:
            if node.key == item_name:
                return node.value
            if item_name > node.key:
                node = node.next
            else:
                node = node.prev

        return None

    def set_at(self, item_name, value):
        return DBMap(self.transaction)


class DBSet(DBMap):
    next = None
    prev = None
    key: int = None
    value = None
    count = 0
    height = 0

    def dump_object(self, at: AtomTransaction):
        if self.transaction_id is None:
            dict_to_dump = dict(
                next=(0, (self.next.transaction_id.int, self.next.offset)) if self.next else None,
                prev=(0, (self.prev.transaction_id.int, self.prev.offset)) if self.prev else None,
                key=self.key or None,
                count=self.count,
                height=self.height,
            )
            dict_to_dump['value'] = self.dump_value(self.value)

            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({'DBSet': dict_to_dump}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()

    def contains(self, obj):
        key = None
        if isinstance(obj, DBObject):
            key = obj.id
        else:
            key = obj.get_hash()

        node = self
        while node is not None:
            if node.hash == key:
                return True
            if key > node.hash:
                node = node.next
            else:
                node = node.prev

        return False

    def add(self, obj):
        key = None
        if isinstance(obj, DBObject):
            key = obj.id
        else:
            key = obj.get_hash()

        return DBSet(self.transaction)


class DBLiteral(DBInmutable):
    literal_value: str = None

    def dump_object(self, at: AtomTransaction):
        if self.transaction_id is None:
            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({'DBLiteral': (self.id, self.literal_value)}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()


class DBLiteralDict(DBInmutable):
    next = None
    prev = None
    key: DBLiteral = None
    count = 0
    height = 0

    def dump_object(self, at: AtomTransaction):
        if self.is_dirty or self.transaction_id is None:
            dict_to_dump = dict(
                next=(0, (self.next.transaction_id.int, self.next.offset)) if self.next else None,
                prev=(0, (self.prev.transaction_id.int, self.prev.offset)) if self.prev else None,
                key=self.dump_value(self.key, at),
                count=self.count,
                height=self.height,
            )

            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({'DBLiteralDict': dict_to_dump}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()

    def __getattr__(self, key: str):
        node = self
        while node is not None:
            if node.literal_value == key:
                return node.value
            if key > node.key:
                node = node.next
            else:
                node = node.prev

        return None

    def __setitem__(self, key, value):
        self.is_dirty = True
        return DBLiteralDict(self.transaction)


class DBDict(DBInmutable):
    back_map: DBMap = None

    def __init__(self, transaction: ObjectTransaction):
        super().__init__(transaction)
        self.back_map = DBMap(self.transaction)

    def dump_object(self, at: AtomTransaction):
        if self.transaction_id is None or self.is_dirty:
            self.offset = at.storage_transaction.dump_buffer(
                json.dumps({'DBDict': (self.transaction_id.int, self.offset)}).encode('UTF-8')
            ).result()
            self.transaction_id = at.storage_transaction.get_id()

    def __getattr__(self, key: str):
        return self.back_map.get_at(self.transaction.get_literal(key).hash())

    def __setitem__(self, key, value):
        self.back_map = self.back_map.set_at(self.transaction.get_literal(key).hash(), value)
        self.is_dirty = True
