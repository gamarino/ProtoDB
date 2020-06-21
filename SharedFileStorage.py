from . import Exceptions
from . import Common
from . import TransactionalStorage
import asyncio
from aiofile import AIOFile
import os
import json
import random
import uuid
import fcntl
from datetime import datetime

ATTEMPTS_ON_KNOWN = 3
ATTEMPTS_ON_NEW = 1


class SFObjectID(TransactionalStorage.ObjectID):
    """
    A shared file identification of any object stored in the database

    """
    partition_name = None
    position = None

    def __init__(self, partition_name, position):
        self.partition_name = partition_name
        self.position = position


class SFStorage(TransactionalStorage.TransactionalStorage):
    """
    A shared file transactional Storage
    An SFStorage is a set of files under a baseDirectory
    One of these files is named ROOT and contains all root's IDs from the creation of the SFStorage till now
    All other files are called Partitions. A transaction use allways just and only one Partition to store
    objects in an append only strategy.
    Only one transaction could use a Partition at the time. So, when a new a transaction is created, a not in use
    Partitions should be find and seized.
    In order to find a not in use Partition, first the local list of knowPartitions is used to try to seize one
    Partition. It is searched in a random search over the known Partitions, taking into the local in use list
    If an already known partition is found, it is attempted to be open using the file system for append(exclusive use)
    If other nodes of the same systems are using the Partition, the opening of the Partition will fail.
    If a Partition could be opened, it will be assigned to transaction till completion
    A number of attempts are tryed in order to find a not in use Partition
    If not unseized Partition is found, a final attempt is made to read all Partitions names, just in case
    other Partitions were created by other nodes of the system and this node was not aware of the new Partitions.
    If there are new Partitions, a final attempt is made on the newly aquired Partitions names.
    Again after a preset number of attempts no Partition is found, then a new Partition will be created and the
    transaction will be assigned to the newly created Partition
    All IO operations will be performed using asynch operation, to maximize the usage of the CPU
    """

    base_directory = None
    current_root = None
    known_partitions = {}
    in_use_partions = set()

    def __init__(self, base_directory, create_if_not_exists=False):
        super().__init__()
        self.base_directory = base_directory

        try:
            self.open_existing()
        except Exception as e:
            if create_if_not_exists:
                self.create()
            else:
                raise e

        self.refresh_known_partitions()
        self.current_root = self.get_currrent_root()

    def open_existing(self):
        self.get_currrent_root()

    def create(self):
        pass

    def refresh_known_partitions(self):
        pass

    def get_currrent_root(self):
        root_full_name = os.path.join(self.base_directory, 'root.json')
        with open(root_full_name, 'r') as root_file:
            root_control = json.load(root_file)
            if root_control:
                self.current_root = root_control.current_root
            else:
                self.current_root = None
        return self.current_root

    def new_transaction(self):
        """
        Create a new transaction on this storage
        :return: a StorageTransaction on this storage
        """
        known_names = list(self.known_partitions.keys())
        new_transaction = None

        self.get_currrent_root()

        if len(known_names):
            attempt_count = 0
            while not new_transaction and attempt_count < ATTEMPTS_ON_KNOWN:
                partition_index = random.randint(0, len(known_names))
                partition_name = known_names[partition_index]
                try:
                    new_transaction = SFTransaction(self, partition_name)
                except IOError:
                    attempt_count += 1
                    continue

        if not new_transaction:
            if len(known_names):
                attempt_count = 0
                while not new_transaction and attempt_count < ATTEMPTS_ON_NEW:
                    partition_name = str(uuid.uuid4())
                    try:
                        new_transaction = SFTransaction(self, partition_name)
                    except IOError as e:
                        attempt_count += 1
                        if attempt_count >= ATTEMPTS_ON_NEW:
                            raise e
                        continue

        return new_transaction

    def get_object(self, object_id):
        """
        Get an object from the storage

        :param object_id: an ObjectID
        :return: the read object
        """
    def get_partition(self, partition_name: str):
        if partition_name not in self.known_partitions:
            partition_full_name = os.path.join(self.base_directory, partition_name)
            self.known_partitions[partition_name] = open(partition_full_name, 'r')
        return self.known_partitions[partition_name]

class SFTransaction(TransactionalStorage.StorageTransaction):
    """
    A shared file based transaction
    """
    partition_name = None
    partition_file = None
    root_file = None
    current_root = None
    ts = None

    def __init__(self, ts: SFStorage, partition_name: str):
        super().__init__(ts)
        self.ts = ts
        self.partitionName = partition_name
        partition_full_name = os.path.join(self.ts.base_directory, partition_name)
        self.partition_file = AIOFile(partition_full_name, 'a')
        ts.get_currrent_root()
        self.current_root = ts.current_root
        fcntl.flock(self.partition_file, fcntl.LOCK_SH)

    def get_object(self, object_id: str):
        """
        Get an object from the storage
        Implement any strategy to cache previous read values

        :param object_id: an ObjectID
        :return: the read object
        """

        partition_name, partition_offset = object_id.split(':')
        partition_file = self.ts.get_partition(partition_name)
        partition_file.seek(partition_offset)
        return json.loads(partition_file)

    def get_root_object(self):
        """
        Get the rooot object from the storage
        If no root yet, return None

        :return: the read object
        """
        return self.get_object(self.current_root)

    def new_object(self, object_data):
        """
        Store a new object on transaction.
        The returned object ID is globally valid even when this transaction is not ended correctly

        :param object_data:
        :return: the object ID of the newly created object
        """
        if not self.partition_file:
            raise Exceptions.ValidationException('Transaction not active')

        position = self.partition_file.fileno().tell()

        self.partition_file.write(f"/nID %{position}d/n%{json.dumps(object_data)}s")

        return f'{self.partition_name}:{position}'

    def start_root_update(self):
        """
        Start the operation of root updating
        Flush any data not yet written, obtain any lock needed to ensure no one else is doing the same update
        Wait til you are the only process updating the root if necesary.
        OPTIMISTIC locking is also accepted, that means that the check will be made on update
        Update currentRoot to the most updated value

        :return: currentRootID
        """
        if self.partition_file:
            # flush any pending write
            pass

        root_full_name = os.path.join(self.ts.base_directory, 'root.json')

        self.root_file = open(root_full_name, 'w')
        fcntl.flock(self.root_file, fcntl.LOCK_SH)
        self.ts.get_currrent_root()

    def update_root(self, new_root_id: str):
        """
        Update storage root
        Ensure that the root is updated on return or fail if it was unsuccesfull (raise)

        :param new_root_id:
        :return: the object ID of the newly created object
        """

        new_root = object()
        new_root.time = datetime.now()
        new_root.previous_root = str(self.ts.current_root) if self.ts.current_root else ''
        new_root.current_root = new_root_id

        self.root_file.write(json.dumps(new_root))
        self.root_file.close()

        self.current_root = new_root_id
        return new_root_id

    def close(self):
        """
        Terminate transaction use. No new object could be created after this call

        :return: None
        """
        if self.partition_file:
            # TODO Something to free? close is enough?
            self.partition_file.close()
        self.partition_file = None
        self.ts.in_use_partions.remove(self.partition_name)
