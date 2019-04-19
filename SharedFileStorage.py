




from . import Exceptions
from . import Common
from . import TransactionalStorage
import asyncio
from aiofile import AIOFile
import os
import json
import random

ATTEMPTS_ON_KNOWN = 3
ATTEMPTS_ON_NEW = 1


class SFObjectID(TransactionalStorage.ObjectID):
    """
    A shared file identification of any object stored in the database

    """
    partitionName = None
    position = None

    def __init__(self, partitionName, position):
        self.partitionName = partitionName
        self.position = position


class SFTransaction(TransactionalStorage.StorageTransaction):
    """
    A shared file based transaction
    """
    partitionName = None
    partitionFile = None

    def __init__(self, ts, partitionName):
        TransactionalStorage.StorageTransaction.__init__(self, ts)
        self.partitionName = partitionName
        partitionFullName = os.path.join(self.ts.baseName, partitionName)
        self.partitionFile = AIOFile(partitionFullName, 'a')
        self.ts.inUsePartions.add(partitionName)

    def newObject(self, objectData):
        """
        Store a new object on transaction.
        The returned object ID is globally valid even when this transaction is not ended correctly

        :param objectData:
        :return: the object ID of the newly created object
        """
        if not self.partitionFile:
            raise Exceptions.ValidationException('Transaction no longer in use')

        position = self.partitionFile.fileno.tell()

        self.partitionFile.write(f"/nID %{position}d/n%{json.dumps(objectData)}s")

        return position

    def close(self):
        """
        Terminate transaction use. No new object could be created after this call

        :return: None
        """
        if self.partitionFile:
            self.partitionFile.close()
        self.partitionFile = None
        self.ts.inUsePartions.remove(self.partitionName)


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

    baseDirectory = None
    currentRoot = None
    knownPartitions = {}
    inUsePartions = set()

    def openExisting(self):
        pass

    def create(self):
        pass

    def refreshKnownPartitions(self):
        pass

    def getCurrrentRoot(self):
        pass

    def __init__(self, baseDirectory, createIfNotExists=False):
        self.baseDirectory = baseDirectory

        try:
            self.openExisting()
        except Exception as e:
            if createIfNotExists:
                self.create()
            else:
                raise e

        self.refreshKnownPartitions()
        self.currentRoot = self.getCurrentRoot()

    def newTransaction(self):
        """
        Create a new transaction on this storage
        :return: a StorageTransaction on this storage
        """
        knownNames = self.knownPartitions.keys()
        newTransaction = None

        if len(knownNames):
            attemptCount = 0
            while not newTransaction and attemptCount < ATTEMPTS_ON_KNOWN:
                partitionIndex = 0
                partitionName = None
                while not partitionIndex:
                    partitionIndex = random.randint(0, len(knownNames))
                    partitionName = knownNames[partitionIndex]
                    if partitionName not in self.inUsePartions:
                        break
                try:
                    newTransaction = SFTransaction(self, partitionName)
                except Exception as e:
                    attemptCount += 1
                    continue

        if not newTransaction:
            self.refreshKnownPartitions()

            if len(knownNames):
                attemptCount = 0
                while not newTransaction and attemptCount < ATTEMPTS_ON_NEW:
                    partitionIndex = 0
                    partitionName = None
                    while not partitionIndex:
                        partitionIndex = random.randint(0, len(knownNames))
                        partitionName = knownNames[partitionIndex]
                        if partitionName not in self.inUsePartions:
                            break
                    try:
                        newTransaction = SFTransaction(self, partitionName)
                    except Exception as e:
                        attemptCount += 1
                        continue

        if not newTransaction:
            while not newTransaction:
                partitionName = f"DATA_{random.randint(0, 1000000000)}"
                try:
                    newTransaction = SFTransaction(self, partitionName)
                except Exception as e:
                    pass

        return newTransaction

    def getObject(self, objectID):
        """
        Get an object from the storage

        :param objectID: an ObjectID
        :return: the read object
        """
