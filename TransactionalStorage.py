from . import Exceptions


class ObjectID(object):
    """
    An identification of any object stored in the database
    Stable and durable
    Concrete implementation depends on the concrete transactional storage
    """


class StorageTransaction(object):
    ts = None

    def __init__(self, ts):
        self.ts = ts

    def newObject(self, objectData):
        """
        Store a new object on transaction.
        The returned object ID is globally valid even when this transaction is not ended correctly

        :param objectData:
        :return: the object ID of the newly created object
        """

    def startRootUpdate(self):
        """
        Start the operation of root updating
        Flush any data not yet written, obtain any lock needed to ensure no one else is doing the same update
        Wait til you are the only process updating the root if necesary.
        OPTIMISTIC locking is also accepted, that means that the check will be made on update
        Update currentRoot to the most updated value

        :return: currentRootID
        """

    def updateRoot(self, newRootID):
        """
        Update storage root
        Ensure that the root is updated on return or fail if it was unsuccesfull (raise)

        :param objectData:
        :return: the object ID of the newly created object
        """

    def close(self):
        """
        Terminate transaction use. No new object could be created after this call

        :return: None
        """

class TransactionalStorage(object):

    def newTransaction(self):
        """
        Create a new transaction on this storage
        :return: a StorageTransaction on this storage
        """
        raise Exceptions.NotImplemented('Transaction storage new transaction NOT implemented')

    def getObject(self, objectID):
        """
        Get an object from the storage
        Implement any strategy to cache previous read values

        :param objectID: an ObjectID
        :return: the read object
        """
