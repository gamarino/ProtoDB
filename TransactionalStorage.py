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

    def get_object(self, object_id):
        """
        Get an object from the storage
        Implement any strategy to cache previous read values

        :param object_id: an ObjectID
        :return: the read object
        """

    def get_root_object(self):
        """
        Get the rooot object from the storage
        If no root yet, return None

        :param object_id: an ObjectID
        :return: the read object
        """

    def new_object(self, object_data):
        """
        Store a new object on transaction.
        The returned object ID is globally valid even when this transaction is not ended correctly

        :param object_data:
        :return: the object ID of the newly created object
        """

    def start_root_update(self):
        """
        Start the operation of root updating
        Flush any data not yet written, obtain any lock needed to ensure no one else is doing the same update
        Wait til you are the only process updating the root if necesary.
        OPTIMISTIC locking is also accepted, that means that the check will be made on update
        Update currentRoot to the most updated value

        :return: currentRootID
        """

    def update_root(self, new_root_id):
        """
        Update storage root
        Ensure that the root is updated on return or fail if it was unsuccesfull (raise)

        :param new_root_id:
        :return: the object ID of the newly created object
        """

    def close(self):
        """
        Terminate transaction use. No new object could be created after this call

        :return: None
        """


class TransactionalStorage(object):
    def new_transaction(self):
        """
        Create a new transaction on this storage
        :return: a StorageTransaction on this storage
        """
        raise Exceptions.NotImplemented('Transaction storage new transaction NOT implemented')

