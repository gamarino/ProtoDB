import io

from . import common
from .common import Future, RootPointer
from abc import ABC, abstractmethod
import uuid


class BlockProvider(ABC):

    @abstractmethod
    def get_reader(self, wal_id: uuid.UUID, position: int) ->io.FileIO:
        """
        Get a streamer initialized at position in WAL file
        wal_id

        :param wal_id:
        :param position:
        :return:
        """

    @abstractmethod
    def get_writer_wal(self) -> uuid.UUID:
        """

        :return:
        """

    @abstractmethod
    def write_streamer(self) -> io.FileIO:
        """

        :return:
        """

    def get_current_root_object(self) -> RootPointer:
        """
        Read current root object from storage
        :return: the current root object
        """

    def update_root_object(self, new_root: RootPointer):
        """
        Updates or create the root object in storage
        On newly created databases, this is the first
        operation to perform

        :param new_root:
        :return:
        """




