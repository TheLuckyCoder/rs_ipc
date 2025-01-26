from enum import Enum
from typing import Callable


class ReaderWaitPolicy:
    class All:
        """
        Wait for all readers to read the message before writing
        """

        def __init__(self):
            pass

    class Count:
        """
        Wait for the specified number of readers to read the message before writing
        """

        def __init__(self, number_of_readers: int):
            assert number_of_readers > 0


class OperationMode(Enum):
    ReadSync = 0,
    """
    The read function will block while it reads a new message
    """
    ReadAsync = 1,
    """
    This starts a background thread that reads the shared memory and
    stores the message in a queue to be read through the read function
    """
    WriteSync = 2,
    """
    The write function will block the current thread while the message is written
    """
    WriteAsync = 3,
    """
    The write function will send the message to a queue to be written by a background thread,
    thus the write function will never block
    """


class SharedMessage(object):
    """
    A shared memory object that can be used to communicate between processes

    This can be used to communicate in various scenarios, based on `ReaderWaitPolicy` and the number of writer/readers:
        - SPSC (Single Producer Single Consumer): `ReaderWaitPolicy` set to `All` or `Count(1)`
        - SPMC (Single Producer Multi Consumer) Broadcast : 1 writer, N readers, `ReaderWaitPolicy` set to `All`
        - MPSC (Multi Producer Single Consumer): N writers, 1 reader, `ReaderWaitPolicy` set to `All` or `Count(1)`
        - Fire-and-Forget: `ReaderWaitPolicy` set to `Count(0)` - No waiting for readers, the writer will write the message
    
    To operate like a FIFO queue, use any 'OperationMode' for writer(s), `OperationMode.ReadAsync` for the reader(s) and `ReaderWaitPolicy.All()`.
    """

    @staticmethod
    def create(name: str, size: int, mode: OperationMode,
               reader_wait_policy: ReaderWaitPolicy = ReaderWaitPolicy.All()) -> 'SharedMessage':
        """
        :param name: the name of the shared memory file
        :param size: cannot be 0
        :param mode: See `OperationMode`
        :param reader_wait_policy: wait for All readers or for the specified number of readers to read the message before writing
        """
        pass

    @staticmethod
    def open(name: str, mode: OperationMode,
             reader_wait_policy: ReaderWaitPolicy = ReaderWaitPolicy.All()) -> 'SharedMessage':
        """
        :param name: the name of the shared memory file
        :param mode: See `OperationMode`
        :param reader_wait_policy: wait for All readers or  for the specified number of readers to read the message before writing
        """
        pass

    def write(self, data: bytes) -> int | None:
        """
        Writes the bytes into the shared memory, will wait for readers based on `ReaderWaitPolicy` policy set
        
        This function releases the GIL, while it's executed
        
        :returns: the version of the message that was written, or None if the shared memory is async mode or closed
        """
        pass

    def read(self, block: bool = True) -> bytes | None:
        """
        This function releases the GIL, while waiting for a new message
        
        :param block: if True, blocks until there is a new message to read, otherwise returns None if there is no new message
        :returns: the message, or None if the shared memory is closed
        """
        pass

    def is_new_version_available(self) -> bool:
        """
        Check if the next read will return a new message
        :returns: true if there is a new version
        """
        pass

    def last_written_version(self) -> int:
        """
        :returns: the latest version that was written by this instance
        """
        pass

    def last_read_version(self) -> int:
        """
        :returns: the latest version that was read by this instance
        """
        pass

    def name(self) -> str:
        """
        :returns: the name of this shared memory file
        """
        pass

    def is_closed(self) -> bool:
        """
        Check if the shared memory has been closed by the writer
        :returns: true if the writer has marked this as closed
        """

    def close(self) -> None:
        """
        Signals to the readers that they should stop reading from the shared memory
        """
        pass


def read_all(readers: list[SharedMessage]) -> list[bytes | None]:
    """
    Reads all the readers and returns a list of the messages
    Each message is read concurrently
    :param readers: list of readers
    :return: list of messages
    """
    return [reader.read(False) for reader in readers]


def read_all_map(readers: list[SharedMessage], map_operation: Callable[[bytes], object]) -> list[object | None]:
    """
    Reads all the readers and returns a list of the messages
    Each message is read and mapped concurrently
    :param readers: list of readers
    :param map_operation: function to apply to the message
    :return: list of mapped messages
    """
    return [map_operation(reader.read(False)) for reader in readers]
