from typing import Callable


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
    def create(name: str, size: int, mode: OperationMode) -> 'SharedMessage':
        """
        :param name: the name of the shared memory file
        :param size: cannot be 0
        :param mode: See `OperationMode`
        """
        pass

    @staticmethod
    def open(name: str, mode: OperationMode) -> 'SharedMessage':
        """
        :param name: the name of the shared memory file
        :param mode: See `OperationMode`
        """
        pass

    def write(self, data: bytes) -> int | None:
        """
        Writes the bytes into the shared memory, will wait for readers based on `ReaderWaitPolicy` policy set
        
        This function releases the GIL, while it's executed
        
        :returns: the version of the message that was written, or None if the shared memory is async mode or stopped
        """
        pass

    def read(self, block: bool = True) -> bytes | None:
        """
        This function releases the GIL, while waiting for a new message

        Note: if there is a new version and the `SharedMessage` has been marked as `stopped`,
        the new message will still be returned.
        If you want the inverse of this behavior manually call `is_stopped()` before calling this
        
        :param block: if True, blocks until there is a new message to read, otherwise returns None if there is no new message
        :returns: the message, or None if the shared memory is stopped
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

    def is_stopped(self) -> bool:
        """
        Check if the shared memory has been stopped by the writer
        :returns: true if the writer has marked this as stopped
        """

    def stop(self) -> None:
        """
        Signals that writers will stop writing to this `SharedMessage`

        If you have `read(block=True)` calls, this will wake those threads and make them return None (if there isn't a new message for them)

        This is also works if you have multiple writer, as none will be allowed to write anymore
        """
        pass


def read_all(readers: list[SharedMessage]) -> list[bytes | None]:
    """
    Reads in parallel from all the readers and returns a list of the messages

    The GIL is released while reading the messages

    :param readers: list of readers
    :return: list of messages
    """
    return [reader.read(False) for reader in readers]


def read_all_map(readers: list[SharedMessage], map_operation: Callable[[bytes], object]) -> list[object | None]:
    """
    Reads in parallel from all the readers and returns a list of the messages

    The GIL is released while reading the messages but re-acquired on each thread while calling the `map_operation` function

    :param readers: list of readers
    :param map_operation: function to apply to the message
    :return: list of mapped messages
    """
    return [map_operation(reader.read(False)) for reader in readers]

class ReaderWaitPolicy:
    """
    Sait for all readers or for the specified number of readers to read the message before writing
    See `OperationMode.WriteSync` and `OperationMode.WriteAsync`
    """

    class All(ReaderWaitPolicy):
        """
        Wait for all readers to read the message before writing
        """
        pass

    class Count(ReaderWaitPolicy):
        """
        Wait for the specified number of readers to read the message before writing
        """

        def __init__(self, number_of_readers: int):
            assert number_of_readers >= 0


class OperationMode:
    class CreateOnly(OperationMode):
        """
        Indicates that this instance will neither read nor write, just hold the memory open
        This only makes sense to use with the `SharedMessage.create` function,
        as the creater needs to be kept alive in order for other processes to open the shared memory file
        """
        pass

    class ReadSync(OperationMode):
        """
        The read function will block while it reads a new message
        """
        pass

    class ReadAsync(OperationMode):
        """
        This starts a background thread that reads the shared memory and
        stores the message in a queue to be read through the read function
        """
        pass

    class WriteSync(OperationMode):
        """
        The write function will block the current thread while the message is written
        """

        def __init__(self, reader_wait_policy: ReaderWaitPolicy):
            pass

    class WriteAsync(OperationMode):
        """
        The write function will send the message to a queue to be written by a background thread,
        thus the write function will never block
        """

        def __init__(self, reader_wait_policy: ReaderWaitPolicy):
            pass

