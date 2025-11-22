from enum import Enum, auto
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
    def create(name: str, size: int, mode: OperationMode, reader_wait_policy: ReaderWaitPolicy) -> 'SharedMessage':
        """
        :param name: the name of the shared memory file
        :param size: cannot be 0
        :param mode: See `OperationMode`
        :param reader_wait_policy: See `ReaderWaitPolicy`
        """
        pass

    @staticmethod
    def open(name: str, mode: OperationMode) -> 'SharedMessage':
        """
        Open an existing shared memory segment.

        :param name: Name of the shared memory file (must be non-empty)
        :param mode: See `OperationMode` for allowed combinations
        :returns: a `SharedMessage` bound to an existing shared memory segment
        """
        pass

    def write(self, data: bytes) -> int | None:
        """
        Write bytes into the shared memory.

        The behavior depends on both `OperationMode` and `ReaderWaitPolicy`.

        This function releases the GIL while the underlying write operation
        is performed (from the current thread or the background writer).

        :param data: Bytes to write; must be at most `size` bytes given at `create()`
        :returns:
            - The version of the message that was written, if the write happened
            - ``None`` if:
                * the AsyncWrite mode, or
                * the shared memory has been stopped before the write could occur
        :raises ValueError:
            - If `data` is larger than the configured maximum payload size
            - if this instance is configured as create or read
            - if the background writer queue has been stopped / closed
        """
        pass

    def read(self, block: bool = True) -> bytes | None:
        """
        Read the next message from shared memory.

        The exact behavior depends on `OperationMode`.

        This function releases the GIL while waiting for a new message.

        :param block:
            If True, blocks until there is a new message to read (or the
            shared memory is stopped with no newer message).
            If False, returns immediately with a message if available, or
            ``None`` otherwise.
        :returns:
            The message bytes, or ``None`` if:
                - there is no new message and ``block=False``, or
                - the shared memory is stopped and there is no newer message
        :raises ValueError:
            - If this instance is configured as create or write
        """
        pass

    def is_new_version_available(self) -> bool:
        """
        Check if the next `read()` call would return a newer message.

        This is a non-blocking hint based on comparing the last read version
        for this instance with the current version in the shared memory.

        Note that:
            - Another process or reader could consume messages concurrently.
            - Stopping the shared memory does not reset the version.
              A stopped but newer version still counts as "available".

        :returns: True if a version newer than `last_read_version()` is available.
        """
        pass

    def last_written_version(self) -> int:
        """
        Get the last version number written *by this instance*.

        In synchronous write mode, this is updated directly in the calling
        thread as soon as the write completes.

        In asynchronous write mode, this is updated by the background writer
        thread once the message is actually written to shared memory.

        :returns: The latest version that was written by this instance
        """
        pass

    def last_read_version(self) -> int:
        """
        Get the last version number successfully read by this instance.

        This is updated whenever `read()` returns a non-None value.

        :returns: The latest version that was read by this instance
        """
        pass

    def name(self) -> str:
        """
        :returns: the name of this shared memory file
        """
        pass

    def payload_max_size(self) -> int:
        """
        Return the maximum payload size (in bytes) that can be written.

        This approximately corresponds to the `size` argument passed to `create()`
        (or the configured size of the opened shared memory), and is the
        upper bound on the length of the `data` argument to `write()`.

        :returns: Maximum number of bytes allowed for a single message payload
        """
        pass

    def is_stopped(self) -> bool:
        """
        Check if the writer(s) have stopped the shared memory.

        Once stopped: See `stop()`

        :returns: True if the shared memory has been marked as stopped
        """

    def stop(self) -> None:
        """
        Signal that writers will stop writing to this `SharedMessage`.

        Effects:
            - No new writes are allowed.
            - Any threads blocked in `read(block=True)` are woken; if there
              is no newer message available, those reads will return ``None``.
            - Background writer/reader threads associated with this instance
              will eventually exit once they observe the stop flag.

        This also works when there are multiple writers, as none will be
        allowed to write after the shared memory is stopped.
        """
        pass


def read_all(readers: list[SharedMessage]) -> list[bytes | None]:
    """
    Read in parallel from all readers and return a list of messages.

    The GIL is released while performing the blocking reads.

    Each reader behaves as if `reader.read(False)` were called, but reads
    may run in parallel at the native level.

    :param readers: Iterable of readers
    :return: list of messages (or ``None`` for readers with no new message)
    """
    return [reader.read(False) for reader in readers]


def read_all_map(readers: list[SharedMessage], map_operation: Callable[[bytes], object]) -> list[object | None]:
    """
    Read in parallel from all readers and apply a mapping function.

    The GIL is released while reading the messages but re-acquired on each
    thread while calling the `map_operation` function.

    :param readers: Iterable of readers
    :param map_operation: function to apply to each non-None message
    :return:
        List of mapped messages, where each element is either:
            - ``map_operation(message)`` if a message was read, or
            - ``None`` if that reader returned no new message
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
            pass


class OperationMode(Enum):
    """
    Indicates that this instance will neither read nor write, just hold the memory open.

    This is intended for use with `SharedMessage.create`, where
    the creator process keeps the shared memory alive so that other
    processes can `open` it.
    """
    CreateOnly = auto()
    """
    Synchronous reading mode.

    Calls to `read()` may block (when `block=True`) until a new message
    is available.
    """
    ReadSync = auto()
    """
    Asynchronous reading mode.

    Starts a background thread that reads from shared memory and stores
    messages in an internal queue. Calls to `read()` consume from that
    queue, and can be non-blocking (`block=False`) or blocking (`block=True`).

    This mode is useful when you want the read path to be non-blocking or
    to integrate with event loops without holding the GIL.
    """
    ReadAsync = auto()
    """
    Synchronous writing mode.

    The `write()` function blocks the calling thread according to the
    `ReaderWaitPolicy` before writing the next message, ensuring that
    enough readers have consumed the previous one.
    """
    WriteSync = auto()

    """
    Asynchronous writing mode.

    The `write()` function enqueues the message to a background writer
    thread and returns immediately, never blocking the calling thread.
    The background thread then performs the actual write, honoring the
    configured `ReaderWaitPolicy`.

    When used with `ReaderWaitPolicy.Count(0)` (fire-and-forget), multiple
    queued writes may be coalesced and intermediate values dropped, so
    only the latest enqueued message is guaranteed to be written.
    """
    WriteAsync = auto()
