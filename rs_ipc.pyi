from enum import Enum, auto
from typing import Callable, Iterable, List, Optional, Union, final

class OperationMode(Enum):
    """
    Defines the operation mode for the SharedMessage instance.
    """
    CreateOnly = auto()
    """
    Indicates that this instance will neither read nor write, just hold the memory open.
    Intended for use with `SharedMessage.create` to keep shared memory alive for other processes.
    """

    ReadSync = auto()
    """
    Synchronous reading mode. Calls to `read(block=True)` block until a new message is available.
    """

    ReadAsync = auto()
    """
    Asynchronous reading mode. Starts a background thread that reads from shared memory into
    an internal queue. Allows integrating with event loops without holding the GIL.
    """

    WriteSync = auto()
    """
    Synchronous writing mode. The `write()` function blocks until the readers have consumed
    the previous message, according to the `ReaderWaitPolicy`.
    """

    WriteAsync = auto()
    """
    Asynchronous writing mode. The `write()` function enqueues the message to a background
    thread and returns immediately.
    """

class ReaderWaitPolicy:
    """
    Policy determining how the writer waits for readers.
    Used in conjunction with `OperationMode.WriteSync` and `OperationMode.WriteAsync`.
    """
    @final
    class All:
        """Wait for all readers to read the message before writing."""
        def __init__(self) -> None: ...

    @final
    class Count:
        """Wait for a specific number of readers to read the message before writing."""
        def __init__(self, count: int) -> None: ...

class ReadGuard:
    """
    RAII guard for zero-copy read access to shared memory.

    This object behaves like a file-like object (implementing `read`, `seek`, `tell`)
    and supports the buffer protocol, making it compatible with `pickle.load`.
    """
    def __enter__(self) -> "ReadGuard": ...
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...
    def __len__(self) -> int: ...

    def sequence(self) -> int:
        """Get the sequence number of this message."""
        ...

    def to_bytes(self) -> bytes:
        """Copy the message data to a Python bytes object."""
        ...

    def read(self, size: Optional[int] = None) -> bytes:
        """
        Read bytes from the buffer (file-like interface).

        :param size: Number of bytes to read. If None or negative, reads until the end.
        """
        ...

    def tell(self) -> int:
        """Return the current cursor position."""
        ...

    def seek(self, pos: int) -> int:
        """Set the cursor position."""
        ...

class WriteGuard:
    """
    RAII guard for zero-copy write access to shared memory.

    This object behaves like a file-like object (implementing `write`, `seek`, `tell`)
    and supports the buffer protocol, making it compatible with `pickle.dump`.

    The data must be published via `publish()` to be visible to readers.
    """
    def __enter__(self) -> "WriteGuard": ...
    def __exit__(self, exc_type, exc_value, traceback) -> None: ...
    def __len__(self) -> int: ...

    def write(self, data: bytes) -> int:
        """
        Write bytes to the buffer (file-like interface).

        :param data: Bytes to write to the buffer.
        :return: Number of bytes written.
        :raises ValueError: If data is larger than buffer capacity.
        """
        ...

    def tell(self) -> int:
        """Return the current cursor position."""
        ...

    def seek(self, pos: int) -> int:
        """Set the cursor position."""
        ...

    def publish(self, size: Optional[int] = None) -> Optional[int]:
        """
        Atomically publish the written data.

        This consumes the guard. It must be called exactly once before the guard is dropped
        or the context manager exits, otherwise the data is discarded.

        :param size: Number of bytes to publish. If None, uses the current cursor position.
        :return: The sequence number of the published message, or None if stopped.
        """
        ...

class SharedMessage:
    """
    A shared memory object for inter-process communication.

    Supports various topologies based on `ReaderWaitPolicy`:
    - **SPSC/MPSC**: `ReaderWaitPolicy.All()` or `ReaderWaitPolicy.Count(1)`
    - **Broadcast (SPMC)**: `ReaderWaitPolicy.All()`
    - **Fire-and-Forget**: `ReaderWaitPolicy.Count(0)`
    """

    @staticmethod
    def create(
            name: str,
            size: int,
            mode: OperationMode,
            reader_wait_policy: Union[ReaderWaitPolicy.All, ReaderWaitPolicy.Count]
    ) -> "SharedMessage":
        """
        Create a new shared memory segment.

        :param name: The name of the shared memory file.
        :param size: The size of the payload buffer (cannot be 0).
        :param mode: The operation mode for this instance.
        :param reader_wait_policy: The policy for waiting on readers.
        """
        ...

    @staticmethod
    def open(name: str, mode: OperationMode) -> "SharedMessage":
        """
        Open an existing shared memory segment.

        :param name: Name of the shared memory file.
        :param mode: The operation mode for this instance.
        """
        ...

    def write(self, data: bytes) -> Optional[int]:
        """
        Write bytes into the shared memory.

        Releases the GIL during operation.

        :param data: Bytes to write. Must not exceed capacity.
        :return: The version of the message written, or None if AsyncWrite or stopped.
        :raises ValueError: If data is too large or instance is not a writer.
        """
        ...

    def read(self, block: bool = True) -> Optional[bytes]:
        """
        Read the next message from shared memory.

        Releases the GIL while waiting.

        :param block: If True, blocks until a new message is available.
        :return: The message bytes, or None if no new message is available (non-blocking) or stopped.
        """
        ...

    def write_guard(self) -> Optional[WriteGuard]:
        """
        Acquire a zero-copy write guard.

        The guard provides a buffer that can be written to directly (e.g., via `pickle.dump`).
        Use in a context manager to ensure cleanup.

        :return: A `WriteGuard` or None if the shared memory is stopped.
        """
        ...

    def read_guard(self, block: bool = True) -> Optional[ReadGuard]:
        """
        Acquire a zero-copy read guard.

        The guard provides a view of the shared memory (e.g., for `pickle.load`).
        Use in a context manager to ensure cleanup.

        :param block: If True, blocks until new data is available.
        :return: A `ReadGuard` or None if no data is available.
        """
        ...

    def is_new_version_available(self) -> bool:
        """
        Check if a newer message is available compared to the last read version.
        This is a non-blocking hint.
        """
        ...

    def last_written_version(self) -> int:
        """Get the last version number written by this instance."""
        ...

    def last_read_version(self) -> int:
        """Get the last version number successfully read by this instance."""
        ...

    def name(self) -> str:
        """Return the name of the shared memory file."""
        ...

    def capacity(self) -> int:
        """Return the maximum payload size in bytes."""
        ...

    def is_stopped(self) -> bool:
        """Check if the shared memory has been marked as stopped."""
        ...

    def stop(self) -> None:
        """
        Signal that no further writes will occur.
        Wakes up blocked readers and eventually stops background threads.
        """
        ...

def read_all(readers: Iterable[SharedMessage]) -> List[Optional[bytes]]:
    """
    Read from multiple readers in parallel.

    :param readers: Iterable of SharedMessage instances.
    :return: A list of messages (bytes) or None for readers with no new data.
    """
    ...

def read_all_map(
        readers: Iterable[SharedMessage],
        map_operation: Callable[[bytes], object]
) -> List[Optional[object]]:
    """
    Read from multiple readers in parallel and apply a mapping function.

    The GIL is released during the read, but re-acquired for the map operation.

    :param readers: Iterable of SharedMessage instances.
    :param map_operation: Function to apply to the bytes.
    :return: List of mapped objects or None.
    """
    ...