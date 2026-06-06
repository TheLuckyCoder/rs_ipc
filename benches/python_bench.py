import argparse
import csv
import multiprocessing
import multiprocessing.shared_memory
import os
import pickle
import struct
import sys
import tempfile
import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Optional imports
try:
    from rs_ipc import SharedMessage, OperationMode, ReaderWaitPolicy
except ImportError:
    SharedMessage = OperationMode = ReaderWaitPolicy = None

try:
    import zmq
    ZMQ_AVAILABLE = True
except ImportError:
    ZMQ_AVAILABLE = False

try:
    import posix_ipc
    import mmap
    POSIX_IPC_AVAILABLE = True
except ImportError:
    POSIX_IPC_AVAILABLE = False



# --- Configuration & Data Types ---

@dataclass(slots=True)
class MockPipeData:
    """Mock data representing a typical computer vision pipeline message."""
    data1: np.ndarray
    data2: np.ndarray
    data3: np.ndarray
    send_time: float = 0.0


def create_payload(width: int, height: int, bpp: int = 3) -> MockPipeData:
    size = width * height * bpp
    return MockPipeData(
        data1=np.random.randint(0, 255, size, dtype=np.uint8),
        data2=np.random.randint(0, 255, size, dtype=np.uint8),
        data3=np.random.randint(0, 255, size, dtype=np.uint8),
    )


def get_required_shm_size(test_case: Dict[str, Any]) -> int:
    if test_case.get("type") == "bytes":
        return test_case["size"] + (1024 * 1024)
    raw_size = test_case["width"] * test_case["height"] * test_case.get("bpp", 3)
    return int(raw_size * 3.3) + 65536


# --- Abstract Backend Interface ---

class IpcBackend(ABC):
    @abstractmethod
    def get_name(self) -> str:
        pass

    def setup_coordinator(self, name: str, size: int, num_readers: int):
        pass

    def get_worker_args(self) -> Dict[str, Any]:
        return {}

    @abstractmethod
    def setup_worker(self, role: str, idx: int, **kwargs):
        pass

    @abstractmethod
    def write(self, obj: Any):
        pass

    @abstractmethod
    def read(self, block: bool) -> Any:
        pass

    def signal_stop(self):
        pass

    def cleanup_coordinator(self):
        pass


# --- Backend Implementations ---

class RsIpcBackend(IpcBackend):
    def __init__(self, mode: str = "zerocopy", wait_policy: str = "all"):
        self.mode = mode
        self.wait_policy_str = wait_policy
        self.shm = None
        self.name = None
        self._coordinator_handle = None

    def get_name(self) -> str:
        suffix = "copy" if self.mode == "standard" else self.mode
        return f"RsIpc_{suffix}"

    def __getstate__(self):
        state = self.__dict__.copy()
        state["shm"] = None
        state["_coordinator_handle"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def setup_coordinator(self, name: str, size: int, num_readers: int):
        self.name = name
        if self.wait_policy_str == "none":
            policy = ReaderWaitPolicy.Count(0)
        elif self.wait_policy_str == "any":
            policy = ReaderWaitPolicy.Count(1)
        else:
            policy = ReaderWaitPolicy.Count(num_readers)
        self._coordinator_handle = SharedMessage.create(name, size, OperationMode.CreateOnly, policy)

    def get_worker_args(self) -> Dict[str, Any]:
        return {"name": self.name}

    def setup_worker(self, role: str, idx: int, name: str):
        mode = OperationMode.WriteSync if role == "writer" else OperationMode.ReadSync
        self.shm = SharedMessage.open(name, mode)

    def write(self, obj: Any):
        if self.mode == "standard":
            self.shm.write(pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL))
        else:
            guard = self.shm.write_guard()
            if guard:
                with guard:
                    pickle.dump(obj, guard, protocol=pickle.HIGHEST_PROTOCOL)
                    guard.publish()

    def read(self, block: bool) -> Any:
        if self.mode == "standard":
            data = self.shm.read(block=block)
            return pickle.loads(data) if data else None
        guard = self.shm.read_guard(block=block)
        if guard:
            with guard:
                return pickle.loads(guard)
        return None

    def signal_stop(self):
        if self._coordinator_handle:
            self._coordinator_handle.stop()

    def cleanup_coordinator(self):
        try:
            self.signal_stop()
        except:
            pass


class MpQueueBackend(IpcBackend):
    def __init__(self, maxsize: int = 1):
        self.maxsize = maxsize
        self.queues = []
        self._my_queue = None

    def get_name(self) -> str:
        return "MpQueue"

    def setup_coordinator(self, name, size, num_readers):
        self.queues = [multiprocessing.Queue(maxsize=self.maxsize) for _ in range(num_readers)]

    def get_worker_args(self):
        return {"queues": self.queues}

    def setup_worker(self, role, idx, queues):
        self.queues = queues
        if role == "reader":
            self._my_queue = queues[idx]

    def write(self, obj):
        for q in self.queues:
            q.put(obj)

    def read(self, block):
        try:
            return self._my_queue.get(block=block, timeout=10.0)
        except:
            return None

    def signal_stop(self):
        for q in self.queues:
            q.put("STOP")


class MpPipeBackend(IpcBackend):
    def __init__(self):
        self._pipes = []
        self._writers = []
        self._reader = None
        self._lock = None

    def get_name(self) -> str:
        return "MpPipe"

    def setup_coordinator(self, name, size, num_readers):
        self._pipes = [multiprocessing.Pipe(duplex=False) for _ in range(num_readers)]
        self._writers = [p[1] for p in self._pipes]
        self._lock = multiprocessing.Lock()

    def get_worker_args(self):
        return {"pipes": self._pipes, "lock": self._lock}

    def setup_worker(self, role, idx, pipes, lock):
        self._lock = lock
        if role == "writer":
            self._writers = [p[1] for p in pipes]
        else:
            self._reader = pipes[idx][0]

    def write(self, obj):
        with self._lock:
            for w in self._writers:
                w.send(obj)

    def read(self, block):
        if not block and not self._reader.poll():
            return None
        try:
            return self._reader.recv()
        except EOFError:
            return None

    def signal_stop(self):
        for w in self._writers:
            try:
                w.send("STOP")
            except:
                pass


class MpShmBackend(IpcBackend):
    def __init__(self, wait_policy="all"):
        self.wait_policy = wait_policy
        self.shm_name = None
        self.shm = None
        self.lock = None
        self.cond = None
        self.version = None
        self.readers_done = None
        self.data_length = None
        self.active_readers = None
        self.stop_flag = None
        self.num_readers = 0
        self._my_version = 0
        self._shm_handle = None

    def get_name(self) -> str:
        return "MpShm"

    def setup_coordinator(self, name, size, num_readers):
        self.shm_name = f"{name}_shm"
        self.num_readers = num_readers
        try:
            self._shm_handle = multiprocessing.shared_memory.SharedMemory(
                create=True, size=size, name=self.shm_name
            )
        except FileExistsError:
            self._shm_handle = multiprocessing.shared_memory.SharedMemory(name=self.shm_name)

        self.lock = multiprocessing.Lock()
        self.cond = multiprocessing.Condition(self.lock)
        self.version = multiprocessing.Value("i", 0)
        self.readers_done = multiprocessing.Value("i", 0)
        self.data_length = multiprocessing.Value("i", 0)
        self.active_readers = multiprocessing.Value("i", 0)
        self.stop_flag = multiprocessing.Value("b", False)

    def get_worker_args(self):
        return {
            "shm_name": self.shm_name,
            "lock": self.lock,
            "cond": self.cond,
            "version": self.version,
            "readers_done": self.readers_done,
            "data_length": self.data_length,
            "active_readers": self.active_readers,
            "stop_flag": self.stop_flag,
            "num_readers": self.num_readers,
        }

    def setup_worker(self, role, idx, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.shm = multiprocessing.shared_memory.SharedMemory(name=self.shm_name)

    def write(self, obj):
        data = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        length = len(data)
        target = self.num_readers if self.wait_policy == "all" else (1 if self.num_readers > 0 else 0)

        with self.lock:
            while self.readers_done.value < target and self.version.value > 0 and not self.stop_flag.value:
                self.cond.wait(timeout=0.01)
            while self.active_readers.value > 0 and not self.stop_flag.value:
                self.cond.wait(timeout=0.01)
            if self.stop_flag.value:
                return

            self.shm.buf[:length] = data
            self.data_length.value = length
            self.version.value += 1
            self.readers_done.value = 0
            self.cond.notify_all()

    def read(self, block):
        with self.lock:
            while self.version.value == self._my_version and not self.stop_flag.value:
                if not block or not self.cond.wait(timeout=0.01):
                    if not block or self.stop_flag.value:
                        break

            if self.stop_flag.value and self.version.value == self._my_version:
                return "STOP"

            self._my_version = self.version.value
            self.active_readers.value += 1

            try:
                return pickle.loads(self.shm.buf[: self.data_length.value])
            finally:
                self.active_readers.value -= 1
                self.readers_done.value += 1
                self.cond.notify_all()

    def signal_stop(self):
        if self.lock:
            with self.lock:
                self.stop_flag.value = True
                self.cond.notify_all()

    def cleanup_coordinator(self):
        if self._shm_handle:
            self._shm_handle.close()
            try:
                self._shm_handle.unlink()
            except:
                pass


class ZmqBackend(IpcBackend):
    """ZeroMQ PUSH/PULL over TCP loopback (one socket per reader for guaranteed delivery)."""

    def __init__(self):
        self._context = None
        self._sockets = []
        self._endpoints = []
        self._num_readers = 0

    def get_name(self) -> str:
        return "ZeroMQ"

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_context"] = None
        state["_sockets"] = []
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def setup_coordinator(self, name: str, size: int, num_readers: int):
        self._num_readers = num_readers
        self._endpoints = [f"ipc:///tmp/zmq_{name}_{i}" for i in range(num_readers)]
        # Clean up any stale socket files
        for ep in self._endpoints:
            path = ep.replace("ipc://", "")
            try:
                os.unlink(path)
            except OSError:
                pass

    def get_worker_args(self) -> Dict[str, Any]:
        return {"endpoints": self._endpoints, "num_readers": self._num_readers}

    def setup_worker(self, role: str, idx: int, endpoints: List[str], num_readers: int):
        self._context = zmq.Context()
        self._num_readers = num_readers
        self._endpoints = endpoints
        if role == "writer":
            self._sockets = []
            for ep in endpoints:
                sock = self._context.socket(zmq.PUSH)
                sock.setsockopt(zmq.SNDHWM, 2)
                sock.setsockopt(zmq.LINGER, 1000)
                sock.connect(ep)
                self._sockets.append(sock)
        else:
            sock = self._context.socket(zmq.PULL)
            sock.setsockopt(zmq.RCVHWM, 2)
            sock.bind(endpoints[idx])
            self._sockets = [sock]

    def write(self, obj: Any):
        data = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        for sock in self._sockets:
            sock.send(data)

    def read(self, block: bool) -> Any:
        flags = 0 if block else zmq.NOBLOCK
        try:
            data = self._sockets[0].recv(flags=flags)
            return pickle.loads(data)
        except zmq.Again:
            return None

    def signal_stop(self):
        data = pickle.dumps("STOP", protocol=pickle.HIGHEST_PROTOCOL)
        for sock in self._sockets:
            try:
                sock.send(data, copy=False)
            except:
                pass

    def cleanup_coordinator(self):
        for ep in self._endpoints:
            path = ep.replace("ipc://", "")
            try:
                os.unlink(path)
            except OSError:
                pass


class PosixIpcBackend(IpcBackend):
    """POSIX shared memory + semaphores (shm_open + sem_open)."""

    HEADER_SIZE = 25  # version(8) + data_length(8) + readers_done(8) + stop(1)

    def __init__(self, wait_policy="all"):
        self.wait_policy = wait_policy
        self._shm_name = None
        self._sem_writer_name = None
        self._sem_reader_name = None
        self._lock_name = None
        self._shm = None
        self._mmap_obj = None
        self._sem_writer = None
        self._sem_reader = None
        self._lock_sem = None
        self._size = 0
        self._num_readers = 0

    def get_name(self) -> str:
        return "PosixIpc"

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_shm"] = None
        state["_mmap_obj"] = None
        state["_sem_writer"] = None
        state["_sem_reader"] = None
        state["_lock_sem"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def setup_coordinator(self, name: str, size: int, num_readers: int):
        self._shm_name = f"/{name}_px"
        self._sem_writer_name = f"/{name}_wr"
        self._sem_reader_name = f"/{name}_rd"
        self._lock_name = f"/{name}_lk"
        self._size = size + self.HEADER_SIZE
        self._num_readers = num_readers

        # Clean up any stale resources
        for sem_name in [self._sem_writer_name, self._sem_reader_name, self._lock_name]:
            try:
                posix_ipc.unlink_semaphore(sem_name)
            except:
                pass
        try:
            posix_ipc.unlink_shared_memory(self._shm_name)
        except:
            pass

        self._shm = posix_ipc.SharedMemory(self._shm_name, posix_ipc.O_CREX, size=self._size)
        self._mmap_obj = mmap.mmap(self._shm.fd, self._size)
        self._shm.close_fd()

        # Initialize header: version=0, data_length=0, readers_done=0, stop=False
        struct.pack_into("QQQ?", self._mmap_obj, 0, 0, 0, 0, False)

        self._sem_writer = posix_ipc.Semaphore(self._sem_writer_name, posix_ipc.O_CREX, initial_value=1)
        self._sem_reader = posix_ipc.Semaphore(self._sem_reader_name, posix_ipc.O_CREX, initial_value=0)
        self._lock_sem = posix_ipc.Semaphore(self._lock_name, posix_ipc.O_CREX, initial_value=1)

    def get_worker_args(self) -> Dict[str, Any]:
        return {
            "shm_name": self._shm_name,
            "sem_writer_name": self._sem_writer_name,
            "sem_reader_name": self._sem_reader_name,
            "lock_name": self._lock_name,
            "size": self._size,
            "num_readers": self._num_readers,
            "wait_policy": self.wait_policy,
        }

    def setup_worker(self, role: str, idx: int, shm_name: str, sem_writer_name: str,
                     sem_reader_name: str, lock_name: str, size: int, num_readers: int,
                     wait_policy: str):
        self._size = size
        self._num_readers = num_readers
        self.wait_policy = wait_policy

        self._shm = posix_ipc.SharedMemory(shm_name)
        self._mmap_obj = mmap.mmap(self._shm.fd, size)
        self._shm.close_fd()

        self._sem_writer = posix_ipc.Semaphore(sem_writer_name)
        self._sem_reader = posix_ipc.Semaphore(sem_reader_name)
        self._lock_sem = posix_ipc.Semaphore(lock_name)

    def write(self, obj: Any):
        data = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        length = len(data)

        self._sem_writer.acquire()

        self._lock_sem.acquire()
        version = struct.unpack_from("Q", self._mmap_obj, 0)[0]
        struct.pack_into("QQQ?", self._mmap_obj, 0, version + 1, length, 0, False)
        self._mmap_obj[self.HEADER_SIZE : self.HEADER_SIZE + length] = data
        self._lock_sem.release()

        for _ in range(self._num_readers):
            self._sem_reader.release()

    def read(self, block: bool) -> Any:
        if block:
            self._sem_reader.acquire()
        else:
            try:
                self._sem_reader.acquire(0)
            except posix_ipc.BusyError:
                return None

        self._lock_sem.acquire()
        _version, length, readers_done, stop = struct.unpack_from("QQQ?", self._mmap_obj, 0)
        if stop:
            self._lock_sem.release()
            return "STOP"

        result = pickle.loads(self._mmap_obj[self.HEADER_SIZE : self.HEADER_SIZE + length])

        readers_done += 1
        struct.pack_into("Q", self._mmap_obj, 16, readers_done)

        target = self._num_readers if self.wait_policy == "all" else 1
        if readers_done >= target:
            self._sem_writer.release()

        self._lock_sem.release()
        return result

    def signal_stop(self):
        if self._lock_sem:
            self._lock_sem.acquire()
            struct.pack_into("?", self._mmap_obj, 24, True)
            self._lock_sem.release()
            for _ in range(self._num_readers * 2):
                try:
                    self._sem_reader.release()
                except:
                    pass

    def cleanup_coordinator(self):
        if self._mmap_obj:
            self._mmap_obj.close()
        try:
            posix_ipc.unlink_shared_memory(self._shm_name)
        except:
            pass
        for sem_name in [self._sem_writer_name, self._sem_reader_name, self._lock_name]:
            try:
                posix_ipc.unlink_semaphore(sem_name)
            except:
                pass




# --- Benchmarking Logic ---

def writer_task(backend: IpcBackend, test_case: Dict, iterations: int, warmup: int,
                barrier: multiprocessing.Barrier, result_queue: multiprocessing.Queue,
                worker_args: Dict, core_id: Optional[int] = None):
    try:
        if core_id is not None:
            os.sched_setaffinity(0, {core_id})
        backend.setup_worker("writer", 0, **worker_args)
        is_bytes = test_case.get("type") == "bytes"
        payload = bytes(test_case["size"]) if is_bytes else create_payload(test_case["width"], test_case["height"])

        barrier.wait()

        start_time = time.perf_counter()
        for _ in range(iterations):
            if is_bytes:
                backend.write((time.perf_counter(), payload))
            else:
                payload.send_time = time.perf_counter()
                backend.write(payload)

        result_queue.put((start_time, time.perf_counter()))
    except Exception:
        traceback.print_exc()
        result_queue.put(None)


def reader_task(backend: IpcBackend, idx: int, expected: int, warmup: int,
                barrier: multiprocessing.Barrier, result_queue: multiprocessing.Queue,
                worker_args: Dict, core_id: Optional[int] = None):
    try:
        if core_id is not None:
            os.sched_setaffinity(0, {core_id})
        backend.setup_worker("reader", idx, **worker_args)
        barrier.wait()

        latencies = []
        warmup_remaining = warmup
        last_recv_time = None
        received = 0

        while received < expected:
            msg = backend.read(block=True)
            if msg is None or msg == "STOP":
                break

            now = time.perf_counter()
            received += 1

            send_time = msg[0] if isinstance(msg, tuple) else getattr(msg, "send_time", None)

            if warmup_remaining > 0:
                warmup_remaining -= 1
                continue

            last_recv_time = now
            if send_time:
                latencies.append((now - send_time) * 1000.0)

        measured = expected - warmup
        result_queue.put({
            "latencies": latencies,
            "p50": float(np.percentile(latencies, 50)) if latencies else 0.0,
            "p99": float(np.percentile(latencies, 99)) if latencies else 0.0,
            "last_recv_time": last_recv_time,
            "success": len(latencies) >= measured,
        })
    except Exception:
        traceback.print_exc()
        result_queue.put(None)


def run_single_trial(backend: IpcBackend, test_case: Dict, iterations: int, warmup: int,
                     num_readers: int, num_writers: int) -> Optional[Dict]:
    """Execute one trial of a benchmark scenario. Returns metrics or None on failure."""
    shm_size = get_required_shm_size(test_case)
    backend.setup_coordinator(f"bench_{int(time.time() * 1000) % 1_000_000}", shm_size, num_readers)
    worker_args = backend.get_worker_args()

    available_cores = os.cpu_count() or 1
    total_procs = num_writers + num_readers
    if total_procs <= available_cores - 1:
        writer_cores = list(range(num_writers))
        reader_cores = list(range(num_writers, num_writers + num_readers))
    else:
        writer_cores = [None] * num_writers
        reader_cores = [None] * num_readers

    barrier = multiprocessing.Barrier(num_readers + num_writers)
    w_q = multiprocessing.Queue()
    r_qs = [multiprocessing.Queue() for _ in range(num_readers)]

    readers = [
        multiprocessing.Process(
            target=reader_task,
            args=(backend, i, iterations * num_writers, warmup, barrier, r_qs[i], worker_args, reader_cores[i]),
        )
        for i in range(num_readers)
    ]
    writers = [
        multiprocessing.Process(
            target=writer_task,
            args=(backend, test_case, iterations, warmup, barrier, w_q, worker_args, writer_cores[j]),
        )
        for j in range(num_writers)
    ]

    for p in readers + writers:
        p.start()

    writer_stats = []
    for _ in range(num_writers):
        try:
            res = w_q.get(timeout=120)
            if res:
                writer_stats.append(res)
        except:
            pass

    reader_stats = []
    for q in r_qs:
        try:
            res = q.get(timeout=120)
            if res:
                reader_stats.append(res)
        except:
            pass

    backend.signal_stop()

    for p in readers + writers:
        p.join(timeout=3.0)
        if p.is_alive():
            p.terminate()

    backend.cleanup_coordinator()

    if len(reader_stats) != num_readers or not writer_stats:
        return None

    all_success = all(s.get("success", False) for s in reader_stats)
    if not all_success:
        return None

    p50 = float(np.mean([s["p50"] for s in reader_stats]))
    p99 = float(np.mean([s["p99"] for s in reader_stats]))

    all_latencies = []
    for s in reader_stats:
        all_latencies.extend(s.get("latencies", []))

    payload_size = (
        test_case["size"]
        if test_case.get("type") == "bytes"
        else test_case["width"] * test_case["height"] * 3 * 3
    )
    total_bytes = payload_size * iterations * num_writers

    t_start = min(s[0] for s in writer_stats)
    t_end = max(s["last_recv_time"] for s in reader_stats if s["last_recv_time"])
    duration = t_end - t_start
    mbs = (total_bytes / max(duration, 1e-9)) / (1024 * 1024)

    return {"p50": p50, "p99": p99, "MB/s": mbs, "latencies": all_latencies}


def run_scenario(name: str, backends: List[IpcBackend], test_case: Dict, iterations: int,
                 num_readers: int, num_writers: int = 1, trials: int = 5, warmup: int = 10,
                 machine_tag: Optional[str] = None):
    """Run a scenario across all backends with multiple trials."""
    print(f"\n>>> Scenario: {name} ({num_writers}W, {num_readers}R, {trials} trials, {warmup} warmup)")
    print(f"{'Backend':<20} | {'p50 (ms)':<12} | {'p99 (ms)':<12} | {'MB/s':<12} | {'Trials'}")
    print("-" * 75)

    results = []
    raw_results = []
    latency_records = []

    for backend in backends:
        trial_data = []

        for trial_idx in range(trials):
            result = run_single_trial(backend, test_case, iterations, warmup, num_readers, num_writers)
            if result:
                trial_data.append(result)
                raw_results.append({
                    "Scenario": name,
                    "Backend": backend.get_name(),
                    "Trial": trial_idx,
                    "p50": result["p50"],
                    "p99": result["p99"],
                    "MB/s": result["MB/s"],
                    "Readers": num_readers,
                    "Writers": num_writers,
                    "Machine": machine_tag or "default",
                })
                for lat in result.get("latencies", []):
                    latency_records.append({
                        "Scenario": name,
                        "Backend": backend.get_name(),
                        "Trial": trial_idx,
                        "Latency_ms": lat,
                        "Machine": machine_tag or "default",
                    })

        if trial_data:
            p50s = [t["p50"] for t in trial_data]
            p99s = [t["p99"] for t in trial_data]
            mbss = [t["MB/s"] for t in trial_data]

            agg = {
                "Scenario": name,
                "Backend": backend.get_name(),
                "Machine": machine_tag or "default",
                "p50_median": float(np.median(p50s)),
                "p50_q1": float(np.percentile(p50s, 25)),
                "p50_q3": float(np.percentile(p50s, 75)),
                "p50_std": float(np.std(p50s, ddof=1)) if len(p50s) > 1 else 0.0,
                "p99_median": float(np.median(p99s)),
                "p99_q1": float(np.percentile(p99s, 25)),
                "p99_q3": float(np.percentile(p99s, 75)),
                "p99_std": float(np.std(p99s, ddof=1)) if len(p99s) > 1 else 0.0,
                "MB/s_median": float(np.median(mbss)),
                "MB/s_q1": float(np.percentile(mbss, 25)),
                "MB/s_q3": float(np.percentile(mbss, 75)),
                "MB/s_std": float(np.std(mbss, ddof=1)) if len(mbss) > 1 else 0.0,
                "trials": len(trial_data),
                "Readers": num_readers,
                "Writers": num_writers,
            }

            p50_med = agg["p50_median"]
            p99_med = agg["p99_median"]
            mbs_med = agg["MB/s_median"]
            n = agg["trials"]
            print(f"{backend.get_name():<20} | {p50_med:<12.4f} | {p99_med:<12.4f} | {mbs_med:<12.2f} | {n}/{trials}")
            results.append(agg)
        else:
            print(f"{backend.get_name():<20} | FAILED")

    return results, raw_results, latency_records


# --- Plotting ---

def generate_plots(csv_path: str, raw_csv_path: Optional[str] = None,
                    latency_csv_path: Optional[str] = None):
    if not os.path.exists(csv_path):
        return

    df = pd.read_csv(csv_path)
    os.makedirs("benches/plots", exist_ok=True)

    BACKEND_COLORS = {
        "RsIpc_zerocopy": "#1f77b4",
        "RsIpc_copy": "#ff7f0e",
        "PosixIpc": "#2ca02c",
        "MpQueue": "#d62728",
        "MpPipe": "#9467bd",
        "MpShm": "#8c564b",
        "ZeroMQ": "#7f7f7f",
    }
    BACKEND_MARKERS = {
        "RsIpc_zerocopy": "o", "RsIpc_copy": "s", "PosixIpc": "^",
        "MpQueue": "D", "MpPipe": "v", "MpShm": "X", "ZeroMQ": "P",
    }

    backend_order = sorted(df["Backend"].unique())
    palette = [BACKEND_COLORS.get(b, "#333333") for b in backend_order]

    sns.set_theme(style="whitegrid", font_scale=1.2, rc={
        "axes.labelsize": 13, "axes.titlesize": 14,
        "xtick.labelsize": 11, "ytick.labelsize": 11,
        "legend.fontsize": 9, "legend.title_fontsize": 10,
    })

    raw_df = None
    if raw_csv_path and os.path.exists(raw_csv_path):
        raw_df = pd.read_csv(raw_csv_path)

    lat_scenarios = ["Latency (1:1)", "Scalability (1:10)", "Contention (10:1)"]

    # 1. Tail Latency (p99)
    fig, ax = plt.subplots(figsize=(8, 5))
    if raw_df is not None:
        lat_raw = raw_df[raw_df["Scenario"].isin(lat_scenarios)]
        if not lat_raw.empty:
            sns.boxplot(
                data=lat_raw, x="Scenario", y="p99", hue="Backend",
                hue_order=backend_order, palette=palette, fliersize=3, ax=ax,
            )
            sns.stripplot(
                data=lat_raw, x="Scenario", y="p99", hue="Backend",
                hue_order=backend_order, palette=palette, dodge=True,
                size=4, alpha=0.6, legend=False, ax=ax,
            )
    else:
        lat_df = df[df["Scenario"].isin(lat_scenarios)]
        if not lat_df.empty:
            sns.barplot(
                data=lat_df, x="Scenario", y="p99_median", hue="Backend",
                hue_order=backend_order, palette=palette, ax=ax,
            )

    ax.set_yscale("log")
    ax.set_ylabel("p99 Latency (ms) — Log Scale")
    ax.set_xlabel("")
    ax.set_title("Tail Latency (p99)")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.13), ncol=4, frameon=True, framealpha=0.9)
    plt.tight_layout(rect=[0, 0.10, 1, 1])
    plt.savefig("benches/plots/01_tail_latency.png", dpi=300, bbox_inches="tight")
    plt.close()

    # 2. Determinism Ratio (p99/p50) — 3 subplots with log y-axis
    fig, axes = plt.subplots(1, 3, figsize=(10, 4.5), sharey=True)
    for i, scenario in enumerate(lat_scenarios):
        ax = axes[i]
        if raw_df is not None:
            scenario_raw = raw_df[raw_df["Scenario"] == scenario].copy()
            if not scenario_raw.empty:
                scenario_raw["Determinism_Ratio"] = scenario_raw["p99"] / scenario_raw["p50"]
                medians = scenario_raw.groupby("Backend")["Determinism_Ratio"].median().reindex(backend_order).dropna()
                bars = ax.bar(
                    range(len(medians)), medians.values,
                    color=[BACKEND_COLORS.get(b, "#333333") for b in medians.index],
                    edgecolor="black", linewidth=0.5,
                )
                ax.set_xticks([])
        else:
            scenario_df = df[df["Scenario"] == scenario].copy()
            if not scenario_df.empty:
                scenario_df["Determinism_Ratio"] = scenario_df["p99_median"] / scenario_df["p50_median"]
                scenario_df = scenario_df.set_index("Backend").reindex(backend_order).dropna(subset=["Determinism_Ratio"])
                bars = ax.bar(
                    range(len(scenario_df)), scenario_df["Determinism_Ratio"].values,
                    color=[BACKEND_COLORS.get(b, "#333333") for b in scenario_df.index],
                    edgecolor="black", linewidth=0.5,
                )
                ax.set_xticks([])

        ax.set_yscale("log")
        ax.axhline(1.0, color="r", linestyle="--", alpha=0.5, linewidth=0.8)
        short_titles = {"Latency (1:1)": "1:1", "Scalability (1:10)": "1:10", "Contention (10:1)": "10:1"}
        ax.set_title(short_titles.get(scenario, scenario))
        ax.set_ylabel("")

    fig.suptitle("Determinism Factor: Lower is More Predictable", fontsize=14, y=0.98)
    handles = [plt.Rectangle((0, 0), 1, 1, facecolor=BACKEND_COLORS.get(b, "#333333")) for b in backend_order]
    fig.legend(handles, backend_order, loc="upper center", bbox_to_anchor=(0.5, -0.02),
               ncol=4, frameon=True, framealpha=0.9)
    plt.tight_layout(rect=[0, 0.08, 1, 0.95])
    plt.savefig("benches/plots/02_determinism.png", dpi=300, bbox_inches="tight")
    plt.close()

    # 3. Efficiency Score (MB/s per ms of latency)
    fig, ax = plt.subplots(figsize=(8, 5))
    if raw_df is not None:
        lat_raw = raw_df[raw_df["Scenario"].isin(lat_scenarios)].copy()
        if not lat_raw.empty:
            lat_raw["Efficiency_Score"] = lat_raw["MB/s"] / lat_raw["p50"]
            sns.boxplot(
                data=lat_raw, x="Scenario", y="Efficiency_Score", hue="Backend",
                hue_order=backend_order, palette=palette, fliersize=3, ax=ax,
            )
            sns.stripplot(
                data=lat_raw, x="Scenario", y="Efficiency_Score", hue="Backend",
                hue_order=backend_order, palette=palette, dodge=True,
                size=4, alpha=0.6, legend=False, ax=ax,
            )
    else:
        lat_df = df[df["Scenario"].isin(lat_scenarios)].copy()
        if not lat_df.empty:
            lat_df["Efficiency_Score"] = lat_df["MB/s_median"] / lat_df["p50_median"]
            sns.barplot(
                data=lat_df, x="Scenario", y="Efficiency_Score", hue="Backend",
                hue_order=backend_order, palette=palette, ax=ax,
            )

    ax.set_yscale("log")
    ax.set_title("Architectural Efficiency: Throughput (MiB/s) per ms of Latency")
    ax.set_ylabel("Efficiency (MiB/s / ms) — Log Scale")
    ax.set_xlabel("")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.13), ncol=4, frameon=True, framealpha=0.9)
    plt.tight_layout(rect=[0, 0.10, 1, 1])
    plt.savefig("benches/plots/03_efficiency.png", dpi=300, bbox_inches="tight")
    plt.close()

    # 4. Size Scaling with confidence bands
    size_df = df[df["Scenario"].str.contains("Size")].copy()
    if not size_df.empty:
        size_df["Size_MB"] = size_df["Scenario"].str.extract(r"(\d+)").astype(float)
        fig, ax = plt.subplots(figsize=(8, 5))

        for backend in backend_order:
            subset = size_df[size_df["Backend"] == backend].sort_values("Size_MB")
            if subset.empty:
                continue
            x = subset["Size_MB"].values
            y = subset["MB/s_median"].values
            y_lo = subset["MB/s_q1"].values
            y_hi = subset["MB/s_q3"].values
            color = BACKEND_COLORS.get(backend, "#333333")
            marker = BACKEND_MARKERS.get(backend, "o")
            ax.fill_between(x, y_lo, y_hi, alpha=0.15, color=color)
            ax.plot(x, y, marker=marker, linewidth=2, label=backend, color=color)

        ax.set_title("Throughput Scaling vs Message Size")
        ax.set_xlabel("Message Size")
        ax.set_ylabel("Throughput (MiB/s)")
        ax.set_xscale("log", base=2)
        unique_sizes = sorted(size_df["Size_MB"].unique())
        ax.set_xticks(unique_sizes)
        ax.set_xticklabels([f"{int(s)} MiB" for s in unique_sizes])
        ax.grid(True, which="both", ls="-", alpha=0.3)
        ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.13), ncol=4, frameon=True, framealpha=0.9)
        plt.tight_layout(rect=[0, 0.10, 1, 1])
        plt.savefig("benches/plots/04_size_scaling.png", dpi=300, bbox_inches="tight")
        plt.close()

    # 5. Latency CDF for Contention (10:1)
    lat_csv = latency_csv_path
    if lat_csv and os.path.exists(lat_csv):
        lat_df = pd.read_csv(lat_csv)
        contention_lat = lat_df[lat_df["Scenario"] == "Contention (10:1)"]

        if not contention_lat.empty:
            fig, ax = plt.subplots(figsize=(7, 4.5))

            for backend in backend_order:
                subset = contention_lat[contention_lat["Backend"] == backend]["Latency_ms"].sort_values()
                if subset.empty:
                    continue
                cdf = np.arange(1, len(subset) + 1) / len(subset)
                color = BACKEND_COLORS.get(backend, "#333333")
                ax.plot(subset.values, cdf, linewidth=2, label=backend, color=color)

            ax.set_xscale("log")
            ax.set_xlabel("Latency (ms)")
            ax.set_ylabel("Cumulative Probability")
            ax.set_title("Latency CDF — Contention (10 Writers : 1 Reader)")
            ax.set_ylim(0, 1.02)
            ax.grid(True, alpha=0.3)
            ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.13), ncol=4,
                      frameon=True, framealpha=0.9)
            plt.tight_layout(rect=[0, 0.10, 1, 1])
            plt.savefig("benches/plots/05_latency_cdf.png", dpi=300, bbox_inches="tight")
            plt.close()

    # 6. Scalability Degradation Bar Chart
    baseline_df = df[df["Scenario"] == "Latency (1:1)"][["Backend", "p50_median"]].rename(
        columns={"p50_median": "p50_baseline"})
    scaled_df = df[df["Scenario"] == "Scalability (1:10)"][["Backend", "p50_median"]].rename(
        columns={"p50_median": "p50_scaled"})
    deg_df = baseline_df.merge(scaled_df, on="Backend")
    deg_df["Degradation"] = deg_df["p50_scaled"] / deg_df["p50_baseline"]
    deg_df = deg_df.sort_values("Degradation")

    if not deg_df.empty:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        bars = ax.bar(
            range(len(deg_df)), deg_df["Degradation"].values,
            color=[BACKEND_COLORS.get(b, "#333333") for b in deg_df["Backend"]],
            edgecolor="black", linewidth=0.5,
        )
        ax.set_xticks(range(len(deg_df)))
        ax.set_xticklabels(deg_df["Backend"].values, rotation=20, ha="right", fontsize=10)
        ax.set_ylabel("Latency Degradation Factor (1:10 / 1:1)")
        ax.set_title("Scalability: Median Latency Degradation (1 → 10 Readers)")
        ax.axhline(1.0, color="gray", linestyle="--", alpha=0.5)
        ax.set_ylim(bottom=0)
        for bar, val in zip(bars, deg_df["Degradation"].values):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                    f"{val:.1f}×", ha="center", fontsize=10)
        plt.tight_layout()
        plt.savefig("benches/plots/06_scalability_degradation.png", dpi=300, bbox_inches="tight")
        plt.close()

    print("Plots saved to benches/plots/")


# --- CLI ---

def get_all_backends(wait_policy: str = "all") -> List[IpcBackend]:
    backends = []
    if SharedMessage:
        backends.append(RsIpcBackend(mode="zerocopy", wait_policy=wait_policy))
        backends.append(RsIpcBackend(mode="standard", wait_policy=wait_policy))
    backends.append(MpQueueBackend())
    backends.append(MpPipeBackend())
    backends.append(MpShmBackend(wait_policy=wait_policy))
    if ZMQ_AVAILABLE:
        backends.append(ZmqBackend())
    if POSIX_IPC_AVAILABLE:
        backends.append(PosixIpcBackend(wait_policy=wait_policy))
    return backends


def filter_backends(backends: List[IpcBackend], names: Optional[List[str]]) -> List[IpcBackend]:
    if names is None:
        return backends
    name_set = set(names)
    return [b for b in backends if b.get_name() in name_set]


def main():
    parser = argparse.ArgumentParser(description="IPC Benchmark Suite")
    subparsers = parser.add_subparsers(dest="command")

    run_p = subparsers.add_parser("run", help="Run all benchmark scenarios")
    run_p.add_argument("--iterations", type=int, default=200)
    run_p.add_argument("--trials", type=int, default=5)
    run_p.add_argument("--warmup", type=int, default=10)
    run_p.add_argument("--output", type=str, default="benches/bench_results.csv")
    run_p.add_argument("--raw-output", type=str, default="benches/bench_results_raw.csv")
    run_p.add_argument("--backends", type=str, nargs="*", default=None,
                       help="Specific backends to run (e.g., RsIpc_zerocopy ZeroMQ Ray)")
    run_p.add_argument("--latency-output", type=str, default="benches/bench_results_latencies.csv")
    run_p.add_argument("--quick", action="store_true",
                       help="Quick mode: 1 trial, 0 warmup, 50 iterations")
    run_p.add_argument("--machine-tag", type=str, default=None,
                       help="Machine identifier (e.g., 'amd_ryzen7_7700'). Appended to output filenames.")

    plot_p = subparsers.add_parser("plot", help="Generate plots from existing results")
    plot_p.add_argument("--input", type=str, default="benches/bench_results.csv")
    plot_p.add_argument("--raw-input", type=str, default="benches/bench_results_raw.csv")
    plot_p.add_argument("--latency-input", type=str, default="benches/bench_results_latencies.csv")

    args = parser.parse_args()

    if args.command == "plot":
        generate_plots(args.input, getattr(args, "raw_input", None),
                       getattr(args, "latency_input", None))
        return

    if args.command != "run":
        parser.print_help()
        return

    if not SharedMessage:
        print("Warning: rs_ipc not found. RsIpc backends will be skipped.")

    if args.quick:
        args.trials = 1
        args.warmup = 0
        args.iterations = 50

    if args.machine_tag:
        tag = args.machine_tag
        if args.output == "benches/bench_results.csv":
            args.output = f"benches/bench_results_{tag}.csv"
        if args.raw_output == "benches/bench_results_raw.csv":
            args.raw_output = f"benches/bench_results_raw_{tag}.csv"
        if args.latency_output == "benches/bench_results_latencies.csv":
            args.latency_output = f"benches/bench_results_latencies_{tag}.csv"

    multiprocessing.set_start_method("spawn", force=True)

    standard_backends = filter_backends(get_all_backends("all"), args.backends)
    fanin_backends = filter_backends(get_all_backends("all"), args.backends)

    all_agg = []
    all_raw = []
    all_latencies = []

    # Print available backends
    if args.machine_tag:
        print(f"Machine: {args.machine_tag}")
    print(f"Backends: {[b.get_name() for b in standard_backends]}")
    print(f"Config: {args.iterations} iterations, {args.trials} trials, {args.warmup} warmup")

    # 1. Standard Latency & Scalability
    tc_mock = {"width": 640, "height": 480}
    agg, raw, lats = run_scenario("Latency (1:1)", standard_backends, tc_mock,
                                  args.iterations, 1, trials=args.trials, warmup=args.warmup,
                                  machine_tag=args.machine_tag)
    all_agg.extend(agg)
    all_raw.extend(raw)
    all_latencies.extend(lats)

    agg, raw, lats = run_scenario("Scalability (1:10)", standard_backends, tc_mock,
                                  args.iterations, 10, trials=args.trials, warmup=args.warmup,
                                  machine_tag=args.machine_tag)
    all_agg.extend(agg)
    all_raw.extend(raw)
    all_latencies.extend(lats)

    # 2. Contention (10 Writers, 1 Reader)
    agg, raw, lats = run_scenario("Contention (10:1)", fanin_backends, tc_mock,
                                  args.iterations // 2, 1, num_writers=10,
                                  trials=args.trials, warmup=args.warmup,
                                  machine_tag=args.machine_tag)
    all_agg.extend(agg)
    all_raw.extend(raw)
    all_latencies.extend(lats)

    # 3. Size Scaling
    for mb in [1, 2, 4, 8, 16, 32]:
        size_iterations = max(args.warmup + 10, args.iterations // (mb // 2 + 1))
        agg, raw, lats = run_scenario(
            f"Size {mb}MB", standard_backends,
            {"type": "bytes", "size": mb * 1024 * 1024},
            size_iterations, 1, trials=args.trials, warmup=args.warmup,
            machine_tag=args.machine_tag,
        )
        all_agg.extend(agg)
        all_raw.extend(raw)
        all_latencies.extend(lats)

    # Save results
    if all_agg:
        pd.DataFrame(all_agg).to_csv(args.output, index=False)
        print(f"\nAggregated results saved to {args.output}")

    if all_raw:
        pd.DataFrame(all_raw).to_csv(args.raw_output, index=False)
        print(f"Raw per-trial results saved to {args.raw_output}")

    if all_latencies:
        pd.DataFrame(all_latencies).to_csv(args.latency_output, index=False)
        print(f"Per-iteration latencies saved to {args.latency_output}")

    if all_agg:
        generate_plots(args.output, args.raw_output, args.latency_output)



if __name__ == "__main__":
    main()
