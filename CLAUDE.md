# rs_ipc

High-performance inter-process communication library for Python, implemented in Rust. Provides zero-copy shared memory communication with flexible blocking strategies, suitable for real-time and data-intensive applications.

Originally developed for the paper "Accelerating Intelligent Vehicle Vision: A Hybrid Python-Rust Architecture with Partial-Blocking IPC" and expanded for a master's thesis on efficient IPC.

## Build & Test

```bash
# Activate venv
source venv/bin/activate

# Build Python and install wheel
maturin develop -r

# Run Rust tests
cargo test

# Run nightly benchmarks
cargo +nightly bench --features nightly-features

# Run full Python benchmarks (10 trials, 20 warmup, 200 iterations)
python benches/python_bench.py run --trials 10 --warmup 20 --iterations 200

# Run with machine tag (for multi-machine comparison)
python benches/python_bench.py run --trials 10 --warmup 20 --iterations 200 --machine-tag amd_ryzen7_7700
```

## Architecture Overview

rs_ipc provides a single-slot `SharedMessage` abstraction for inter-process communication using POSIX shared memory (`shm_open`, `mmap`). It targets single-machine Python-to-Python IPC for large payloads (1-32 MiB). The design prioritizes:

1. **Zero-copy semantics**: Processes read/write directly to shared memory
2. **Lock-free version checking**: Readers detect new messages and access data without acquiring any mutex
3. **Flexible blocking strategies**: Non-blocking, partial-blocking, and full-blocking modes
4. **GIL-free operations**: All blocking IPC operations release Python's GIL via `py.detach()`
5. **RAII safety**: All shared resources managed through Rust ownership (auto-cleanup on drop)

### Layered Architecture

1. **OS layer**: POSIX shared memory (`shm_open`/`mmap`) + Linux futex syscalls (via rustix)
2. **Sync layer**: FutexLock (three-state, follows Rust stdlib pattern) + SharedCondvar (counter-based, mutex-free)
3. **Protocol layer**: `SharedMessage` struct with cache-aligned layout + read/write coordination protocol
4. **Python layer**: PyO3 bindings with GIL release, buffer protocol guards, async background threads

### SharedMessage Memory Layout

```
#[repr(C)] SharedMessage<[u8]> — 256-byte header + variable payload

Region 1 (bytes 0-127, read-mostly — polled by readers):
┌───────────────────┬─────────────┬────────────────────────────────────────┐
│ sequence_and_flags│ data_size   │ _pad1                                  │
│ AtomicU64 (8B)    │ AtomicUsize │ [u8; 112] padding to 128B              │
│ [stopped:1|wip:1| │ (8B)        │                                        │
│  sequence:62]     │             │                                        │
└───────────────────┴─────────────┴────────────────────────────────────────┘

Region 2 (bytes 128-255, high contention — read-modify-write by readers):
┌───────────────────┬──────────────┬─────────────────┬────────────────────┐
│ readers_state     │ writer_mutex │ writer_condvar  │ reader_done_condvar│
│ AtomicU64 (8B)    │ SharedMutex  │ SharedCondvar(4)│ SharedCondvar (4B) │
│ [target:16|cons:16│ (4B)         │                 │                    │
│  active:16|done:16│              │                 │                    │
└───────────────────┴──────────────┴─────────────────┴────────────────────┘
  + _pad2 [u8; 108] to fill 128-byte block

Payload (bytes 256+):
┌─────────────────────────────────────────────────────────────────────────┐
│ data: UnsafeCell<[u8]>  (fixed-size buffer, determined at creation)     │
└─────────────────────────────────────────────────────────────────────────┘
```

128-byte region separation prevents false sharing AND hardware prefetcher interference. Compile-time static assertions verify alignment.

### Bit-Packed Atomic Fields (`src/shared_message/packed.rs`)

**SequenceState** (AtomicU64) — single Acquire load gives reader everything it needs:
- Bit 63: `stopped` flag
- Bit 62: `writing_in_progress` flag
- Bits 0-61: sequence counter (wraps from max to 1, skipping 0 which means "never written")

**ReadersStateCount** (AtomicU64) — updated via CAS retry loop:
- Bits 0-15: `target_read` (partial-blocking threshold)
- Bits 16-31: `consumers` (registered reader count)
- Bits 32-47: `active_readers` (currently accessing payload)
- Bits 48-63: `data_consumed` (finished reading current version)

### Read/Write Protocol

**Write path** (`start_write` + `publish_write` in `src/shared_message/mod.rs`):
1. Check stopped → acquire writer_mutex
2. Wait for previous message consumption: block until `data_consumed >= min(target_read, consumers)`
3. Set WRITING_IN_PROGRESS flag (fetch_or, Release)
4. Drain active readers: wait until `active_readers == 0`
5. Caller writes data (either memcpy or incremental via write guard)
6. Publish: CAS loop to atomically increment sequence + clear WIP flag
7. notify_all on writer_condvar to wake blocked readers
8. Release writer_mutex (guard drop)

Critical section = steps 2-8. In copy mode the data write (step 5) is ~2μs memcpy. In zerocopy mode with pickle it's ~800μs.

**Read path** (`read` in `src/shared_message/mod.rs`) — NO MUTEX ACQUIRED:
1. Read condvar ticket (prevents lost wakeup)
2. Single Acquire load of sequence_and_flags (lock-free version check)
3. If no new data: return None (non-blocking) or wait on condvar (blocking)
4. If writing_in_progress: wait on condvar
5. CAS increment active_readers (register as reader)
6. Double-check: re-load sequence_and_flags. If state changed (writer started between steps 5-6), unregister + retry
7. Return ReadGuard. On drop: decrement active_readers + increment data_consumed (single CAS), notify writer if active_readers hits 0

### Producer-Consumer Policies (`ReaderWaitPolicy`)

| Policy | `target_read` value | Writer behavior |
|--------|---------------------|-----------------|
| `Count(0)` | 0 | Overwrite immediately (latest-wins, real-time) |
| `Count(n)` | n | Wait for n readers to consume before overwriting |
| `All()` | u16::MAX | Wait for all registered consumers (full delivery) |

Effective threshold = `min(target_read, consumers)`.

### Synchronization Primitives (`src/sync/`)

**FutexLock** (`src/sync/lock/futex_lock.rs`): Three-state lock (UNLOCKED/LOCKED/CONTENDED) following Drepper's pattern, closely based on Rust's stdlib Mutex. Spin phase: 100 iterations with relaxed loads. Falls back to futex_wait. 4 bytes total.

**SharedCondvar** (`src/sync/condvar.rs`): Counter-based notification. No associated mutex required. Spin 1000 iterations checking counter, then futex_wait. Ticket-based lost-wakeup prevention: caller reads counter before releasing lock, passes expected to wait(). If notify happened in between, counter already differs and wait returns immediately.

### Python Integration (`src/python/`)

**Module**: `#[pymodule(gil_used = false)]` — compatible with free-threaded Python 3.13+

**Two data paths**:
- **Copy mode** (`write(data)` / `read()`): serialize outside lock → memcpy under lock (~2μs critical section). Safe under writer contention.
- **Zerocopy mode** (`write_guard()` / `read_guard()`): Guards implement buffer protocol + file-like interface (read/write/seek/tell). Enables `pickle.dump(obj, guard)` / `pickle.load(guard)` directly into/from shared memory. ~800μs critical section with pickle. Catastrophic tail latency under multi-writer contention.

**Async modes** (`OperationMode.WriteAsync` / `ReadAsync`):
- WriteAsync: background writer thread + mpsc channel. Python caller returns immediately.
- ReadAsync: background reader thread reads continuously, buffers in channel for main thread.

**Parallel read** (`read_all`): Rayon par_iter with GIL released. Reads N SharedMessages concurrently. Reduces multi-feed latency from sum to max.

**Guards** (`src/python/guards.rs`): PythonReadGuard/PythonWriteGuard implement Python's buffer protocol (__getbuffer__) AND file-like interface (read/write/seek/tell). This enables pickle, memoryview, and numpy interop. Lifetime managed via Py<PythonSharedMessage> Arc keeping the mapping alive.

### Memory Management (`src/memory_mapper.rs`)

`SharedMemoryMapper<T: SlicePtrCast>`: RAII wrapper around shm_open/mmap lifecycle.
- `create()`: shm_open(O_CREAT|O_RDWR|O_TRUNC) → ftruncate → mmap(MAP_SHARED_VALIDATE) → madvise(HUGEPAGE)
- `open()`: shm_open(O_RDWR) → fstat (get size) → mmap
- Drop: munmap always; shm_unlink only if creator (prevents orphaned shm objects)
- `SlicePtrCast` trait: safe DST construction from void ptr (alignment check + fat pointer)
- Implements Deref<Target=T>, Send, Sync

## Key Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Module exports, feature flags |
| `src/shared_message/mod.rs` | Core SharedMessage struct and read/write logic |
| `src/shared_message/packed.rs` | Bit-packed atomic fields (SequenceState, ReadersStateCount) |
| `src/shared_message/guard.rs` | RAII guards for read/write access |
| `src/memory_mapper.rs` | POSIX shm_open/mmap wrapper with RAII cleanup |
| `src/sync/futex.rs` | Thin rustix futex wrappers |
| `src/sync/lock/futex_lock.rs` | Three-state futex mutex |
| `src/sync/condvar.rs` | Futex-based condition variable |
| `src/python/mod.rs` | PyO3 module definition, `read_all` functions |
| `src/python/message.rs` | `PythonSharedMessage` wrapper, async threads |
| `src/python/guards.rs` | PythonReadGuard/PythonWriteGuard with buffer protocol |
| `src/python/bytes.rs` | RustPyBytes wrapper for zero-copy returns |
| `src/python/operation_mode.rs` | Read/Write/Async mode enum |
| `src/python/reader_wait_policy.rs` | Blocking policy configuration |
| `src/python/queue_data.rs` | Helper structs for async queue operations |
| `rs_ipc.pyi` | Python type stubs with documentation |
| `benches/ipc_bench.rs` | Criterion benchmarks for Rust |
| `benches/python_bench.py` | Comprehensive Python benchmarks |

## Performance Analysis

### Current Benchmarks

Comprehensive benchmarks in `benches/` comparing rs_ipc against Python multiprocessing:

**Backends tested:**
- `RsIpc_zerocopy`: Direct shared memory access (fastest)
- `RsIpc_copy`: With data copy for comparison
- `MpQueue`: `multiprocessing.Queue`
- `MpPipe`: `multiprocessing.Pipe`
- `MpShm`: `multiprocessing.shared_memory`
- `ZeroMQ`: pyzmq PUSH/PULL over IPC (message-passing baseline)
- `PosixIpc`: posix_ipc SharedMemory + Semaphore (same mechanism, Python sync)

**Scenarios:**
- Latency (1:1): Single producer, single consumer
- Scalability (1:10): One writer, ten readers
- Contention (10:1): Ten writers, one reader
- Variable sizes: 1MB, 2MB, 4MB, 8MB, 16MB, 32MB payloads

**Statistical methodology:** Multiple independent trials (default 5), warmup iterations discarded, reports median with IQR across trials. CLI flags: `--trials`, `--warmup`, `--quick`.

**Sample results** (p50 latency in ms, from `benches/bench_results.csv`):

| Scenario | RsIpc (zerocopy) | ZeroMQ | PosixIpc | MpQueue |
|----------|------------------|--------|----------|---------|
| Latency 1:1 | 0.83 | 1.20 | 0.24 | 1.79 |
| Scale 1:10 | 1.97 | 9.77 | 4.64 | 5.96 |
| Size 4MB | 0.22 | 1.27 | 0.59 | 3.30 |
| Size 32MB | 12.19 | 27.03 | 14.87 | 44.85 |

Run benchmarks: `python benches/python_bench.py run`
Quick validation: `python benches/python_bench.py run --quick`

Generated plots in `benches/plots/`:
- `01_tail_latency.png`: p50/p99 comparison
- `02_determinism.png`: Latency variance
- `03_efficiency.png`: Throughput comparison
- `04_size_scaling.png`: Performance vs message size

### Rust Criterion Benchmarks

In `benches/ipc_bench.rs`:
- `pure_write`: Write without readers
- `write_with_reader`: Round-trip with blocking reader

Run: `cargo bench`

### Performance Factors

1. **Memory copy**: Dominant cost at large sizes. Transparent huge pages (`MADV_HUGEPAGE`) were explored but are unreliable with `shm_open`-backed regions.
2. **Futex syscalls**: ~1μs overhead when contended. Spin loop amortizes for short waits.
3. **PyO3 overhead**: ~50ns per call for type conversion. Negligible vs memory copy.
4. **GIL acquisition**: ~100ns. Released during blocking ops to avoid contention.

## Code Style

- `rustfmt` for formatting
- `clippy` with default lints
- Python 3.12+ support (see pyproject.toml)
- `#[repr(C)]` on all shared memory structs
- No `unsafe` without safety comments

## Thesis Context

This library is documented as part of a master's thesis expanding on "Accelerating Intelligent Vehicle Vision: A Hybrid Python-Rust Architecture with Partial-Blocking IPC".

### Thesis Contributions Beyond Original Paper

1. **Redesigned synchronization**: Replaced mutex-associated condvar with standalone counter-based design (lock-free reader waits); switched from `linux-futex` crate to direct syscalls via `rustix`
2. **Async read/write modes**: Background threads for non-blocking Python API
3. **Refined memory layout**: Smaller struct, packed stopped bit, explicit blocking policy
4. **GIL release patterns**: Safe patterns for Python multithreading with Rust

---

## Thesis Writing Guide

Thesis latex file can be found at 'paper/main.tex' and it's chapters at 'paper/chapters/*.tex'

### Title

**English**: Efficient Inter-Process Communication with Zero-Copy Semantics: A Rust-Based Shared Memory Library for Python

**German**: Effiziente Interprozesskommunikation mit Zero-Copy-Semantik: Eine Rust-basierte Shared-Memory-Bibliothek für Python

**Romanian**: Comunicare Eficientă între Procese cu Semantică Zero-Copy: O Bibliotecă Bazată pe Rust pentru Comunicare prin Memorie Partajată în Python

### Terminology (be consistent)

| Correct | Avoid | Notes |
|---------|-------|-------|
| zero-copy | zero copy, zerocopy | Hyphenated as adjective |
| shared memory | shared-memory | No hyphen |
| futex-based | futex based | Hyphenated as adjective |
| GIL-free | GIL free | Hyphenated |
| lock-free read path | lock-free IPC | The entire read path is lock-free (atomics + CAS only); only the write path acquires a mutex |
| partial-blocking | partial blocking | Hyphenated as adjective |
| PyO3 | pyo3, Pyo3 | Official capitalization |

### Claims to Avoid (if editing prose)

- **"Lock-free synchronization"** — The entire read path is lock-free (atomics + CAS only); the write path acquires a mutex. Say "lock-free read path" not "lock-free IPC."
- **"Zero overhead"** — Say "minimal overhead" or "reduced overhead."
- **"Real-time guarantees"** — Say "low-latency" or "suitable for real-time applications."

### Original Paper Reference

Conference paper: "Accelerating Intelligent Vehicle Vision: A Hybrid Python-Rust Architecture with Partial-Blocking IPC"

### Chapter Structure

| Chapter | Label | Content |
|---------|-------|---------|
| 1. Introduction | `chap:intro` | Motivation, problem statement, contributions, thesis structure |
| 2. Foundations and Requirements | `chap:ch2` | POSIX shared memory, mmap, futex, synchronization primitives |
| 3. Related Work | `chap:ch3` | ZeroMQ, nanomsg, ipc-channel, Cap'n Proto, positioning of rs\_ipc |
| 4. Architecture & Implementation | `chap:ch4` | System overview, memory management, sync primitives, SharedMessage layout (evolution from paper), read/write protocol, Python integration (GIL, copy/zerocopy, async), trade-offs |
| 5. Performance Evaluation | `chap:ch5` | Benchmarking methodology, results, comparison with alternatives |
| 6. Conclusions | `chap:conclusions` | Summary, limitations, future work |

### Figures Location

- `paper/figures/` — thesis-specific figures
- `common/figures/` — shared with original paper (referenced in `original_paper.tex`)

### Writing Style

- Academic formal tone
- Third person preferred ("the library provides" not "we provide")
- Active voice where possible
- No contractions (don't → do not)
- Spell out numbers under 10
- Use `\texttt{}` for code/identifiers in LaTeX
- Use proper mathematical notation for speedups: `1.24$\times$`

### Status

All chapters are written and reviewed. The thesis compiles cleanly and is ready for submission.

### Chapter 5 Verification

A script at `paper/verify_chapter5.py` checks all numerical claims in Chapter 5 against `benches/bench_results_workstation.csv`:

```bash
# Verify all tables and prose claims (217 checks)
python paper/verify_chapter5.py

# Show per-check detail with line numbers
python paper/verify_chapter5.py --verbose
```

Run this after any benchmark data change to identify stale numbers. Exit code 0 = all pass, 1 = failures found.

### Platform Limitations to Document

- **Linux-only**: Requires futex syscalls (Linux-specific)
- **Python 3.12+**: Due to PyO3 features used
- **x86_64/aarch64**: Tested architectures
