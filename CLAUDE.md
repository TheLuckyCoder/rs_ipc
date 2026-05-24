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
```

## Architecture Overview

rs_ipc provides a `SharedMessage` abstraction for inter-process communication using POSIX shared memory (`shm_open`, `mmap`). The design prioritizes:

1. **Zero-copy semantics**: Processes read/write directly to shared memory
2. **Lock-free version checking**: Readers detect new messages without acquiring locks
3. **Flexible blocking strategies**: Non-blocking, partial-blocking, and full-blocking modes
4. **GIL-free operations**: Python threads can perform IPC without holding the GIL

### Design Evolution from Original Paper

The original paper proposed a SharedMessage specification that has been refined in the current implementation for better performance and safety.

#### Paper Specification (Section III-B)

```
┌────────────────┬──────────────┬─────────────────┬──────────────┬─────────┐
│ Message Version│ Consumer Cnt │ Message Read Cnt│ Message Size │ Message │
│    (8 bytes)   │  (8 bytes)   │    (8 bytes)    │  (8 bytes)   │(C bytes)│
└────────────────┴──────────────┴─────────────────┴──────────────┴─────────┘
Total: 40 + C bytes (with padding)
```

#### Current Implementation

```
SharedMessage<[u8]> (256-byte cache-aligned header + payload)

Cache Line 1 (read-mostly, 0-127 bytes):
┌───────────────────┬─────────────┬────────────────────────────────────────┐
│ sequence_and_flags│ data_size   │ _pad1                                  │
│ AtomicU64 (8B)    │ AtomicUsize │ [u8; 112] cache padding                │
│ [stopped:1|wip:1| │ (8B)        │                                        │
│  sequence:62]     │             │                                        │
└───────────────────┴─────────────┴────────────────────────────────────────┘

Cache Line 2 (high contention, 128-255 bytes):
┌───────────────────┬──────────────┬─────────────────┬────────────────────┐
│ readers_state     │ writer_mutex │ writer_condvar  │ reader_done_condvar│
│ AtomicU64 (8B)    │ FutexLock(4B)│ SharedCondvar(4)│ SharedCondvar (4B) │
│ [target:16|cons:16│              │                 │                    │
│  active:16|done:16│              │                 │                    │
└───────────────────┴──────────────┴─────────────────┴────────────────────┘
  + _pad2 [u8; 104] to complete 128-byte block

Data Section:
┌─────────────────────────────────────────────────────────────────────────┐
│ data: UnsafeCell<[u8]>  (payload buffer, variable size C bytes)        │
└─────────────────────────────────────────────────────────────────────────┘
Total: 256 + C bytes
```

**Key changes:**

| Aspect | Paper | Current | Rationale |
|--------|-------|---------|-----------|
| Count fields | 8 bytes each | u16 (2 bytes) | Realistic consumer limits (65535 sufficient) |
| Stopped flag | Separate field | Packed in version MSB | Reduces struct size, atomic read of both |
| Sync primitives | pthread via FFI | Raw futex syscalls | Performance: ~15% lower latency |
| Condvars | pthread_cond_t | Custom futex-based | Cross-process safe without pthread attributes |
| Blocking policy | Implicit in code | `target_read_count` field | Explicit partial-blocking configuration |

### Memory Layout Details

The `SharedMessage` struct uses `#[repr(C)]` for predictable memory layout across the FFI boundary. Key design decisions:

**Bit-packed fields** (`src/shared_message/packed.rs`):

`SequenceState` (AtomicU64):
- Bit 63: `stopped` flag
- Bit 62: `writing_in_progress` flag  
- Bits 0-61: 62-bit sequence counter

`ReadersStateCount` (AtomicU64):
- Bits 0-15: `target_read` (partial-blocking threshold)
- Bits 16-31: `consumers` (registered reader count)
- Bits 32-47: `active_readers` (currently accessing data)
- Bits 48-63: `data_consumed` (finished reading current version)

**Version wraparound** (`src/shared_message/mod.rs`):
Version 0 is reserved as "no message written yet". On overflow, version wraps to 1, not 0, to maintain this invariant.

### Synchronization Primitives

The paper's SHM-PTH implementation used pthread via Python's ctypes FFI. The current implementation uses raw Linux futex syscalls for better performance.

#### Why Futex over pthread

1. **No FFI overhead**: Direct syscall via rustix, no ctypes marshalling
2. **Simpler cross-process setup**: pthread_mutex requires `PTHREAD_PROCESS_SHARED` attribute and careful initialization in shared memory
3. **Smaller footprint**: Futex is 4 bytes vs pthread_mutex_t (40+ bytes on Linux)
4. **Spin-then-block**: Custom spin loop before syscall reduces latency for short critical sections

#### FutexLock Implementation (`src/sync/lock/futex_lock.rs`)

Three-state lock inspired by `std::sync::Mutex`:
- `UNLOCKED (0)`: Available
- `LOCKED (1)`: Held, no waiters
- `CONTENDED (2)`: Held, threads waiting

```rust
pub fn lock(&self) {
    // Fast path: CAS from UNLOCKED to LOCKED
    if self.0.compare_exchange(UNLOCKED, LOCKED, Acquire, Relaxed).is_err() {
        self.lock_contended();
    }
}

fn lock_contended(&self) {
    let mut state = self.spin();  // Spin up to 100 iterations
    // ... transition to CONTENDED and futex_wait
}
```

The spin phase (`spin()`) performs relaxed loads to avoid cache-line bouncing before falling back to the kernel.

#### SharedCondvar Implementation (`src/sync/condvar.rs`)

Uses a notification counter rather than traditional condvar semantics:

```rust
pub fn wait(&self, expected: u32) {
    for _ in 0..1000 {
        if self.0.load(Relaxed) != expected {
            return;  // Counter changed, wakeup detected
        }
        spin_loop();
    }
    futex_wait(&self.0, expected);  // Sleep if still unchanged
}

pub fn notify_all(&self) {
    self.0.fetch_add(1, Release);       // Increment counter
    assert!(futex_wake_all(&self.0));   // Wake all waiters
}
```

The caller reads the counter value before releasing any lock, then passes it to `wait()`. If `notify_all` is called between lock release and `wait()`, the counter will have changed and the spin loop returns immediately.

### Producer-Consumer Strategies

Configured via `ReaderWaitPolicy` at creation time:

| Policy | `target_read_count` | Behavior |
|--------|---------------------|----------|
| `Count(0)` | 0 | Non-blocking: overwrite immediately |
| `Count(n)` | n | Partial-blocking: wait for n readers |
| `All()` | u16::MAX | Full-blocking: wait for all registered consumers |

The writer blocks in `start_write()` while waiting for readers to consume the previous message. It waits until `data_consumed >= min(target_read, consumers)` before allowing the next write.

### PyO3 Integration & GIL Release

The module is declared with `gil_used = false` (`src/python/mod.rs`):
```rust
#[pymodule(gil_used = false)]
fn rs_ipc(m: &Bound<'_, PyModule>) -> PyResult<()> { ... }
```

Blocking operations release the GIL using `py.detach()`:
```rust
fn read_py(&self, block: bool, py: Python<'_>) -> Option<RustPyBytes> {
    py.detach(|| self.read(block))  // GIL released during read
}
```

**Safety invariants for GIL release:**
1. No Python objects accessed inside the closure
2. `RustPyBytes` wraps `Arc<[u8]>` (Rust-owned), not a Python buffer
3. Shared state uses Rust atomics/mutexes, not Python locks

#### Parallel Read (`read_all`)

Uses rayon to read from multiple SharedMessages in parallel:
```rust
fn read_all(readers: Vec<Py<PythonSharedMessage>>, py: Python<'_>) -> Vec<Option<RustPyBytes>> {
    py.detach(|| {
        readers.into_par_iter()
            .map(|reader| reader.get().read(false))
            .collect()
    })
}
```

### Async Background Threads

The paper listed async operations as future work. Now implemented:

**WriteAsync** (`src/python/message.rs`):
- Background thread blocks on write operations
- Main thread enqueues via `mpsc::channel`
- With `Count(0)`, coalesces writes: only latest message sent

**ReadAsync** (`src/python/message.rs`):
- Background thread calls blocking read continuously
- Results buffered in channel for main thread
- Useful for event-loop integration

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
| Latency 1:1 | 0.85 | 1.22 | 0.26 | 1.76 |
| Scale 1:10 | 2.05 | 9.22 | 4.44 | 5.80 |
| Size 4MB | 0.24 | 1.92 | 1.35 | 3.81 |
| Size 32MB | 27.50 | 99.85 | 50.59 | 78.70 |

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

1. **Futex-based synchronization**: Replaced pthread FFI with direct futex syscalls
2. **Async read/write modes**: Background threads for non-blocking Python API
3. **Refined memory layout**: Smaller struct, packed stopped bit, explicit blocking policy
4. **GIL release patterns**: Safe patterns for Python multithreading with Rust

### TODO: Additional Benchmarks

Criterion benchmarks needed for thesis:
- [x] Latency distribution (p50, p99, p999)
- [x] Throughput at various message sizes
- [x] Multi-consumer scaling curves
- [x] Comparison with `multiprocessing.Pipe`, `Queue`, `shared_memory`
- [x] Comparison with ZeroMQ (PUSH/PULL, message-passing baseline)
- [x] Comparison with posix_ipc (same shm mechanism, Python POSIX semaphores)
- [x] Statistical rigor (multiple trials, IQR, warmup iterations)

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
| lock-free version checking | lock-free IPC | Only version checking is lock-free; read/write uses mutex |
| partial-blocking | partial blocking | Hyphenated as adjective |
| PyO3 | pyo3, Pyo3 | Official capitalization |

### Claims to Avoid

- **"Lock-free synchronization"** — Misleading. Only version checking & multiple readers at once avoid locks; actual read/write acquires the FutexLock mutex.
- **"Zero overhead"** — There's always some overhead (futex syscalls, PyO3 FFI). Say "minimal overhead" or "reduced overhead."
- **"Real-time guarantees"** — We don't provide hard real-time guarantees. Say "low-latency" or "suitable for real-time applications."

### Original Paper Reference

Conference paper: "Accelerating Intelligent Vehicle Vision: A Hybrid Python-Rust Architecture with Partial-Blocking IPC"

### Chapter Structure

| Chapter | Label | Content |
|---------|-------|---------|
| 1. Introduction | `chap:intro` | Motivation, problem statement, contributions, thesis structure |
| 2. Foundations and Requirements | `chap:ch2` | POSIX shared memory, mmap, futex, synchronization primitives |
| 3. Related Work | `chap:ch3` | ZeroMQ, nanomsg, ipc-channel, Cap'n Proto, positioning of rs\_ipc |
| 4. Architecture & Implementation | `chap:ch4` | SharedMessage, FutexLock, SharedCondvar, PyO3 integration |
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

### What Still Needs Writing

- [x] Chapter 2: Foundations and Requirements
- [ ] Chapter 3: Related Work (ZeroMQ, nanomsg, ipc-channel, Cap'n Proto, multiprocessing alternatives)
- [ ] Chapter 4: Architecture & Implementation  
- [ ] Chapter 5: Performance Evaluation
- [ ] Chapter 6: Conclusions

### Platform Limitations to Document

- **Linux-only**: Requires futex syscalls (Linux-specific)
- **Python 3.12+**: Due to PyO3 features used
- **x86_64/aarch64**: Tested architectures
