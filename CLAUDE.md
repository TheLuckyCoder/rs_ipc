# rs_ipc

High-performance IPC library for Python, implemented in Rust. Designed for real-time computer vision pipelines in autonomous vehicles.

Originally developed for the paper "Accelerating Intelligent Vehicle Vision: A Hybrid Python-Rust Architecture with Partial-Blocking IPC" and expanded for a master's thesis.

## Build & Test

```bash
# Build Python wheel (development)
maturin develop

# Build release wheel
maturin build --release

# Run Rust tests
cargo test

# Run nightly benchmarks
cargo +nightly bench --features nightly-features

# Run Python tests
pytest
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
    self.0.fetch_add(1, Release);  // Increment counter
    futex_wake_all(&self.0);       // Wake all waiters
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
| `rs_ipc.pyi` | Python type stubs with documentation |

## Performance Analysis

### Original Paper Results

| Resolution | MP-Pipe (ms) | rs_ipc (ms) | Speedup |
|------------|--------------|-------------|---------|
| 256x256    | 0.247        | 0.200       | 1.24x   |
| 512x512    | 0.908        | 0.728       | 1.25x   |
| 1920x1080  | 16.161       | 11.26       | 1.44x   |

Overall pipeline speedup: **4.3x** vs naive parallel design with partial-blocking.

### Benchmarking Methodology

The paper benchmarked write-read round-trip latency over 50,000 iterations with:
- Mock PipeData: 3x frame size (initial frame + processed frame + features)
- Blocking operations to isolate transfer time from processing
- Intel i5-12600K workstation

Nightly benchmarks in `src/python/message.rs` (feature `nightly-features`) cover:
- `pure_write`: Write without readers
- `write_waiting_with_N_readers`: Scaling with consumer count
- `write_and_read_same_thread`: Round-trip latency

### Performance Factors

1. **Memory copy**: Dominant cost at large sizes. Using `madvise(MADV_HUGEPAGE)` for TLB efficiency.
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
- [ ] Comparison with other Rust IPC crates (ipc-channel, crossbeam-channel)
