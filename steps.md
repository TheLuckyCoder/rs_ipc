# Master's Thesis: rs_ipc Next Steps & Outline

## 1. The Immediate Next Step: Gather Your Data
Before writing heavily, complete the **TODO: Additional Benchmarks** listed in your document. Your thesis will revolve around proving that your new Rust/futex-based implementation is superior. 
- Implement the Criterion benchmarks.
- Generate graphs for latency (p50, p99, p999), throughput, and multi-consumer scaling.
- Run the comparisons against Python's `multiprocessing` tools and other Rust IPC crates.

## 2. Proposed Thesis Outline

**Chapter 1: Introduction**
* **Context:** The increasing demand for real-time computer vision in autonomous vehicles.
* **Problem:** Python is the standard for ML/Vision, but its standard IPC (multiprocessing, Pipes, Queues) and the GIL introduce unacceptable latency for real-time systems.
* **Foundation:** Briefly introduce the original paper ("Accelerating Intelligent Vehicle Vision...") and its partial-blocking IPC approach.
* **Contributions:** Clearly state your thesis contributions (Futex synchronization, Async modes, Refined memory layout, GIL release patterns).

**Chapter 2: Background & Related Work**
* **Python in ML vs. System Constraints:** Discuss the GIL, Python memory management, and why traditional IPC fails at high frequencies.
* **Existing IPC Mechanisms:** Review POSIX shared memory, `multiprocessing.shared_memory`, Unix domain sockets, and Rust crates like `ipc-channel`.
* **The Original Architecture:** Summarize the paper's SHM-PTH (pthread via FFI) implementation to set the baseline for your improvements.

**Chapter 3: Architecture and Design Evolution**
* **Refined Memory Layout:** Detail the 256-byte cache-aligned header, bit-packed fields (SequenceState), and how it prevents cache-line bouncing.
* **Futex-based Synchronization:** Explain the shift from pthread FFI to raw Linux futex syscalls. Discuss the 3-state `FutexLock` and `SharedCondvar` implementations.
* **Zero-Copy & Partial Blocking:** Explain the `ReaderWaitPolicy` and lock-free version checking.

**Chapter 4: Python-Rust Integration (PyO3)**
* **Defeating the GIL:** Deep dive into your "GIL release patterns". Explain how `py.detach()` works safely with `RustPyBytes`.
* **Async Operations:** Detail the addition of background threads for non-blocking Python APIs (`WriteAsync`, `ReadAsync`) and parallel reads using Rayon.

**Chapter 5: Evaluation & Benchmarks**
* **Methodology:** Describe the test environment (e.g., Intel i5-12600K) and workload (Mock PipeData).
* **Microbenchmarks:** Present the futex vs. pthread latency, and PyO3 overhead vs. memory copy costs.
* **System Benchmarks:** Show latency distributions (p99/p999 are critical for autonomous vehicles), throughput scaling, and multi-consumer curves.
* **Comparative Analysis:** Contrast `rs_ipc` against Python `multiprocessing` and other Rust crates. Show the pipeline speedup.

**Chapter 6: Conclusion & Future Work**
* Summarize how the hybrid architecture successfully bridges Python's ML ecosystem with Rust's systems-level performance.
* Mention potential future work (e.g., cross-machine IPC via RDMA, Windows support, etc.).

## 4. Current Benchmark Results (May 2026)

The following results were captured using the unified `python_bench.py` suite. These benchmarks compare the final optimized `rs_ipc` against standard Python alternatives and manual shared memory implementations.

### Key Findings

1. **The "Zero-Copy" Trade-off**:
   - For **large raw data** (32MB), `RsIpc_zerocopy` (3157 MB/s) is **2x faster** than `RsIpc_copy` (1529 MB/s) and outperforms all other backends.
   - For **complex objects** (MockPipeData), `RsIpc_copy` is faster in high-contention scenarios because it performs the CPU-intensive pickling *outside* the shared memory lock, reducing the critical section duration.

2. **Superior Scalability (1:10 Readers)**:
   - `RsIpc_zerocopy` maintained a latency of **~2.3ms**, while `MpShm` (manual SHM) spiked to **~10ms** when forced to hold a global lock for safety.
   - `rs_ipc`'s futex-based granular synchronization allows 4x better scalability than traditional POSIX-lock-based SHM.

3. **High-Contention Efficiency (10:1 Writers)**:
   - `RsIpc_copy` achieved **~11,000 MB/s**, demonstrating that for fan-in architectures, minimizing the time spent holding the writer lock is the primary performance driver.

4. **Reliable Tail Latency**:
   - Across all scenarios, `rs_ipc` showed significantly more stable p99 latencies compared to `MpQueue`, which suffered from erratic spikes (up to 400ms under contention).

### Next Steps for Thesis Evaluation:
- [ ] Integrate these MB/s and ms figures into Chapter 5 tables.
- [ ] Use the generated plots (`benches/plots/01-04`) to illustrate the "Knee of the Curve" where `zerocopy` overtakes `copy` mode.
- [ ] Document the "Safe vs. Fast" MpShm comparison to justify the library's architectural complexity.









  1. "Who" to compare (The Contenders)
  You only need to compare a few carefully selected milestones. Think of them as characters in your thesis story:

   * The Baseline (Python Standard Library): multiprocessing.Queue and multiprocessing.shared_memory. This proves why Python needed your
     help in the first place.
   * The State-of-the-Art (Alternative Crates): ipc-channel or even ZeroMQ. This shows you are aware of the ecosystem and how you stack
     up against general-purpose tools.
   * The Ancestor: The original paper's implementation (SHM-PTH). This is your starting point.
   * The Final Boss: Your current, final, optimized rs_ipc (with futex and async).
   * The "Ablation" Version (Optional but highly recommended): Pick exactly one major turning point in your development. For example, if
     you switched from Pthreads to Futexes, benchmark the last Pthread version vs. the first Futex version. This proves to the grading
     committee why you made that specific architectural change.

  2. "What" to measure (The Scenarios)
  Since your context is "autonomous vehicles," the grading committee will care about predictable performance over raw speed. You need
  three distinct test suites:

   1. Latency Distribution (The most important one):
       * Don't just report averages. Report p50 (median), p99, and p999 latency.
       * Autonomous driving cares about the worst-case scenario (p999). If a frame takes 100ms to arrive, the car crashes, even if the
         average is 1ms.
   2. Throughput vs. Payload Size:
       * Send messages of sizes: 1KB (control signals), 1MB (small images/features), 10MB (1080p raw images).
       * Measure MB/s. This shows where memory-copying bottlenecks dominate your futex/lock optimizations.
   3. Scalability (Fan-out):
       * 1 Writer -> 1 Reader
       * 1 Writer -> 3 Readers
       * 1 Writer -> 10 Readers
       * This proves your lock-free version checking and target_read_count scale better than traditional locks.

  3. "How" to execute it (The Methodology)
  You need to separate your benchmarks into two layers:

   * Layer 1: Micro-benchmarks (Rust/Criterion): Use the criterion crate in Rust. It does statistical analysis for you. Use this to
     benchmark purely internal Rust mechanisms (e.g., Futex acquisition time vs. Pthread lock time).
   * Layer 2: System/Macro-benchmarks (Python): This is what you have in benchmark.py. You need a single Python script that can be run
     with an argument like --backend rs_ipc or --backend python_shm. It should run 10,000 iterations and spit out a CSV of times.


  3. Next Step: Thesis Writing (LaTeX)
  Since you are using LaTeX, we can now start drafting Chapter 5. I recommend the following structure:
   1. Experimental Setup: Detail your machine specs (Ryzen 7 7700, Arch Linux, Python 3.14).
   2. Micro-benchmarks: Present the Rust-level Criterion results.
   3. System-level Benchmarks: The Python macro-benchmarks we just ran.
   4. Discussion: Explain why the futex + zerocopy approach wins (e.g., lower context switching, zero serialization
      overhead).

