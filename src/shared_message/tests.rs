use super::*;
use std::alloc::{Layout, alloc, dealloc};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

struct TestBuffer {
    ptr: *mut u8,
    layout: Layout,
    total_size: usize,
}

unsafe impl Send for TestBuffer {}
unsafe impl Sync for TestBuffer {}

impl TestBuffer {
    fn new(payload_size: usize) -> Self {
        let header_size = SharedMessage::size_of_fields();
        let total_size = header_size + payload_size;
        let layout =
            Layout::from_size_align(total_size, SharedMessage::align_of_fields()).unwrap();
        unsafe {
            let ptr = alloc(layout);
            if ptr.is_null() {
                panic!("Allocation failed");
            }
            std::ptr::write_bytes(ptr, 0, total_size);
            Self {
                ptr,
                layout,
                total_size,
            }
        }
    }

    fn get_message(&self) -> &SharedMessage {
        unsafe {
            <SharedMessage as SlicePtrCast>::cast_from_void_ptr(
                NonNull::new(self.ptr.cast()).unwrap(),
                self.total_size,
            )
            .unwrap()
            .as_ref()
        }
    }
}

impl Drop for TestBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}

#[test]
fn test_shared_message_basic() {
    let buffer = TestBuffer::new(1024);
    let msg = buffer.get_message();

    let data = b"hello world";
    msg.write_slice(data);
    assert_eq!(msg.current_sequence(), 1);

    let read_guard = msg.read(0, false).unwrap();
    assert_eq!(read_guard.data(), data);
    assert_eq!(read_guard.sequence(), 1);
}

#[test]
fn test_sync_write_try_read() {
    let buffer = TestBuffer::new(1024);
    let msg = buffer.get_message();
    let data = (0u8..255u8).collect::<Vec<_>>();

    assert!(msg.read(0, false).is_none());

    msg.write_slice(&data);
    let seq = msg.current_sequence();

    let guard = msg.read(0, false).expect("Should have data");
    assert_eq!(guard.data(), data);
    assert_eq!(guard.sequence(), seq);

    assert!(msg.read(seq, false).is_none());
    msg.stop();
    assert!(msg.is_stopped());
    assert!(msg.read(seq, true).is_none());
}

#[test]
fn test_sync_write_blocking_read() {
    let buffer = Arc::new(TestBuffer::new(1024));
    let msg = buffer.get_message();
    let data = (0u8..100u8).collect::<Vec<_>>();

    let b_clone = buffer.clone();
    let d_clone = data.clone();
    let handle = thread::spawn(move || {
        let msg = b_clone.get_message();
        let guard = msg.read(0, true).expect("Should unblock and read");
        assert_eq!(guard.data(), d_clone);
    });

    // Give reader time to block (not strictly necessary but helps in some environments)
    thread::sleep(Duration::from_millis(10));
    msg.write_slice(&data);
    handle.join().unwrap();
}

#[test]
fn test_write_multiple_readers() {
    let buffer = Arc::new(TestBuffer::new(1024));
    let msg = buffer.get_message();
    let num_readers = 5;
    let num_writes = 100;

    let mut handles = vec![];
    for _ in 0..num_readers {
        let b = buffer.clone();
        handles.push(thread::spawn(move || {
            let msg = b.get_message();
            let mut last_seq = 0;
            loop {
                if let Some(guard) = msg.read(last_seq, true) {
                    last_seq = guard.sequence();
                } else if msg.is_stopped() {
                    break;
                }
            }
        }));
    }

    for i in 0..num_writes {
        msg.write_slice(&[i as u8]).unwrap();
    }
    msg.stop();

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_stop_signal_wakeups() {
    let buffer = Arc::new(TestBuffer::new(1024));
    let b_clone = buffer.clone();
    
    let handle = thread::spawn(move || {
        let msg = b_clone.get_message();
        // This should block until stop() is called
        let result = msg.read(0, true);
        assert!(result.is_none(), "Should return None after stop");
    });

    thread::sleep(Duration::from_millis(50));
    buffer.get_message().stop();
    handle.join().unwrap();
}

#[test]
fn test_reader_wait_policies() {
    let buffer = Arc::new(TestBuffer::new(1024));
    let msg = buffer.get_message();
    
    // Set target read count to 1 and register one consumer
    msg.set_target_read_count(1);
    msg.add_reader();
    
    let b_clone = buffer.clone();
    let writer_handle = thread::spawn(move || {
        let msg = b_clone.get_message();
        msg.write_slice(b"first").unwrap();
        // This second write should block until a reader consumes the first
        msg.write_slice(b"second").unwrap();
    });

    // Wait for the first write to land
    while msg.current_sequence() < 1 {
        thread::yield_now();
    }
    thread::sleep(Duration::from_millis(50));
    assert_eq!(msg.current_sequence(), 1, "Writer should be blocked on backpressure");
    // Reader consumes first message
    {
        let guard = msg.read(0, true).unwrap();
        assert_eq!(guard.data(), b"first");
        // guard dropped here, release_active_reader called, writer unblocked
    }

    writer_handle.join().unwrap();
    
    let guard = msg.read(1, true).unwrap();
    assert_eq!(guard.data(), b"second");
}

#[test]
fn test_concurrent_simultaneous_reads() {
    let buffer = Arc::new(TestBuffer::new(1024));
    let msg = buffer.get_message();
    msg.write_slice(b"data").unwrap();
    let seq = msg.current_sequence();

    let mut handles = vec![];
    for _ in 0..20 {
        let b = buffer.clone();
        handles.push(thread::spawn(move || {
            let msg = b.get_message();
            let guard = msg.read(seq - 1, true).unwrap();
            assert_eq!(guard.data(), b"data");
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_writer_writer_contention() {
    let buffer = Arc::new(TestBuffer::new(1024));
    let num_writes_per_thread = 50;

    let handles: Vec<_> = (0..2)
        .map(|t| {
            let b = buffer.clone();
            thread::spawn(move || {
                for i in 0..num_writes_per_thread {
                    b.get_message().write_slice(&[t, i as u8]);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(buffer.get_message().current_sequence(), 100);
}

#[test]
fn test_read_during_write_retry() {
    let buffer = Arc::new(TestBuffer::new(1024));
    let msg = buffer.get_message();
    msg.write_slice(b"initial");

    let num_readers = 10;
    let mut handles = vec![];

    for _ in 0..num_readers {
        let b = buffer.clone();
        handles.push(thread::spawn(move || {
            let msg = b.get_message();
            let guard = msg.read(1, true).unwrap();
            assert_eq!(guard.data(), b"final");
        }));
    }

    thread::yield_now();

    msg.write_slice(b"final");

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
#[should_panic(expected = "Data size (2048 bytes) exceeds capacity (1024 bytes)")]
fn test_capacity_boundary() {
    let buffer = TestBuffer::new(1024);
    let msg = buffer.get_message();
    let large_data = vec![0u8; 2048];
    msg.write_slice(&large_data);
}
