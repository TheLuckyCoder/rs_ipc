use crate::container::message::SharedMessage;
use crate::python::bytes::RustPyBytes;
use crate::helpers::queue_data::{ReceiverQueueData, SenderQueueData};
use crate::primitives::memory_mapper::SharedMemoryMapper;
use crate::python::OperationMode;
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyBytes, PyBytesMethods};
use pyo3::{pyclass, pymethods, Bound, PyResult, Python};
use std::ffi::CString;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use crate::python::reader_wait_policy::ReaderWaitPolicy;

#[pyclass]
#[pyo3(frozen, name = "SharedMessage")]
pub struct PythonSharedMessage {
    shared_memory: Arc<SharedMemoryMapper<SharedMessage>>,
    name: String,
    op_mode: OperationMode,
    reader_wait_policy: ReaderWaitPolicy,
    last_written_version: Arc<AtomicUsize>,
    last_read_version: Arc<AtomicUsize>,
    sender: Mutex<Option<Sender<SenderQueueData>>>,
    receiver: Mutex<Option<Receiver<ReceiverQueueData>>>,
}

impl PythonSharedMessage {
    fn new(
        shared_memory: SharedMemoryMapper<SharedMessage>,
        name: String,
        op_mode: OperationMode,
        reader_wait_policy: ReaderWaitPolicy,
    ) -> Self {
        if op_mode.can_read() {
            shared_memory.add_reader();
        }

        let shared_memory = Arc::new(shared_memory);
        let last_read_version = Arc::new(AtomicUsize::default());
        let receiver = op_mode
            .can_read()
            .then(|| Self::start_reader_thread(shared_memory.clone(), last_read_version.clone()));

        Self {
            shared_memory,
            name,
            op_mode,
            reader_wait_policy,
            last_written_version: Arc::default(),
            last_read_version: Arc::default(),
            sender: Mutex::default(),
            receiver: Mutex::new(receiver),
        }
    }
}

#[pymethods]
impl PythonSharedMessage {
    #[staticmethod]
    #[pyo3(signature = (name, size, mode, reader_wait_policy = ReaderWaitPolicy::All()))]
    fn create(
        name: String,
        size: NonZeroU32,
        mode: OperationMode,
        reader_wait_policy: ReaderWaitPolicy,
    ) -> PyResult<Self> {
        if name.is_empty() {
            return Err(PyValueError::new_err("Name cannot be empty"));
        }

        let c_name = CString::new(name.clone())?;
        let shared_memory = unsafe {
            SharedMemoryMapper::<SharedMessage>::create(
                c_name,
                SharedMessage::size_of_fields() + size.get() as usize,
            )?
        };

        Ok(Self::new(shared_memory, name, mode, reader_wait_policy))
    }

    #[staticmethod]
    #[pyo3(signature = (name, mode, reader_wait_policy = ReaderWaitPolicy::All()))]
    fn open(name: String, mode: OperationMode, reader_wait_policy: ReaderWaitPolicy) -> PyResult<Self> {
        if name.is_empty() {
            return Err(PyValueError::new_err("Name cannot be empty"));
        }

        let c_name = CString::new(name.clone())?;
        let shared_memory = unsafe { SharedMemoryMapper::<SharedMessage>::open(c_name)? };

        Ok(Self::new(shared_memory, name, mode, reader_wait_policy))
    }

    fn write(&self, data: Bound<'_, PyBytes>) -> PyResult<Option<usize>> {
        self.op_mode.check_write_permission();

        let data_bytes = data.as_bytes();
        if data_bytes.len() > self.payload_max_size() {
            return Err(PyValueError::new_err(format!(
                "Message is too large to be sent! Max size: {}. Current message size: {}",
                self.payload_max_size(),
                data_bytes.len()
            )));
        }

        Ok(if self.op_mode == OperationMode::WriteAsync {
            self.write_async(data)?;
            None
        } else {
            Some(data.py().allow_threads(|| self.write_sync(data_bytes)))
        })
    }

    #[pyo3(name = "read", signature = (block = true))]
    fn read_py(&self, block: bool, py: Python<'_>) -> Option<RustPyBytes> {
        self.op_mode.check_read_permission();

        py.allow_threads(|| self.read(block))
    }

    fn is_new_version_available(&self) -> bool {
        self.op_mode.check_read_permission();

        let last_read_version = self.last_read_version.load(Ordering::Relaxed);
        self.shared_memory
            .is_new_version_available(last_read_version)
    }

    fn last_written_version(&self) -> usize {
        self.last_written_version.load(Ordering::Relaxed)
    }

    fn last_read_version(&self) -> usize {
        self.last_read_version.load(Ordering::Relaxed)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn payload_max_size(&self) -> usize {
        self.shared_memory.mapped_memory_size() - SharedMessage::size_of_fields()
    }

    fn is_closed(&self) -> bool {
        self.shared_memory.get_version().closed
    }

    fn close(&self) {
        self.op_mode.check_write_permission();
        self.shared_memory.close();
    }
}

impl PythonSharedMessage {
    fn write_sync(&self, data: &[u8]) -> usize {
        let version = if self.reader_wait_policy == ReaderWaitPolicy::Count(0) {
            self.shared_memory.write(data)
        } else {
            self.shared_memory
                .write_waiting_for_readers(data, self.reader_wait_policy.to_count())
        };
        self.last_written_version.store(version, Ordering::Relaxed);
        version
    }

    fn write_async(&self, data: Bound<'_, PyBytes>) -> PyResult<()> {
        let queue_data = SenderQueueData::new(data);

        let mut guard = self.sender.lock().unwrap();
        let sender = guard.get_or_insert_with(|| {
            let (sender, receiver) = channel::<SenderQueueData>();

            let last_written_version = self.last_written_version.clone();
            let shared_memory = self.shared_memory.clone();
            let reader_wait_policy = self.reader_wait_policy;

            std::thread::spawn(move || loop {
                let Ok(data) = receiver.recv() else {
                    break;
                };
                let new_version = if reader_wait_policy == ReaderWaitPolicy::Count(0) {
                    // If we are not waiting for readers, we only care about the latest data
                    let data = receiver.try_iter().last().unwrap_or(data);
                    shared_memory.write(data.bytes())
                } else {
                    shared_memory.write_waiting_for_readers(data.bytes(), reader_wait_policy.to_count())
                };

                last_written_version.store(new_version, Ordering::Relaxed);
            });

            sender
        });

        sender.send(queue_data).map_err(|_| {
            PyValueError::new_err("Failed to send data, the queue has been closed".to_string())
        })
    }

    pub(crate) fn read(&self, block: bool) -> Option<RustPyBytes> {
        if self.op_mode == OperationMode::ReadAsync {
            self.read_async(block)
        } else {
            self.read_sync(block)
        }
    }

    fn read_sync(&self, block: bool) -> Option<RustPyBytes> {
        let last_read_version = self.last_read_version.load(Ordering::Relaxed);
        let mut result = None;

        if block {
            self.shared_memory
                .blocking_read(last_read_version, |new_version, data| {
                    self.last_read_version.store(new_version, Ordering::Relaxed);
                    result = Some(RustPyBytes::new(data));
                });
        } else {
            self.shared_memory
                .try_read(last_read_version, |new_version, data| {
                    self.last_read_version.store(new_version, Ordering::Relaxed);
                    result = Some(RustPyBytes::new(data));
                });
        }

        result
    }

    fn read_async(&self, block: bool) -> Option<RustPyBytes> {
        let receiver_guard = self.receiver.lock().unwrap();
        let receiver = receiver_guard
            .as_ref()
            .expect("A reader must have a receiver");

        let message = if block {
            receiver.recv().ok()
        } else {
            receiver.try_recv().ok()
        };

        message.map(|message| {
            self.last_read_version
                .store(message.version, Ordering::Relaxed);
            message.data
        })
    }

    fn start_reader_thread(
        shared_memory: Arc<SharedMemoryMapper<SharedMessage>>,
        last_read_version: Arc<AtomicUsize>,
    ) -> Receiver<ReceiverQueueData> {
        let (sender, receiver) = channel();
        let mut local_last_reader_version = last_read_version.load(Ordering::Relaxed);

        std::thread::spawn(move || {
            while !shared_memory.get_version().closed {
                let mut queue_data = None;
                shared_memory.blocking_read(local_last_reader_version, |new_version, data| {
                    queue_data = Some(ReceiverQueueData {
                        version: new_version,
                        data: RustPyBytes::new(data),
                    });
                });

                if let Some(queue_data) = queue_data {
                    local_last_reader_version = queue_data.version;
                    last_read_version.store(local_last_reader_version, Ordering::Relaxed);
                    let _ = sender.send(queue_data);
                }
            }
        });

        receiver
    }
}

impl Drop for PythonSharedMessage {
    fn drop(&mut self) {
        if self.op_mode.can_read() {
            self.shared_memory.remove_reader();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZero;
    use std::thread;
    use std::time::Duration;

    const DEFAULT_SIZE: u32 = 1024;

    fn init(name: &str, op_mode: OperationMode, reader_wait_policy: ReaderWaitPolicy) -> PythonSharedMessage {
        PythonSharedMessage::create(
            name.to_string(),
            NonZero::new(DEFAULT_SIZE).unwrap(),
            op_mode,
            reader_wait_policy,
        )
        .unwrap()
    }

    #[test]
    fn sync_write_try_read() {
        let data_vec = (0u8..255u8).collect::<Vec<_>>();

        let memory = init("sync_write_try_read", OperationMode::WriteSync, ReaderWaitPolicy::Count(0));
        let none = memory.read(false);
        assert!(none.is_none());

        memory.write_sync(&data_vec);
        let version = memory.last_written_version();

        let bytes = memory.read(false).unwrap();
        assert_eq!(bytes.0.as_ref(), data_vec);
        assert_eq!(version, memory.last_read_version());

        assert!(memory.read(false).is_none());
        memory.close();
        assert!(memory.is_closed());
        assert!(memory.read(true).is_none());
        assert!(memory.read(false).is_none());
    }

    #[test]
    fn sync_write_blocking_read() {
        let data_vec = (0u8..255u8).collect::<Vec<_>>();

        let memory = init("sync_write_blocking_read", OperationMode::WriteSync, ReaderWaitPolicy::Count(0));
        assert!(memory.read(false).is_none());

        memory.write_sync(&data_vec);
        let version = memory.last_written_version();

        let bytes = memory.read(true).unwrap();
        assert_eq!(bytes.0.as_ref(), data_vec);
        assert_eq!(version, memory.last_read_version());

        assert!(memory.read(false).is_none());
        memory.close();
    }

    #[test]
    fn async_write() {
        Python::with_gil(|py| {
            let memory = init("async_write", OperationMode::ReadAsync, ReaderWaitPolicy::Count(0));

            memory.write_async(PyBytes::new(py, &[1])).unwrap();
            memory.write_async(PyBytes::new(py, &[2])).unwrap();
            memory.write_async(PyBytes::new(py, &[3])).unwrap();
            memory.write_async(PyBytes::new(py, &[4])).unwrap();
            thread::sleep(Duration::from_millis(100));
            assert!(memory.is_new_version_available());
            assert_eq!(memory.read(true).unwrap(), RustPyBytes::new(&[4]));
        });
    }

    #[test]
    fn async_write_try_read() {
        Python::with_gil(|py| {
            let data = PyBytes::new(py, &(0u8..255u8).collect::<Vec<_>>());

            let memory = init("async_write_try_read", OperationMode::ReadAsync, ReaderWaitPolicy::All());
            let none = memory.read(false);
            assert!(none.is_none());

            memory.write_async(data.clone()).unwrap();
            thread::sleep(Duration::from_millis(200));
            let version = memory.last_written_version();

            let bytes = memory.read(false).unwrap();
            assert_eq!(bytes.0.as_ref(), data);
            assert_eq!(version, memory.last_read_version());

            assert!(memory.read(false).is_none());
        });
    }

    #[test]
    fn async_write_blocking_read() {
        Python::with_gil(|py| {
            let data = PyBytes::new(py, &(0u8..255u8).collect::<Vec<_>>());

            let memory = init("async_write_blocking_read", OperationMode::ReadAsync, ReaderWaitPolicy::All());

            memory.write_async(data.clone()).unwrap();
            thread::sleep(Duration::from_millis(200));
            let version = memory.last_written_version();

            let bytes = memory.read(true).unwrap();
            assert_eq!(bytes.0.as_ref(), data);
            assert_eq!(version, memory.last_read_version());
        });
    }

    #[test]
    fn multiple_writes() {
        Python::with_gil(|py| {
            let memory = init("async_multiple_writes", OperationMode::ReadAsync, ReaderWaitPolicy::All());

            for i in 0..100 {
                memory.write_async(PyBytes::new(py, &[i])).unwrap();
            }

            for i in 0..100 {
                assert_eq!(memory.read(true).unwrap(), RustPyBytes::new(&[i]));
            }
        });
    }
}
