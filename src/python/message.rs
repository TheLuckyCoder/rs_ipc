use crate::python::OperationMode;
use crate::python::bytes::RustPyBytes;
use crate::python::guards::{PythonReadGuard, PythonWriteGuard};
use crate::python::operation_mode::OperationMode::WriteAsync;
use crate::python::queue_data::{ReceiverQueueData, SenderQueueData};
use crate::python::reader_wait_policy::ReaderWaitPolicy;
use crate::shared_message::{SharedMessage, SharedMessageMapper};
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyBytes, PyBytesMethods};
use pyo3::{Bound, PyResult, Python, pyclass, pymethods};
use std::ffi::CString;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};

#[pyclass(module = "rs_ipc")]
#[pyo3(frozen, name = "SharedMessage")]
pub struct PythonSharedMessage {
    shared_memory: Arc<SharedMessageMapper>,
    name: String,
    op_mode: OperationMode,
    pub(crate) last_written_sequence: Arc<AtomicU64>,
    last_read_sequence: Arc<AtomicU64>,
    sender: Mutex<Option<Sender<SenderQueueData>>>,
    receiver: Mutex<Option<Receiver<ReceiverQueueData>>>,
}

static MAX_CAPACITY_BYTES: AtomicUsize = AtomicUsize::new(1024 * 1024 * 1024);

#[pymethods]
impl PythonSharedMessage {
    #[staticmethod]
    pub(crate) fn create(
        name: String,
        mode: OperationMode,
        reader_wait_policy: ReaderWaitPolicy,
    ) -> PyResult<Self> {
        if name.is_empty() {
            return Err(PyValueError::new_err("Name cannot be empty"));
        }

        let c_name = CString::new(name.clone())?;
        let shared_memory =
            SharedMessageMapper::create(c_name, SharedMessage::size_of_fields() + MAX_CAPACITY_BYTES.load(Ordering::Relaxed))?;

        shared_memory.set_target_read_count(reader_wait_policy.to_count());

        Ok(Self::new(shared_memory, name, mode))
    }

    #[staticmethod]
    fn open(name: String, mode: OperationMode) -> PyResult<Self> {
        if name.is_empty() {
            return Err(PyValueError::new_err("Name cannot be empty"));
        }
        if mode == OperationMode::CreateOnly {
            return Err(PyValueError::new_err("Mode cannot be create"));
        }

        let c_name = CString::new(name.clone())?;
        let shared_memory = SharedMessageMapper::open(c_name)?;

        Ok(Self::new(shared_memory, name, mode))
    }

    #[staticmethod]
    fn set_max_capacity(capacity_mb: NonZeroUsize) {
        MAX_CAPACITY_BYTES.store(capacity_mb.get() * 1024 * 1024, Ordering::Relaxed);
    }

    fn write(&self, data: Bound<'_, PyBytes>) -> PyResult<Option<u64>> {
        self.op_mode.check_write_permission();

        let data_bytes = data.as_bytes();
        let capacity = self.capacity();
        if data_bytes.len() > capacity {
            return Err(PyValueError::new_err(format!(
                "Message is too large to be sent! Max size: {}. Current message size: {}",
                capacity,
                data_bytes.len()
            )));
        }

        Ok(if self.op_mode == WriteAsync {
            self.write_async(data)?;
            None
        } else {
            data.py().detach(|| self.write_sync(data_bytes))
        })
    }

    fn write_guard(slf: &Bound<'_, Self>) -> PyResult<Option<PythonWriteGuard>> {
        let shared_message = slf.get();
        shared_message.op_mode.check_write_permission();

        let guard = slf.py().detach(|| shared_message.shared_memory.write());
        let object = slf.clone().unbind();

        Ok(guard.map(|guard| PythonWriteGuard::new(object, guard)))
    }

    #[pyo3(name = "read", signature = (block = true))]
    fn read_py(&self, block: bool, py: Python<'_>) -> Option<RustPyBytes> {
        self.op_mode.check_read_permission();

        py.detach(|| self.read(block))
    }

    fn read_guard(slf: &Bound<'_, Self>, block: bool) -> PyResult<Option<PythonReadGuard>> {
        let shared_message = slf.get();
        shared_message.op_mode.check_read_permission();

        let last_read = shared_message.last_read_sequence.load(Ordering::Relaxed);
        let guard = slf
            .py()
            .detach(|| shared_message.shared_memory.read(last_read, block));
        let object = slf.clone().unbind();

        if let Some(guard) = guard {
            shared_message
                .last_read_sequence
                .store(guard.sequence(), Ordering::Relaxed);
            return Ok(Some(PythonReadGuard::new(object, guard)));
        }

        Ok(None)
    }

    pub(crate) fn is_new_version_available(&self) -> bool {
        self.op_mode.check_read_permission();

        let last_sequence = self.last_read_sequence.load(Ordering::Relaxed);
        self.shared_memory.has_new_data(last_sequence)
    }

    pub(crate) fn last_written_version(&self) -> u64 {
        self.last_written_sequence.load(Ordering::Relaxed)
    }

    pub(crate) fn last_read_version(&self) -> u64 {
        self.last_read_sequence.load(Ordering::Relaxed)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn capacity(&self) -> usize {
        self.shared_memory.capacity()
    }

    fn is_stopped(&self) -> bool {
        self.shared_memory.is_stopped()
    }

    pub(crate) fn stop(&self) {
        self.shared_memory.stop();

        // Close the writer queue so the background writer exits
        if let Ok(mut guard) = self.sender.lock() {
            drop(guard.take())
        }
    }
}

impl PythonSharedMessage {
    fn new(shared_memory: SharedMessageMapper, name: String, op_mode: OperationMode) -> Self {
        if op_mode.can_read() {
            shared_memory.add_reader();
        }

        let shared_memory = Arc::new(shared_memory);
        let receiver = (op_mode == OperationMode::ReadAsync)
            .then(|| Self::start_reader_thread(shared_memory.clone(), &name));

        Self {
            shared_memory,
            name,
            op_mode,
            last_written_sequence: Arc::default(),
            last_read_sequence: Arc::default(),
            sender: Mutex::default(),
            receiver: Mutex::new(receiver),
        }
    }

    fn write_sync(&self, data: &[u8]) -> Option<u64> {
        let sequence = self.shared_memory.write_slice(data);

        if let Some(sequence) = sequence {
            self.last_written_sequence
                .store(sequence, Ordering::Relaxed);
        }

        sequence
    }

    pub(crate) fn write_async(&self, data: Bound<'_, PyBytes>) -> PyResult<()> {
        let queue_data = SenderQueueData::new(data);

        let mut guard = self
            .sender
            .lock()
            .map_err(|_| PyValueError::new_err("Lock poisoned in SharedMessage::write_async"))?;

        let sender = guard.get_or_insert_with(|| {
            let (sender, receiver) = channel::<SenderQueueData>();

            let last_written = self.last_written_sequence.clone();
            let shared_memory = self.shared_memory.clone();

            std::thread::Builder::new()
                .name(format!("{} writer thread", self.name))
                .spawn(move || {
                    loop {
                        let Ok(data) = receiver.recv() else {
                            break;
                        };
                        let new_version = shared_memory.write_slice(data.bytes());

                        let Some(new_version) = new_version else {
                            break;
                        };

                        last_written.store(new_version, Ordering::Relaxed);
                    }
                })
                .expect("Failed to create writer thread");

            sender
        });

        sender
            .send(queue_data)
            .map_err(|_| PyValueError::new_err("Failed to send data, the queue has been stopped"))
    }

    pub(crate) fn read(&self, block: bool) -> Option<RustPyBytes> {
        if self.op_mode == OperationMode::ReadAsync {
            self.read_async(block)
        } else {
            self.read_sync(block)
        }
    }

    fn read_sync(&self, block: bool) -> Option<RustPyBytes> {
        let last_read_version = self.last_read_sequence.load(Ordering::Relaxed);

        let guard = self.shared_memory.read(last_read_version, block)?;
        self.last_read_sequence
            .store(guard.sequence(), Ordering::Relaxed);
        Some(RustPyBytes::new(guard.data()))
    }

    fn read_async(&self, block: bool) -> Option<RustPyBytes> {
        let receiver_guard = self.receiver.lock().expect("Poisoned receiver guard");
        let receiver = receiver_guard
            .as_ref()
            .expect("A reader must have a receiver");

        let message = if block {
            receiver.recv().ok()
        } else {
            receiver.try_recv().ok()
        };

        message.map(|message| {
            self.last_read_sequence
                .store(message.sequence, Ordering::Relaxed);
            message.data
        })
    }

    fn start_reader_thread(
        shared_memory: Arc<SharedMessageMapper>,
        name: &str,
    ) -> Receiver<ReceiverQueueData> {
        let (sender, receiver) = channel();
        let mut last_reader_version = 0;

        std::thread::Builder::new()
            .name(format!("{} reader thread", name))
            .spawn(move || {
                while !shared_memory.is_stopped() {
                    let Some(guard) = shared_memory.read(last_reader_version, true) else {
                        continue;
                    };
                    let queue_data = ReceiverQueueData {
                        sequence: guard.sequence(),
                        data: RustPyBytes::new(guard.data()),
                    };
                    drop(guard);

                    last_reader_version = queue_data.sequence;
                    if sender.send(queue_data).is_err() {
                        // The other side of the queue was closed
                        break;
                    }
                }
            })
            .expect("Failed to create reader thread");

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

