use crate::python::bytes::RustPyBytes;
use crate::python::operation_mode::OperationMode;
use crate::python::reader_wait_policy::ReaderWaitPolicy;
use crate::zero_copy::{ZeroCopySharedMessage, ZeroCopySharedMessageMapper};
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyBytes, PyBytesMethods};
use pyo3::{pyclass, pymethods, Bound, PyResult, Python};
use std::ffi::CString;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use crate::python::read_write_guards::{PythonReadGuard, PythonWriteGuard};

#[pyclass(module = "rs_ipc")]
#[pyo3(frozen, name = "ZeroCopySharedMessage")]
pub struct PythonZeroCopySharedMessage {
    shared_memory: Arc<ZeroCopySharedMessageMapper>,
    name: String,
    op_mode: OperationMode,
    last_read_sequence: Arc<AtomicU64>,
}

#[pymethods]
impl PythonZeroCopySharedMessage {
    #[staticmethod]
    fn create(
        name: String,
        size: NonZeroUsize,
        mode: OperationMode,
        reader_wait_policy: ReaderWaitPolicy,
    ) -> PyResult<Self> {
        if name.is_empty() {
            return Err(PyValueError::new_err("Name cannot be empty"));
        }

        let c_name = CString::new(name.clone())?;
        let shared_memory = ZeroCopySharedMessageMapper::create(
            c_name,
            ZeroCopySharedMessage::size_of_fields() + size.get(),
        )?;

        shared_memory.set_target_read_count(reader_wait_policy.to_count());

        Ok(Self::new(shared_memory, name, mode))
    }

    #[staticmethod]
    fn open(name: String, mode: OperationMode) -> PyResult<Self> {
        if name.is_empty() {
            return Err(PyValueError::new_err("Name cannot be empty"));
        }

        let c_name = CString::new(name.clone())?;
        let shared_memory = ZeroCopySharedMessageMapper::open(c_name)?;

        Ok(Self::new(shared_memory, name, mode))
    }

    fn write(&self, data: Bound<'_, PyBytes>, py: Python<'_>) -> PyResult<Option<u64>> {
        self.op_mode.check_write_permission();

        let data_bytes = data.as_bytes();
        if data_bytes.len() > self.payload_max_size() {
            return Err(PyValueError::new_err(format!(
                "Message is too large to be sent! Max size: {}. Current message size: {}",
                self.payload_max_size(),
                data_bytes.len()
            )));
        }

        Ok(py.detach(|| self.shared_memory.write(data_bytes)))
    }

    fn write_guard(&self, py: Python<'_>) -> PyResult<Option<PythonWriteGuard>> {
        self.op_mode.check_write_permission();

        let guard = py.detach(|| self.shared_memory.acquire_write_guard());

        Ok(guard.map(PythonWriteGuard::new))
    }

    #[pyo3(signature = (block = true))]
    fn read_guard(&self, block: bool, py: Python<'_>) -> PyResult<Option<PythonReadGuard>> {
        self.op_mode.check_read_permission();

        let last_seq = self.last_read_sequence.load(Ordering::Relaxed);
        let guard = py.detach(|| {
            if block {
                self.shared_memory.blocking_read(last_seq)
            } else {
                self.shared_memory.try_read(last_seq)
            }
        });

        match guard {
            Some(guard) => {
                let seq: u64 = guard.sequence();
                self.last_read_sequence.store(seq, Ordering::Relaxed);
                Ok(Some(PythonReadGuard::new(guard)))
            }
            None => Ok(None),
        }
    }

    #[pyo3(signature = (block = true))]
    fn read_copy(&self, block: bool, py: Python<'_>) -> Option<RustPyBytes> {
        self.op_mode.check_read_permission();

        let last_seq = self.last_read_sequence.load(Ordering::Relaxed);
        let guard = py.detach(|| {
            if block {
                self.shared_memory.blocking_read(last_seq)
            } else {
                self.shared_memory.try_read(last_seq)
            }
        });

        guard.map(|guard| {
            let seq: u64 = guard.sequence();
            self.last_read_sequence.store(seq, Ordering::Relaxed);
            RustPyBytes::new(guard.as_ref())
        })
    }

    fn is_new_version_available(&self) -> bool {
        self.op_mode.check_read_permission();

        let last_seq = self.last_read_sequence.load(Ordering::Relaxed);
        self.shared_memory.has_new_data(last_seq)
    }

    fn last_read_version(&self) -> u64 {
        self.last_read_sequence.load(Ordering::Relaxed)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn payload_max_size(&self) -> usize {
        self.shared_memory.buffer_size()
    }

    fn is_stopped(&self) -> bool {
        self.shared_memory.is_stopped()
    }

    fn stop(&self) {
        self.shared_memory.stop();
    }
}

impl PythonZeroCopySharedMessage {
    fn new(
        shared_memory: ZeroCopySharedMessageMapper,
        name: String,
        op_mode: OperationMode,
    ) -> Self {
        if op_mode.can_read() {
            shared_memory.add_reader();
        }

        Self {
            shared_memory: Arc::new(shared_memory),
            name,
            op_mode,
            last_read_sequence: Arc::default(),
        }
    }
}

impl Drop for PythonZeroCopySharedMessage {
    fn drop(&mut self) {
        if self.op_mode.can_read() {
            self.shared_memory.remove_reader();
        }
    }
}
