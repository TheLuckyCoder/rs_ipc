use crate::python::bytes::RustPyBytes;
use crate::python::operation_mode::OperationMode;
use crate::python::reader_wait_policy::ReaderWaitPolicy;
use crate::zero_copy::{ReadGuard, ZeroCopySharedMessage, ZeroCopySharedMessageMapper};
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyBytes, PyBytesMethods};
use pyo3::{pyclass, pymethods, Bound, PyErr, PyRef, PyRefMut, PyResult, Python};
use std::ffi::{c_int, CString};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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

    #[pyo3(signature = (block = true))]
    fn read(&self, block: bool, py: Python<'_>) -> PyResult<Option<PythonReadGuard>> {
        self.op_mode.check_read_permission();

        let last_seq = self.last_read_sequence.load(Ordering::Relaxed);
        let guard = py.detach(|| {
            if block {
                self.shared_memory.read(last_seq)
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
                self.shared_memory.read(last_seq)
            } else {
                self.shared_memory.try_read(last_seq)
            }
        });

        guard.map(|guard| {
            let seq: u64 = guard.sequence();
            self.last_read_sequence.store(seq, Ordering::Relaxed);
            RustPyBytes::new(guard.data())
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

/// Python wrapper for ReadGuard that implements the buffer protocol
#[pyclass(module = "rs_ipc")]
pub struct PythonReadGuard {
    guard: Option<ReadGuard<'static>>,
}

impl PythonReadGuard {
    fn new(guard: ReadGuard<'_>) -> Self {
        // SAFETY: We're extending the lifetime here, but it's safe because:
        // 1. The ReadGuard holds a reference count on the buffer
        // 2. The buffer is in shared memory that persists beyond any single reference
        // 3. The guard will properly decrement the reference count when dropped
        let guard = unsafe { std::mem::transmute::<ReadGuard<'_>, ReadGuard<'static>>(guard) };
        Self { guard: Some(guard) }
    }
}

#[pymethods]
impl PythonReadGuard {
    fn __len__(&self) -> usize {
        self.guard.as_ref().map(|g| g.len()).unwrap_or(0)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        mut slf: PyRefMut<'_, Self>,
        _exc_type: Option<&Bound<'_, pyo3::types::PyAny>>,
        _exc_value: Option<&Bound<'_, pyo3::types::PyAny>>,
        _traceback: Option<&Bound<'_, pyo3::types::PyAny>>,
    ) -> PyResult<bool> {
        // Drop the guard to release the reader reference
        drop(slf.guard.take());
        Ok(false)
    }

    unsafe fn __getbuffer__(
        slf: PyRef<Self>,
        view: *mut pyo3::ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let guard = slf
            .guard
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("Guard has been released"))?;

        let data = guard.data();
        let ret = pyo3::ffi::PyBuffer_FillInfo(
            view,
            slf.as_ptr() as *mut _,
            data.as_ptr() as *mut _,
            data.len().try_into()?,
            1, // read only
            flags,
        );
        if ret == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut pyo3::ffi::Py_buffer) {}

    /// Get the sequence number of this message
    fn sequence(&self) -> PyResult<u64> {
        self.guard
            .as_ref()
            .map(|g| g.sequence())
            .ok_or_else(|| PyValueError::new_err("Guard has been released"))
    }

    /// Copy the data to a Python bytes object
    fn to_bytes<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let guard = self
            .guard
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("Guard has been released"))?;
        Ok(PyBytes::new(py, guard.data()))
    }
}
