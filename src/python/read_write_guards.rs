use std::ffi::c_int;
use pyo3::{pyclass, pymethods, Bound, PyErr, PyRef, PyRefMut, PyResult, Python};
use pyo3::exceptions::PyValueError;
use pyo3::types::PyBytes;
use crate::{MessageReadGuard, MessageWriteGuard};

/// Python wrapper for ReadGuard that implements the buffer protocol
#[pyclass(module = "rs_ipc")]
#[pyo3(name = "ReadGuard")]
pub struct PythonReadGuard {
    guard: Option<MessageReadGuard<'static>>,
}

impl PythonReadGuard {
    pub(crate) fn new(guard: MessageReadGuard<'_>) -> Self {
        // SAFETY: We're extending the lifetime here, but it's safe because:
        // 1. The ReadGuard holds a reference count on the buffer
        // 2. The buffer is in shared memory that persists beyond any single reference
        // 3. The guard will properly decrement the reference count when dropped
        let guard = unsafe { std::mem::transmute::<MessageReadGuard<'_>, MessageReadGuard<'static>>(guard) };
        Self { guard: Some(guard) }
    }
}

#[pymethods]
impl PythonReadGuard {
    fn __len__(&self) -> usize {
        self.guard.as_ref().map(|g| g.data().len()).unwrap_or(0)
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

/// Python wrapper for WriteGuard that implements the buffer protocol
#[pyclass(module = "rs_ipc")]
#[pyo3(name = "WriteGuard")]
pub struct PythonWriteGuard {
    guard: Option<MessageWriteGuard<'static>>,
}

impl PythonWriteGuard {
    pub(crate) fn new(guard: MessageWriteGuard<'_>) -> Self {
        // SAFETY: We're extending the lifetime here, but it's safe because:
        // 1. The WriteGuard holds exclusive access to the buffer
        // 2. The buffer is in shared memory that persists beyond any single reference
        // 3. The guard will properly publish or drop the buffer when done
        let guard = unsafe { std::mem::transmute::<MessageWriteGuard<'_>, MessageWriteGuard<'static>>(guard) };
        Self { guard: Some(guard) }
    }
}

#[pymethods]
impl PythonWriteGuard {
    fn __len__(&self) -> usize {
        self.guard.as_ref().map(|g| g.capacity()).unwrap_or(0)
    }

    fn __enter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }

    fn __exit__(
        mut slf: PyRefMut<'_, Self>,
        _exc_type: Option<&Bound<'_, pyo3::types::PyAny>>,
        _exc_value: Option<&Bound<'_, pyo3::types::PyAny>>,
        _traceback: Option<&Bound<'_, pyo3::types::PyAny>>,
    ) -> PyResult<bool> {
        // Drop the guard without publishing if user didn't call publish()
        drop(slf.guard.take());
        Ok(false)
    }

    unsafe fn __getbuffer__(
        mut slf: PyRefMut<Self>,
        view: *mut pyo3::ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let guard = slf
            .guard
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("Guard has been released"))?;

        let data = guard.buffer_mut();
        let data_ptr = data.as_mut_ptr();
        let data_len = data.len();

        let ret = pyo3::ffi::PyBuffer_FillInfo(
            view,
            slf.as_ptr() as *mut _,
            data_ptr as *mut _,
            data_len.try_into()?,
            0, // writable
            flags,
        );
        if ret == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut pyo3::ffi::Py_buffer) {}

    /// Publish the written data with the given size.
    /// Returns the sequence number if successful, None if stopped.
    fn publish(mut slf: PyRefMut<'_, Self>, size: usize) -> PyResult<Option<u64>> {
        let guard = slf
            .guard
            .take()
            .ok_or_else(|| PyValueError::new_err("Guard has already been published or released"))?;
        Ok(guard.publish(size))
    }

    /// Get the capacity of the write buffer
    fn capacity(&self) -> PyResult<usize> {
        self.guard
            .as_ref()
            .map(|g| g.capacity())
            .ok_or_else(|| PyValueError::new_err("Guard has been released"))
    }
}
