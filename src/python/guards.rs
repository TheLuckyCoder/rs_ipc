use crate::python::message::PythonSharedMessage;
use crate::{MessageReadGuard, MessageWriteGuard};
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods, PyBytes};
use pyo3::{
    Bound, Py, PyErr, PyRef, PyRefMut, PyResult, PyTraverseError, PyVisit, Python, pyclass,
    pymethods,
};
use std::ffi::c_int;
use std::sync::atomic::Ordering;

#[pyclass(module = "rs_ipc")]
#[pyo3(name = "ReadGuard")]
pub struct PythonReadGuard {
    // Holds the ref-counted parent and the guard itself
    // We keep them together in an Option so we can drop them in __exit__ or __clear__
    inner: Option<(Py<PythonSharedMessage>, MessageReadGuard<'static>)>,

    // Tracks position for file-like read operations (pickle.load)
    cursor: usize,
}

impl PythonReadGuard {
    pub(crate) fn new(shared_memory: Py<PythonSharedMessage>, guard: MessageReadGuard<'_>) -> Self {
        // SAFETY: We extend the lifetime to static because we are keeping the
        // shared_memory owner alive in the struct alongside the guard.
        let guard = unsafe {
            std::mem::transmute::<MessageReadGuard<'_>, MessageReadGuard<'static>>(guard)
        };

        Self {
            inner: Some((shared_memory, guard)),
            cursor: 0,
        }
    }
}

#[pymethods]
impl PythonReadGuard {
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let Some((obj, _)) = &self.inner {
            visit.call(obj)?;
        }
        Ok(())
    }

    fn __clear__(&mut self) {
        drop(self.inner.take());
    }

    fn __len__(&self) -> usize {
        self.inner
            .as_ref()
            .map(|(_, g)| g.data().len())
            .unwrap_or(0)
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
        drop(slf.inner.take());
        Ok(false)
    }

    unsafe fn __getbuffer__(
        slf: PyRef<Self>,
        view: *mut pyo3::ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let (_, guard) = slf
            .inner
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("Guard has been released"))?;

        let data = guard.data();
        let ret = unsafe {
            pyo3::ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr() as *mut _,
                data.as_ptr() as *mut _,
                data.len().try_into()?,
                1, // read only
                flags,
            )
        };
        if ret == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut pyo3::ffi::Py_buffer) {}

    fn sequence(&self) -> PyResult<u64> {
        self.inner
            .as_ref()
            .map(|(_, g)| g.sequence())
            .ok_or_else(|| PyValueError::new_err("Guard has been released"))
    }

    /// Read bytes from the buffer (file-like interface).
    /// Allows: `obj = pickle.load(guard)`
    #[pyo3(signature = (size=None))]
    fn read<'py>(&mut self, py: Python<'py>, size: Option<isize>) -> PyResult<Bound<'py, PyBytes>> {
        let (_, guard) = self
            .inner
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("Guard has been released"))?;

        let data = guard.data();
        let current_pos = self.cursor;

        // Safety check: if cursor is past end, return empty bytes
        if current_pos >= data.len() {
            return Ok(PyBytes::new(py, &[]));
        }

        let available = data.len() - current_pos;

        // Determine how many bytes to read
        let to_read = match size {
            Some(n) if n >= 0 => std::cmp::min(n as usize, available),
            _ => available, // Read all if size is None or negative
        };

        let slice = &data[current_pos..current_pos + to_read];

        // Advance cursor
        self.cursor += to_read;

        Ok(PyBytes::new(py, slice))
    }

    fn tell(&self) -> usize {
        self.cursor
    }

    fn seek(&mut self, pos: usize) -> usize {
        self.cursor = pos;
        pos
    }

    fn writable(&self) -> bool {
        false
    }

    fn seekable(&self) -> bool {
        true
    }

    fn readable(&self) -> bool {
        true
    }

    fn to_bytes<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let (_, guard) = self
            .inner
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("Guard has been released"))?;
        Ok(PyBytes::new(py, guard.data()))
    }
}
#[pyclass(module = "rs_ipc")]
#[pyo3(name = "WriteGuard")]
pub struct PythonWriteGuard {
    inner: Option<(Py<PythonSharedMessage>, MessageWriteGuard<'static>)>,
    cursor: usize,
}

impl PythonWriteGuard {
    pub(crate) fn new(
        shared_memory: Py<PythonSharedMessage>,
        guard: MessageWriteGuard<'_>,
    ) -> Self {
        // SAFETY: We extend the lifetime to static because we are keeping the
        // shared_memory owner alive in the struct alongside the guard.
        let guard = unsafe {
            std::mem::transmute::<MessageWriteGuard<'_>, MessageWriteGuard<'static>>(guard)
        };
        Self {
            inner: Some((shared_memory, guard)),
            cursor: 0,
        }
    }
}

#[pymethods]
impl PythonWriteGuard {
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        let reference = self.inner.as_ref().map(|(obj, _)| obj);
        visit.call(reference)
    }

    fn __clear__(&mut self) {
        drop(self.inner.take());
    }

    fn __len__(&self) -> PyResult<usize> {
        self.inner
            .as_ref()
            .map(|(_, guard)| guard.capacity())
            .ok_or_else(|| PyValueError::new_err("Guard has been released"))
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
        drop(slf.inner.take());
        Ok(false)
    }

    unsafe fn __getbuffer__(
        mut slf: PyRefMut<Self>,
        view: *mut pyo3::ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let (_, guard) = slf
            .inner
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("Guard has been released"))?;

        let data = guard.data_mut();
        let data_ptr = data.as_mut_ptr();
        let data_len = data.len();

        let ret = unsafe {
            pyo3::ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr() as *mut _,
                data_ptr as *mut _,
                data_len.try_into()?,
                0, // writable
                flags,
            )
        };
        if ret == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut pyo3::ffi::Py_buffer) {}

    /// Write bytes to the buffer (file-like interface).
    /// Returns the number of bytes written.
    ///
    /// This enables using the guard directly with pickle.dump():
    /// ```python
    /// with shm.write_guard() as guard:
    ///     pickle.dump(obj, guard)  # Writes directly to shared memory
    ///     guard.publish()
    /// ```
    fn write(&mut self, data: Bound<'_, pyo3::types::PyAny>) -> PyResult<usize> {
        let (_, guard) = self
            .inner
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("Guard has been released"))?;

        let buffer_view: PyBuffer<u8> = data.extract()?;
        let write_len = buffer_view.len_bytes();
        let buffer = guard.data_mut();

        if self.cursor + write_len > buffer.len() {
            return Err(PyValueError::new_err(format!(
                "Data too large: cursor at {}, writing {} bytes, capacity {}",
                self.cursor,
                write_len,
                buffer.len()
            )));
        }

        let target_slice = &mut buffer[self.cursor..self.cursor + write_len];

        // If the data is C-contiguous (standard flat memory), we can memcpy directly.
        // If it is non-contiguous (e.g. numpy slices), we must use copy_to_slice.
        if buffer_view.is_c_contiguous() {
            // SAFETY: We checked is_c_contiguous, so buf_ptr points to a flat array of len_bytes
            let src_slice = unsafe {
                std::slice::from_raw_parts(buffer_view.buf_ptr() as *const u8, write_len)
            };
            target_slice.copy_from_slice(src_slice);
        } else {
            // This copies element-by-element or row-by-row as needed
            buffer_view.copy_to_slice(data.py(), target_slice)?;
        }
        self.cursor += write_len;

        Ok(write_len)
    }

    fn tell(&self) -> usize {
        self.cursor
    }

    fn seek(&mut self, pos: usize) -> usize {
        self.cursor = pos;
        pos
    }

    fn writable(&self) -> bool {
        true
    }

    fn seekable(&self) -> bool {
        true
    }

    fn readable(&self) -> bool {
        false
    }

    /// Publish the written data with the given size, or default to cursor position.
    /// Returns the sequence number if successful, None if stopped.
    #[pyo3(signature = (size=None))]
    fn publish(mut slf: PyRefMut<'_, Self>, size: Option<usize>) -> PyResult<Option<u64>> {
        let (shared_memory, guard) = slf
            .inner
            .take()
            .ok_or_else(|| PyValueError::new_err("Guard has already been published or released"))?;

        let final_size = size.unwrap_or(slf.cursor);
        let result = guard.publish(final_size);

        if let Some(sequence) = result {
            shared_memory
                .get()
                .last_written_sequence
                .store(sequence, Ordering::Relaxed);
        }

        Ok(result)
    }
}
