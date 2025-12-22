use crate::python::bytes::RustPyBytes;
use pyo3::types::PyBytes;
use pyo3::{Bound, Py};

// Rust only helper structs

pub struct ReceiverQueueData {
    pub version: usize,
    pub data: RustPyBytes,
}

pub struct SenderQueueData {
    _py_bytes: Py<PyBytes>,
    bytes: *const [u8],
}

impl SenderQueueData {
    pub fn new(data: Bound<PyBytes>) -> Self {
        let gil = data.py();
        let py_bytes = data.unbind();
        let bytes = py_bytes.as_bytes(gil);

        Self {
            // bypass the rust borrow checker
            bytes: bytes as *const [u8],
            _py_bytes: py_bytes,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        // SAFETY:
        // the [PyBytes::as_bytes] mentions the following:
        //     "the result may be used for as long as the reference to
        //      `self` is held, including when the GIL is released"
        unsafe { &*self.bytes }
    }
}
