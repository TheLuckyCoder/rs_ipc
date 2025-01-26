use pyo3::types::PyBytes;
use pyo3::{Bound, Py};
use crate::python::bytes::RustPyBytes;

pub struct ReceiverQueueData {
    pub version: usize,
    pub data: RustPyBytes,
}

pub struct SenderQueueData {
    _py_bytes: Py<PyBytes>,
    bytes: &'static [u8],
}

impl SenderQueueData {
    pub fn new(data: Bound<PyBytes>) -> Self {
        let gil = data.py();
        let py_bytes = data.unbind();
        let bytes = py_bytes.as_bytes(gil);

        Self {
            // bypass the rust borrow checker
            bytes: unsafe { &*(bytes as *const [u8]) },
            _py_bytes: py_bytes,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        self.bytes
    }
}
