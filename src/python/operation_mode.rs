use pyo3::pyclass;

#[pyclass(eq, eq_int)]
#[pyo3(frozen)]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum OperationMode {
    ReadSync = 0,
    ReadAsync = 1,
    WriteSync = 2,
    WriteAsync = 3,
}

impl OperationMode {
    pub fn can_read(self) -> bool {
        self == Self::ReadSync || self == Self::ReadAsync
    }

    pub fn can_write(self) -> bool {
        self == Self::WriteSync || self == Self::WriteAsync
    }

    pub fn check_read_permission(self) {
        if !self.can_read() {
            panic!("Shared memory was opened as write-only")
        }
    }

    pub fn check_write_permission(self) {
        if !self.can_write() {
            panic!("Shared memory was opened as read-only")
        }
    }
}
