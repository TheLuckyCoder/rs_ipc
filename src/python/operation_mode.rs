use pyo3::pyclass;

#[pyclass(module = "rs_ipc")]
#[pyo3(frozen, eq, eq_int)]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum OperationMode {
    CreateOnly,
    ReadSync,
    ReadAsync,
    WriteSync,
    WriteAsync,
}

impl OperationMode {
    pub fn can_read(self) -> bool {
        matches!(self, OperationMode::ReadSync | OperationMode::ReadAsync)
    }

    pub fn can_write(self) -> bool {
        matches!(
            self,
            OperationMode::WriteSync | OperationMode::WriteAsync
        )
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