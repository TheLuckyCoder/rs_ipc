use crate::python::reader_wait_policy::ReaderWaitPolicy;
use pyo3::pyclass;

#[pyclass(module = "rs_ipc")]
#[pyo3(frozen)]
#[derive(Copy, Clone, PartialEq)]
pub enum OperationMode {
    CreateOnly(),
    ReadSync(),
    ReadAsync(),
    WriteSync(ReaderWaitPolicy),
    WriteAsync(ReaderWaitPolicy),
}

impl OperationMode {
    pub fn can_read(self) -> bool {
        matches!(self, OperationMode::ReadSync() | OperationMode::ReadAsync())
    }

    pub fn can_write(self) -> bool {
        matches!(
            self,
            OperationMode::WriteSync(_) | OperationMode::WriteAsync(_)
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

    pub fn reader_wait_policy(self) -> ReaderWaitPolicy {
        match self {
            OperationMode::WriteSync(policy) => policy,
            OperationMode::WriteAsync(policy) => policy,
            _ => ReaderWaitPolicy::All(),
        }
    }
}
