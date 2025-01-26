use pyo3::pyclass;

#[pyclass]
#[pyo3(frozen)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ReaderWaitPolicy {
    All(),
    Count(u32),
}

impl ReaderWaitPolicy {
    pub fn to_count(self) -> u32 {
        match self {
            ReaderWaitPolicy::All() => u32::MAX,
            ReaderWaitPolicy::Count(count) => count,
        }
    }
}
