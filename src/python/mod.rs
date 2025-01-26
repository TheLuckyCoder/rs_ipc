use bytes::RustPyBytes;
use crate::python::message::PythonSharedMessage;
use crate::python::operation_mode::OperationMode;
use pyo3::prelude::*;
use pyo3::types::PyFunction;
use pyo3::{pymodule, Bound, PyResult};
use rayon::prelude::*;
use crate::python::reader_wait_policy::ReaderWaitPolicy;

mod message;
mod operation_mode;
mod reader_wait_policy;
pub mod bytes;

#[pymodule(gil_used = false)]
fn rs_ipc(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<OperationMode>()?;
    m.add_class::<ReaderWaitPolicy>()?;
    m.add_class::<RustPyBytes>()?;
    m.add_class::<PythonSharedMessage>()?;

    m.add_function(wrap_pyfunction!(read_all, m)?)?;
    m.add_function(wrap_pyfunction!(read_all_map, m)?)?;

    Ok(())
}

#[pyfunction]
fn read_all(readers: Vec<Py<PythonSharedMessage>>, py: Python<'_>) -> Vec<Option<RustPyBytes>> {
    py.allow_threads(|| {
        readers
            .into_par_iter()
            .map(|reader| reader.get().read(false))
            .collect()
    })
}

#[pyfunction]
fn read_all_map(
    readers: Vec<Py<PythonSharedMessage>>,
    map_operation: Py<PyFunction>,
    py: Python<'_>,
) -> Vec<Option<Py<PyAny>>> {
    py.allow_threads(|| {
        readers
            .into_par_iter()
            .map(|reader| reader.get().read(false))
            .map(|bytes| {
                bytes.map(|bytes| Python::with_gil(|py| map_operation.call1(py, (bytes,)).unwrap()))
            })
            .collect()
    })
}
