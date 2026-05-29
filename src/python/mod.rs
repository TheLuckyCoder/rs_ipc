use crate::python::message::PythonSharedMessage;
use crate::python::operation_mode::OperationMode;
use crate::python::reader_wait_policy::ReaderWaitPolicy;
use bytes::RustPyBytes;
use pyo3::prelude::*;
use pyo3::types::PyFunction;
use pyo3::{Bound, PyResult, pymodule};
use rayon::prelude::*;

mod bytes;
mod guards;
mod message;
mod operation_mode;
mod queue_data;
mod reader_wait_policy;

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
    py.detach(|| {
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
    py.detach(|| {
        readers
            .into_par_iter()
            .map(|reader| reader.get().read(false))
            .map(|bytes| {
                bytes.map(|bytes| Python::attach(|py| map_operation.call1(py, (bytes,)).unwrap()))
            })
            .collect()
    })
}
