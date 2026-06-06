use crate::python::OperationMode;
use crate::python::reader_wait_policy::ReaderWaitPolicy;
use crate::python::message::PythonSharedMessage;
use crate::python::bytes::RustPyBytes;
use pyo3::types::PyBytes;
use pyo3::Python;
use std::thread;
use std::time::Duration;

fn get_test_data() -> Vec<u8> {
    let mut data = vec![0u8; 1024 * 100]; // 100 KB
    for i in 0..data.len() {
        data[i] = (i % 255) as u8;
    }
    data
}

fn init(
    name: &str,
    op_mode: OperationMode,
    reader_wait_policy: ReaderWaitPolicy,
) -> PythonSharedMessage {
    PythonSharedMessage::create(name.to_string(), op_mode, reader_wait_policy).unwrap()
}

#[test]
fn async_write() {
    Python::attach(|py| {
        let memory = init(
            "async_write",
            OperationMode::ReadSync,
            ReaderWaitPolicy::Count(0),
        );

        memory.write_async(PyBytes::new(py, &[1])).unwrap();
        memory.write_async(PyBytes::new(py, &[2])).unwrap();
        memory.write_async(PyBytes::new(py, &[3])).unwrap();
        memory.write_async(PyBytes::new(py, &[4])).unwrap();
        thread::sleep(Duration::from_millis(100));
        assert!(memory.is_new_version_available());
        assert_eq!(memory.read(true).unwrap(), RustPyBytes::new(&[4]));
        memory.stop();
    });
}

#[test]
fn async_write_try_read() {
    Python::attach(|py| {
        let data = (0u8..255u8).collect::<Vec<_>>();

        let memory = init(
            "async_write_try_read",
            OperationMode::ReadSync,
            ReaderWaitPolicy::All(),
        );
        let none = memory.read(false);
        assert!(none.is_none());

        memory.write_async(PyBytes::new(py, &data)).unwrap();
        thread::sleep(Duration::from_millis(200));
        let version = memory.last_written_version();

        let bytes = memory.read(false).unwrap();
        assert_eq!(bytes.0.as_ref(), data);
        assert_eq!(version, memory.last_read_version());

        assert!(memory.read(false).is_none());
        memory.stop();
    });
}

#[test]
fn async_write_blocking_read() {
    Python::attach(|py| {
        let data = get_test_data();

        let memory = init(
            "async_write_blocking_read",
            OperationMode::ReadSync,
            ReaderWaitPolicy::All(),
        );

        memory.write_async(PyBytes::new(py, &data)).unwrap();
        thread::sleep(Duration::from_millis(200));
        let version = memory.last_written_version();

        let bytes = memory.read(true).unwrap();
        assert_eq!(bytes.0.as_ref(), data);
        assert_eq!(version, memory.last_read_version());
        memory.stop();
    });
}

#[test]
fn multiple_writes() {
    Python::attach(|py| {
        let memory = init(
            "async_multiple_writes",
            OperationMode::ReadSync,
            ReaderWaitPolicy::All(),
        );

        for i in 0..255 {
            memory.write_async(PyBytes::new(py, &[i])).unwrap();
        }

        for i in 0..255 {
            assert_eq!(memory.read(true).unwrap().0.as_ref(), &[i]);
        }

        memory.stop();
    });
}
