//! Custom Python exceptions for Hypertile.

use pyo3::create_exception;
use pyo3::exceptions::PyException;

create_exception!(
    hypertile,
    PanicInTask,
    PyException,
    "Raised when an in-flight Rust task panics inside the shared executor."
);

create_exception!(
    hypertile,
    TaskCancelled,
    PyException,
    "Raised when a task is cancelled cooperatively."
);

create_exception!(
    hypertile,
    RegistrationError,
    PyException,
    "Raised when worker registration fails or is invoked with invalid state."
);
