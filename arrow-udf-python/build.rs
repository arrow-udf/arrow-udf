use pyo3_build_config::PythonVersion;

fn main() {
    let version = pyo3_build_config::get().version;
    let minimum_version = PythonVersion {
        major: 3,
        minor: 12,
    };
    assert!(
        version >= minimum_version,
        "arrow-udf-python requires Python 3.12 or later, but found {}\nhint: you can set `PYO3_PYTHON` environment varibale, e.g. `PYO3_PYTHON=python3.12`",
        version
    );
}
