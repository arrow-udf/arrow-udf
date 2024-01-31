use anyhow::{Context, Result};
use std::process::Command;

/// Build a wasm binary from a Rust UDF script.
///
/// The `manifest` is a TOML string that will be appended to the generated `Cargo.toml`.
/// The `script` is a Rust source code string that will be written to `src/lib.rs`.
///
/// This function will block the current thread until the build is finished.
///
/// # Example
///
/// ```ignore
/// let manifest = r#"
/// [dependencies]
/// chrono = "0.4"
/// "#;
///
/// let script = r#"
/// use arrow_udf::function;
///
/// #[function("gcd(int, int) -> int")]
/// fn gcd(mut a: i32, mut b: i32) -> i32 {
///     while b != 0 {
///         (a, b) = (b, a % b);
///     }
///     a
/// }
/// "#;
///
/// let binary = arrow_udf_wasm::build(manifest, script).unwrap();
/// ```
pub fn build(manifest: &str, script: &str) -> Result<Vec<u8>> {
    let manifest = format!(
        r#"
[package]
name = "udf"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies.arrow-udf]
version = "0.1"

[dependencies.genawaiter]
version = "0.99"

{manifest}"#,
    );

    // create a new cargo package at temporary directory
    let dir = tempfile::tempdir()?;
    std::fs::create_dir(dir.path().join("src"))?;
    std::fs::write(dir.path().join("src/lib.rs"), script)?;
    std::fs::write(dir.path().join("Cargo.toml"), manifest)?;

    let output = Command::new("cargo")
        .arg("build")
        .arg("--release")
        .arg("--target")
        .arg("wasm32-wasi")
        .current_dir(dir.path())
        .output()
        .context("failed to run cargo build")?;
    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "failed to build wasm ({})\n--- stdout\n{}\n--- stderr\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let binary_path = dir.path().join("target/wasm32-wasi/release/udf.wasm");
    println!("binary_path: {:?}", binary_path);
    // strip the wasm binary if wasm-tools exists
    if Command::new("wasm-strip").arg("--version").output().is_ok() {
        let output = Command::new("wasm-strip")
            .arg(&binary_path)
            .output()
            .context("failed to strip wasm")?;
        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "failed to strip wasm. ({})\n--- stdout\n{}\n--- stderr\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    let binary = std::fs::read(binary_path).context("failed to read wasm binary")?;
    Ok(binary)
}
