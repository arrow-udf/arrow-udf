//! Build WASM binaries from source.

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
/// let binary = arrow_udf_wasm::build::build(manifest, script).unwrap();
/// ```
pub fn build(manifest: &str, script: &str) -> Result<Vec<u8>> {
    let opts = BuildOpts {
        manifest: manifest.to_string(),
        script: script.to_string(),
        ..Default::default()
    };
    build_with(&opts)
}

/// Options for building wasm binaries.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct BuildOpts {
    /// A TOML string that will be appended to the generated `Cargo.toml`.
    pub manifest: String,
    /// A Rust source code string that will be written to `src/lib.rs`.
    pub script: String,
    /// Whether to build offline.
    pub offline: bool,
    /// The toolchain to use.
    pub toolchain: Option<String>,
}

/// Build a wasm binary with options.
pub fn build_with(opts: &BuildOpts) -> Result<Vec<u8>> {
    // install wasm32-wasi target
    if !opts.offline {
        let mut command = Command::new("rustup");
        if let Some(toolchain) = &opts.toolchain {
            command.arg(format!("+{}", toolchain));
        }
        command
            .arg("target")
            .arg("add")
            .arg("wasm32-wasi")
            .output()
            .context("failed to run `rustup target add wasm32-wasi`")?;
        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "failed to install wasm32-wasi target. ({})\n--- stdout\n{}\n--- stderr\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }

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

{}"#,
        opts.manifest
    );

    // create a new cargo package at temporary directory
    let dir = tempfile::tempdir()?;
    std::fs::create_dir(dir.path().join("src"))?;
    std::fs::write(dir.path().join("src/lib.rs"), &opts.script)?;
    std::fs::write(dir.path().join("Cargo.toml"), manifest)?;

    let mut command = Command::new("cargo");
    if let Some(toolchain) = &opts.toolchain {
        command.arg(format!("+{}", toolchain));
    }
    command
        .arg("build")
        .arg("--release")
        .arg("--target")
        .arg("wasm32-wasi")
        .current_dir(dir.path());
    if opts.offline {
        command.arg("--offline");
    }
    let output = command.output().context("failed to run cargo build")?;
    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "failed to build wasm ({})\n--- stdout\n{}\n--- stderr\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let binary_path = dir.path().join("target/wasm32-wasi/release/udf.wasm");
    // strip the wasm binary if wasm-strip exists
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
