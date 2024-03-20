use std::{
    env,
    path::{Path, PathBuf},
};

use deno_core::JsRuntimeForSnapshot;

pub fn build_snapshot(runtime_snapshot_path: PathBuf) {
    let js_runtime = arrow_udf_js_deno_runtime::create_runtime_snapshot();
    create_snapshot(js_runtime, runtime_snapshot_path.as_path());
}

pub fn create_snapshot(js_runtime: JsRuntimeForSnapshot, snapshot_path: &Path) {
    let snapshot = js_runtime.snapshot();
    println!("Snapshot size: {}", snapshot.len());

    std::fs::write(snapshot_path, snapshot).unwrap();
    println!("Snapshot written to: {} ", snapshot_path.display());
}

fn main() {
    // Skip building from docs.rs.
    if env::var_os("DOCS_RS").is_some() {
        return;
    }

    let out_dir: PathBuf = env::var_os("OUT_DIR").expect("$OUT_DIR not set.").into();

    let runtime_snapshot_path = out_dir.join("ARROW_DENO_RUNTIME.snap");
    build_snapshot(runtime_snapshot_path);

    println!("cargo:rustc-env=TARGET={}", env::var("TARGET").unwrap());
}
