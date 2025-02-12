use arrow_udf::function;
use duckdb::{ffi, Connection, Result};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use std::error::Error;

#[function("hello(varchar) -> varchar", duckdb = "Hello")]
fn hello(name: &str) -> String {
    format!("Hello {}!", name)
}

#[duckdb_entrypoint_c_api(ext_name = "arrow_udf_duckdb_example", min_duckdb_version = "v1.2.0")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<Hello>("hello")
        .expect("Failed to register hello function");
    Ok(())
}
