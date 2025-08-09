use arrow_udf::function;
use duckdb::{ffi, Connection, Result};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use std::error::Error;
use std::fmt::Write;
use std::sync::Arc;

#[function("hello(varchar) -> varchar", duckdb = "Hello")]
fn hello(name: &str, writer: &mut impl Write) {
    write!(writer, "Hello {}!", name).unwrap();
}

#[function("split_words(varchar) -> varchar[]", duckdb = "SplitWords")]
fn split_words(input: &str) -> impl Iterator<Item = &str> {
    input.split_whitespace()
}

#[function("generate_numbers(int32) ->> int32", duckdb = "GenerateNumbers")]
fn generate_numbers(n: i32) -> impl Iterator<Item = i32> {
    0..n
}

#[duckdb_entrypoint_c_api(ext_name = "arrow_udf_duckdb_example", min_duckdb_version = "v1.0.0")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<Hello>("hello")
        .expect("Failed to register hello function");
    con.register_scalar_function::<SplitWords>("split_words")
        .expect("Failed to register split_words function");
    con.register_table_function::<GenerateNumbers>("generate_numbers")
        .expect("Failed to register generate_numbers table function");
    Ok(())
}
