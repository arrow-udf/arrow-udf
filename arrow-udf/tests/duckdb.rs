use arrow_udf::function;
use duckdb::{Connection, Result};
use std::error::Error;

#[function("hello_duckdb(varchar) -> varchar", duckdb = "HelloDuckdb")]
fn hello_duckdb(name: &str) -> String {
    format!("Hello {}!", name)
}

#[function("generate_numbers(int32) ->> int32", duckdb = "GenerateNumbers")]
fn generate_numbers(n: i32) -> impl Iterator<Item = i32> {
    0..n
}

#[function("generate_strings(varchar) ->> varchar", duckdb = "GenerateStrings")]
fn generate_strings(prefix: &str) -> impl Iterator<Item = String> {
    let prefix_owned = prefix.to_string();
    (0..3).map(move |i| format!("{}_{}", prefix_owned, i))
}

#[function("repeat_value(int32, int32) ->> int32", duckdb = "RepeatValue")]
fn repeat_value(value: i32, count: i32) -> impl Iterator<Item = i32> {
    std::iter::repeat(value).take(count as usize)
}

#[test]
fn test_duckdb_scalar_function() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_scalar_function::<HelloDuckdb>("hello_duckdb")?;

    let batches = conn
        .prepare("SELECT hello_duckdb('world') as greeting")?
        .query_arrow([])?
        .collect::<Vec<_>>();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 1);

    let array = batch.column(0);
    let string_array = array
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(string_array.value(0), "Hello world!");

    Ok(())
}

#[test]
fn test_duckdb_table_function() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<GenerateNumbers>("generate_numbers")?;

    let batches = conn
        .prepare("SELECT value FROM generate_numbers(5) ORDER BY value")?
        .query_arrow([])?
        .collect::<Vec<_>>();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 5);

    let array = batch.column(0);
    let int_array = array
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .unwrap();

    // Check that we get values 0, 1, 2, 3, 4
    for i in 0..5 {
        assert_eq!(int_array.value(i), i as i32);
    }

    Ok(())
}

#[test]
fn test_duckdb_table_function_with_different_parameters() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<GenerateNumbers>("generate_numbers")?;

    // Test with parameter 3
    let batches = conn
        .prepare("SELECT value FROM generate_numbers(3) ORDER BY value")?
        .query_arrow([])?
        .collect::<Vec<_>>();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 3);

    let array = batch.column(0);
    let int_array = array
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .unwrap();

    for i in 0..3 {
        assert_eq!(int_array.value(i), i as i32);
    }

    // Test with parameter 0 (empty result)
    let batches = conn
        .prepare("SELECT value FROM generate_numbers(0)")?
        .query_arrow([])?
        .collect::<Vec<_>>();

    // When a table function returns 0 rows, DuckDB may not return any batches
    if batches.is_empty() {
        // This is acceptable for empty results
    } else {
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 0);
    }

    Ok(())
}

#[test]
fn test_duckdb_string_table_function() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<GenerateStrings>("generate_strings")?;

    let batches = conn
        .prepare("SELECT value FROM generate_strings('test') ORDER BY value")?
        .query_arrow([])?
        .collect::<Vec<_>>();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 3);

    let array = batch.column(0);
    let string_array = array
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();

    // Check that we get expected string values
    assert_eq!(string_array.value(0), "test_0");
    assert_eq!(string_array.value(1), "test_1");
    assert_eq!(string_array.value(2), "test_2");

    Ok(())
}

#[test]
fn test_duckdb_multi_param_table_function() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<RepeatValue>("repeat_value")?;

    let batches = conn
        .prepare("SELECT value FROM repeat_value(42, 3) ORDER BY value")?
        .query_arrow([])?
        .collect::<Vec<_>>();

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 3);

    let array = batch.column(0);
    let int_array = array
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .unwrap();

    // Check that all values are 42
    for i in 0..3 {
        assert_eq!(int_array.value(i), 42);
    }

    Ok(())
}
