use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_wasm_runtime::Runtime;

fn main() -> wasmtime::Result<()> {
    let filename = std::env::args().nth(1).expect("no filename");
    let mut runtime = Runtime::new(&std::fs::read(filename).unwrap()).unwrap();

    println!("functions:");
    for (encoded, decoded) in runtime.functions() {
        println!("  {decoded:20}{encoded}");
    }

    println!("\ncall gcd");

    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![Some(15), None])),
            Arc::new(Int32Array::from(vec![25, 2])),
        ],
    )
    .unwrap();

    let output = runtime
        .call("arrowudf_Z2NkKGludCxpbnQpLT5pbnQ", &input)
        .unwrap();

    arrow_cast::pretty::print_batches(std::slice::from_ref(&input)).unwrap();
    arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();

    println!("\ncall length");

    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["rising", "wave"]))],
    )
    .unwrap();

    let output = runtime
        .call("arrowudf_bGVuZ3RoKHZhcmNoYXIpLT5pbnQ", &input)
        .unwrap();

    arrow_cast::pretty::print_batches(std::slice::from_ref(&input)).unwrap();
    arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();

    println!("\ncall segfault");

    let input = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )
    .unwrap();

    let output = runtime.call("arrowudf_c2VnZmF1bHQoKS0$aW50", &input);
    println!("{}", output.unwrap_err());

    println!("\nBye!");
    Ok(())
}
