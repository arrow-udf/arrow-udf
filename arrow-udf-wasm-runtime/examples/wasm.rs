use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_wasm_runtime::Runtime;

fn main() {
    let filename = std::env::args().nth(1).expect("no filename");
    let runtime = Runtime::new(&std::fs::read(filename).unwrap()).unwrap();

    println!("{runtime:#?}");

    println!("\ncall oom");

    let input = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )
    .unwrap();

    let output = runtime.call("oom()->void", &input);
    println!("{}", output.unwrap_err());

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

    let output = runtime.call("gcd(int4,int4)->int4", &input).unwrap();

    arrow_cast::pretty::print_batches(std::slice::from_ref(&input)).unwrap();
    arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();

    println!("\ncall length");

    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["rising", "wave"]))],
    )
    .unwrap();

    let output = runtime.call("length(varchar)->int4", &input).unwrap();

    arrow_cast::pretty::print_batches(std::slice::from_ref(&input)).unwrap();
    arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();

    println!("\ncall key_value");

    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["rising=wave", "???"]))],
    )
    .unwrap();

    let output = runtime
        .call(
            "key_value(varchar)->struct<key:varchar,value:varchar>",
            &input,
        )
        .unwrap();

    arrow_cast::pretty::print_batches(std::slice::from_ref(&input)).unwrap();
    arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();
}
