use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_wasm_runtime::Runtime;

fn main() -> wasmtime::Result<()> {
    let filename = std::env::args().nth(1).expect("no filename");
    let mut runtime = Runtime::new(&std::fs::read(filename).unwrap()).unwrap();

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
        .call("arrowudf_Y3JlYXRlX2ZpbGUoKS0$aW50", &input)
        .unwrap();
    println!("{:?}", output);

    Ok(())
}
