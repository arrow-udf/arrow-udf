use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_python_wasm::Runtime;

fn main() {
    let mut runtime = Runtime::new("arrow-udf-python/target/wasm32-wasi/wasi-deps").unwrap();
    runtime.add_function(
        "gcd",
        DataType::Int32,
        r#"
def gcd(a: int, b: int) -> int:
    if a is None or b is None:
        return None
    while b:
        a, b = b, a % b
    return a
    "#,
    );

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

    let output = runtime.call("gcd", &input).unwrap();

    arrow_cast::pretty::print_batches(std::slice::from_ref(&input)).unwrap();
    arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();
}
