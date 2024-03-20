// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_js_deno::{CallMode, Runtime};

#[tokio::main]
async fn main() {
    let runtime = Runtime::new();

    runtime
        .add_function(
            "gcd",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function gcd(a, b) {
                while (b != 0) {
                    let t = b;
                    b = a % b;
                    a = t;
                }
                return a;
            }
            "#,
        )
        .await
        .unwrap();

    runtime
        .add_function(
            "fib",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function fib(x) {
                if (x <= 1) 
                    return x;
                return fib(x - 1) + fib(x - 2);
            }
            "#,
        )
        .await
        .unwrap();

    println!("call gcd");
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

    let output = runtime.call("gcd", input.clone()).await.unwrap();

    arrow_cast::pretty::print_batches(std::slice::from_ref(&input)).unwrap();
    arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();

    println!("call fib");
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![10]))],
    )
    .unwrap();

    let output = runtime.call("fib", input.clone()).await.unwrap();

    arrow_cast::pretty::print_batches(std::slice::from_ref(&input)).unwrap();
    arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();

    runtime
        .add_function(
            "range",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
        export function* range(n) {
            for (let i = 0; i < n; i++) {
                yield i;
            }
        }
        "#,
        )
        .await
        .unwrap();

    let chunk_size = 1024;
    let mut outputs = runtime
        .call_table_function("range", input, chunk_size)
        .await
        .unwrap();

    while let Some(result) = outputs.next().await {
        let output = result.unwrap();
        arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();
        // do something with the output
    }
}
