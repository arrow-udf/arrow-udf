// Copyright 2023 RisingWave Labs
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

//! This example shows how to define functions in Rust and call them locally.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf::function;

#[function("gcd(int, int) -> int", output = "gcd_batch")]
fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

fn main() {
    let schema = Schema::new(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from(vec![Some(25), None]);
    let arg1 = Int32Array::from(vec![Some(15), None]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    // call the generated function directly
    let output = gcd_batch(&input).unwrap();

    // or find the function from the global registry
    #[cfg(feature = "global_registry")]
    {
        let sig = arrow_udf::sig::REGISTRY
            .get("gcd", &[DataType::Int32, DataType::Int32], &DataType::Int32)
            .expect("gcd function");
        let _output = (sig.function)(&input).unwrap();
    }

    arrow_cast::pretty::print_batches(std::slice::from_ref(&input)).unwrap();
    arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();
}
