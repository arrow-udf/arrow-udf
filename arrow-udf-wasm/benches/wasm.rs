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

use arrow_arith::arity::binary;
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_js::Runtime as JsRuntime;
use arrow_udf_python::Function as PythonRuntime;
use arrow_udf_python_wasm::Runtime as PythonWasmRuntime;
use arrow_udf_wasm::Runtime as WasmRuntime;
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_eval_gcd(c: &mut Criterion) {
    let a = Int32Array::from_iter(0..1024);
    let b = Int32Array::from_iter((0..2048).step_by(2));
    c.bench_function("gcd/native", |bencher| {
        bencher.iter(|| {
            let _: Int32Array = binary(&a, &b, gcd).unwrap();
        })
    });

    let filepath = "../target/wasm32-wasi/release/arrow_udf_example.wasm";
    let binary = std::fs::read(filepath).unwrap();
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from_iter(0..1024)),
            Arc::new(Int32Array::from_iter((0..2048).step_by(2))),
        ],
    )
    .unwrap();

    c.bench_function("gcd/wasm", |bencher| {
        let rt = WasmRuntime::new(&binary).unwrap();
        bencher.iter(|| rt.call("gcd(int4,int4)->int4", &input).unwrap())
    });

    c.bench_function("gcd/js", |bencher| {
        let mut rt = JsRuntime::new().unwrap();
        rt.add_function(
            "gcd",
            DataType::Int32,
            arrow_udf_js::CallMode::ReturnNullOnNullInput,
            JS_CODE,
        )
        .unwrap();
        bencher.iter(|| rt.call("gcd", &input).unwrap())
    });

    c.bench_function("gcd/python", |bencher| {
        let rt = PythonRuntime::new(
            "gcd",
            DataType::Int32,
            arrow_udf_python::CallMode::ReturnNullOnNullInput,
            PYTHON_CODE,
        )
        .unwrap();
        bencher.iter(|| rt.call(&input).unwrap())
    });

    c.bench_function("gcd/python-wasm", |bencher| {
        let mut rt =
            PythonWasmRuntime::new("../arrow-udf-python/target/wasm32-wasi/wasi-deps").unwrap();
        rt.add_function("gcd", DataType::Int32, PYTHON_CODE);
        bencher.iter(|| rt.call("gcd", &input).unwrap())
    });
}

fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

const JS_CODE: &str = r#"
export function gcd(a, b) {
    while (b) {
        let t = b;
        b = a % b;
        a = t;
    }
    return a;
}
"#;

const PYTHON_CODE: &str = r#"
def gcd(a: int, b: int) -> int:
    while b:
        a, b = b, a % b
    return a
"#;

criterion_group!(benches, bench_eval_gcd);
criterion_main!(benches);
