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
use arrow_array::{Int32Array, LargeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf::function;
use arrow_udf_js::Runtime as JsRuntime;
use arrow_udf_python::Function as PythonRuntime;
use arrow_udf_python_wasm::Runtime as PythonWasmRuntime;
use arrow_udf_wasm::Runtime as WasmRuntime;
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_eval_gcd(c: &mut Criterion) {
    #[function("gcd(int, int) -> int")]
    fn gcd(mut a: i32, mut b: i32) -> i32 {
        while b != 0 {
            (a, b) = (b, a % b);
        }
        a
    }

    let js_code = r#"
    export function gcd(a, b) {
        while (b) {
            let t = b;
            b = a % b;
            a = t;
        }
        return a;
    }
    "#;

    let python_code = r#"
def gcd(a: int, b: int) -> int:
    while b:
        a, b = b, a % b
    return a
"#;

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

    c.bench_function("gcd/native", |bencher| {
        let a = Int32Array::from_iter(0..1024);
        let b = Int32Array::from_iter((0..2048).step_by(2));
        bencher.iter(|| {
            let _: Int32Array = binary(&a, &b, gcd).unwrap();
        })
    });

    c.bench_function("gcd/rust", |bencher| {
        bencher.iter(|| gcd_int4_int4_int4_eval(&input).unwrap())
    });

    c.bench_function("gcd/wasm", |bencher| {
        let filepath = "../target/wasm32-wasi/release/arrow_udf_example.wasm";
        let binary = std::fs::read(filepath).unwrap();
        let rt = WasmRuntime::new(&binary).unwrap();
        bencher.iter(|| rt.call("gcd(int4,int4)->int4", &input).unwrap())
    });

    c.bench_function("gcd/js", |bencher| {
        let mut rt = JsRuntime::new().unwrap();
        rt.add_function(
            "gcd",
            DataType::Int32,
            arrow_udf_js::CallMode::ReturnNullOnNullInput,
            js_code,
        )
        .unwrap();
        bencher.iter(|| rt.call("gcd", &input).unwrap())
    });

    c.bench_function("gcd/python", |bencher| {
        let rt = PythonRuntime::new(
            "gcd",
            DataType::Int32,
            arrow_udf_python::CallMode::ReturnNullOnNullInput,
            python_code,
        )
        .unwrap();
        bencher.iter(|| rt.call(&input).unwrap())
    });

    c.bench_function("gcd/python-wasm", |bencher| {
        let mut rt =
            PythonWasmRuntime::new("../arrow-udf-python/target/wasm32-wasi/wasi-deps").unwrap();
        rt.add_function("gcd", DataType::Int32, python_code);
        bencher.iter(|| rt.call("gcd", &input).unwrap())
    });
}

fn bench_eval_range(c: &mut Criterion) {
    let js_code = r#"
    export function* range(n) {
        for (let i = 0; i < n; i++) {
            yield i;
        }
    }
    "#;

    let a = Int32Array::from(vec![16 * 1024]);
    c.bench_function("range/native", |bencher| {
        bencher.iter(|| {
            for i in 0..16 {
                let _ = Int32Array::from_iter((0..1024).map(|_| 0));
                let _ = Int32Array::from_iter(i * 1024..(i + 1) * 1024);
            }
        })
    });

    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
        vec![Arc::new(a.clone())],
    )
    .unwrap();

    c.bench_function("range/wasm", |bencher| {
        let filepath = "../target/wasm32-wasi/release/arrow_udf_example.wasm";
        let binary = std::fs::read(filepath).unwrap();
        let rt = WasmRuntime::new(&binary).unwrap();
        bencher.iter(|| {
            rt.call_table_function("range(int4)->>int4", &input)
                .unwrap()
                .for_each(|_| {})
        })
    });

    c.bench_function("range/js", |bencher| {
        let mut rt = JsRuntime::new().unwrap();
        rt.add_function(
            "range",
            DataType::Int32,
            arrow_udf_js::CallMode::ReturnNullOnNullInput,
            js_code,
        )
        .unwrap();
        bencher.iter(|| {
            rt.call_table_function("range", &input, 1024)
                .unwrap()
                .for_each(|_| {})
        })
    });
}

fn bench_eval_decimal(c: &mut Criterion) {
    let js_code = r#"
    export function decimal(a) {
        return a;
    }
    "#;

    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::LargeBinary,
            true,
        )])),
        vec![Arc::new(LargeBinaryArray::from(vec![&b"0.0"[..]; 1024]))],
    )
    .unwrap();

    c.bench_function("decimal/js", |bencher| {
        let mut rt = JsRuntime::new().unwrap();
        rt.add_function(
            "decimal",
            DataType::LargeBinary,
            arrow_udf_js::CallMode::ReturnNullOnNullInput,
            js_code,
        )
        .unwrap();
        bencher.iter(|| rt.call("decimal", &input).unwrap())
    });
}

criterion_group!(
    benches,
    bench_eval_gcd,
    bench_eval_range,
    bench_eval_decimal
);
criterion_main!(benches);
