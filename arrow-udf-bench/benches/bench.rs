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
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf::function;
use arrow_udf_js::{AggregateOptions, FunctionOptions, Runtime as JsRuntime};
use arrow_udf_python::Runtime as PythonRuntime;
use arrow_udf_wasm::Runtime as WasmRuntime;
use criterion::async_executor::{AsyncExecutor as _, FuturesExecutor};
use criterion::{criterion_group, criterion_main, Criterion};
use futures_util::stream::StreamExt;

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

    let mut group = c.benchmark_group("gcd");

    group.bench_function("native", |bencher| {
        let a = Int32Array::from_iter(0..1024);
        let b = Int32Array::from_iter((0..2048).step_by(2));
        bencher.iter(|| {
            let _: Int32Array = binary(&a, &b, gcd).unwrap();
        })
    });

    group.bench_function("rust", |bencher| {
        bencher.iter(|| gcd_int32_int32_int32_eval(&input).unwrap())
    });

    group.bench_function("wasm", |bencher| {
        let filepath = "../target/wasm32-wasip1/release/arrow_udf_example.wasm";
        let binary = std::fs::read(filepath).unwrap();
        let rt = WasmRuntime::new(&binary).unwrap();
        let gcd = rt
            .find_function(
                "gcd",
                vec![DataType::Int32, DataType::Int32],
                DataType::Int32,
            )
            .unwrap();
        bencher.iter(|| rt.call(&gcd, &input).unwrap())
    });

    group.bench_with_input("js", &input, |b, input| {
        // Workaround for https://github.com/bheisler/criterion.rs/issues/751
        let executor = FuturesExecutor;
        let rt = executor.block_on(async {
            let mut rt = JsRuntime::new().await.unwrap();
            rt.add_function(
                "gcd",
                DataType::Int32,
                js_code,
                FunctionOptions::default().return_null_on_null_input(),
            )
            .await
            .unwrap();
            rt
        });
        b.to_async(FuturesExecutor).iter_with_setup(
            || &rt,
            |rt| async move { rt.call("gcd", input).await.unwrap() },
        )
    });

    group.bench_function("python", |bencher| {
        let mut rt = PythonRuntime::new().unwrap();
        rt.add_function(
            "gcd",
            DataType::Int32,
            arrow_udf_python::CallMode::ReturnNullOnNullInput,
            python_code,
        )
        .unwrap();
        bencher.iter(|| rt.call("gcd", &input).unwrap())
    });

    group.finish();
}

fn bench_eval_range(c: &mut Criterion) {
    let js_code = r#"
    export function* range(n) {
        for (let i = 0; i < n; i++) {
            yield i;
        }
    }
    "#;

    let python_code = r#"
def range1(n: int):
    for i in range(n):
        yield i
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

    let mut group = c.benchmark_group("range");

    group.bench_function("wasm", |bencher| {
        let filepath = "../target/wasm32-wasip1/release/arrow_udf_example.wasm";
        let binary = std::fs::read(filepath).unwrap();
        let rt = WasmRuntime::new(&binary).unwrap();
        let range = rt
            .find_table_function("range", vec![DataType::Int32], DataType::Int32)
            .unwrap();
        bencher.iter(|| {
            rt.call_table_function(&range, &input)
                .unwrap()
                .for_each(|_| {})
        })
    });

    group.bench_with_input("js", &input, |b, input| {
        // Workaround for https://github.com/bheisler/criterion.rs/issues/751
        let executor = FuturesExecutor;
        let rt = executor.block_on(async {
            let mut rt = JsRuntime::new().await.unwrap();
            rt.add_function(
                "range",
                DataType::Int32,
                js_code,
                FunctionOptions::default().return_null_on_null_input(),
            )
            .await
            .unwrap();
            rt
        });
        b.to_async(FuturesExecutor).iter_with_setup(
            || &rt,
            |rt| async move {
                rt.call_table_function("range", input, 1024)
                    .unwrap()
                    .for_each(|_| async {})
                    .await;
            },
        )
    });

    group.bench_function("python", |bencher| {
        let mut rt = PythonRuntime::new().unwrap();
        rt.add_function(
            "range1",
            DataType::Int32,
            arrow_udf_python::CallMode::ReturnNullOnNullInput,
            python_code,
        )
        .unwrap();
        bencher.iter(|| {
            rt.call_table_function("range1", &input, 1024)
                .unwrap()
                .for_each(|_| {})
        })
    });

    group.finish();
}

fn bench_eval_decimal(c: &mut Criterion) {
    #[function("decimal(decimal) -> decimal")]
    fn decimal<T>(a: T) -> T {
        a
    }

    let js_code = r#"
    export function decimal(a) {
        return a;
    }
    "#;

    let python_code = r#"
def decimal_(a):
    return a
    "#;

    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![decimal_field("a")])),
        vec![Arc::new(StringArray::from(vec!["0.0"; 1024]))],
    )
    .unwrap();

    let mut group = c.benchmark_group("decimal");

    group.bench_with_input("js", &input, |b, input| {
        // Workaround for https://github.com/bheisler/criterion.rs/issues/751
        let executor = FuturesExecutor;
        let rt = executor.block_on(async {
            let mut rt = JsRuntime::new().await.unwrap();
            rt.add_function(
                "decimal",
                decimal_field("decimal"),
                js_code,
                FunctionOptions::default().return_null_on_null_input(),
            )
            .await
            .unwrap();
            rt
        });
        b.to_async(FuturesExecutor).iter_with_setup(
            || &rt,
            |rt| async move { rt.call("decimal", input).await.unwrap() },
        )
    });

    group.bench_function("python", |bencher| {
        let mut rt = PythonRuntime::new().unwrap();
        rt.add_function(
            "decimal_",
            decimal_field("decimal"),
            arrow_udf_python::CallMode::ReturnNullOnNullInput,
            python_code,
        )
        .unwrap();
        bencher.iter(|| rt.call("decimal_", &input).unwrap())
    });

    group.finish();
}

fn bench_eval_sum(c: &mut Criterion) {
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from_iter(0..1024))],
    )
    .unwrap();

    let mut group = c.benchmark_group("sum");

    group.bench_with_input("js", &input, |b, input| {
        // Workaround for https://github.com/bheisler/criterion.rs/issues/751
        let executor = FuturesExecutor;
        let (rt, state) = executor.block_on(async {
            let mut rt = JsRuntime::new().await.unwrap();
            rt.add_aggregate(
                "sum",
                DataType::Int32,
                DataType::Int32,
                r#"
                    export function create_state() {
                        return 0;
                    }
                    export function accumulate(state, value) {
                        return state + value;
                    }
                    export function retract(state, value) {
                        return state - value;
                    }
                "#,
                AggregateOptions::default().return_null_on_null_input(),
            )
            .await
            .unwrap();
            let state = rt.create_state("sum").await.unwrap();
            (rt, state)
        });
        b.to_async(FuturesExecutor).iter_with_setup(
            || (&rt, &state),
            |(rt, state)| async move { rt.accumulate("sum", state, input).await.unwrap() },
        )
    });

    group.bench_function("python", |bencher| {
        let mut rt = PythonRuntime::new().unwrap();
        rt.add_aggregate(
            "sum",
            DataType::Int32,
            DataType::Int32,
            arrow_udf_python::CallMode::ReturnNullOnNullInput,
            r#"
def create_state():
    return 0

def accumulate(state, value):
    return state + value

def retract(state, value):
    return state - value
"#,
        )
        .unwrap();
        let state = rt.create_state("sum").unwrap();
        bencher.iter(|| rt.accumulate("sum", &state, &input).unwrap())
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_eval_gcd,
    bench_eval_range,
    bench_eval_decimal,
    bench_eval_sum
);
criterion_main!(benches);

/// Returns a field with decimal type.
fn decimal_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
        .with_metadata([("ARROW:extension:name".into(), "arrowudf.decimal".into())].into())
}
