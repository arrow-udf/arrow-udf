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

use arrow_array::{types::*, Int32Array, ListArray, RecordBatch};
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_js_deno::{CallMode, Runtime};
use expect_test::{expect, Expect};

#[tokio::test(flavor = "current_thread")]
async fn test_range_async() {
    let runtime = Runtime::new();

    runtime
        .add_function(
            "range",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
            export async function* range(n) {
                for (let i = 0; i < n; i++) {
                    yield i;
                }
            }
            "#,
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut outputs = runtime
        .call_table_function("range", input, 2)
        .await
        .unwrap();

    assert_eq!(outputs.schema().field(0).name(), "row");
    assert_eq!(outputs.schema().field(1).name(), "range");
    assert_eq!(outputs.schema().field(1).data_type(), &DataType::Int32);

    let o1 = outputs.try_next().await.unwrap().unwrap();
    let o2 = outputs.try_next().await.unwrap().unwrap();
    let o3 = outputs.try_next().await.unwrap().unwrap();
    let o4 = outputs.try_next().await.unwrap().unwrap();
    assert_eq!(o1.num_rows(), 1);
    assert_eq!(o2.num_rows(), 1);
    assert_eq!(o3.num_rows(), 1);
    assert_eq!(o4.num_rows(), 1);
    assert!(outputs.try_next().await.unwrap().is_none());

    check(
        &[o1, o2, o3, o4],
        expect![[r#"
        +-----+-------+
        | row | range |
        +-----+-------+
        | 0   | 0     |
        | 2   | 0     |
        | 2   | 1     |
        | 2   | 2     |
        +-----+-------+"#]],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_range_async_iterator() {
    let runtime = Runtime::new();

    runtime
        .add_function(
            "range",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function range(delays) {
                const delayedResponses = {
                    delays: delays,
                  
                    wait(delay) {
                      return new Promise((resolve) => {
                        setTimeout(resolve, delay);
                      });
                    },
                  
                    async *[Symbol.asyncIterator]() {
                      for (const delay of this.delays) {
                        await this.wait(delay);
                        yield delay;
                      }
                    },
                  };
                return delayedResponses;
            }
            "#,
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "x",
        DataType::new_list(DataType::Int32, true),
        true,
    )]);
    let arg0 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(50)]),
        None,
        Some(vec![Some(10), Some(15), Some(25)]),
    ]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut outputs = runtime
        .call_table_function("range", input, 2)
        .await
        .unwrap();

    assert_eq!(outputs.schema().field(0).name(), "row");
    assert_eq!(outputs.schema().field(1).name(), "range");
    assert_eq!(outputs.schema().field(1).data_type(), &DataType::Int32);

    let o1 = outputs.try_next().await.unwrap().unwrap();
    let o2 = outputs.try_next().await.unwrap().unwrap();
    let o3 = outputs.try_next().await.unwrap().unwrap();
    let o4 = outputs.try_next().await.unwrap().unwrap();
    assert_eq!(o1.num_rows(), 1);
    assert_eq!(o2.num_rows(), 1);
    assert_eq!(o3.num_rows(), 1);
    assert_eq!(o4.num_rows(), 1);
    assert!(outputs.try_next().await.unwrap().is_none());

    check(
        &[o1, o2, o3, o4],
        expect![[r#"
        +-----+-------+
        | row | range |
        +-----+-------+
        | 0   | 50    |
        | 2   | 10    |
        | 2   | 15    |
        | 2   | 25    |
        +-----+-------+"#]],
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_range_async_iterator_async() {
    let runtime = Runtime::new();

    runtime
        .add_function(
            "range",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
            export async function range(delays) {
                const delayedResponses = {
                    delays: delays,
                  
                    wait(delay) {
                      return new Promise((resolve) => {
                        setTimeout(resolve, delay);
                      });
                    },
                  
                    async *[Symbol.asyncIterator]() {
                      for (const delay of this.delays) {
                        await this.wait(delay);
                        yield delay;
                      }
                    },
                  };
                return delayedResponses;
            }
            "#,
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "x",
        DataType::new_list(DataType::Int32, true),
        true,
    )]);
    let arg0 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(50)]),
        None,
        Some(vec![Some(10), Some(15), Some(25)]),
    ]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut outputs = runtime
        .call_table_function("range", input, 2)
        .await
        .unwrap();

    assert_eq!(outputs.schema().field(0).name(), "row");
    assert_eq!(outputs.schema().field(1).name(), "range");
    assert_eq!(outputs.schema().field(1).data_type(), &DataType::Int32);

    let o1 = outputs.try_next().await.unwrap().unwrap();
    let o2 = outputs.try_next().await.unwrap().unwrap();
    let o3 = outputs.try_next().await.unwrap().unwrap();
    let o4 = outputs.try_next().await.unwrap().unwrap();
    assert_eq!(o1.num_rows(), 1);
    assert_eq!(o2.num_rows(), 1);
    assert_eq!(o3.num_rows(), 1);
    assert_eq!(o4.num_rows(), 1);
    assert!(outputs.try_next().await.unwrap().is_none());

    check(
        &[o1, o2, o3, o4],
        expect![[r#"
        +-----+-------+
        | row | range |
        +-----+-------+
        | 0   | 50    |
        | 2   | 10    |
        | 2   | 15    |
        | 2   | 25    |
        +-----+-------+"#]],
    );
}

/// Compare the actual output with the expected output.
#[track_caller]
fn check(actual: &[RecordBatch], expect: Expect) {
    expect.assert_eq(&pretty_format_batches(actual).unwrap().to_string());
}
