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

use arrow_array::{RecordBatch, RecordBatchOptions};
#[cfg(feature = "with-fetch")]
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{DataType, Schema};

#[cfg(feature = "with-fetch")]
use arrow_schema::Field;

use arrow_udf_js_deno::{CallMode, Runtime};

#[cfg(feature = "with-fetch")]
use expect_test::{expect, Expect};

#[cfg(feature = "with-fetch")]
use httpmock::prelude::*;

#[tokio::test(flavor = "current_thread")]
#[cfg(feature = "with-fetch")]
async fn test_fetch() {
    let server = MockServer::start();

    // Create a mock on the server.
    let mock = server.mock(|when, then| {
        let value = serde_json::json!({"service": "value"});
        let bytes = serde_json::to_vec(&value).expect("should convert");
        when.method(GET).path("/api");
        then.status(200)
            .header("content-type", "application/json")
            .body(bytes);
    });
    let url = server.url("/api");

    let runtime = Runtime::new();

    runtime
        .add_function(
            "from_fetch",
            DataType::Struct(vec![Field::new("service", DataType::Utf8, true)].into()),
            CallMode::ReturnNullOnNullInput,
            &format!(
                r#"
            export async function from_fetch() {{
                return await fetch("{}").then(response => response.json());
            }}
            "#,
                url
            ),
        )
        .await
        .unwrap();

    let input = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )
    .unwrap();

    let output = runtime.call("from_fetch", input).await.unwrap();

    check(
        &[output],
        expect![[r#"
        +------------------+
        | from_fetch       |
        +------------------+
        | {service: value} |
        +------------------+"#]],
    );

    mock.assert();
}

#[tokio::test(flavor = "current_thread")]
async fn test_crypto() {
    let runtime = Runtime::new();

    runtime
        .add_function(
            "digest",
            DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::UInt8,
                true,
            ))),
            CallMode::ReturnNullOnNullInput,
            r#"
            export async function digest() {
                const subtle = crypto.subtle; 
                const key = await subtle.generateKey({
                    name: 'HMAC',
                    hash: 'SHA-256',
                    length: 256,
                  }, true, ['sign', 'verify']);
                
                  const enc = new TextEncoder();
                  const message = enc.encode('I love risingwave');
                
                  const result = await subtle.sign({
                    name: 'HMAC',
                  }, key, message);
                  return result;
            }
            "#,
        )
        .await
        .unwrap();

    let input = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )
    .unwrap();

    let output = runtime.call("digest", input).await.unwrap();
    let result = output
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .unwrap()
        .value(0);

    assert_eq!(result.len(), 32);
}

#[tokio::test(flavor = "current_thread")]
#[cfg(feature = "with-fetch")]
async fn test_fetch_error() {
    let server = MockServer::start();

    // Create a mock on the server.
    let mock = server.mock(|when, then| {
        when.method(GET).path("/api");
        then.header("content-type", "application/json")
            .body(r#"{ "error":  "Invalid credentials"}"#)
            .status(401);
    });
    let url = server.url("/api");

    let runtime = Runtime::new();

    runtime
        .add_function(
            "from_fetch",
            DataType::Struct(vec![Field::new("service", DataType::Utf8, true)].into()),
            CallMode::ReturnNullOnNullInput,
            &format!(
                r#"
            export async function from_fetch() {{
                const response = await fetch("{}");
                if (!response.ok) {{
                    throw new Error(response.statusText);
                }}
                return await response.json();
            }}
            "#,
                url
            ),
        )
        .await
        .unwrap();

    let input = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )
    .unwrap();

    let result = runtime.call("from_fetch", input).await;
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Unauthorized"));
    mock.assert();
}

/// Compare the actual output with the expected output.
#[cfg(feature = "with-fetch")]
#[track_caller]
fn check(actual: &[RecordBatch], expect: Expect) {
    expect.assert_eq(&pretty_format_batches(actual).unwrap().to_string());
}
