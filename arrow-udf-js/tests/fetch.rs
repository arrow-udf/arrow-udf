use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_js::{CallMode, Runtime};
use expect_test::{expect, Expect};

#[tokio::test]
async fn test_fetch_get() {
    let mut runtime = Runtime::new().await.unwrap();
    runtime.enable_fetch().await.unwrap();

    let js_code = r#"
        export async function test_fetch(id) {
            const response = await fetch("https://dummyjson.com/todos/" + id);
            const data = await response.json();
            return data.todo;
        }
    "#;
    runtime
        .add_function(
            "test_fetch",
            DataType::Utf8,
            CallMode::ReturnNullOnNullInput,
            js_code,
            true,
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("test_fetch", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +----------------------------------------------+
        | test_fetch                                   |
        +----------------------------------------------+
        | Do something nice for someone you care about |
        |                                              |
        +----------------------------------------------+"#]],
    );
}

#[tokio::test]
async fn test_fetch_get_with_headers() {
    let mut runtime = Runtime::new().await.unwrap();
    runtime.enable_fetch().await.unwrap();

    let js_code = r#"
        export async function test_fetch(id) {
            const headers = {
                'Authorization': 'Bearer dummy-token',
                'Content-Type': 'application/json'
            };
            const response = await fetch("https://dummyjson.com/auth/todos/" + id, {
                headers
            });
            const data = await response.json();
            return data.message || data.todo;
        }
    "#;
    runtime
        .add_function(
            "test_fetch",
            DataType::Utf8,
            CallMode::ReturnNullOnNullInput,
            js_code,
            true,
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("test_fetch", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +------------------------+
        | test_fetch             |
        +------------------------+
        | Invalid/Expired Token! |
        +------------------------+"#]],
    );
}

#[tokio::test]
async fn test_fetch_post_with_body() {
    let mut runtime = Runtime::new().await.unwrap();
    runtime.enable_fetch().await.unwrap();

    let js_code = r#"
        export async function test_fetch(username) {
            const body = JSON.stringify({
                username: username,
                password: 'emilyspass',
                expiresInMins: 30
            });
            const response = await fetch("https://dummyjson.com/auth/login", {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body
            });
            const data = await response.json();
            return data.message || data.firstName;
        }
    "#;
    runtime
        .add_function(
            "test_fetch",
            DataType::Utf8,
            CallMode::ReturnNullOnNullInput,
            js_code,
            true,
        )
        .await
        .unwrap();

    let schema = Schema::new(vec![Field::new("username", DataType::Utf8, true)]);
    let arg0 = arrow_array::StringArray::from(vec![Some("emilys")]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("test_fetch", &input).await.unwrap();
    check(
        &[output],
        expect![[r#"
        +------------+
        | test_fetch |
        +------------+
        | Emily      |
        +------------+"#]],
    );
}

/// Compare the actual output with the expected output.
#[track_caller]
fn check(actual: &[RecordBatch], expect: Expect) {
    expect.assert_eq(&pretty_format_batches(actual).unwrap().to_string());
}
