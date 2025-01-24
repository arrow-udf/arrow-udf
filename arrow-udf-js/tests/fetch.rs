#[cfg(feature = "fetch")]
mod tests {

    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_cast::pretty::pretty_format_batches;
    use arrow_schema::{DataType, Field, Schema};
    use arrow_udf_js::{CallMode, Runtime};
    use expect_test::{expect, Expect};
    use mockito::Server;
    use rquickjs::async_with;

    #[tokio::test]
    async fn run_javascript_tests() {
        let runtime = Runtime::new().await.unwrap();
        runtime.enable_fetch().await.unwrap();

        async_with!(runtime.context() => |ctx| {
            ctx.eval_file::<(), _>("src/fetch/headers.test.js")
                .inspect_err(|e| inspect_error(e, &ctx))
                .unwrap();
            ctx.eval_file::<(), _>("src/fetch/fetch.test.js")
                .inspect_err(|e| inspect_error(e, &ctx))
                .unwrap();
        })
        .await;
    }

    #[tokio::test]
    async fn test_fetch_get() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/todos/1")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"todo": "Have fun!"}"#)
            .create();

        let mut runtime = Runtime::new().await.unwrap();
        runtime.enable_fetch().await.unwrap();

        let js_code = r#"
        function assert(cond) {
            if (!cond) {
                throw new Error("Assertion failed");
            }
        }
        export async function test_fetch(id) {
            const response = await fetch("$URL/todos/" + id);
            assert(response.ok);
            assert(response.status === 200);
            assert(response.statusText === "OK");
            assert(!response.bodyUsed);
            const data = await response.json();
            assert(response.bodyUsed);
            return data.todo;
        }
    "#
        .replace("$URL", &server.url());

        runtime
            .add_function(
                "test_fetch",
                DataType::Utf8,
                CallMode::ReturnNullOnNullInput,
                &js_code,
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
        +------------+
        | test_fetch |
        +------------+
        | Have fun!  |
        |            |
        +------------+"#]],
        );

        mock.assert();
    }

    #[tokio::test]
    async fn test_fetch_get_with_headers() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/todos/1")
            .match_header("authorization", "Bearer dummy-token")
            .with_status(401)
            .with_header("content-type", "application/json")
            .with_body(r#"{"todo": "Have fun!"}"#)
            .create();

        let mut runtime = Runtime::new().await.unwrap();
        runtime.enable_fetch().await.unwrap();

        let js_code = r#"
        export async function test_fetch(id) {
            const headers = {
                'Authorization': 'Bearer dummy-token',
            };
            const response = await fetch("$URL/todos/" + id, {
                headers
            });
            const data = await response.json();
            return data.todo;
        }
    "#
        .replace("$URL", &server.url());

        runtime
            .add_function(
                "test_fetch",
                DataType::Utf8,
                CallMode::ReturnNullOnNullInput,
                &js_code,
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
        +------------+
        | test_fetch |
        +------------+
        | Have fun!  |
        +------------+"#]],
        );

        mock.assert();
    }

    #[tokio::test]
    async fn test_fetch_post_with_body() {
        let mut server = Server::new_async().await;

        let mock_hello = server
            .mock("POST", "/echo")
            .match_header("authorization", "Bearer dummy-token")
            .match_body(r#"{"input":"hello"}"#)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"output":"hello"}"#)
            .create();
        let mock_buddy = server
            .mock("POST", "/echo")
            .match_header("authorization", "Bearer dummy-token")
            .match_body(r#"{"input":"buddy"}"#)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"output":"buddy"}"#)
            .create();

        let mut runtime = Runtime::new().await.unwrap();
        runtime.enable_fetch().await.unwrap();

        let js_code = r#"
        export async function test_fetch(input) {
            const body = JSON.stringify({
                input: input
            });
            const response = await fetch("$URL/echo", {
                method: 'POST',
                headers: {
                    'Authorization': 'Bearer dummy-token'
                },
                body
            });
            const data = await response.json();
            return data.output;
        }
    "#
        .replace("$URL", &server.url());

        runtime
            .add_function(
                "test_fetch",
                DataType::Utf8,
                CallMode::ReturnNullOnNullInput,
                &js_code,
                true,
            )
            .await
            .unwrap();

        let schema = Schema::new(vec![Field::new("input", DataType::Utf8, true)]);
        let arg0 = arrow_array::StringArray::from(vec![Some("hello"), Some("buddy")]);
        let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

        let output = runtime.call("test_fetch", &input).await.unwrap();
        check(
            &[output],
            expect![[r#"
        +------------+
        | test_fetch |
        +------------+
        | hello      |
        | buddy      |
        +------------+"#]],
        );

        mock_hello.assert();
        mock_buddy.assert();
    }

    #[tokio::test]
    async fn test_fetch_get_503() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/todos/1")
            .with_status(503)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error": "Oops! Service Unavailable"}"#)
            .create();

        let mut runtime = Runtime::new().await.unwrap();
        runtime.enable_fetch().await.unwrap();

        let js_code = r#"
        function assert(cond) {
            if (!cond) {
                throw new Error("Assertion failed");
            }
        }
        export async function test_fetch(id) {
            const response = await fetch("$URL/todos/" + id);
            assert(!response.ok);
            assert(response.status === 503);
            assert(response.statusText === "Service Unavailable");
            assert(!response.bodyUsed);
            const res = await response.json();
            assert(response.bodyUsed);
            throw new Error(res.error);
        }
    "#
        .replace("$URL", &server.url());

        runtime
            .add_function(
                "test_fetch",
                DataType::Utf8,
                CallMode::ReturnNullOnNullInput,
                &js_code,
                true,
            )
            .await
            .unwrap();

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
        let arg0 = Int32Array::from(vec![Some(1)]);
        let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

        let error = runtime.call("test_fetch", &input).await.unwrap_err();
        let error_msg = format!("{:?}", error);
        assert!(error_msg.contains("Oops! Service Unavailable"));

        mock.assert();
    }

    /// Compare the actual output with the expected output.
    fn check(actual: &[RecordBatch], expect: Expect) {
        expect.assert_eq(&pretty_format_batches(actual).unwrap().to_string());
    }

    fn inspect_error(err: &rquickjs::Error, ctx: &rquickjs::Ctx) {
        match err {
            rquickjs::Error::Exception => {
                eprintln!("exception generated by QuickJS: {:?}", ctx.catch())
            }
            e => eprintln!("{:?}", e),
        }
    }
}
