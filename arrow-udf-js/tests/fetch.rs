#[cfg(feature = "fetch")]
mod tests {

    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_cast::pretty::pretty_format_batches;
    use arrow_schema::{DataType, Field, Schema};
    use arrow_udf_js::{CallMode, Runtime};
    use expect_test::{expect, Expect};
    use mockito::Server;
    use rquickjs::{async_with, AsyncContext};

    async fn run_async_js_code(context: &AsyncContext, js_code: &str) {
        async_with!(context => |ctx| {
            ctx.eval_promise::<_>(js_code)
            .inspect_err(|e| inspect_error(e, &ctx))
            .unwrap()
            .into_future::<()>()
            .await
            .inspect_err(|e| inspect_error(e, &ctx))
            .unwrap();
        })
        .await;
    }

    #[tokio::test]
    async fn run_javascript_tests() {
        let runtime = Runtime::new().await.unwrap();
        runtime.enable_fetch().await.unwrap();

        run_async_js_code(
            runtime.context(),
            &std::fs::read_to_string("src/fetch/fetch.test.js").unwrap(),
        )
        .await;
        run_async_js_code(
            runtime.context(),
            &std::fs::read_to_string("src/fetch/headers.test.js").unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_fetch_in_udf() {
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
        let mock_bad_request = server
            .mock("POST", "/echo")
            .match_header("authorization", "Bearer dummy-token")
            .match_body(r#"{"input":null}"#)
            .with_status(400)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"Bad Request"}"#)
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
            if (!response.ok) {
                const m = await response.json();
                throw new Error(m.error);
            }
            const m = await response.json();
            return m.output;
        }
    "#
        .replace("$URL", &server.url());

        runtime
            .add_function(
                "test_fetch",
                DataType::Utf8,
                CallMode::CalledOnNullInput,
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

        let schema = Schema::new(vec![Field::new("input", DataType::Utf8, true)]);
        let arg0 = arrow_array::StringArray::from(vec![None] as Vec<Option<String>>);
        let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

        let error = runtime.call("test_fetch", &input).await.unwrap_err();
        assert!(error.source().unwrap().to_string().contains("Bad Request"));

        mock_hello.assert();
        mock_buddy.assert();
        mock_bad_request.assert();
    }

    /// Auxiliary function to run a test with a given js code and server url
    async fn test(server_url: &str, js_code: &str) {
        let runtime = Runtime::new().await.unwrap();
        runtime.enable_fetch().await.unwrap();

        const JS_ASSERT: &str = r#"
            function assert(cond) {
                if (!cond) {
                    throw new Error("Assertion failed");
                }
            }"#;
        run_async_js_code(&runtime.context(), JS_ASSERT).await;

        let js_code = js_code.replace("$URL", server_url);
        run_async_js_code(&runtime.context(), &js_code).await;
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

        test(
            &server.url(),
            r#"
            const response = await fetch("$URL/todos/1");
            assert(response.ok);
            assert(response.status === 200);
            assert(response.statusText === "OK");
            assert(!response.bodyUsed);
            const data = await response.json();
            assert(response.bodyUsed);
            assert(data.todo === "Have fun!");"#,
        )
        .await;

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

        test(
            &server.url(),
            r#"
            const headers = {
                'Authorization': 'Bearer dummy-token'
            };
            const response = await fetch("$URL/todos/1", { headers });
            assert(response.status === 401);
            const data = await response.json();
            assert(data.todo === "Have fun!");"#,
        )
        .await;

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

        test(
            &server.url(),
            r#"
            const inputs = ["hello", "buddy"];
            for (const input of inputs) {
                const body = JSON.stringify({ input });
                const headers = { 'Authorization': 'Bearer dummy-token' };
                const response = await fetch("$URL/echo", {
                    method: 'POST',
                    headers,
                    body
                });
                assert(response.ok);
                const data = await response.json();
                assert(data.output === input);
            }"#,
        )
        .await;

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

        test(
            &server.url(),
            r#"
            const response = await fetch("$URL/todos/1");
            assert(!response.ok);
            assert(response.status === 503);
            assert(response.statusText === "Service Unavailable");
            assert(!response.bodyUsed);
            const data = await response.json();
            assert(response.bodyUsed);
            assert(data.error === "Oops! Service Unavailable");"#,
        )
        .await;

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
