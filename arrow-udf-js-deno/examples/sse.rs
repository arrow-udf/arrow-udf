use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_js_deno::{CallMode, Runtime};

#[tokio::main]
async fn main() {
    let runtime = Runtime::new();

    let original_code = include_str!("./sse/bundled/bundled.js");

    let code = original_code.replace("{{SERVER_URL}}", "http://127.0.0.1:4200/graphql/stream");

    runtime
        .add_function(
            "createAsyncIterable",
            DataType::Struct(
                vec![Field::new(
                    "data",
                    DataType::Struct(vec![Field::new("greetings", DataType::Utf8, true)].into()),
                    true,
                )]
                .into(),
            ),
            CallMode::ReturnNullOnNullInput,
            &code,
        )
        .await
        .unwrap();

    let input = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )
    .unwrap();

    let mut outputs = runtime
        .call_table_function("createAsyncIterable", input, 10)
        .await
        .unwrap();

    while let Some(result) = outputs.next().await {
        let output = result.unwrap();
        arrow_cast::pretty::print_batches(std::slice::from_ref(&output)).unwrap();
        // do something with the output
    }
}
