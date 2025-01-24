use reqwest;
use rquickjs::prelude::*;
use rquickjs::{Class, Result};

/// The Response interface of the Fetch API represents the response to a request.
///
/// See also https://developer.mozilla.org/en-US/docs/Web/API/Response
#[derive(rquickjs::class::Trace, Default, Debug)]
#[rquickjs::class(rename_all = "camelCase")]
pub struct Response {
    /// The status code of the response. (This will be 200 for a success).
    #[qjs(get)]
    status: u16,

    /// The status message corresponding to the status code. (e.g., OK for 200).
    #[qjs(get)]
    status_text: String,

    /// A boolean indicating whether the response was successful (status in the range 200 – 299) or not.
    #[qjs(get)]
    ok: bool,

    #[qjs(skip_trace)]
    response: Option<reqwest::Response>,
}

#[rquickjs::methods(rename_all = "camelCase")]
impl Response {
    // A constructor is required to export to JavaScript. Idk why :)
    #[qjs(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores a boolean value that declares whether the body has been used in a response yet.
    #[qjs(get)]
    pub fn body_used(&self) -> bool {
        self.response.is_none()
    }

    /// Read and convert response body to text
    #[qjs(rename = "text")]
    pub async fn text(&mut self) -> Result<String> {
        let response = self.response.take().ok_or_else(|| {
            rquickjs::Error::new_from_js_message(
                "Response",
                "Response",
                "Response already consumed",
            )
        })?;

        response.text().await.map_err(|e| {
            rquickjs::Error::new_from_js_message("Response", "Response", e.to_string())
        })
    }

    /// Read and convert response body to JSON
    #[qjs(rename = "json")]
    pub async fn json<'js>(&mut self, ctx: Ctx<'js>) -> Result<rquickjs::Value<'js>> {
        let text = self.text().await?;
        ctx.json_parse(text)
    }
}

#[rquickjs::module]
pub mod fetch {
    use super::*;

    use std::collections::HashMap;
    use std::str::FromStr;
    use std::time::Duration;

    pub use super::Response;

    /// Native implementation for `async function do_fetch(method, url, headers, body, timeout_ns)`
    ///
    /// This function is used to send an HTTP request to the given URL with the specified method, headers, body, and timeout.
    /// We use JavaScript to wrap it into a standard Fetch API later.
    #[rquickjs::function]
    pub async fn do_fetch<'js>(
        ctx: Ctx<'js>,
        method: String,
        url: String,
        headers: Option<HashMap<String, String>>,
        body: Option<String>,
        timeout_ns: Option<u64>,
    ) -> Result<Class<'js, Response>> {
        // TODO: reuse client
        let client = reqwest::Client::new();
        // TODO: better error handling
        let method = reqwest::Method::from_str(&method)
            .map_err(|e| rquickjs::Error::new_from_js_message("fetch", "fetch", e.to_string()))?;
        let mut request = client.request(method, url);
        if let Some(headers) = headers {
            for (key, value) in headers {
                request = request.header(key, value);
            }
        }
        if let Some(body) = body {
            request = request.body(body);
        }
        if let Some(timeout_ns) = timeout_ns {
            request = request.timeout(Duration::from_nanos(timeout_ns));
        }
        let res = request
            .send()
            .await
            .map_err(|e| rquickjs::Error::new_from_js_message("fetch", "fetch", e.to_string()))?;

        let response = Response {
            status: res.status().as_u16(),
            status_text: res.status().canonical_reason().unwrap_or("").to_string(),
            ok: res.status().is_success(),
            response: Some(res),
        };
        Class::instance(ctx, response)
    }
}
