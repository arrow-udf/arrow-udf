use reqwest;
use rquickjs::prelude::*;
use rquickjs::{Class, Result};

#[derive(rquickjs::class::Trace)]
#[rquickjs::class]
pub struct Response {
    #[qjs(skip_trace)]
    response: Option<reqwest::Response>,
}

#[rquickjs::methods]
impl Response {
    // A constructor is required to export to JavaScript. Idk why :)
    #[qjs(constructor)]
    pub fn new() -> Self {
        Self { response: None }
    }

    /// Return response status
    #[qjs(get)]
    pub fn status(&self) -> Result<u16> {
        self.response
            .as_ref()
            .map(|r| r.status().as_u16())
            .ok_or_else(|| {
                rquickjs::Error::new_from_js_message(
                    "Response",
                    "Response",
                    "Response already consumed",
                )
            })
    }

    /// Return if response was successful (status in 200-299 range)
    #[qjs(get)]
    pub fn ok(&self) -> Result<bool> {
        self.response
            .as_ref()
            .map(|r| r.status().is_success())
            .ok_or_else(|| {
                rquickjs::Error::new_from_js_message(
                    "Response",
                    "Response",
                    "Response already consumed",
                )
            })
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
        let response = request
            .send()
            .await
            .map_err(|e| rquickjs::Error::new_from_js_message("fetch", "fetch", e.to_string()))?;

        let response = Response {
            response: Some(response),
        };
        Class::instance(ctx, response)
    }
}
