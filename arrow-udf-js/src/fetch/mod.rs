use response::Response;
use rquickjs::prelude::Async;
use rquickjs::{Class, Ctx, Module, Result};

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

mod response;

const FETCH_JS: &str = include_str!("fetch.js");
const HEADERS_JS: &str = include_str!("headers.js");

#[derive(Clone)]
pub struct SendHttpRequest;

/// Native implementation for `async function send_http_request(method, url, headers, body, timeout_ns)`
///
/// We use JavaScript to wrap it into a standard Fetch API later.
async fn send_http_request<'js>(
    client: Arc<reqwest::Client>,
    ctx: Ctx<'js>,
    method: String,
    url: String,
    headers: Option<HashMap<String, String>>,
    body: Option<String>,
    timeout_ns: Option<u64>,
) -> Result<Class<'js, Response>> {
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

    let response = Response::new(res);
    Class::instance(ctx, response)
}

impl<'js> rquickjs::IntoJs<'js> for SendHttpRequest {
    fn into_js(self, ctx: &Ctx<'js>) -> Result<rquickjs::Value<'js>> {
        let client = Arc::new(reqwest::Client::new());
        rquickjs::Function::new(
            ctx.clone(),
            Async(
                move |ctx: Ctx<'js>,
                      method: String,
                      url: String,
                      headers: Option<HashMap<String, String>>,
                      body: Option<String>,
                      timeout_ns: Option<u64>| {
                    // NOTE(eric): It seems better to pass a reference instead of `Arc`, but
                    // the borrow checker just doesn't like it :/
                    send_http_request(client.clone(), ctx, method, url, headers, body, timeout_ns)
                },
            ),
        )?
        .into_js(ctx)
    }
}

/// Enable fetch API in the given `AsyncContext`.
pub fn enable_fetch<'js>(ctx: &Ctx<'js>) -> Result<()> {
    ctx.globals().set("sendHttpRequest", SendHttpRequest)?;
    Module::declare(ctx.clone(), "headers.js", HEADERS_JS)?;
    Module::declare(ctx.clone(), "fetch.js", FETCH_JS)?;
    Module::evaluate(
        ctx.clone(),
        "enable_fetch",
        r"
        import { fetch, Headers, Request } from 'fetch.js';
        globalThis.fetch = fetch;
        globalThis.Headers = Headers;
        globalThis.Request = Request;",
    )?;
    Ok(())
}
