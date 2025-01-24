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
pub struct Fetch {
    client: Arc<reqwest::Client>,
}

impl Fetch {
    pub fn new() -> Self {
        Self {
            client: Arc::new(reqwest::Client::new()),
        }
    }

    /// Native implementation for `async function do_fetch(method, url, headers, body, timeout_ns)`
    ///
    /// This function is used to send an HTTP request to the given URL with the specified method, headers, body, and timeout.
    /// We use JavaScript to wrap it into a standard Fetch API later.
    pub async fn do_fetch<'js, 'a>(
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
}

impl<'js> rquickjs::IntoJs<'js> for Fetch {
    fn into_js(self, ctx: &Ctx<'js>) -> Result<rquickjs::Value<'js>> {
        let f = move |ctx_: Ctx<'js>,
                      method: String,
                      url: String,
                      headers: Option<HashMap<String, String>>,
                      body: Option<String>,
                      timeout_ns: Option<u64>| {
            Self::do_fetch(
                self.client.clone(),
                ctx_,
                method,
                url,
                headers,
                body,
                timeout_ns,
            )
        };
        rquickjs::Function::new(ctx.clone(), Async(f))?.into_js(ctx)
    }
}

/// Enable fetch API in the given `AsyncContext`.
pub fn enable_fetch<'js>(ctx: &Ctx<'js>) -> Result<()> {
    ctx.globals().set("do_fetch", Fetch::new())?;
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
