use response::Response;
use rquickjs::loader::Bundle;
use rquickjs::prelude::*;
use rquickjs::{
    async_with, embed, AsyncContext, AsyncRuntime, Class, Ctx, Exception, Module, Result,
};

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::check_exception;

mod response;

/// The bundled JS modules for the Fetch API. They will be compiled into bytecode at build time.
static BUNDLE: Bundle = embed! {
    "headers.js": "src/fetch/headers.js",
    "fetch.js": "src/fetch/fetch.js",
};

pub struct SendHttpRequest;

/// Native implementation for `async function send_http_request(method, url, headers, body, timeout_ms)`
///
/// This is not supposed to be used directly, but rather wrapped into a standard Fetch API by JS code later.
async fn send_http_request<'js>(
    client: Arc<reqwest::Client>,
    ctx: Ctx<'js>,
    method: String,
    url: String,
    headers: Option<HashMap<String, String>>,
    body: Option<String>,
    timeout_ms: Option<u64>,
) -> Result<Class<'js, Response>> {
    let method = reqwest::Method::from_str(&method)
        .map_err(|e| Exception::throw_syntax(&ctx, &e.to_string()))?;
    let mut request = client.request(method, url);
    if let Some(headers) = headers {
        for (key, value) in headers {
            request = request.header(key, value);
        }
    }
    if let Some(body) = body {
        request = request.body(body);
    }
    if let Some(timeout_ms) = timeout_ms {
        request = request.timeout(Duration::from_millis(timeout_ms));
    }
    let res = request
        .send()
        .await
        .map_err(|e| Exception::throw_message(&ctx, &e.to_string()))?;

    let response = Response::new(res);
    Class::instance(ctx, response)
}

impl<'js> rquickjs::IntoJs<'js> for SendHttpRequest {
    fn into_js(self, ctx: &Ctx<'js>) -> Result<rquickjs::Value<'js>> {
        // Shared client for all requests in this `quickjs` instance.
        let client = Arc::new(reqwest::Client::new());
        rquickjs::Function::new(
            ctx.clone(),
            Async(
                move |ctx: Ctx<'js>,
                      method: String,
                      url: String,
                      headers: Option<HashMap<String, String>>,
                      body: Option<String>,
                      timeout_ms: Option<u64>| {
                    // NOTE(eric): It seems better to pass a reference instead of `Arc`, but
                    // the borrow checker just doesn't like it :/
                    send_http_request(client.clone(), ctx, method, url, headers, body, timeout_ms)
                },
            ),
        )?
        .into_js(ctx)
    }
}

/// Enable fetch API in the given `AsyncContext`.
pub async fn enable_fetch<'js>(rt: &AsyncRuntime, ctx: &AsyncContext) -> anyhow::Result<()> {
    rt.set_loader(BUNDLE, BUNDLE).await;
    async_with!(ctx => |ctx| {
        ctx.globals()
            .set("sendHttpRequest", SendHttpRequest)
            .map_err(|e| check_exception(e, &ctx))?;
        Module::evaluate(
            ctx.clone(),
            "enable_fetch",
            r"
            import { fetch, Headers, Request } from 'fetch.js';
            globalThis.fetch = fetch;
            globalThis.Headers = Headers;
            globalThis.Request = Request;",
        ).map_err(|e| check_exception(e, &ctx))?;
        Ok(())
    })
    .await
}
