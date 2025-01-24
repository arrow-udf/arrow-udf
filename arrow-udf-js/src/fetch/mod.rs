use rquickjs::{Ctx, Module, Result};

mod do_fetch;

const FETCH_JS: &str = include_str!("fetch.js");
const HEADERS_JS: &str = include_str!("headers.js");

/// Enable fetch API in the given `AsyncContext`.
pub fn enable_fetch<'js>(ctx: &Ctx<'js>) -> Result<()> {
    Module::declare_def::<do_fetch::js_fetch, _>(ctx.clone(), "native_fetch")?;
    Module::declare(ctx.clone(), "headers.js", HEADERS_JS)?;
    Module::declare(ctx.clone(), "fetch.js", FETCH_JS)?;
    Module::evaluate(
        ctx.clone(),
        "enable_fetch",
        r"
        import { fetch, Headers, Request, Response } from 'fetch.js';
        globalThis.fetch = fetch;
        globalThis.Headers = Headers;
        globalThis.Request = Request;
        globalThis.Response = Response;",
    )?;
    Ok(())
}
