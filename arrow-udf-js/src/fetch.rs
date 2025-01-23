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
    /// Return response status
    #[qjs(get)]
    pub fn status(&self) -> Result<u16> {
        self.response
            .as_ref()
            .map(|r| r.status().as_u16())
            .ok_or_else(|| {
                rquickjs::Error::new_from_js_message(
                    "Response",
                    "status",
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
                rquickjs::Error::new_from_js_message("Response", "ok", "Response already consumed")
            })
    }

    /// Read and convert response body to text
    #[qjs(rename = "text")]
    pub async fn text(&mut self) -> Result<String> {
        let response = self.response.take().ok_or_else(|| {
            rquickjs::Error::new_from_js_message("Response", "text", "Response already consumed")
        })?;

        response
            .text()
            .await
            .map_err(|e| rquickjs::Error::new_from_js_message("Response", "text", e.to_string()))
    }

    /// Read and convert response body to JSON
    #[qjs(rename = "json")]
    pub async fn json<'js>(&mut self, ctx: Ctx<'js>) -> Result<rquickjs::Value<'js>> {
        let text = self.text().await?;
        ctx.json_parse(text)
    }
}

#[rquickjs::module(rename_vars = "camelCase")]
pub mod fetch {
    use super::*;

    #[rquickjs::function]
    pub async fn fetch(ctx: Ctx<'_>, url: String) -> Result<Class<Response>> {
        let client = reqwest::Client::new();
        let response =
            client.get(&url).send().await.map_err(|e| {
                rquickjs::Error::new_from_js_message("Fetch", "fetch", e.to_string())
            })?;

        let response = Response {
            response: Some(response),
        };
        Class::instance(ctx, response)
    }
}
