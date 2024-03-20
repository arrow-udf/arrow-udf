// Modified from <https://github.com/denoland/deno/blob/main/runtime/ops/http.rs>
// Including only the `op_http_upgrade` function
// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

use std::cell::RefCell;
use std::rc::Rc;

use deno_core::error::custom_error;
use deno_core::error::AnyError;
use deno_core::op2;
use deno_core::OpState;
use deno_core::RcRef;
use deno_core::ResourceId;
use deno_core::ToJsBuffer;
use deno_http::HttpRequestReader;
use deno_http::HttpStreamResource;
use deno_net::io::TcpStreamResource;
use deno_net::ops_tls::TlsStream;
use deno_net::ops_tls::TlsStreamResource;
use hyper::upgrade::Parts;
use serde::Serialize;
use tokio::net::TcpStream;

#[cfg(unix)]
use deno_net::io::UnixStreamResource;
#[cfg(unix)]
use tokio::net::UnixStream;

deno_core::extension!(deno_http_runtime, ops = [op_http_upgrade],);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpUpgradeResult {
    conn_rid: ResourceId,
    conn_type: &'static str,
    read_buf: ToJsBuffer,
}

#[op2(async)]
#[serde]
async fn op_http_upgrade(
    state: Rc<RefCell<OpState>>,
    #[smi] rid: ResourceId,
) -> Result<HttpUpgradeResult, AnyError> {
    let stream = state
        .borrow_mut()
        .resource_table
        .get::<HttpStreamResource>(rid)?;
    let mut rd = RcRef::map(&stream, |r| &r.rd).borrow_mut().await;

    let request = match &mut *rd {
        HttpRequestReader::Headers(request) => request,
        _ => {
            return Err(custom_error(
                "Http",
                "cannot upgrade because request body was used",
            ))
        }
    };

    let transport = hyper::upgrade::on(request).await?;
    let transport = match transport.downcast::<TcpStream>() {
        Ok(Parts {
            io: tcp_stream,
            read_buf,
            ..
        }) => {
            return Ok(HttpUpgradeResult {
                conn_type: "tcp",
                conn_rid: state
                    .borrow_mut()
                    .resource_table
                    .add(TcpStreamResource::new(tcp_stream.into_split())),
                read_buf: read_buf.to_vec().into(),
            });
        }
        Err(transport) => transport,
    };
    #[cfg(unix)]
    let transport = match transport.downcast::<UnixStream>() {
        Ok(Parts {
            io: unix_stream,
            read_buf,
            ..
        }) => {
            return Ok(HttpUpgradeResult {
                conn_type: "unix",
                conn_rid: state
                    .borrow_mut()
                    .resource_table
                    .add(UnixStreamResource::new(unix_stream.into_split())),
                read_buf: read_buf.to_vec().into(),
            });
        }
        Err(transport) => transport,
    };
    match transport.downcast::<TlsStream>() {
        Ok(Parts {
            io: tls_stream,
            read_buf,
            ..
        }) => Ok(HttpUpgradeResult {
            conn_type: "tls",
            conn_rid: state
                .borrow_mut()
                .resource_table
                .add(TlsStreamResource::new(tls_stream.into_split())),
            read_buf: read_buf.to_vec().into(),
        }),
        Err(_) => Err(custom_error(
            "Http",
            "encountered unsupported transport while upgrading",
        )),
    }
}
