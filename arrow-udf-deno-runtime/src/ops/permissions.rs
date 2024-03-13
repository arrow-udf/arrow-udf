// Modified from <https://github.com/denoland/deno/blob/main/runtime/ops/permissions.rs>
// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

use crate::permissions::PermissionState;
use crate::permissions::PermissionsContainer;
use deno_core::error::custom_error;
use deno_core::error::type_error;
use deno_core::error::uri_error;
use deno_core::error::AnyError;
use deno_core::op2;
use deno_core::url;
use deno_core::OpState;
use serde::Deserialize;
use serde::Serialize;
use std::path::Path;

deno_core::extension!(
    deno_permissions,
    ops = [
        op_query_permission,
        op_revoke_permission,
        op_request_permission,
    ]
);

#[derive(Deserialize)]
pub struct PermissionArgs {
    name: String,
    path: Option<String>,
    host: Option<String>,
    variable: Option<String>,
    kind: Option<String>,
    command: Option<String>,
}

#[derive(Serialize)]
pub struct PermissionStatus {
    state: String,
    partial: bool,
}

impl From<PermissionState> for PermissionStatus {
    fn from(state: PermissionState) -> Self {
        PermissionStatus {
            state: if state == PermissionState::GrantedPartial {
                PermissionState::Granted.to_string()
            } else {
                state.to_string()
            },
            partial: state == PermissionState::GrantedPartial,
        }
    }
}

pub fn parse_sys_kind(kind: &str) -> Result<&str, AnyError> {
    match kind {
        "hostname" | "osRelease" | "osUptime" | "loadavg" | "networkInterfaces"
        | "systemMemoryInfo" | "uid" | "gid" => Ok(kind),
        _ => Err(type_error(format!("unknown system info kind \"{kind}\""))),
    }
}

#[op2]
#[serde]
pub fn op_query_permission(
    state: &mut OpState,
    #[serde] args: PermissionArgs,
) -> Result<PermissionStatus, AnyError> {
    let permissions = state.borrow::<PermissionsContainer>().0.lock();
    let path = args.path.as_deref();
    let perm = match args.name.as_ref() {
        "read" => permissions.read.query(path.map(Path::new)),
        "write" => permissions.write.query(path.map(Path::new)),
        "net" => permissions.net.query(
            match args.host.as_deref() {
                None => None,
                Some(h) => Some(parse_host(h)?),
            }
            .as_ref(),
        ),
        "env" => permissions.env.query(args.variable.as_deref()),
        "sys" => permissions
            .sys
            .query(args.kind.as_deref().map(parse_sys_kind).transpose()?),
        "run" => permissions.run.query(args.command.as_deref()),
        "ffi" => permissions.ffi.query(args.path.as_deref().map(Path::new)),
        "hrtime" => permissions.hrtime.query(),
        n => {
            return Err(custom_error(
                "ReferenceError",
                format!("No such permission name: {n}"),
            ))
        }
    };
    Ok(PermissionStatus::from(perm))
}

#[op2]
#[serde]
pub fn op_revoke_permission(
    state: &mut OpState,
    #[serde] args: PermissionArgs,
) -> Result<PermissionStatus, AnyError> {
    let mut permissions = state.borrow_mut::<PermissionsContainer>().0.lock();
    let path = args.path.as_deref();
    let perm = match args.name.as_ref() {
        "read" => permissions.read.revoke(path.map(Path::new)),
        "write" => permissions.write.revoke(path.map(Path::new)),
        "net" => permissions.net.revoke(
            match args.host.as_deref() {
                None => None,
                Some(h) => Some(parse_host(h)?),
            }
            .as_ref(),
        ),
        "env" => permissions.env.revoke(args.variable.as_deref()),
        "sys" => permissions
            .sys
            .revoke(args.kind.as_deref().map(parse_sys_kind).transpose()?),
        "run" => permissions.run.revoke(args.command.as_deref()),
        "ffi" => permissions.ffi.revoke(args.path.as_deref().map(Path::new)),
        "hrtime" => permissions.hrtime.revoke(),
        n => {
            return Err(custom_error(
                "ReferenceError",
                format!("No such permission name: {n}"),
            ))
        }
    };
    Ok(PermissionStatus::from(perm))
}

#[op2]
#[serde]
pub fn op_request_permission(
    state: &mut OpState,
    #[serde] args: PermissionArgs,
) -> Result<PermissionStatus, AnyError> {
    let mut permissions = state.borrow_mut::<PermissionsContainer>().0.lock();
    let path = args.path.as_deref();
    let perm = match args.name.as_ref() {
        "read" => permissions.read.request(path.map(Path::new)),
        "write" => permissions.write.request(path.map(Path::new)),
        "net" => permissions.net.request(
            match args.host.as_deref() {
                None => None,
                Some(h) => Some(parse_host(h)?),
            }
            .as_ref(),
        ),
        "env" => permissions.env.request(args.variable.as_deref()),
        "sys" => permissions
            .sys
            .request(args.kind.as_deref().map(parse_sys_kind).transpose()?),
        "run" => permissions.run.request(args.command.as_deref()),
        "ffi" => permissions.ffi.request(args.path.as_deref().map(Path::new)),
        "hrtime" => permissions.hrtime.request(),
        n => {
            return Err(custom_error(
                "ReferenceError",
                format!("No such permission name: {n}"),
            ))
        }
    };
    Ok(PermissionStatus::from(perm))
}

fn parse_host(host_str: &str) -> Result<(String, Option<u16>), AnyError> {
    let url =
        url::Url::parse(&format!("http://{}/", host_str)).map_err(|_| uri_error("Invalid host"))?;
    if url.path() != "/" {
        return Err(uri_error("Invalid host"));
    }
    let hostname = url.host_str().unwrap();
    Ok((hostname.to_string(), url.port()))
}
