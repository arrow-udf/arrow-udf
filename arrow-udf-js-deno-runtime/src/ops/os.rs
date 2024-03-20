// Modified from <https://github.com/denoland/deno/blob/main/runtime/ops/os/mod.rs>
// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

use crate::permissions::PermissionsContainer;

use super::utils::into_string;
use deno_core::error::{type_error, AnyError};
use deno_core::url::Url;
use deno_core::Op;
use deno_core::{op2, OpState};
use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct ExitCode(pub(crate) Arc<AtomicI32>);

impl ExitCode {
    #[allow(dead_code)]
    pub fn get(&self) -> i32 {
        self.0.load(Ordering::Relaxed)
    }

    pub fn set(&mut self, code: i32) {
        self.0.store(code, Ordering::Relaxed);
    }
}

deno_core::extension!(
    deno_os,
    ops = [
        op_delete_env,
        op_env,
        op_get_env,
        op_set_env,
        op_set_exit_code,
        op_set_raw,
    ],
    options = {
      exit_code: ExitCode,
    },
    state = |state, options| {
      state.put::<ExitCode>(options.exit_code);
    },
);

deno_core::extension!(
    deno_os_worker,
    ops = [
        op_delete_env,
        op_env,
        op_get_env,
        op_set_env,
        op_set_exit_code,
    ],
    middleware = |op| match op.name {
        "op_exit" | "op_set_exit_code" =>
            op.with_implementation_from(&deno_core::op_void_sync::DECL),
        _ => op,
    },
);

#[op2(fast)]
fn op_set_env(
    state: &mut OpState,
    #[string] key: &str,
    #[string] value: &str,
) -> Result<(), AnyError> {
    state.borrow_mut::<PermissionsContainer>().check_env(key)?;
    if key.is_empty() {
        return Err(type_error("Key is an empty string."));
    }
    if key.contains(&['=', '\0'] as &[char]) {
        return Err(type_error(format!(
            "Key contains invalid characters: {key:?}"
        )));
    }
    if value.contains('\0') {
        return Err(type_error(format!(
            "Value contains invalid characters: {value:?}"
        )));
    }
    env::set_var(key, value);
    Ok(())
}

#[op2]
#[serde]
fn op_env(state: &mut OpState) -> Result<HashMap<String, String>, AnyError> {
    state.borrow_mut::<PermissionsContainer>().check_env_all()?;
    Ok(env::vars().collect())
}

#[op2]
#[string]
fn op_get_env(state: &mut OpState, #[string] key: String) -> Result<Option<String>, AnyError> {
    state.borrow_mut::<PermissionsContainer>().check_env(&key)?;

    if key.is_empty() {
        return Err(type_error("Key is an empty string."));
    }

    if key.contains(&['=', '\0'] as &[char]) {
        return Err(type_error(format!(
            "Key contains invalid characters: {key:?}"
        )));
    }

    let r = match env::var(key) {
        Err(env::VarError::NotPresent) => None,
        v => Some(v?),
    };
    Ok(r)
}

#[op2]
#[string]
fn op_exec_path(state: &mut OpState) -> Result<String, AnyError> {
    let current_exe = env::current_exe().unwrap();
    state
        .borrow_mut::<PermissionsContainer>()
        .check_read_blind(&current_exe, "exec_path", "Deno.execPath()")?;
    // Now apply URL parser to current exe to get fully resolved path, otherwise
    // we might get `./` and `../` bits in `exec_path`
    let exe_url = Url::from_file_path(current_exe).unwrap();
    let path = exe_url.to_file_path().unwrap();

    into_string(path.into_os_string())
}

#[op2(fast)]
fn op_delete_env(state: &mut OpState, #[string] key: String) -> Result<(), AnyError> {
    state.borrow_mut::<PermissionsContainer>().check_env(&key)?;
    if key.is_empty() || key.contains(&['=', '\0'] as &[char]) {
        return Err(type_error("Key contains invalid characters."));
    }
    env::remove_var(key);
    Ok(())
}

#[op2(fast)]
fn op_set_exit_code(state: &mut OpState, #[smi] code: i32) {
    state.borrow_mut::<ExitCode>().set(code);
}

#[op2(fast)]
fn op_set_raw(state: &mut OpState, rid: u32, _is_raw: bool, _cbreak: bool) -> Result<(), AnyError> {
    let _handle_or_fd = state.resource_table.get_fd(rid)?;
    Ok(())
}
