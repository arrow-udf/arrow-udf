// Modified from <https://github.com/denoland/deno/blob/main/runtime/ops/tty.rs>
// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

use deno_core::error::AnyError;
use deno_core::op2;
use deno_core::OpState;

deno_core::extension!(
    deno_tty,
    ops = [op_stdin_set_raw, op_isatty, op_console_size,],
);

#[op2(fast)]
fn op_stdin_set_raw(_state: &mut OpState, _is_raw: bool, _cbreak: bool) -> Result<(), AnyError> {
    Ok(())
}

#[op2(fast)]
fn op_isatty(_state: &mut OpState, _rid: u32) -> Result<bool, AnyError> {
    Ok(true)
}

#[op2(fast)]
fn op_console_size(_state: &mut OpState, #[buffer] _result: &mut [u32]) -> Result<(), AnyError> {
    Ok(())
}
