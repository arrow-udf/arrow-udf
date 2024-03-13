// Copy from <https://github.com/denoland/deno/blob/main/runtime/ops/bootstrap.rs>
// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

use deno_core::op2;
use deno_core::OpState;
use serde::Serialize;

use crate::BootstrapOptions;

deno_core::extension!(
  deno_bootstrap,
  ops = [
    op_bootstrap_args,
    op_bootstrap_pid,
    op_bootstrap_numcpus,
    op_bootstrap_user_agent,
    op_bootstrap_language,
    op_bootstrap_no_color,
    op_bootstrap_is_tty,
    op_bootstrap_unstable_args,
    op_snapshot_options,
  ],
  options = {
    snapshot_options: Option<SnapshotOptions>,
  },
  state = |state, options| {
    if let Some(snapshot_options) = options.snapshot_options {
      state.put::<SnapshotOptions>(snapshot_options);
    }
  },
);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotOptions {
    pub deno_version: String,
    pub ts_version: String,
    pub v8_version: &'static str,
    pub target: String,
}

// Note: Called at snapshot time, op perf is not a concern.
#[op2]
#[serde]
pub fn op_snapshot_options(state: &mut OpState) -> SnapshotOptions {
    state.take::<SnapshotOptions>()
}

#[op2]
#[serde]
pub fn op_bootstrap_args(state: &mut OpState) -> Vec<String> {
    state.borrow::<BootstrapOptions>().args.clone()
}

#[op2]
#[serde]
pub fn op_bootstrap_unstable_args(state: &mut OpState) -> Vec<String> {
    let _options = state.borrow::<BootstrapOptions>();
    Vec::new()
}

#[op2(fast)]
#[smi]
pub fn op_bootstrap_pid() -> u32 {
    std::process::id()
}

#[op2(fast)]
#[smi]
pub fn op_bootstrap_numcpus(state: &mut OpState) -> u32 {
    state.borrow::<BootstrapOptions>().cpu_count as u32
}

#[op2]
#[string]
pub fn op_bootstrap_user_agent(state: &mut OpState) -> String {
    state.borrow::<BootstrapOptions>().user_agent.clone()
}

#[op2]
#[string]
pub fn op_bootstrap_language(state: &mut OpState) -> String {
    state.borrow::<BootstrapOptions>().locale.clone()
}

#[op2(fast)]
pub fn op_bootstrap_no_color(_state: &mut OpState) -> bool {
    true
}

#[op2(fast)]
pub fn op_bootstrap_is_tty(_state: &mut OpState) -> bool {
    false
}
