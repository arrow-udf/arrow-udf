// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    path::Path,
    rc::Rc,
    sync::{atomic::AtomicI32, Arc},
};

use deno_ast::{MediaType, ParseParams, SourceTextInfo};
use deno_core::{
    error::AnyError, located_script_name, Extension, JsRuntime, JsRuntimeForSnapshot,
    ModuleCodeString, ModuleName, ModuleSpecifier, NoopModuleLoader, RuntimeOptions, SourceMapData,
};

#[cfg(not(feature = "with-fetch"))]
use deno_core::FastStaticString;
#[cfg(feature = "with-fetch")]
use deno_http::DefaultHttpPropertyExtractor;
use ops::os::ExitCode;
use permissions::{Permissions, PermissionsContainer};

pub mod deno_runtime;
mod ops;
mod permissions;

const DENO_VERSION: &str = "1.44.2";
const TYPESCRIPT_VERSION: &str = "5.2.2";

#[derive(Clone)]
pub struct BootstrapOptions {
    pub args: Vec<String>,
    pub cpu_count: usize,
    pub enable_testing_features: bool,
    pub location: Option<ModuleSpecifier>,
    pub locale: String,
    /// Sets `Deno.noColor` in JS runtime.
    pub no_color: bool,
    pub is_tty: bool,
    pub unstable: bool,
    pub user_agent: String,
    pub has_node_modules_dir: bool,
    pub maybe_binary_npm_command_name: Option<String>,
}

impl BootstrapOptions {
    pub fn as_json(&self) -> String {
        let payload = serde_json::json!({
          // Shared bootstrap args
          "args": self.args,
          "location": self.location,
          "locale": self.locale,
          "noColor": self.no_color,
          "isTty": self.is_tty,
          "unstableFlag": self.unstable,
          // Web worker only
          "enableTestingFeaturesFlag": self.enable_testing_features,
          // Env values
          "pid": std::process::id(),
          "userAgent": self.user_agent,
          "hasNodeModulesDir": self.has_node_modules_dir,
          "maybeBinaryNpmCommandName": self.maybe_binary_npm_command_name,
        });
        serde_json::to_string_pretty(&payload).unwrap()
    }
}

deno_core::extension!(runtime,
  deps = [
      deno_webidl,
      deno_console,
      deno_url,
      deno_tls,
      deno_web,
      deno_fetch,
      deno_websocket,
      deno_crypto,
      deno_net,
      deno_http,
      deno_io
  ],
  esm_entry_point = "ext:runtime/99_main.js",
  esm = [
    dir "js/runtime",
      "01_errors.js",
      "01_version.ts",
      "06_util.js",
      "10_permissions.js",
      "13_buffer.js",
      "30_os.js",
      "90_deno_ns.js",
      "98_global_scope_shared.js",
      "98_global_scope_window.js",
      "98_global_scope_worker.js",
      "99_main.js"
  ],
  customizer = | ext: &mut Extension | {
    #[cfg(not(feature = "with-fetch"))]
    #[allow(deprecated)]
    {
        ext.deps = &[
            "deno_webidl",
            "deno_console",
            "deno_url",
            "deno_tls",
            "deno_web",
            "deno_crypto",
            "deno_net",
            "deno_io",
        ];
        for esm_file in ext.esm_files.to_mut().iter_mut() {
            if esm_file.specifier == "ext:runtime/98_global_scope_shared.js" {
                const STR: v8::OneByteConst = FastStaticString::create_external_onebyte_const(
                    include_bytes!("../js/runtime/98_global_scope_shared_nofetch.js"),
                );
                let s: &'static v8::OneByteConst = &STR;
                esm_file.code = ExtensionFileSourceCode::IncludedInBinary(FastStaticString::new(s))
            } else if esm_file.specifier == "ext:runtime/90_deno_ns.js" {
                const STR: v8::OneByteConst = FastStaticString::create_external_onebyte_const(
                    include_bytes!("../js/runtime/90_deno_ns_nofetch.js"),
                );
                let s: &'static v8::OneByteConst = &STR;
                esm_file.code = ExtensionFileSourceCode::IncludedInBinary(FastStaticString::new(s))
            }
        }
    }
  }
);

pub fn create_runtime_snapshot() -> JsRuntimeForSnapshot {
    #[cfg(feature = "with-fetch")]
    let user_agent = "arrow-udf-js-deno".to_owned();

    let options = get_bootstrap_options();
    let options_clone = options.clone();

    let perm_ext = Extension {
        name: "deno_runtime",
        op_state_fn: Some(Box::new(move |state| {
            // TODO: Set the right permissions
            state.put::<PermissionsContainer>(PermissionsContainer::new(Permissions::allow_net(
                vec![],
                None,
            )));
            state.put(options_clone);
        })),
        ..Default::default()
    };

    let exit_code = ExitCode(Arc::new(AtomicI32::new(0)));

    if std::env::var("TARGET").is_err() {
        std::env::set_var("TARGET", std::env!("TARGET"));
    }

    let extensions: Vec<Extension> = vec![
        deno_webidl::deno_webidl::init_ops_and_esm(),
        deno_console::deno_console::init_ops_and_esm(),
        deno_url::deno_url::init_ops_and_esm(),
        deno_tls::deno_tls::init_ops_and_esm(),
        deno_web::deno_web::init_ops_and_esm::<PermissionsContainer>(
            Default::default(),
            Default::default(),
        ),
        #[cfg(feature = "with-fetch")]
        deno_fetch::deno_fetch::init_ops_and_esm::<PermissionsContainer>(deno_fetch::Options {
            user_agent: user_agent.clone(),
            ..Default::default()
        }),
        #[cfg(feature = "with-fetch")]
        deno_websocket::deno_websocket::init_ops_and_esm::<PermissionsContainer>(
            user_agent, None, None,
        ),
        deno_crypto::deno_crypto::init_ops_and_esm(None),
        deno_io::deno_io::init_ops_and_esm(Some(Default::default())),
        deno_net::deno_net::init_ops_and_esm::<PermissionsContainer>(None, None),
        ops::runtime::deno_runtime::init_ops("deno:runtime".parse().unwrap()),
        ops::os::deno_os::init_ops_and_esm(exit_code),
        ops::permissions::deno_permissions::init_ops_and_esm(),
        ops::signal::deno_signal::init_ops_and_esm(),
        ops::tty::deno_tty::init_ops_and_esm(),
        #[cfg(feature = "with-fetch")]
        deno_http::deno_http::init_ops_and_esm::<DefaultHttpPropertyExtractor>(),
        #[cfg(feature = "with-fetch")]
        ops::http::deno_http_runtime::init_ops_and_esm(),
        ops::bootstrap::deno_bootstrap::init_ops_and_esm(Some(ops::bootstrap::SnapshotOptions {
            deno_version: TYPESCRIPT_VERSION.to_string(),
            ts_version: DENO_VERSION.to_string(),
            v8_version: deno_core::v8_version(),
            target: std::env::var("TARGET").unwrap(),
        })),
        perm_ext,
        runtime::init_ops_and_esm(),
    ];

    JsRuntimeForSnapshot::new(RuntimeOptions {
        module_loader: Some(Rc::new(NoopModuleLoader)),
        extensions,
        inspector: false,
        extension_transpiler: Some(Rc::new(|specifier, source| {
            maybe_transpile_source(specifier, source)
        })),
        ..Default::default()
    })
}

pub fn create_runtime(snapshot: &'static [u8]) -> deno_runtime::DenoRuntime {
    #[cfg(feature = "with-fetch")]
    let user_agent = "arrow-udf-js-deno".to_owned();

    let options = get_bootstrap_options();
    let options_clone = options.clone();

    let perm_ext = Extension {
        name: "deno_runtime",
        op_state_fn: Some(Box::new(move |state| {
            // TODO: Set the right permissions
            state.put::<PermissionsContainer>(PermissionsContainer::new(Permissions::allow_net(
                vec![],
                None,
            )));
            state.put(options_clone);
        })),
        ..Default::default()
    };

    let exit_code = ExitCode(Arc::new(AtomicI32::new(0)));

    let extensions: Vec<Extension> = vec![
        deno_webidl::deno_webidl::init_ops(),
        deno_console::deno_console::init_ops(),
        deno_url::deno_url::init_ops(),
        deno_tls::deno_tls::init_ops(),
        deno_web::deno_web::init_ops::<PermissionsContainer>(
            Default::default(),
            Default::default(),
        ),
        #[cfg(feature = "with-fetch")]
        deno_fetch::deno_fetch::init_ops::<PermissionsContainer>(deno_fetch::Options {
            user_agent: user_agent.clone(),
            ..Default::default()
        }),
        #[cfg(feature = "with-fetch")]
        deno_websocket::deno_websocket::init_ops::<PermissionsContainer>(user_agent, None, None),
        deno_crypto::deno_crypto::init_ops(None),
        deno_io::deno_io::init_ops(Some(Default::default())),
        deno_net::deno_net::init_ops::<PermissionsContainer>(None, None),
        ops::runtime::deno_runtime::init_ops("deno:runtime".parse().unwrap()),
        ops::os::deno_os::init_ops(exit_code),
        ops::permissions::deno_permissions::init_ops(),
        ops::signal::deno_signal::init_ops(),
        ops::tty::deno_tty::init_ops(),
        #[cfg(feature = "with-fetch")]
        deno_http::deno_http::init_ops::<DefaultHttpPropertyExtractor>(),
        #[cfg(feature = "with-fetch")]
        ops::http::deno_http_runtime::init_ops(),
        ops::bootstrap::deno_bootstrap::init_ops(None),
        perm_ext,
        runtime::init_ops(),
    ];

    let mut runtime = JsRuntime::new(RuntimeOptions {
        module_loader: Some(Rc::new(NoopModuleLoader)),
        startup_snapshot: Some(snapshot),
        extensions,
        inspector: false,
        ..Default::default()
    });

    bootstrap(&mut runtime, &options);

    deno_runtime::DenoRuntime::new(runtime)
}

pub fn bootstrap(runtime: &mut JsRuntime, options: &BootstrapOptions) {
    runtime.op_state().borrow_mut().put(options.clone());
    let script = format!("bootstrap.mainRuntime({})", options.as_json());
    let result = runtime.execute_script(located_script_name!(), script);
    if result.is_err() {
        println!("Error executing script: {result:?}");
    }
    result.expect("Failed to execute bootstrap script");

    let decimal_code = include_str!("../js/decimal/big.decimal.js");

    let result = runtime.execute_script("decimal.js", decimal_code);
    result.expect("Failed to execute decimal script");

    #[cfg(feature = "with-dayjs")]
    {
        let day_js_scripts = [
            include_str!("../js/dayjs/dayjs.min.js"),
            include_str!("../js/dayjs/plugin/utc.js"),
            include_str!("../js/dayjs/plugin/timezone.js"),
            include_str!("../js/dayjs/plugin/duration.js"),
            include_str!("../js/dayjs/plugin/bigIntSupport.js"),
            include_str!("../js/dayjs/plugin/isSameOrAfter.js"),
            include_str!("../js/dayjs/plugin/isSameOrBefore.js"),
            include_str!("../js/dayjs/plugin/isBetween.js"),
            include_str!("../js/dayjs/plugin/relativeTime.js"),
            include_str!("../js/dayjs/plugin/weekOfYear.js"),
            include_str!("../js/dayjs/plugin/dayOfYear.js"),
            include_str!("../js/dayjs/plugin/quarterOfYear.js"),
            include_str!("../js/dayjs/plugin/isoWeek.js"),
            include_str!("../js/dayjs/setup.dayjs.js"),
        ];

        for script in day_js_scripts.into_iter() {
            let result = runtime.execute_script("dayjs_init", script);
            result.expect("Failed to execute dayjs plugin script");
        }
    }
}

pub fn get_bootstrap_options() -> BootstrapOptions {
    BootstrapOptions {
        args: vec![],
        cpu_count: std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1),
        enable_testing_features: false,
        location: None,
        no_color: true,
        is_tty: true,
        unstable: false,
        user_agent: "risingwave".to_string(),
        has_node_modules_dir: true,
        maybe_binary_npm_command_name: None,
        locale: deno_core::v8::icu::get_language_tag(),
    }
}

pub fn maybe_transpile_source(
    name: ModuleName,
    source: ModuleCodeString,
) -> Result<(ModuleCodeString, Option<SourceMapData>), AnyError> {
    // Always transpile `node:` built-in modules, since they might be TypeScript.
    let media_type = if name.starts_with("node:") {
        MediaType::TypeScript
    } else {
        MediaType::from_path(Path::new(&name))
    };

    match media_type {
        MediaType::TypeScript => {}
        MediaType::JavaScript => return Ok((source, None)),
        MediaType::Mjs => return Ok((source, None)),
        _ => panic!(
            "Unsupported media type for snapshotting {media_type:?} for file {}",
            name
        ),
    }

    let parsed = deno_ast::parse_module(ParseParams {
        specifier: deno_core::url::Url::parse(&name).unwrap(),
        text_info: SourceTextInfo::from_string(source.as_str().to_owned()),
        media_type,
        capture_tokens: false,
        scope_analysis: false,
        maybe_syntax: None,
    })?;
    let transpiled_source = parsed.transpile(&deno_ast::EmitOptions {
        imports_not_used_as_values: deno_ast::ImportsNotUsedAsValues::Remove,
        inline_source_map: false,
        source_map: cfg!(debug_assertions),
        ..Default::default()
    })?;

    let maybe_source_map: Option<SourceMapData> = transpiled_source
        .source_map
        .map(|sm| sm.into_bytes().into());

    Ok((transpiled_source.text.into(), maybe_source_map))
}
