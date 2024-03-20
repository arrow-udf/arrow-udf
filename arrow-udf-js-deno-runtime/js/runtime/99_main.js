// Copyright 2018-2023 the Deno authors. All rights reserved. MIT license.
// Remove Intl.v8BreakIterator because it is a non-standard API.
delete Intl.v8BreakIterator;

import { core, internals, primordials } from "ext:core/mod.js";
const ops = core.ops;
import {
  op_bootstrap_args,
  op_bootstrap_is_tty,
  op_bootstrap_no_color,
  op_bootstrap_pid,
  op_main_module,
  op_ppid,
  op_set_format_exception_callback,
  op_snapshot_options,
} from "ext:core/ops";
const {
  ArrayPrototypeIncludes,
  ArrayPrototypeMap,
  DateNow,
  Error,
  ErrorPrototype,
  FunctionPrototypeCall,
  ObjectAssign,
  ObjectDefineProperties,
  ObjectDefineProperty,
  ObjectKeys,
  ObjectPrototypeIsPrototypeOf,
  ObjectSetPrototypeOf,
  PromisePrototypeThen,
  PromiseResolve,
  Symbol,
  TypeError,
} = primordials;
const {
  isNativeError,
} = core;
import * as util from "ext:runtime/06_util.js";
import * as event from "ext:deno_web/02_event.js";
import * as location from "ext:deno_web/12_location.js";
import * as version from "ext:runtime/01_version.ts";
import * as os from "ext:runtime/30_os.js";
import * as timers from "ext:deno_web/02_timers.js";
import * as net from "ext:deno_net/01_net.js";
import {
  getDefaultInspectOptions,
  getNoColor,
  inspectArgs,
  quoteString,
  setNoColorFn,
} from "ext:deno_console/01_console.js";
import * as performance from "ext:deno_web/15_performance.js";
import * as url from "ext:deno_url/00_url.js";
import * as messagePort from "ext:deno_web/13_message_port.js";
import { denoNs, denoNsUnstable } from "ext:runtime/90_deno_ns.js";
import { errors } from "ext:runtime/01_errors.js";
import * as webidl from "ext:deno_webidl/00_webidl.js";
import { DOMException } from "ext:deno_web/01_dom_exception.js";
import {
  windowOrWorkerGlobalScope,
} from "ext:runtime/98_global_scope_shared.js";
import {
  mainRuntimeGlobalProperties,
  memoizeLazy,
} from "ext:runtime/98_global_scope_window.js";
import {
  workerRuntimeGlobalProperties,
} from "ext:runtime/98_global_scope_worker.js";
import { SymbolAsyncDispose, SymbolDispose } from "ext:deno_web/00_infra.js";

// deno-lint-ignore prefer-primordials
if (Symbol.dispose) throw "V8 supports Symbol.dispose now, no need to shim it!";
ObjectDefineProperties(Symbol, {
  dispose: {
    value: SymbolDispose,
    enumerable: false,
    writable: false,
    configurable: false,
  },
  asyncDispose: {
    value: SymbolAsyncDispose,
    enumerable: false,
    writable: false,
    configurable: false,
  },
});

let windowIsClosing = false;
let globalThis_;

function windowClose() {
  if (!windowIsClosing) {
    windowIsClosing = true;
    // Push a macrotask to exit after a promise resolve.
    // This is not perfect, but should be fine for first pass.
    PromisePrototypeThen(
      PromiseResolve(),
      () =>
        FunctionPrototypeCall(timers.setTimeout, null, () => {
          // This should be fine, since only Window/MainWorker has .close()
          os.exit(0);
        }, 0),
    );
  }
}


async function pollForMessages() {
   // Do nothing since web workers are not enabled  
}

let loadedMainWorkerScript = false;

function importScripts(...urls) {
  if (op_worker_get_type() === "module") {
    throw new TypeError("Can't import scripts in a module worker.");
  }

  const baseUrl = location.getLocationHref();
  const parsedUrls = ArrayPrototypeMap(urls, (scriptUrl) => {
    try {
      return new url.URL(scriptUrl, baseUrl ?? undefined).href;
    } catch {
      throw new DOMException(
        "Failed to parse URL.",
        "SyntaxError",
      );
    }
  });

  // A classic worker's main script has looser MIME type checks than any
  // imported scripts, so we use `loadedMainWorkerScript` to distinguish them.
  // TODO(andreubotella) Refactor worker creation so the main script isn't
  // loaded with `importScripts()`.
  const scripts = op_worker_sync_fetch(
    parsedUrls,
    !loadedMainWorkerScript,
  );
  loadedMainWorkerScript = true;

  for (let i = 0; i < scripts.length; ++i) {
    const { url, script } = scripts[i];
    const err = core.evalContext(script, url)[1];
    if (err !== null) {
      throw err.thrown;
    }
  }
}

function opMainModule() {
  return op_main_module();
}

const opArgs = memoizeLazy(() => op_bootstrap_args());
const opPid = memoizeLazy(() => op_bootstrap_pid());
const opPpid = memoizeLazy(() => op_ppid());
setNoColorFn(() => op_bootstrap_no_color() || !op_bootstrap_is_tty());

function formatException(error) {
  if (
    isNativeError(error) ||
    ObjectPrototypeIsPrototypeOf(ErrorPrototype, error)
  ) {
    return null;
  } else if (typeof error == "string") {
    return `Uncaught ${
      inspectArgs([quoteString(error, getDefaultInspectOptions())], {
        colors: !getNoColor(),
      })
    }`;
  } else {
    return `Uncaught ${inspectArgs([error], { colors: !getNoColor() })}`;
  }
}

core.registerErrorClass("NotFound", errors.NotFound);
core.registerErrorClass("PermissionDenied", errors.PermissionDenied);
core.registerErrorClass("ConnectionRefused", errors.ConnectionRefused);
core.registerErrorClass("ConnectionReset", errors.ConnectionReset);
core.registerErrorClass("ConnectionAborted", errors.ConnectionAborted);
core.registerErrorClass("NotConnected", errors.NotConnected);
core.registerErrorClass("AddrInUse", errors.AddrInUse);
core.registerErrorClass("AddrNotAvailable", errors.AddrNotAvailable);
core.registerErrorClass("BrokenPipe", errors.BrokenPipe);
core.registerErrorClass("AlreadyExists", errors.AlreadyExists);
core.registerErrorClass("InvalidData", errors.InvalidData);
core.registerErrorClass("TimedOut", errors.TimedOut);
core.registerErrorClass("WouldBlock", errors.WouldBlock);
core.registerErrorClass("WriteZero", errors.WriteZero);
core.registerErrorClass("UnexpectedEof", errors.UnexpectedEof);
core.registerErrorClass("Http", errors.Http);
core.registerErrorClass("Busy", errors.Busy);
core.registerErrorClass("NotSupported", errors.NotSupported);
core.registerErrorClass("FilesystemLoop", errors.FilesystemLoop);
core.registerErrorClass("IsADirectory", errors.IsADirectory);
core.registerErrorClass("NetworkUnreachable", errors.NetworkUnreachable);
core.registerErrorClass("NotADirectory", errors.NotADirectory);
core.registerErrorBuilder(
  "DOMExceptionOperationError",
  function DOMExceptionOperationError(msg) {
    return new DOMException(msg, "OperationError");
  },
);
core.registerErrorBuilder(
  "DOMExceptionQuotaExceededError",
  function DOMExceptionQuotaExceededError(msg) {
    return new DOMException(msg, "QuotaExceededError");
  },
);
core.registerErrorBuilder(
  "DOMExceptionNotSupportedError",
  function DOMExceptionNotSupportedError(msg) {
    return new DOMException(msg, "NotSupported");
  },
);
core.registerErrorBuilder(
  "DOMExceptionNetworkError",
  function DOMExceptionNetworkError(msg) {
    return new DOMException(msg, "NetworkError");
  },
);
core.registerErrorBuilder(
  "DOMExceptionAbortError",
  function DOMExceptionAbortError(msg) {
    return new DOMException(msg, "AbortError");
  },
);
core.registerErrorBuilder(
  "DOMExceptionInvalidCharacterError",
  function DOMExceptionInvalidCharacterError(msg) {
    return new DOMException(msg, "InvalidCharacterError");
  },
);
core.registerErrorBuilder(
  "DOMExceptionDataError",
  function DOMExceptionDataError(msg) {
    return new DOMException(msg, "DataError");
  },
);

function runtimeStart(runtimeOptions, source) {
  core.setReportExceptionCallback(event.reportException);
  op_set_format_exception_callback(formatException);
  version.setVersions(
    denoVersion,
    v8Version,
    tsVersion,
  );
  core.setBuildInfo(target); 
  Error.prepareStackTrace = core.prepareStackTrace;
}

core.setUnhandledPromiseRejectionHandler(processUnhandledPromiseRejection);
core.setHandledPromiseRejectionHandler(processRejectionHandled);
// Notification that the core received an unhandled promise rejection that is about to
// terminate the runtime. If we can handle it, attempt to do so.
function processUnhandledPromiseRejection(promise, reason) {
  const rejectionEvent = new event.PromiseRejectionEvent(
    "unhandledrejection",
    {
      cancelable: true,
      promise,
      reason,
    },
  );

  // Note that the handler may throw, causing a recursive "error" event
  globalThis_.dispatchEvent(rejectionEvent);

  // If event was not yet prevented, try handing it off to Node compat layer
  // (if it was initialized)
  if (
    !rejectionEvent.defaultPrevented &&
    typeof internals.nodeProcessUnhandledRejectionCallback !== "undefined"
  ) {
    internals.nodeProcessUnhandledRejectionCallback(rejectionEvent);
  }

  // If event was not prevented (or "unhandledrejection" listeners didn't
  // throw) we will let Rust side handle it.
  if (rejectionEvent.defaultPrevented) {
    return true;
  }

  return false;
}

let hasBootstrapped = false;
function processRejectionHandled(promise, reason) {
  const rejectionHandledEvent = new event.PromiseRejectionEvent(
    "rejectionhandled",
    { promise, reason },
  );

  // Note that the handler may throw, causing a recursive "error" event
  globalThis_.dispatchEvent(rejectionHandledEvent);

  if (typeof internals.nodeProcessRejectionHandledCallback !== "undefined") {
    internals.nodeProcessRejectionHandledCallback(rejectionHandledEvent);
  }
}

// Set up global properties shared by main and worker runtime.
ObjectDefineProperties(globalThis, windowOrWorkerGlobalScope);

// NOTE(bartlomieju): remove all the ops that have already been imported using
// "virtual op module" (`ext:core/ops`).
const NOT_IMPORTED_OPS = [
  // Related to `Deno.bench()` API
  "op_bench_now",
  "op_dispatch_bench_event",
  "op_register_bench",

  // Related to `Deno.jupyter` API
  "op_jupyter_broadcast",

  // Related to `Deno.test()` API
  "op_test_event_step_result_failed",
  "op_test_event_step_result_ignored",
  "op_test_event_step_result_ok",
  "op_test_event_step_wait",
  "op_test_op_sanitizer_collect",
  "op_test_op_sanitizer_finish",
  "op_test_op_sanitizer_get_async_message",
  "op_test_op_sanitizer_report",
  "op_restore_test_permissions",
  "op_register_test_step",
  "op_register_test",
  "op_pledge_test_permissions",

  // TODO(bartlomieju): used in various integration tests - figure out a way
  // to not depend on them.
  "op_set_exit_code",
  "op_napi_open",
  "op_npm_process_state",
];

function removeImportedOps() {
  const allOpNames = ObjectKeys(ops);
  for (let i = 0; i < allOpNames.length; i++) {
    const opName = allOpNames[i];
    if (!ArrayPrototypeIncludes(NOT_IMPORTED_OPS, opName)) {
      delete ops[opName];
    }
  }
}


// FIXME(bartlomieju): temporarily add whole `Deno.core` to
// `Deno[Deno.internal]` namespace. It should be removed and only necessary
// methods should be left there.
ObjectAssign(internals, { core });
const internalSymbol = Symbol("Deno.internal");

const finalDenoNs = {
  core,
  internal: internalSymbol,
  [internalSymbol]: internals,
  resources: core.resources,
  close: core.close,
  ...denoNs,
};

const {
  denoVersion,
  tsVersion,
  v8Version,
  target,
} = op_snapshot_options();

function bootstrapMainRuntime(runtimeOptions) {
  if (hasBootstrapped) {
    throw new Error("Worker runtime already bootstrapped");
  }
  const nodeBootstrap = globalThis.nodeBootstrap;

  removeImportedOps();

  performance.setTimeOrigin(DateNow());
  globalThis_ = globalThis;
  const hasNodeModulesDir = runtimeOptions.hasNodeModulesDir;
  const maybeBinaryNpmCommandName = runtimeOptions.maybeBinaryNpmCommandName;

  // Remove bootstrapping data from the global scope
  delete globalThis.__bootstrap;
  delete globalThis.bootstrap;
  delete globalThis.nodeBootstrap;
  hasBootstrapped = true;

  // If the `--location` flag isn't set, make `globalThis.location` `undefined` and
  // writable, so that they can mock it themselves if they like. If the flag was
  // set, define `globalThis.location`, using the provided value.
  if (runtimeOptions.location == null) {
    mainRuntimeGlobalProperties.location = {
      writable: true,
    };
  } else {
    location.setLocationHref(runtimeOptions.location);
  }

  if (runtimeOptions.unstableFlag) {
    ObjectDefineProperties(globalThis, unstableWindowOrWorkerGlobalScope);
  }
  ObjectDefineProperties(globalThis, mainRuntimeGlobalProperties);
  ObjectDefineProperties(globalThis, {
    // TODO(bartlomieju): in the future we might want to change the
    // behavior of setting `name` to actually update the process name.
    // Empty string matches what browsers do.
    name: core.propWritable(""),
    close: core.propWritable(windowClose),
    closed: core.propGetterOnly(() => windowIsClosing),
  });
  ObjectSetPrototypeOf(globalThis, Window.prototype);

  if (runtimeOptions.inspectFlag) {
    const consoleFromDeno = globalThis.console;
    core.wrapConsole(consoleFromDeno, core.v8Console);
  }

  event.setEventTargetData(globalThis);
  event.saveGlobalThisReference(globalThis);

  event.defineEventHandler(globalThis, "error");
  event.defineEventHandler(globalThis, "load");
  event.defineEventHandler(globalThis, "beforeunload");
  event.defineEventHandler(globalThis, "unload");
  event.defineEventHandler(globalThis, "unhandledrejection");

  runtimeStart(runtimeOptions);

  // These have to initialized here and not in `90_deno_ns.js` because
  // the op function that needs to be passed will be invalidated by creating
  // a snapshot
  ObjectAssign(internals, {
    nodeUnstable: {
      listenDatagram: net.createListenDatagram(
        ops.op_node_unstable_net_listen_udp,
        ops.op_node_unstable_net_listen_unixpacket,
      ),
    },
  });

  ObjectDefineProperties(finalDenoNs, {
    pid: util.getterOnly(opPid),
    ppid: util.getterOnly(opPpid),
    noColor: util.getterOnly(() => ops.op_bootstrap_no_color()),
    args: util.getterOnly(opArgs),
    mainModule: util.getterOnly(opMainModule),
  });

  if (runtimeOptions.unstableFlag) {
    ObjectAssign(finalDenoNs, denoNsUnstable);
    // These have to initialized here and not in `90_deno_ns.js` because
    // the op function that needs to be passed will be invalidated by creating
    // a snapshot
    ObjectAssign(finalDenoNs, {
      listenDatagram: net.createListenDatagram(
        ops.op_net_listen_udp,
        ops.op_net_listen_unixpacket,
      ),
    });
  }

  // Removes the `__proto__` for security reasons.
  // https://tc39.es/ecma262/#sec-get-object.prototype.__proto__
  delete Object.prototype.__proto__;

  // Setup `Deno` global - we're actually overriding already existing global
  // `Deno` with `Deno` namespace from "./deno.ts".
  ObjectDefineProperty(globalThis, "Deno", util.readOnly(finalDenoNs));

  util.log("args", runtimeOptions.args);
  
  if (nodeBootstrap) {
    nodeBootstrap(hasNodeModulesDir, maybeBinaryNpmCommandName);
  }
}

function bootstrapWorkerRuntime(
  runtimeOptions,
  name,
  internalName,
) {
  if (hasBootstrapped) {
    throw new Error("Worker runtime already bootstrapped");
  }

  performance.setTimeOrigin(DateNow());
  globalThis_ = globalThis;

  // Remove bootstrapping data from the global scope
  delete globalThis.__bootstrap;
  delete globalThis.bootstrap;
  hasBootstrapped = true;

  if (runtimeOptions.unstableFlag) {
    ObjectDefineProperties(globalThis, unstableWindowOrWorkerGlobalScope);
  }
  ObjectDefineProperties(globalThis, workerRuntimeGlobalProperties);
  ObjectDefineProperties(globalThis, {
    name: util.writable(name), 
  });
  if (runtimeOptions.enableTestingFeaturesFlag) {
    ObjectDefineProperty(
      globalThis,
      "importScripts",
      util.writable(importScripts),
    );
  }
  ObjectSetPrototypeOf(globalThis, DedicatedWorkerGlobalScope.prototype);

  const consoleFromDeno = globalThis.console;
  core.wrapConsole(consoleFromDeno, core.v8Console);

  event.setEventTargetData(globalThis);
  event.saveGlobalThisReference(globalThis);

  event.defineEventHandler(self, "message");
  event.defineEventHandler(self, "error", undefined, true);
  event.defineEventHandler(self, "unhandledrejection");


  // `Deno.exit()` is an alias to `self.close()`. Setting and exit
  // code using an op in worker context is a no-op.
  os.setExitHandler((_exitCode) => {
    workerClose();
  });

  runtimeStart(
    runtimeOptions,
    internalName ?? name,
  );

  location.setLocationHref(runtimeOptions.location);

  setNumCpus(runtimeOptions.cpuCount);
  setLanguage(runtimeOptions.locale);

  globalThis.pollForMessages = pollForMessages;

  // These have to initialized here and not in `90_deno_ns.js` because
  // the op function that needs to be passed will be invalidated by creating
  // a snapshot
  ObjectAssign(internals, {
    nodeUnstable: {
      listenDatagram: net.createListenDatagram(
        ops.op_node_unstable_net_listen_udp,
        ops.op_node_unstable_net_listen_unixpacket,
      ),
    },
  });

  // FIXME(bartlomieju): temporarily add whole `Deno.core` to
  // `Deno[Deno.internal]` namespace. It should be removed and only necessary
  // methods should be left there.
  ObjectAssign(internals, {
    core,
  });

  if (runtimeOptions.unstableFlag) {
    ObjectAssign(finalDenoNs, denoNsUnstable);
    // These have to initialized here and not in `90_deno_ns.js` because
    // the op function that needs to be passed will be invalidated by creating
    // a snapshot
    ObjectAssign(finalDenoNs, {
      listenDatagram: net.createListenDatagram(
        ops.op_net_listen_udp,
        ops.op_net_listen_unixpacket,
      ),
    });
  }
  ObjectDefineProperties(finalDenoNs, {
    pid: util.readOnly(runtimeOptions.pid),
    noColor: util.readOnly(runtimeOptions.noColor),
    args: util.readOnly(ObjectFreeze(runtimeOptions.args)),
  });
  // Setup `Deno` global - we're actually overriding already
  // existing global `Deno` with `Deno` namespace from "./deno.ts".
  ObjectDefineProperty(globalThis, "Deno", util.readOnly(finalDenoNs));
}

globalThis.bootstrap = {
  mainRuntime: bootstrapMainRuntime,
  workerRuntime: bootstrapWorkerRuntime,
};
