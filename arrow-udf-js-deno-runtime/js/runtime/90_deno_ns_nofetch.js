// Copyright 2018-2023 the Deno authors. All rights reserved. MIT license.

const core = globalThis.Deno.core;
const ops = core.ops;
import * as timers from "ext:deno_web/02_timers.js";
import * as console from "ext:deno_console/01_console.js";
import * as net from "ext:deno_net/01_net.js";
import * as tls from "ext:deno_net/02_tls.js";
import * as errors from "ext:runtime/01_errors.js";
import * as version from "ext:runtime/01_version.ts";
import * as permissions from "ext:runtime/10_permissions.js";
import * as io from "ext:deno_io/12_io.js";
import * as buffer from "ext:runtime/13_buffer.js";
import * as os from "ext:runtime/30_os.js";

const denoNs = {
  metrics: core.metrics,
  memoryUsage: () => ops.op_runtime_memory_usage(),
  version: version.version,
  build: core.build,
  errors: errors.errors,
  // TODO(kt3k): Remove this export at v2
  // See https://github.com/denoland/deno/issues/9294
  customInspect: console.customInspect,
  inspect: console.inspect,
  env: os.env,
  exit: os.exit,
  execPath: os.execPath,
  Buffer: buffer.Buffer,
  readAll: buffer.readAll,
  readAllSync: buffer.readAllSync,
  writeAll: buffer.writeAll,
  writeAllSync: buffer.writeAllSync,
  copy: io.copy,
  iter: io.iter,
  iterSync: io.iterSync,
  SeekMode: io.SeekMode,
  read: io.read,
  readSync: io.readSync,
  write: io.write,
  writeSync: io.writeSync,
  connect: net.connect,
  listen: net.listen,
  loadavg: os.loadavg,
  connectTls: tls.connectTls,
  listenTls: tls.listenTls,
  startTls: tls.startTls,
  shutdown: net.shutdown,
  permissions: permissions.permissions,
  Permissions: permissions.Permissions,
  PermissionStatus: permissions.PermissionStatus,
  resolveDns: net.resolveDns,
  refTimer: timers.refTimer,
  unrefTimer: timers.unrefTimer,
  osRelease: os.osRelease,
  osUptime: os.osUptime,
  hostname: os.hostname,
  systemMemoryInfo: os.systemMemoryInfo,
  networkInterfaces: os.networkInterfaces,
  gid: os.gid,
  uid: os.uid,
};

const denoNsUnstable = {
  listenDatagram: net.listenDatagram,
};

export { denoNs, denoNsUnstable };
