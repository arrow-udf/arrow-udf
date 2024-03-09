// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

import { core } from "ext:core/mod.js";

import * as event from "ext:deno_web/02_event.js";
import * as timers from "ext:deno_web/02_timers.js";
import * as base64 from "ext:deno_web/05_base64.js";
import * as encoding from "ext:deno_web/08_text_encoding.js";
import * as console from "ext:deno_console/01_console.js";
import * as compression from "ext:deno_web/14_compression.js";
import * as performance from "ext:deno_web/15_performance.js";
import * as streams from "ext:deno_web/06_streams.js";
import * as crypto from "ext:deno_crypto/00_crypto.js";
import * as url from "ext:deno_url/00_url.js";
import * as urlPattern from "ext:deno_url/01_urlpattern.js";
import * as fileReader from "ext:deno_web/10_filereader.js";
import * as messagePort from "ext:deno_web/13_message_port.js";
import * as file from "ext:deno_web/09_file.js";
import * as webidl from "ext:deno_webidl/00_webidl.js";
import { DOMException } from "ext:deno_web/01_dom_exception.js";
import * as abortSignal from "ext:deno_web/03_abort_signal.js";
import * as imageData from "ext:deno_web/16_image_data.js";

// https://developer.mozilla.org/en-US/docs/Web/API/WindowOrWorkerGlobalScope
const windowOrWorkerGlobalScope = {
  AbortController: core.propNonEnumerable(abortSignal.AbortController),
  AbortSignal: core.propNonEnumerable(abortSignal.AbortSignal),
  ByteLengthQueuingStrategy: core.propNonEnumerable(
    streams.ByteLengthQueuingStrategy,
  ),
  CloseEvent: core.propNonEnumerable(event.CloseEvent),
  CompressionStream: core.propNonEnumerable(compression.CompressionStream),
  CountQueuingStrategy: core.propNonEnumerable(
    streams.CountQueuingStrategy,
  ),
  CryptoKey: core.propNonEnumerable(crypto.CryptoKey),
  CustomEvent: core.propNonEnumerable(event.CustomEvent),
  DecompressionStream: core.propNonEnumerable(compression.DecompressionStream),
  DOMException: core.propNonEnumerable(DOMException),
  Performance: core.propNonEnumerable(performance.Performance),
  PerformanceEntry: core.propNonEnumerable(performance.PerformanceEntry),
  PerformanceMark: core.propNonEnumerable(performance.PerformanceMark),
  PerformanceMeasure: core.propNonEnumerable(performance.PerformanceMeasure),
  PromiseRejectionEvent: core.propNonEnumerable(event.PromiseRejectionEvent),
  ProgressEvent: core.propNonEnumerable(event.ProgressEvent),
  ReadableStream: core.propNonEnumerable(streams.ReadableStream),
  ReadableStreamDefaultReader: core.propNonEnumerable(
    streams.ReadableStreamDefaultReader,
  ),
  TextDecoder: core.propNonEnumerable(encoding.TextDecoder),
  TextEncoder: core.propNonEnumerable(encoding.TextEncoder),
  TextDecoderStream: core.propNonEnumerable(encoding.TextDecoderStream),
  TextEncoderStream: core.propNonEnumerable(encoding.TextEncoderStream),
  TransformStream: core.propNonEnumerable(streams.TransformStream),
  URL: core.propNonEnumerable(url.URL),
  URLPattern: core.propNonEnumerable(urlPattern.URLPattern),
  URLSearchParams: core.propNonEnumerable(url.URLSearchParams),
  atob: core.propWritable(base64.atob),
  btoa: core.propWritable(base64.btoa),
  
  clearInterval: core.propWritable(timers.clearInterval),
  clearTimeout: core.propWritable(timers.clearTimeout),
  console: core.propNonEnumerable(
    new console.Console((msg, level) => core.print(msg, level > 1)),
  ),
  crypto: core.propReadOnly(crypto.crypto),
  Crypto: core.propNonEnumerable(crypto.Crypto),
  SubtleCrypto: core.propNonEnumerable(crypto.SubtleCrypto),
  performance: core.propWritable(performance.performance),
  setInterval: core.propWritable(timers.setInterval),
  setTimeout: core.propWritable(timers.setTimeout),
  structuredClone: core.propWritable(messagePort.structuredClone),
  // Branding as a WebIDL object
  [webidl.brand]: core.propNonEnumerable(webidl.brand),
};

const unstableForWindowOrWorkerGlobalScope = {};

export { unstableForWindowOrWorkerGlobalScope, windowOrWorkerGlobalScope };
