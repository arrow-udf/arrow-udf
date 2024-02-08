# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.4] - 2024-02-08

### Added

- Support arrow-udf v0.2.
- Allow specifying the Rust toolchain to build the wasm binary.

## [0.1.3] - 2024-02-04

### Fixed

- Force to build with stable toolchain.

## [0.1.2] - 2024-02-04

### Added

- Add `build_with` and `BuildOpts` to allow building in offline mode.
- Automatically install `wasm32-wasi` target when building without offline.

## [0.1.1] - 2024-01-31

### Added

- Add `build` feature to build the wasm binary from source.

### Changed

- Update `wasmtime` to v17.

## [0.1.0] - 2024-01-13

### Added

- Initial release. Support basic scalar functions and table functions.
