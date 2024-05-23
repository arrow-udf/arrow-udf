# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2024-05-23

### Added

- Add `Runtime::{set_memory_limit, set_timeout}` to limit memory usage and execution time.
- Add methods to support aggregate functions.
    - `add_aggregate`
    - `create_state`
    - `accumulate`
    - `accumulate_or_retract`
    - `merge`
    - `finish`

### Changed

- Update rquickjs to v0.6.2.

## [0.2.0] - 2024-04-25

### Breaking Changes

- `json` and `decimal` type are no longer mapped to `LargeString` and `LargeBinary` respectively. They are now mapped to [extension types](https://arrow.apache.org/docs/format/Columnar.html#format-metadata-extension-types) with `String` as the storage type.
    - `json`: `ARROW:extension:name` = `arrorudf.json`
    - `decimal`: `ARROW:extension:name` = `arrowudf.decimal`

### Added

- Add support for `LargeString` and `LargeBinary` type.

### Changed

- Update `arrow` version to >=50.

## [0.1.2] - 2024-03-04

### Fixed

- Make `Runtime` Send and Sync again.

## [0.1.1] - 2024-02-19

### Changed

- Improve performance of decimal inputs.
- Update rquickjs to v0.5.0.

## [0.1.0] - 2024-01-31

### Added

- Initial release. Support basic scalar functions and table functions.
