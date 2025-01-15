# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2] - todo

### Fixed

- Fix results out-of-order issue when `io_threads` is set larger than `1`. This is a CRITICAL fix. We highly encourage every user to upgrade.

## [0.2.1] - 2024-05-07

### Fixed

- Fix list, struct and null issues.

## [0.2.0] - 2024-05-07

### Added

- Add support for `null`, `int8`, `uint8`, `uint16`, `uint32`, `uint64`, `large_string`, and `large_binary` type.
- Support listing functions.

### Breaking Changes

- `json` and `decimal` type are no longer mapped to `LargeString` and `LargeBinary` respectively. They are now mapped to [extension types](https://arrow.apache.org/docs/format/Columnar.html#format-metadata-extension-types) with `String` as the storage type.
    - `json`: `ARROW:extension:name` = `arrorudf.json`
    - `decimal`: `ARROW:extension:name` = `arrowudf.decimal`

## [0.1.1] - 2023-12-06

### Fixed

- Fix decimal type output.

## [0.1.0] - 2023-12-01

### Fixed

- Fix unconstrained decimal type.

## [0.0.12] - 2023-11-28

### Changed

- Change the default struct field name to `f{i}`.

### Fixed

- Fix parsing nested struct type.


## [0.0.11] - 2023-11-06

### Fixed

- Hook SIGTERM to stop the UDF server gracefully.
