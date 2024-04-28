# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Add support for `int8`, `uint8`, `uint16`, `uint32`, `uint64`, `large_string`, and `large_binary` type.
- Support listing functions.

### Breaking Changes

- `json` and `decimal` type are no longer mapped to `LargeString` and `LargeBinary` respectively. They are now mapped to [extension types](https://arrow.apache.org/docs/format/Columnar.html#format-metadata-extension-types) with `String` as the storage type.
    - `json`: `ARROW:extension:name` = `arrorudf.json`
    - `decimal`: `ARROW:extension:name` = `arrowudf.decimal`

## [0.1.3] - 2023-12-06

### Fixed

- Fix decimal type output.

## [0.1.2] - 2023-12-04

### Fixed

- Fix index-out-of-bound error when string or string list is large.
- Fix memory leak.

## [0.1.1] - 2023-12-03

### Added

- Support struct in struct and struct[] in struct.

### Changed

- Bump Arrow version to 14.

### Fixed

- Fix unconstrained decimal type.

## [0.1.0] - 2023-09-01

- Initial release.