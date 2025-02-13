# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.1] - 2025-02-13

### Added

- Support generating DuckDB scalar functions.

### Fixed

- Fix `global_registry` feature.

## [0.5.0] - 2024-12-24

### Changed

- Replace unmaintained `genawaiter` with forked version `genawaiter2`, for `arrow-udf` 0.5.2.

## [0.4.1] - 2024-10-08

### Fixed

- Fix complilation of Rust UDF using primitive array data types.
