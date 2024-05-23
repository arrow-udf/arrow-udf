# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2024-05-23

### Added

- Add methods to support aggregate functions.
    - `add_aggregate`
    - `create_state`
    - `accumulate`
    - `accumulate_or_retract`
    - `merge`
    - `finish`

- Add extension type `arrowudf.pickle`.

### Fixed

- Fix the abort issue when using decimal for the second time.

## [0.1.0] - 2024-04-25

### Added

- Initial release. Support basic scalar functions and table functions.
