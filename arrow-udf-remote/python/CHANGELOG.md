# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1] - 2025-05-09

### Fixed

- Lock `pyarrow` version to `19` to avoid the `maps_as_pydicts` bug ([#129](https://github.com/arrow-udf/arrow-udf/issues/129)).

## [0.3.0] - 2025-02-12

### Added

- Add `batch` keyword parameter to the `udf` decorator. When it is set to `True`, the UDF will receive a batch of input instead of just one. Example:
    ```py
    @udf(input_types=["string"], result_type="float32[]", batch=True)
    def text_embedding(texts: List[str]) -> List[List[float]]:
        embeddings = [
            e.embedding
            for e in openai.embeddings.create(
                model="text-embedding-ada-002",
                input=texts,
                encoding_format="float",
            ).data
        ]
        return embeddings
    ```

## [0.2.2] - 2025-01-15

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
