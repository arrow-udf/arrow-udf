# Remote UDF based on Arrow Flight

[![Crate](https://img.shields.io/crates/v/arrow-udf-flight.svg)](https://crates.io/crates/arrow-udf-flight)
[![Docs](https://docs.rs/arrow-udf-flight/badge.svg)](https://docs.rs/arrow-udf-flight)

Run user-defined functions in a separate process and call them via [Arrow Flight RPC].

[Arrow Flight RPC]: https://arrow.apache.org/docs/format/Flight.html

## Server

Currently the following languages are supported:

- [Python](https://github.com/risingwavelabs/arrow-udf/tree/main/arrow-udf-flight/python)
- [Java](https://github.com/risingwavelabs/arrow-udf/tree/main/arrow-udf-flight/java)

Please click the link to see the specific usage.

## Client

Add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf-flight = "0.4"
```

```rust,ignore
use arrow_udf_flight::Client;

// Connect to the UDF server
let client = Client::new("localhost:8815").await.unwrap();

// Call functions
let input: RecordBatch = ...;
let output: RecordBatch = client.call("gcd", &input).await.unwrap();
```

## Communication Protocol

The communication protocol between client and server is based on Arrow Flight RPC. 

Details to be added.
