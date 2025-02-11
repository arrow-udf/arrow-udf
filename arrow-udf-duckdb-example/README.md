# DuckDB Extension using Arrow UDFs

This is an example of a DuckDB extension that uses arrow-udf to register a UDF for DuckDB.

```sh
$ make configure
$ make release

$ duckdb -unsigned
D load 'build//release/arrow_udf_duckdb_example.duckdb_extension';
D select hello('🦀');
```
