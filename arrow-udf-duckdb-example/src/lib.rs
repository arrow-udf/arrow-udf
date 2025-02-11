use arrow_udf::function;
use duckdb::{
    arrow::{
        array::{Array, RecordBatch},
        datatypes::DataType,
    },
    vscalar::arrow::{ArrowFunctionSignature, VArrowScalar},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use std::{error::Error, sync::Arc};

#[function("hello(varchar) -> varchar", output = "eval_hello")]
fn hello(name: &str) -> String {
    format!("Hello {}!", name)
}

struct HelloScalarArrow {}

impl VArrowScalar for HelloScalarArrow {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: RecordBatch,
    ) -> Result<Arc<dyn Array>, Box<dyn std::error::Error>> {
        let batch = eval_hello(&input)?;
        Ok(batch.column(0).clone())
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Utf8,
        )]
    }
}

#[duckdb_entrypoint_c_api(ext_name = "arrow_udf_duckdb_example", min_duckdb_version = "v1.2.0")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<HelloScalarArrow>("hello")
        .expect("Failed to register hello function");
    Ok(())
}
