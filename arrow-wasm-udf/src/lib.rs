pub use arrow::error::{ArrowError as Error, Result};
use arrow::{array::ArrayRef, datatypes::DataType, record_batch::RecordBatch};
pub use arrow_wasm_udf_macros::function;

/// A function signature.
pub struct FunctionSignature {
    /// The name of the function.
    pub name: String,

    /// The argument types.
    pub inputs_type: Vec<SigDataType>,

    /// Whether the function is variadic.
    pub variadic: bool,

    /// The return type.
    pub return_type: SigDataType,

    /// The function
    pub function: BoxScalarFunction,
}

/// An extended data type that can be used to declare a function's argument or result type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SigDataType {
    /// Exact data type
    Exact(DataType),
    /// Accepts any data type
    Any,
}

pub trait ScalarFunction {
    fn eval(&self, input: &RecordBatch) -> Result<ArrayRef>;
}

pub type BoxScalarFunction = Box<dyn ScalarFunction>;

pub mod codegen {
    pub use arrow;
    pub use itertools;
}
