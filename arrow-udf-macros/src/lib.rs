// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use syn::{Error, Result};

mod gen;
mod parse;
mod types;
mod utils;

/// Defining a function on Arrow arrays.
///
/// # Table of Contents
///
/// - [SQL Function Signature](#sql-function-signature)
///     - [Multiple Function Definitions](#multiple-function-definitions)
/// - [Rust Function Signature](#rust-function-signature)
///     - [Nullable Arguments](#nullable-arguments)
///     - [Return Value](#return-value)
///     - [Optimization](#optimization)
///     - [Functions Returning Strings](#functions-returning-strings)
/// - [Table Function](#table-function)
/// - [Registration and Invocation](#registration-and-invocation)
/// - [Appendix: Type Matrix](#appendix-type-matrix)
///
/// The following example demonstrates a simple usage:
///
/// ```ignore
/// #[function("add(int, int) -> int")]
/// fn add(x: i32, y: i32) -> i32 {
///     x + y
/// }
/// ```
///
/// # SQL Function Signature
///
/// Each function must have a signature, specified in the `function("...")` part of the macro
/// invocation. The signature follows this pattern:
///
/// ```text
/// name ( [arg_types],* [...] ) [ -> [setof] return_type ]
/// ```
///
/// Where `name` is the function name.
///
/// `arg_types` is a comma-separated list of argument types. The allowed data types are listed in
/// in the `name` column of the appendix's [type matrix]. Wildcards or `auto` can also be used, as
/// explained below. If the function is variadic, the last argument can be denoted as `...`.
///
/// When `setof` appears before the return type, this indicates that the function is a set-returning
/// function (table function), meaning it can return multiple values instead of just one. For more
/// details, see the section on table functions.
///
/// If no return type is specified, the function returns `void`.
///
/// ## Multiple Function Definitions
///
/// Multiple `#[function]` macros can be applied to a single generic Rust function to define
/// multiple SQL functions of different types. For example:
///
/// ```ignore
/// #[function("add(int2, int2) -> int2")]
/// #[function("add(int4, int4) -> int4")]
/// #[function("add(int8, int8) -> int8")]
/// fn add<T: Add>(x: T, y: T) -> T {
///     x + y
/// }
/// ```
///
/// # Rust Function Signature
///
/// The `#[function]` macro can handle various types of Rust functions.
/// Each argument corresponds to the *Rust type* `T` in the [type matrix].
/// The return value type can be any type that implements `AsRef<T>`.
///
/// ## Nullable Arguments
///
/// The functions above will only be called when all arguments are not null. If null arguments need
/// to be considered, the `Option` type can be used:
///
/// ```ignore
/// #[function("add(int, int) -> int")]
/// fn add(x: Option<i32>, y: i32) -> i32 {...}
/// ```
///
/// ## Return Value
///
/// Similarly, the return value type can be one of the following:
///
/// - `T`: Indicates that a non-null value is always returned, and errors will not occur.
/// - `Option<T>`: Indicates that a null value may be returned, but errors will not occur.
/// - `Result<T>`: Indicates that an error may occur, but a null value will not be returned.
/// - `Result<Option<T>>`: Indicates that a null value may be returned, and an error may also occur.
///
/// ## Optimization
///
/// When all input and output types of the function are *primitive type* (refer to the [type
/// matrix]) and do not contain any Option or Result, the `#[function]` macro will automatically
/// generate SIMD vectorized execution code.
///
/// Therefore, try to avoid returning `Option` and `Result` whenever possible.
///
/// ## Functions Returning Strings
///
/// For functions that return varchar types, you can also use the writer style function signature to
/// avoid memory copying and dynamic memory allocation:
///
/// ```ignore
/// #[function("trim(varchar) -> varchar")]
/// fn trim(s: &str, writer: &mut impl Write) {
///     writer.write_str(s.trim()).unwrap();
/// }
/// ```
///
/// If errors may be returned, then the return value should be `Result<()>`:
///
/// ```ignore
/// #[function("trim(varchar) -> varchar")]
/// fn trim(s: &str, writer: &mut impl Write) -> Result<()> {
///     writer.write_str(s.trim()).unwrap();
///     Ok(())
/// }
/// ```
///
/// If null values may be returned, then the return value should be `Option<()>`:
///
/// ```ignore
/// #[function("trim(varchar) -> varchar")]
/// fn trim(s: &str, writer: &mut impl Write) -> Option<()> {
///     if s.is_empty() {
///         None
///     } else {
///         writer.write_str(s.trim()).unwrap();
///         Some(())
///     }
/// }
/// ```
///
/// # Registration and Invocation
///
/// Every function defined by `#[function]` is automatically registered in the global function registry.
///
/// You can lookup the function by name and types:
///
/// ```ignore
/// use arrow_udf::sig::REGISTRY;
/// use arrow_schema::DataType::Int32;
///
/// let sig = REGISTRY.get("add", &[Int32, Int32], &Int32).unwrap();
/// ```
///
/// # Appendix: Type Matrix
///
/// ## Base Types
///
/// | SQL type           | Rust type                      | primitive? |
/// | ------------------ | ------------------------------ | ---------- |
/// | `boolean`          | `bool`                         | no         |
/// | `smallint`         | `i16`                          | yes        |
/// | `integer`          | `i32`                          | yes        |
/// | `bigint`           | `i64`                          | yes        |
/// | `real`             | `f32`                          | yes        |
/// | `double precision` | `f64`                          | yes        |
/// | `numeric`          | [`rust_decimal::Decimal`]      | yes        |
/// | `date`             | [`chrono::NaiveDate`]          | yes        |
/// | `time`             | [`chrono::NaiveTime`]          | yes        |
/// | `timestamp`        | [`chrono::NaiveDateTime`]      | yes        |
/// | `interval`         | [`arrow_udf::types::Interval`] | yes        |
/// | `json`             | [`serde_json::Value`]          | no         |
/// | `varchar`          | `&str`                         | no         |
/// | `bytea`            | `&[u8]`                        | no         |
///
/// [type matrix]: #appendix-type-matrix
/// [`rust_decimal::Decimal`]: https://docs.rs/rust_decimal/1.33.1/rust_decimal/struct.Decimal.html
/// [`chrono::NaiveDate`]: https://docs.rs/chrono/0.4.31/chrono/naive/struct.NaiveDate.html
/// [`chrono::NaiveTime`]: https://docs.rs/chrono/0.4.31/chrono/naive/struct.NaiveTime.html
/// [`chrono::NaiveDateTime`]: https://docs.rs/chrono/0.4.31/chrono/naive/struct.NaiveDateTime.html
/// [`arrow_udf::types::Interval`]: https://docs.rs/arrow_udf/0.1.0/arrow_udf/types/struct.Interval.html
/// [`serde_json::Value`]: https://docs.rs/serde_json/1.0.108/serde_json/enum.Value.html
#[proc_macro_attribute]
pub fn function(attr: TokenStream, item: TokenStream) -> TokenStream {
    fn inner(attr: TokenStream, item: TokenStream) -> Result<TokenStream2> {
        let fn_attr: FunctionAttr = syn::parse(attr)?;
        let user_fn: UserFunctionAttr = syn::parse(item.clone())?;

        let mut tokens: TokenStream2 = item.into();
        for attr in fn_attr.expand() {
            tokens.extend(attr.generate_function_descriptor(&user_fn)?);
        }
        Ok(tokens)
    }
    match inner(attr, item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[derive(Debug, Clone, Default)]
struct FunctionAttr {
    /// Function name
    name: String,
    /// Input argument types
    args: Vec<String>,
    /// Return type
    ret: String,
    /// Whether it is a table function
    is_table_function: bool,
    /// Whether it is an append-only aggregate function
    append_only: bool,
    /// Optional function for batch evaluation.
    batch_fn: Option<String>,
    /// State type for aggregate function.
    /// If not specified, it will be the same as return type.
    state: Option<String>,
    /// Initial state value for aggregate function.
    /// If not specified, it will be NULL.
    init_state: Option<String>,
    /// Type inference function.
    type_infer: Option<String>,
    /// Generic type.
    generic: Option<String>,
    /// Whether the function is volatile.
    volatile: bool,
    /// Generated batch function name.
    /// If not specified, the macro will not generate batch function.
    output: Option<String>,
}

/// Attributes from function signature `fn(..)`
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct UserFunctionAttr {
    /// Function name
    name: String,
    /// Whether the function is async.
    async_: bool,
    /// Whether contains argument `&Context`.
    context: bool,
    /// Whether contains argument `&mut impl Write`.
    write: bool,
    /// Whether the last argument type is `retract: bool`.
    retract: bool,
    /// Whether each argument type is `Option<T>`.
    args_option: Vec<bool>,
    /// If the first argument type is `&mut T`, then `Some(T)`.
    first_mut_ref_arg: Option<String>,
    /// The return type kind.
    return_type_kind: ReturnTypeKind,
    /// The kind of inner type `T` in `impl Iterator<Item = T>`
    iterator_item_kind: Option<ReturnTypeKind>,
    /// The core return type without `Option` or `Result`.
    core_return_type: String,
    /// The number of generic types.
    generic: usize,
    /// The span of return type.
    return_type_span: proc_macro2::Span,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ReturnTypeKind {
    T,
    Option,
    Result,
    ResultOption,
}

impl ReturnTypeKind {
    /// Returns true if the type is `Result<..>`.
    const fn is_result(&self) -> bool {
        matches!(self, ReturnTypeKind::Result | ReturnTypeKind::ResultOption)
    }
}

impl FunctionAttr {
    /// Return a unique name that can be used as an identifier.
    fn ident_name(&self) -> String {
        format!("{}_{}_{}", self.name, self.args.join("_"), self.ret)
            .replace("[]", "array")
            .replace("...", "variadic")
            .replace(['<', ' ', ',', ':'], "_")
            .replace('>', "")
            .replace("__", "_")
    }

    /// Return a unique signature of the function.
    fn normalize_signature(&self) -> String {
        format!(
            "{}({}){}{}",
            self.name,
            self.args.join(","),
            if self.is_table_function { "->>" } else { "->" },
            self.ret
        )
    }
}

impl UserFunctionAttr {
    /// Returns true if the function is like `fn(T1, T2, .., Tn) -> T`.
    fn is_pure(&self) -> bool {
        !self.async_
            && !self.write
            && !self.context
            && self.args_option.iter().all(|b| !b)
            && self.return_type_kind == ReturnTypeKind::T
    }

    /// Returns true if the function may return error.
    fn has_error(&self) -> bool {
        self.return_type_kind.is_result()
            || matches!(&self.iterator_item_kind, Some(k) if k.is_result())
    }
}
