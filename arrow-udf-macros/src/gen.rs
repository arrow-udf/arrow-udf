// Copyright 2023 RisingWave Labs
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

//! Generate code for the functions.

use itertools::Itertools;
use proc_macro2::{Ident, Span};
use quote::{format_ident, quote};

use super::*;

impl FunctionAttr {
    /// Expands the wildcard in function arguments or return type.
    pub fn expand(&self) -> Vec<Self> {
        let args = self.args.iter().map(|ty| types::expand_type_wildcard(ty));
        let ret = types::expand_type_wildcard(&self.ret);
        // multi_cartesian_product should emit an empty set if the input is empty.
        let args_cartesian_product =
            args.multi_cartesian_product()
                .chain(match self.args.is_empty() {
                    true => vec![vec![]],
                    false => vec![],
                });
        let mut attrs = Vec::new();
        for (args, ret) in args_cartesian_product.cartesian_product(ret) {
            let attr = FunctionAttr {
                args: args.iter().map(|s| s.to_string()).collect(),
                ret: ret.to_string(),
                ..self.clone()
            };
            attrs.push(attr);
        }
        attrs
    }

    /// Generate the type infer function.
    fn generate_type_infer_fn(&self) -> Result<TokenStream2> {
        if let Some(func) = &self.type_infer {
            if func == "panic" {
                return Ok(quote! { |_| panic!("type inference function is not implemented") });
            }
            // use the user defined type inference function
            return Ok(func.parse().unwrap());
        } else if self.ret == "any" {
            // TODO: if there are multiple "any", they should be the same type
            if let Some(i) = self.args.iter().position(|t| t == "any") {
                // infer as the type of "any" argument
                return Ok(quote! { |args| Ok(args[#i].clone()) });
            }
            if let Some(i) = self.args.iter().position(|t| t == "anyarray") {
                // infer as the element type of "anyarray" argument
                return Ok(quote! { |args| Ok(args[#i].as_list().clone()) });
            }
        } else if self.ret == "anyarray" {
            if let Some(i) = self.args.iter().position(|t| t == "anyarray") {
                // infer as the type of "anyarray" argument
                return Ok(quote! { |args| Ok(args[#i].clone()) });
            }
            if let Some(i) = self.args.iter().position(|t| t == "any") {
                // infer as the array type of "any" argument
                return Ok(quote! { |args| Ok(DataType::List(Box::new(args[#i].clone()))) });
            }
        } else if self.ret == "struct" {
            if let Some(i) = self.args.iter().position(|t| t == "struct") {
                // infer as the type of "struct" argument
                return Ok(quote! { |args| Ok(args[#i].clone()) });
            }
        } else {
            // the return type is fixed
            let ty = data_type(&self.ret);
            return Ok(quote! { |_| Ok(#ty) });
        }
        Err(Error::new(
            Span::call_site(),
            "type inference function is required",
        ))
    }

    /// Generate a descriptor of the scalar or table function.
    ///
    /// The types of arguments and return value should not contain wildcard.
    pub fn generate_function_descriptor(&self, user_fn: &UserFunctionAttr) -> Result<TokenStream2> {
        // if self.is_table_function {
        //     return self.generate_table_function_descriptor(user_fn);
        // }
        let name = self.name.clone();
        let variadic = matches!(self.args.last(), Some(t) if t == "...");
        let args = match variadic {
            true => &self.args[..self.args.len() - 1],
            false => &self.args[..],
        }
        .iter()
        .map(|ty| sig_data_type(ty))
        .collect_vec();
        let ret = sig_data_type(&self.ret);

        let ctor_name = format_ident!("{}_sig", self.ident_name());
        let export_name = format!("arrowudf_{}", base64_encode(&self.normalize_signature()));
        let function = self.generate_scalar_function(user_fn)?;

        Ok(quote! {
            #[cfg(target_arch = "wasm32")]
            #[doc(hidden)]
            #[export_name = #export_name]
            unsafe extern "C" fn #ctor_name(ptr: *const u8, len: usize) -> u64 {
                match arrow_udf::codegen::ffi_wrapper(#function, ptr, len) {
                    Ok(data) => {
                        let ptr = data.as_ptr();
                        let len = data.len();
                        std::mem::forget(data);
                        ((ptr as u64) << 32) | (len as u64)
                    }
                    Err(_) => 0,
                }
            }

            #[cfg(not(target_arch = "wasm32"))]
            fn #ctor_name() -> arrow_udf::FunctionSignature {
                use arrow_udf::{FunctionSignature, SigDataType, codegen::arrow_schema};

                FunctionSignature {
                    name: #name.into(),
                    arg_types: vec![#(#args),*],
                    variadic: #variadic,
                    return_type: #ret,
                    function: #function,
                }
            }
        })
    }

    /// Generate a scalar function.
    fn generate_scalar_function(&self, user_fn: &UserFunctionAttr) -> Result<TokenStream2> {
        let variadic = matches!(self.args.last(), Some(t) if t == "...");
        let num_args = self.args.len() - if variadic { 1 } else { 0 };
        let fn_name = format_ident!("{}", user_fn.name);

        let children_indices = (0..num_args).collect_vec();

        /// Return a list of identifiers with the given prefix and indices.
        fn idents(prefix: &str, indices: &[usize]) -> Vec<Ident> {
            indices
                .iter()
                .map(|i| format_ident!("{prefix}{i}"))
                .collect()
        }
        let inputs = idents("i", &children_indices);
        let arrays = idents("a", &children_indices);
        let arg_arrays = children_indices
            .iter()
            .map(|i| format_ident!("{}", types::array_type(&self.args[*i])));
        let ret_array_type = format_ident!("{}", types::array_type(&self.ret));
        let builder_type = format_ident!("{}", types::array_builder_type(&self.ret));

        // evaluate variadic arguments in `eval`
        let eval_variadic = variadic.then(|| {
            quote! {
                let mut columns = Vec::with_capacity(self.children.len() - #num_args);
                for child in &self.children[#num_args..] {
                    columns.push(child.eval(input).await?);
                }
                let variadic_input = DataChunk::new(columns, input.visibility().clone());
            }
        });

        let variadic_args = variadic.then(|| quote! { variadic_row, });
        let context = user_fn.context.then(|| quote! { &self.context, });
        let writer = user_fn.write.then(|| quote! { &mut writer, });
        let await_ = user_fn.async_.then(|| quote! { .await });
        // call the user defined function
        // inputs: [ Option<impl ScalarRef> ]
        let mut output = quote! { #fn_name(
            #(#inputs,)*
            #variadic_args
            #context
            #writer
        ) #await_ };
        // handle error if the function returns `Result`
        // wrap a `Some` if the function doesn't return `Option`
        output = match user_fn.return_type_kind {
            ReturnTypeKind::T => quote! { Some(#output) },
            ReturnTypeKind::Option => output,
            ReturnTypeKind::Result => quote! { Some(#output?) },
            ReturnTypeKind::ResultOption => quote! { #output? },
        };
        // if user function accepts non-option arguments, we assume the function
        // returns null on null input, so we need to unwrap the inputs before calling.
        if !user_fn.arg_option {
            output = quote! {
                match (#(#inputs,)*) {
                    (#(Some(#inputs),)*) => #output,
                    _ => None,
                }
            };
        };
        // now the `output` is: Option<impl ScalarRef or Scalar>
        let append_output = if user_fn.write {
            if self.ret != "varchar" && self.ret != "bytea" {
                return Err(Error::new(
                    Span::call_site(),
                    "`&mut Write` can only be used for returning `varchar` or `bytea`",
                ));
            }
            quote! {{
                let mut writer = builder.writer();
                if #output.is_some() {
                    writer.finish();
                } else {
                    drop(writer);
                    builder.append_null();
                }
            }}
        } else {
            /// Generate code to append the `v` to the `builder`.
            fn gen_append_value(ty: &str) -> TokenStream2 {
                if ty.starts_with("struct") {
                    let append_fields = types::iter_fields(ty).enumerate().map(|(i, (_, ty))| {
                        let index = syn::Index::from(i);
                        let builder_type = format_ident!("{}", types::array_builder_type(ty));
                        let append = gen_append_value(ty);
                        quote! {{
                            let builder = builder.field_builder::<#builder_type>(#i).unwrap();
                            let v = v.#index;
                            #append
                        }}
                    });
                    quote! {{
                        #(#append_fields)*
                        builder.append(true);
                    }}
                } else if ty == "void" {
                    quote! { builder.append_empty_value() }
                } else {
                    quote! { builder.append_value(v) }
                }
            }
            /// Generate code to append null to the `builder`.
            fn gen_append_null(ty: &str) -> TokenStream2 {
                if ty.starts_with("struct") {
                    let append_fields = types::iter_fields(ty).enumerate().map(|(i, (_, ty))| {
                        let append = gen_append_null(ty);
                        let builder_type = format_ident!("{}", types::array_builder_type(ty));
                        quote! {{
                            let builder = builder.field_builder::<#builder_type>(#i).unwrap();
                            #append
                        }}
                    });
                    quote! {{
                        #(#append_fields)*
                        builder.append(false);
                    }}
                } else {
                    quote! { builder.append_null() }
                }
            }
            let append_value = gen_append_value(&self.ret);
            let append_null = gen_append_null(&self.ret);
            quote! {
                match #output {
                    Some(v) => #append_value,
                    None => #append_null,
                }
            }
        };
        // the main body in `eval`
        let eval = if let Some(batch_fn) = &self.batch_fn {
            assert!(
                !variadic,
                "customized batch function is not supported for variadic functions"
            );
            // user defined batch function
            let fn_name = format_ident!("{}", batch_fn);
            quote! {
                let c = #fn_name(#(#arrays),*);
                Ok(Arc::new(c))
            }
        } else if types::is_primitive(&self.ret)
            && self.args.iter().all(|ty| types::is_primitive(ty))
            && self.args.len() <= 2
            && user_fn.is_pure()
            && !variadic
        {
            // SIMD optimization for primitive types
            match self.args.len() {
                0 => quote! {
                    let c = #ret_array_type::from_iter_values(
                        std::iter::repeat_with(|| #fn_name()).take(input.num_rows())
                    );
                    Ok(Arc::new(c))
                },
                1 => quote! {
                    let c: #ret_array_type = arrow_arith::arity::unary(a0, #fn_name);
                    Ok(Arc::new(c))
                },
                2 => quote! {
                    let c: #ret_array_type = arrow_arith::arity::binary(a0, a1, #fn_name)?;
                    Ok(Arc::new(c))
                },
                n => todo!("SIMD optimization for {n} arguments"),
            }
        } else {
            // no optimization
            let array_zip = match children_indices.len() {
                0 => quote! { std::iter::repeat(()).take(input.num_rows()) },
                _ => quote! { itertools::multizip((#(#arrays.iter(),)*)) },
            };
            let let_variadic = variadic.then(|| {
                quote! {
                    let variadic_row = variadic_input.row_at_unchecked_vis(i);
                }
            });
            let builder = match self.ret.as_str() {
                "varchar" => {
                    quote! { arrow_udf::codegen::StringBuilder::with_capacity(input.num_rows(), 1024) }
                }
                "bytea" => {
                    quote! { arrow_udf::codegen::BinaryBuilder::with_capacity(input.num_rows(), 1024) }
                }
                s if s.starts_with("struct") => {
                    let fields = fields(s);
                    quote! { StructBuilder::from_fields(#fields, input.num_rows()) }
                }
                _ => quote! { #builder_type::with_capacity(input.num_rows()) },
            };
            quote! {
                let mut builder = #builder;
                for (i, (#(#inputs,)*)) in #array_zip.enumerate() {
                    #let_variadic
                    #append_output
                }
                Ok(Arc::new(builder.finish()))
            }
        };

        Ok(quote! {
            {
                use std::sync::Arc;
                use arrow_udf::{Result, Error};
                use arrow_udf::codegen::arrow_array::RecordBatch;
                use arrow_udf::codegen::arrow_array::array::*;
                use arrow_udf::codegen::arrow_array::builder::*;
                use arrow_udf::codegen::arrow_arith;
                use arrow_udf::codegen::arrow_schema;
                use arrow_udf::codegen::itertools;

                fn eval(input: &RecordBatch) -> Result<ArrayRef> {
                    #(
                        let #arrays: &#arg_arrays = input.column(#children_indices).as_any().downcast_ref()
                            .ok_or_else(|| Error::CastError(format!("expect {} for the {}-th argument", stringify!(#arg_arrays), #children_indices)))?;
                    )*
                    #eval_variadic
                    #eval
                }
                eval
            }
        })
    }
}

fn sig_data_type(ty: &str) -> TokenStream2 {
    match ty {
        "any" => quote! { SigDataType::Any },
        "anyarray" => quote! { SigDataType::AnyArray },
        "struct" => quote! { SigDataType::AnyStruct },
        _ => {
            let datatype = data_type(ty);
            quote! { SigDataType::Exact(#datatype) }
        }
    }
}

/// Returns a `DataType` from type name.
fn data_type(ty: &str) -> TokenStream2 {
    if let Some(ty) = ty.strip_suffix("[]") {
        let inner_type = data_type(ty);
        return quote! { arrow_schema::DataType::List(Box::new(#inner_type)) };
    }
    if ty.starts_with("struct<") && ty.ends_with('>') {
        let fields = fields(ty);
        return quote! { arrow_schema::DataType::Struct(#fields) };
    }
    let variant = format_ident!("{}", types::data_type(ty));
    quote! { arrow_schema::DataType::#variant }
}

/// Returns a `Fields` from struct type name.
fn fields(ty: &str) -> TokenStream2 {
    let fields = types::iter_fields(ty).map(|(name, ty)| {
        let ty = data_type(ty);
        quote! { arrow_schema::Field::new(#name, #ty, true) }
    });
    quote! { arrow_schema::Fields::from(vec![#(#fields,)*]) }
}

/// Encode a string to a symbol name using customized base64.
fn base64_encode(input: &str) -> String {
    use base64::{
        alphabet::Alphabet,
        engine::{general_purpose::NO_PAD, GeneralPurpose},
        Engine,
    };
    // standard base64 uses '+' and '/', which is not a valid symbol name.
    // we use '$' and '_' instead.
    let alphabet =
        Alphabet::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789$_").unwrap();
    let engine = GeneralPurpose::new(&alphabet, NO_PAD);
    engine.encode(input)
}
