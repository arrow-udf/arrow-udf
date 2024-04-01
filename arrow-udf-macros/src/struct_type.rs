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

use itertools::Itertools;
use proc_macro2::TokenStream;
use quote::{format_ident, quote, ToTokens};
use syn::{Data, DeriveInput, Result};

use crate::{gen, types};

pub fn gen(tokens: TokenStream) -> Result<TokenStream> {
    let input: DeriveInput = syn::parse2(tokens)?;

    let struct_name = &input.ident;
    let generics = &input.generics;
    let Data::Struct(struct_) = &input.data else {
        return Err(syn::Error::new_spanned(input, "expect struct"));
    };

    let fields = struct_
        .fields
        .iter()
        .map(Field::parse)
        .collect::<Result<Vec<Field>>>()?;

    let names = fields.iter().map(|f| &f.name);
    let types = fields.iter().map(|f| gen::data_type(&f.type_));
    let append_values = fields.iter().enumerate().map(|(i, f)| {
        let field = &f.ident;
        let append_value = gen::gen_append_value(&f.type_);
        let append_null = gen::gen_append_null(&f.type_);
        let builder_type = gen::builder_type(&f.type_);
        match f.option {
            false => quote! {{
                let builder = builder.field_builder::<#builder_type>(#i).unwrap();
                let v = self.#field;
                #append_value
            }},
            true => quote! {{
                let builder = builder.field_builder::<#builder_type>(#i).unwrap();
                match self.#field {
                    Some(v) => #append_value,
                    None => #append_null,
                }
            }},
        }
    });
    let append_nulls = fields.iter().enumerate().map(|(i, f)| {
        let builder_type = gen::builder_type(&f.type_);
        let append_null = gen::gen_append_null(&f.type_);
        quote! {{
            let builder = builder.field_builder::<#builder_type>(#i).unwrap();
            #append_null
        }}
    });
    let static_name = format_ident!("{}_METADATA", struct_name.to_string().to_uppercase());
    let export_name = format!(
        "arrowudt_{}",
        // example: "KeyValue=key:varchar,value:varchar"
        gen::base64_encode(&format!(
            "{}={}",
            struct_name,
            fields
                .iter()
                .map(|f| format!("{}:{}", f.name, f.type_))
                .join(",")
        ))
    );

    Ok(quote! {
        // export a symbol to describe the struct type
        #[export_name = #export_name]
        static #static_name: () = ();

        impl #generics ::arrow_udf::types::StructType for #struct_name #generics {
            fn fields() -> ::arrow_udf::codegen::arrow_schema::Fields {
                use ::arrow_udf::codegen::arrow_schema::{self, Field, TimeUnit, IntervalUnit};
                let fields: Vec<Field> = vec![
                    #(Field::new(#names, #types, true),)*
                ];
                fields.into()
            }
            fn append_to(self, builder: &mut ::arrow_udf::codegen::arrow_array::builder::StructBuilder) {
                use ::arrow_udf::codegen::arrow_array::builder::*;
                #(#append_values)*
                builder.append(true);
            }
            fn append_null(builder: &mut ::arrow_udf::codegen::arrow_array::builder::StructBuilder) {
                use ::arrow_udf::codegen::arrow_array::builder::*;
                #(#append_nulls)*
                builder.append_null();
            }
        }
    })
}

/// Parsed field of a struct.
#[derive(Debug)]
struct Field {
    /// The identifier in source code.
    ident: syn::Ident,
    /// The name of the field. `r#` is stripped.
    name: String,
    /// The normalized type of the field. e.g. `int4` for `i32`.
    type_: String,
    /// Whether the field is nullable.
    option: bool,
}

impl Field {
    /// Parses a field from a `syn::Field`.
    fn parse(field: &syn::Field) -> Result<Self> {
        let ident = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new_spanned(field, "expect field name"))?;
        let mut name = ident.to_string();
        if name.starts_with("r#") {
            // strip leading `r#`
            name = name[2..].to_string();
        }
        let ty = &field.ty;
        let (option, ty) = match strip_outer_type(ty, "Option") {
            Some(ty) => (true, ty),
            None => (false, ty),
        };
        let (list, ty) = match strip_outer_type(ty, "Vec") {
            // exclude `Vec<u8>` from list
            Some(ty) if ty.to_token_stream().to_string() != "u8" => (true, ty),
            _ => (false, ty),
        };
        let mut type_ =
            types::type_of(&ty.to_token_stream().to_string().replace(' ', "")).to_string();
        if list {
            type_ += "[]";
        }
        Ok(Self {
            ident,
            name,
            type_,
            option,
        })
    }
}

/// Check if the type is `type_<T>` and return `T`.
fn strip_outer_type<'a>(ty: &'a syn::Type, type_: &str) -> Option<&'a syn::Type> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    let seg = path.path.segments.last()?;
    if seg.ident != type_ {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    let Some(syn::GenericArgument::Type(ty)) = args.args.first() else {
        return None;
    };
    Some(ty)
}

#[cfg(test)]
mod tests {
    use proc_macro2::TokenStream;
    use syn::File;

    fn pretty_print(output: TokenStream) -> String {
        let output: File = syn::parse2(output).unwrap();
        prettyplease::unparse(&output)
    }

    #[test]
    fn test_struct_type() {
        let code = include_str!("testdata/struct.input.rs");
        let input: TokenStream = str::parse(code).unwrap();
        let output = super::gen(input).unwrap();
        let output = pretty_print(output);
        let expected = expect_test::expect_file!["testdata/struct.output.rs"];
        expected.assert_eq(&output);
    }
}
