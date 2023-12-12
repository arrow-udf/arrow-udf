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

//! This module provides utility functions for SQL data type conversion and manipulation.

//  name                data type   array prefix owned type      ref type            primitive
const TYPE_MATRIX: &str = "
    boolean,bool        Boolean     Boolean     bool            bool                y
    smallint,int2       Int16       Int16       i16             i16                 y
    integer,int,int4    Int32       Int32       i32             i32                 y
    bigint,int8         Int64       Int64       i64             i64                 y
    real,float4         Float32     Float32     f32             f32                 y
    float8              Float64     Float64     f64             f64                 y
    date                Date32      Date32      Date            Date                y
    time                Time64      Time64      Time            Time                y
    timestamp           Timestamp   Timestamp   Timestamp       Timestamp           y
    timestamptz         Timestamptz Timestamptz Timestamptz     Timestamptz         y
    interval            Interval    Interval    Interval        Interval            y
    varchar             Utf8        String      String          &str                _
    bytea               Binary      Binary      Box<[u8]>       &[u8]               _
";

/// Maps a data type to its corresponding data type name.
pub fn data_type(ty: &str) -> &str {
    lookup_matrix(ty, 1)
}

/// Maps a data type to its corresponding array type name.
pub fn array_type(ty: &str) -> String {
    format!("{}Array", lookup_matrix(ty, 2))
}

/// Maps a data type to its corresponding array type name.
pub fn array_builder_type(ty: &str) -> String {
    format!("{}Builder", lookup_matrix(ty, 2))
}

/// Maps a data type to its corresponding `Scalar` type name.
pub fn owned_type(ty: &str) -> &str {
    lookup_matrix(ty, 3)
}

/// Maps a data type to its corresponding `ScalarRef` type name.
pub fn ref_type(ty: &str) -> &str {
    lookup_matrix(ty, 4)
}

/// Checks if a data type is primitive.
pub fn is_primitive(ty: &str) -> bool {
    lookup_matrix(ty, 5) == "y"
}

fn lookup_matrix(mut ty: &str, idx: usize) -> &str {
    if ty.ends_with("[]") {
        ty = "anyarray";
    } else if ty.starts_with("struct") {
        ty = "struct";
    } else if ty == "void" {
        // XXX: we don't support void type yet.
        //      replace it with int for now.
        ty = "int4";
    }
    let s = TYPE_MATRIX.trim().lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        let name = parts.next()?;
        if name.split(',').any(|n| n == ty) {
            Some(parts.nth(idx - 1).unwrap())
        } else {
            None
        }
    });
    s.unwrap_or_else(|| panic!("unknown type: {}", ty))
}

/// Expands a type wildcard string into a list of concrete types.
pub fn expand_type_wildcard(ty: &str) -> Vec<&str> {
    match ty {
        "*" => TYPE_MATRIX
            .trim()
            .lines()
            .map(|l| l.split_whitespace().next().unwrap())
            .filter(|l| *l != "any")
            .collect(),
        "*int" => vec!["int2", "int4", "int8"],
        "*float" => vec!["float4", "float8"],
        _ => vec![ty],
    }
}
