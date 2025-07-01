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

//! This module provides utility functions for Arrow data type conversion and manipulation.

//  name    primitive   rust type       array prefix            data type
const TYPE_MATRIX: &str = "
    null        _       ()              Null                    Null
    boolean     _       bool            Boolean                 Boolean
    int8        y       i8              Int8                    Int8
    int16       y       i16             Int16                   Int16
    int32       y       i32             Int32                   Int32
    int64       y       i64             Int64                   Int64
    uint8       y       u8              UInt8                   UInt8
    uint16      y       u16             UInt16                  UInt16
    uint32      y       u32             UInt32                  UInt32
    uint64      y       u64             UInt64                  UInt64
    float32     y       f32             Float32                 Float32
    float64     y       f64             Float64                 Float64
    date32      _       NaiveDate       Date32                  Date32
    time64      _       NaiveTime       Time64Microsecond       Time64(TimeUnit::Microsecond)
    timestamp   _       NaiveDateTime   TimestampMicrosecond    Timestamp(TimeUnit::Microsecond,None)
    interval    _       Interval        IntervalMonthDayNano    Interval(IntervalUnit::MonthDayNano)
    decimal     _       Decimal         String                  Utf8
    json        _       Value           String                  Utf8
    string      _       String,str      String                  Utf8
    binary      _       Vec<u8>,[u8]    Binary                  Binary
    largestring _       String,str      LargeString             LargeUtf8
    largebinary _       Vec<u8>,[u8]    LargeBinary             LargeBinary
    array       _       _               List                    List
    struct      _       _               Struct                  Struct
";

/// Maps a data type to its corresponding data type name.
pub fn data_type(ty: &str) -> &str {
    lookup_matrix(ty, 4)
}

/// Maps a data type to its corresponding array type name.
pub fn array_type(ty: &str) -> String {
    format!("{}Array", lookup_matrix(ty, 3))
}

/// Maps a data type to its corresponding array type name.
pub fn array_builder_type(ty: &str) -> String {
    format!("{}Builder", lookup_matrix(ty, 3))
}

/// Checks if a data type is primitive.
pub fn is_primitive(ty: &str) -> bool {
    lookup_matrix(ty, 1) == "y"
}

/// Maps a Rust type to its corresponding data type name.
pub fn type_of(rust_type: &str) -> String {
    if let Some(ty) = TYPE_MATRIX.trim().lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        let ty = parts.next()?;
        let rust_types = parts.nth(1)?;
        if rust_types.split(',').any(|t| rust_type.ends_with(t)) {
            Some(ty)
        } else {
            None
        }
    }) {
        return ty.to_string();
    }
    let struct_type = match rust_type.find('<') {
        // strip generic type
        Some(i) => &rust_type[..i],
        None => rust_type,
    };
    format!("struct {struct_type}")
}

fn lookup_matrix(mut ty: &str, idx: usize) -> &str {
    if ty.ends_with("[]") {
        ty = "array";
    } else if ty.starts_with("struct") {
        ty = "struct";
    }
    let s = TYPE_MATRIX.trim().lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        if parts.next()? == ty {
            Some(parts.nth(idx - 1).unwrap())
        } else {
            None
        }
    });
    s.unwrap_or_else(|| panic!("unknown type: {ty}"))
}

/// Normalizes a data type string.
///
/// # Examples
/// ```text
/// "int" => "int32"
/// "int[]" => "int32[]"
/// "struct  Key" => "struct Key"
/// ```
pub fn normalize_type(ty: &str) -> String {
    if let Some(t) = ty.strip_suffix("[]") {
        return format!("{}[]", normalize_type(t));
    }
    if let Some(s) = ty.strip_prefix("struct ") {
        return format!("struct {}", s.trim());
    }
    match ty {
        "bool" => "boolean",
        "smallint" => "int16",
        "int" | "integer" => "int32",
        "bigint" => "int64",
        "real" => "float32",
        "double precision" => "float64",
        "numeric" => "decimal",
        "varchar" | "character varying" => "string",
        "bytea" => "binary",
        "jsonb" => "json",
        "date" => "date32",
        "time" => "time64",
        _ => ty,
    }
    .to_string()
}

/// Expands a type wildcard string into a list of concrete types.
pub fn expand_type_wildcard(ty: &str) -> Vec<&str> {
    match ty {
        "*" => TYPE_MATRIX
            .trim()
            .lines()
            .map(|l| l.split_whitespace().next().unwrap())
            .filter(|l| *l != "any" && *l != "null")
            .collect(),
        "int*" => vec!["int8", "int16", "int32", "int64"],
        "uint*" => vec!["uint8", "uint16", "uint32", "uint64"],
        "float*" => vec!["float32", "float64"],
        _ => vec![ty],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_type() {
        assert_eq!(normalize_type("bool"), "boolean");
        assert_eq!(normalize_type("smallint"), "int16");
        assert_eq!(normalize_type("int"), "int32");
        assert_eq!(normalize_type("bigint"), "int64");
        assert_eq!(normalize_type("real"), "float32");
        assert_eq!(normalize_type("double precision"), "float64");
        assert_eq!(normalize_type("numeric"), "decimal");
        assert_eq!(normalize_type("varchar"), "string");
        assert_eq!(normalize_type("character varying"), "string");
        assert_eq!(normalize_type("jsonb"), "json");
        assert_eq!(normalize_type("int[]"), "int32[]");
        assert_eq!(normalize_type("struct   Key"), "struct Key");
    }
}
