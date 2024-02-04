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

use itertools::Itertools;

//  name    primitive   rust type       array prefix            data type
const TYPE_MATRIX: &str = "
    void        _       ()              Null                    Null
    boolean     _       bool            Boolean                 Boolean
    int2        y       i16             Int16                   Int16
    int4        y       i32             Int32                   Int32
    int8        y       i64             Int64                   Int64
    float4      y       f32             Float32                 Float32
    float8      y       f64             Float64                 Float64
    decimal     _       Decimal         LargeBinary             LargeBinary
    date        _       NaiveDate       Date32                  Date32
    time        _       NaiveTime       Time64Microsecond       Time64(TimeUnit::Microsecond)
    timestamp   _       NaiveDateTime   TimestampMicrosecond    Timestamp(TimeUnit::Microsecond,None)
    interval    _       Interval        IntervalMonthDayNano    Interval(IntervalUnit::MonthDayNano)
    json        _       Value           LargeString             LargeUtf8
    varchar     _       String,str      String                  Utf8
    bytea       _       Vec<u8>,[u8]    Binary                  Binary
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
    format!("struct {}", struct_type)
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
    s.unwrap_or_else(|| panic!("unknown type: {}", ty))
}

/// Normalizes a data type string.
///
/// # Examples
/// ```text
/// "int" => "int4"
/// "int[]" => "int4[]"
/// "struct< a: int, b:int >" => "struct<a:int4,b:int4>"
/// ```
pub fn normalize_type(ty: &str) -> String {
    if let Some(t) = ty.strip_suffix("[]") {
        return format!("{}[]", normalize_type(t));
    }
    if ty.starts_with("struct<") && ty.ends_with('>') {
        return format!(
            "struct<{}>",
            iter_fields(ty)
                .map(|(name, ty)| format!("{}:{}", name, normalize_type(ty)))
                .join(",")
        );
    }
    match ty {
        "bool" => "boolean",
        "smallint" => "int2",
        "int" | "integer" => "int4",
        "bigint" => "int8",
        "real" => "float4",
        "double precision" => "float8",
        "numeric" => "decimal",
        "character varying" => "varchar",
        "jsonb" => "json",
        _ => ty,
    }
    .to_string()
}

/// Iterates over the fields of a struct type.
///
/// # Examples
///
/// ```text
/// "struct<a:struct<c:int,d:int>,b:varchar>"
/// yield ("a", "struct<c:int,d:int>")
/// yield ("b", "varchar")
/// ```
pub fn iter_fields(ty: &str) -> impl Iterator<Item = (&str, &str)> {
    let ty = ty.strip_prefix("struct<").unwrap();
    let mut ty = ty.strip_suffix('>').unwrap();
    std::iter::from_fn(move || {
        if ty.is_empty() {
            return None;
        }
        let mut depth = 0;
        let mut i = 0;
        for b in ty.bytes() {
            match b {
                b'<' => depth += 1,
                b'>' => depth -= 1,
                b',' if depth == 0 => break,
                _ => {}
            }
            i += 1;
        }
        // ty[i] is Some(',') or None
        let field = &ty[..i];
        ty = &ty[(i + 1).min(ty.len())..];
        let (name, t) = field.split_once(':').unwrap();
        Some((name.trim(), t.trim()))
    })
}

/// Expands a type wildcard string into a list of concrete types.
pub fn expand_type_wildcard(ty: &str) -> Vec<&str> {
    match ty {
        "*" => TYPE_MATRIX
            .trim()
            .lines()
            .map(|l| l.split_whitespace().next().unwrap())
            .filter(|l| *l != "any" && *l != "void")
            .collect(),
        "int*" => vec!["int2", "int4", "int8"],
        "float*" => vec!["float4", "float8"],
        _ => vec![ty],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_type() {
        assert_eq!(normalize_type("bool"), "boolean");
        assert_eq!(normalize_type("smallint"), "int2");
        assert_eq!(normalize_type("int"), "int4");
        assert_eq!(normalize_type("bigint"), "int8");
        assert_eq!(normalize_type("real"), "float4");
        assert_eq!(normalize_type("double precision"), "float8");
        assert_eq!(normalize_type("numeric"), "decimal");
        assert_eq!(normalize_type("character varying"), "varchar");
        assert_eq!(normalize_type("jsonb"), "json");
        assert_eq!(normalize_type("int[]"), "int4[]");
        assert_eq!(
            normalize_type("struct< a: int, b: struct< c:int, d: varchar> >"),
            "struct<a:int4,b:struct<c:int4,d:varchar>>"
        );
    }

    #[test]
    fn test_iter_fields() {
        assert_eq!(
            iter_fields("struct<a:int,b:struct<c:int,d:int>>").collect::<Vec<_>>(),
            vec![("a", "int"), ("b", "struct<c:int,d:int>")]
        );
    }
}
