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

use itertools::Itertools;

//  name        data type   array prefix primitive
const TYPE_MATRIX: &str = "
    void        Null        Null        _
    boolean     Boolean     Boolean     _
    int2        Int16       Int16       y
    int4        Int32       Int32       y
    int8        Int64       Int64       y
    float4      Float32     Float32     y
    float8      Float64     Float64     y
    date        Date32      Date32      y
    time        Time64      Time64      y
    timestamp   Timestamp   Timestamp   y
    timestamptz Timestamptz Timestamptz y
    interval    Interval    Interval    y
    varchar     Utf8        String      _
    bytea       Binary      Binary      _
    array       List        List        _
    struct      Struct      Struct      _
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

/// Checks if a data type is primitive.
pub fn is_primitive(ty: &str) -> bool {
    lookup_matrix(ty, 3) == "y"
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
    if ty.starts_with("struct<") && ty.ends_with(">") {
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
        "character varying" => "varchar",
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
        assert_eq!(normalize_type("int"), "int4");
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
