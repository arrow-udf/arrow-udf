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

use arrow_array::builder::StructBuilder;
use arrow_schema::Fields;
pub use arrow_udf_macros::StructType;

/// Interval type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub nanos: i64,
}

/// A trait for user-defined struct types.
///
/// This trait can be automatically derived with [`#[derive(StructType)]`](derive@StructType).
pub trait StructType {
    /// Returns the fields of the struct type.
    fn fields() -> Fields;
    /// Appends the struct value to the builder.
    fn append_to(self, builder: &mut StructBuilder);
    /// Appends a null value to the builder.
    fn append_null(builder: &mut StructBuilder);
}
