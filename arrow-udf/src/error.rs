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

use std::fmt::Display;

use arrow_array::ArrayRef;
pub use arrow_schema::ArrowError;
use thiserror::Error;

/// A specialized `Result` type for Arrow UDF operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Alias for a type-erased error type.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Errors returned by the function.
    ///
    /// Successful results are returned as `ArrayRef`, where error rows are `None`.
    /// The errors are collected in the `MultiError`.
    #[error("function errors:\n{1}")]
    Function(ArrayRef, MultiError),

    /// Arrow errors.
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}

/// A collection of multiple errors.
#[derive(Error, Debug)]
pub struct MultiError(Box<[BoxError]>);

impl Display for MultiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, e) in self.0.iter().enumerate() {
            writeln!(f, "{i}: {e}")?;
        }
        Ok(())
    }
}

impl From<Vec<BoxError>> for MultiError {
    fn from(v: Vec<BoxError>) -> Self {
        Self(v.into_boxed_slice())
    }
}

impl IntoIterator for MultiError {
    type IntoIter = std::vec::IntoIter<BoxError>;
    type Item = BoxError;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_vec().into_iter()
    }
}
