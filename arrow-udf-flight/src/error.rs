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

use arrow_flight::error::FlightError;
use thiserror::Error;

/// A specialized `Result` type for UDF operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The error type for UDF operations.
#[derive(Error, Debug)]
pub enum Error {
    #[error("failed to send requests to UDF service: {0}")]
    Tonic(#[from] tonic::Status),

    #[error("failed to call UDF: {0}")]
    Flight(#[from] FlightError),

    #[error("type mismatch: {0}")]
    TypeMismatch(String),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("UDF unsupported: {0}")]
    // TODO(error-handling): should prefer use error types than strings.
    Unsupported(String),

    #[error("UDF service returned no data")]
    NoReturned,

    #[error("Flight service error: {0}")]
    ServiceError(String),
}

impl Error {
    /// Returns true if the error is caused by a connection error.
    pub fn is_connection_error(&self) -> bool {
        match self {
            // Connection refused
            Error::Tonic(status) if status.code() == tonic::Code::Unavailable => true,
            _ => false,
        }
    }

    pub fn is_tonic_error(&self) -> bool {
        matches!(self, Error::Tonic(_) | Error::Flight(FlightError::Tonic(_)))
    }
}
