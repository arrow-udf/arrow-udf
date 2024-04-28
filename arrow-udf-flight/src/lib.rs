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

#![doc = include_str!("../README.md")]

mod error;

pub use error::{Error, Result};

use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Criteria, FlightData, FlightDescriptor};
use arrow_schema::Schema;
use futures_util::{stream, FutureExt, Stream, StreamExt, TryStreamExt};
use ginepro::{LoadBalancedChannel, ResolutionStrategy};
use tokio::time::Duration as TokioDuration;
use tonic::transport::Channel;

// Interval between two successive probes of the UDF DNS.
const DNS_PROBE_INTERVAL_SECS: u64 = 5;
// Timeout duration for performing an eager DNS resolution.
const EAGER_DNS_RESOLVE_TIMEOUT_SECS: u64 = 5;
const REQUEST_TIMEOUT_SECS: u64 = 5;
const CONNECT_TIMEOUT_SECS: u64 = 5;

/// Client for a remote Arrow UDF service.
#[derive(Debug)]
pub struct Client {
    client: FlightServiceClient<Channel>,
    addr: String,
}

impl Client {
    /// Connect to a UDF service.
    pub async fn connect(addr: &str) -> Result<Self> {
        Self::connect_inner(
            addr,
            ResolutionStrategy::Eager {
                timeout: TokioDuration::from_secs(EAGER_DNS_RESOLVE_TIMEOUT_SECS),
            },
        )
        .await
    }

    /// Connect to a UDF service lazily (i.e. only when the first request is sent).
    pub fn connect_lazy(addr: &str) -> Result<Self> {
        Self::connect_inner(addr, ResolutionStrategy::Lazy)
            .now_or_never()
            .unwrap()
    }

    async fn connect_inner(
        mut addr: &str,
        resolution_strategy: ResolutionStrategy,
    ) -> Result<Self> {
        if let Some(a) = addr.strip_prefix("http://") {
            addr = a;
        }
        if let Some(a) = addr.strip_prefix("https://") {
            addr = a;
        }
        let (host, port) = addr
            .split_once(':')
            .ok_or_else(|| Error::ServiceError(format!("invalid address: {addr}")))?;
        let port: u16 = port
            .parse()
            .map_err(|_| Error::ServiceError(format!("invalid port number: {port}")))?;
        let channel = LoadBalancedChannel::builder((host.to_owned(), port))
            .dns_probe_interval(std::time::Duration::from_secs(DNS_PROBE_INTERVAL_SECS))
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .resolution_strategy(resolution_strategy)
            .channel()
            .await
            .map_err(|e| {
                Error::ServiceError(format!(
                    "failed to create LoadBalancedChannel, address: {}, err: {}",
                    addr, e
                ))
            })?;
        let client = FlightServiceClient::new(channel.into());
        Ok(Self {
            client,
            addr: addr.into(),
        })
    }

    /// Check if the function is available and the schema is match.
    pub async fn check(&self, name: &str, args: &Schema, returns: &Schema) -> Result<()> {
        let descriptor = FlightDescriptor::new_path(vec![name.into()]);

        let response = self.client.clone().get_flight_info(descriptor).await?;

        // check schema
        let info = response.into_inner();
        let input_num = info.total_records as usize;
        let full_schema = Schema::try_from(info)
            .map_err(|e| FlightError::DecodeError(format!("error decoding schema: {}", e)))?;
        if input_num > full_schema.fields.len() {
            return Err(Error::ServiceError(format!(
                "function {:?} schema info not consistency: input_num: {}, total_fields: {}",
                name,
                input_num,
                full_schema.fields.len()
            )));
        }

        let (input_fields, return_fields) = full_schema.fields.split_at(input_num);
        let actual_input_types: Vec<_> = input_fields.iter().map(|f| f.data_type()).collect();
        let actual_result_types: Vec<_> = return_fields.iter().map(|f| f.data_type()).collect();
        let expect_input_types: Vec<_> = args.fields.iter().map(|f| f.data_type()).collect();
        let expect_result_types: Vec<_> = returns.fields.iter().map(|f| f.data_type()).collect();
        if !data_types_match(&expect_input_types, &actual_input_types) {
            return Err(Error::TypeMismatch(format!(
                "function: {:?}, expect arguments: {:?}, actual: {:?}",
                name, expect_input_types, actual_input_types
            )));
        }
        if !data_types_match(&expect_result_types, &actual_result_types) {
            return Err(Error::TypeMismatch(format!(
                "function: {:?}, expect return: {:?}, actual: {:?}",
                name, expect_result_types, actual_result_types
            )));
        }
        Ok(())
    }

    /// List all available functions.
    pub async fn list(&self) -> Result<Vec<Function>> {
        let response = self
            .client
            .clone()
            .list_flights(Criteria::default())
            .await?;
        let mut functions = vec![];
        let mut response = response.into_inner();
        while let Some(flight_info) = response.next().await {
            let flight_info = flight_info?;
            let name = flight_info.flight_descriptor.as_ref().unwrap().path[0].clone();
            let input_num = flight_info.total_records as usize;
            let schema = Schema::try_from(flight_info)
                .map_err(|e| FlightError::DecodeError(format!("error decoding schema: {}", e)))?;
            let (input_fields, return_fields) = schema.fields.split_at(input_num);
            functions.push(Function {
                name,
                args: Schema::new(input_fields.to_vec()),
                returns: Schema::new(return_fields.to_vec()),
            });
        }
        Ok(functions)
    }

    /// Call a function.
    pub async fn call(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        self.call_internal(name, input).await
    }

    async fn call_internal(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        let input = input.clone();
        let mut output_stream = self
            .call_stream_internal(name, stream::once(async { input }))
            .await?;
        let mut batches = vec![];
        while let Some(batch) = output_stream.next().await {
            batches.push(batch?);
        }
        Ok(arrow_select::concat::concat_batches(
            output_stream.schema().ok_or(Error::NoReturned)?,
            batches.iter(),
        )?)
    }

    /// Call a function, retry up to 5 times / 3s if connection is broken.
    pub async fn call_with_retry(&self, id: &str, input: &RecordBatch) -> Result<RecordBatch> {
        let mut backoff = Duration::from_millis(100);
        for i in 0..5 {
            match self.call(id, input).await {
                Err(err) if err.is_connection_error() && i != 4 => {
                    tracing::error!(error = %err, "UDF connection error. retry...");
                }
                ret => return ret,
            }
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
        unreachable!()
    }

    /// Always retry on connection error
    pub async fn call_with_always_retry_on_network_error(
        &self,
        id: &str,
        input: &RecordBatch,
    ) -> Result<RecordBatch> {
        let mut backoff = Duration::from_millis(100);
        loop {
            match self.call(id, input).await {
                Err(err) if err.is_tonic_error() => {
                    tracing::error!(error = %err, "UDF tonic error. retry...");
                }
                ret => {
                    if ret.is_err() {
                        tracing::error!(error = %ret.as_ref().unwrap_err(), "UDF error. exiting...");
                    }
                    return ret;
                }
            }
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
    }

    /// Call a table function.
    pub async fn call_table_function(
        &self,
        name: &str,
        input: &RecordBatch,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + 'static> {
        let input = input.clone();
        Ok(self
            .call_stream_internal(name, stream::once(async { input }))
            .await?
            .map_err(|e| e.into()))
    }

    async fn call_stream_internal(
        &self,
        name: &str,
        inputs: impl Stream<Item = RecordBatch> + Send + 'static,
    ) -> Result<FlightRecordBatchStream> {
        let descriptor = FlightDescriptor::new_path(vec![name.into()]);
        let flight_data_stream =
            FlightDataEncoderBuilder::new()
                .build(inputs.map(Ok))
                .map(move |res| FlightData {
                    // TODO: fill descriptor only for the first message
                    flight_descriptor: Some(descriptor.clone()),
                    ..res.unwrap()
                });

        // call `do_exchange` on Flight server
        let response = self.client.clone().do_exchange(flight_data_stream).await?;

        // decode response
        let stream = response.into_inner();
        Ok(FlightRecordBatchStream::new_from_flight_data(
            // convert tonic::Status to FlightError
            stream.map_err(|e| e.into()),
        ))
    }

    /// Get the remote address of the UDF service.
    pub fn remote_addr(&self) -> &str {
        &self.addr
    }
}

/// Check if two list of data types match, ignoring field names.
fn data_types_match(a: &[&arrow_schema::DataType], b: &[&arrow_schema::DataType]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    #[allow(clippy::disallowed_methods)]
    a.iter().zip(b.iter()).all(|(a, b)| a.equals_datatype(b))
}

/// Function signature.
#[derive(Debug)]
pub struct Function {
    /// Function name.
    pub name: String,
    /// The schema of function arguments.
    pub args: Schema,
    /// The schema of function return values.
    pub returns: Schema,
}
