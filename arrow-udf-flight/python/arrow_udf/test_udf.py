# Copyright 2024 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from decimal import Decimal
from multiprocessing import Process
from arrow_udf import udf, UdfServer, DecimalType, JsonType
import pyarrow as pa
import pyarrow.flight as flight
import time
import datetime
from typing import Any


def flight_server():
    server = UdfServer(location="localhost:8815")
    server.add_function(add)
    server.add_function(wait)
    server.add_function(wait_concurrent)
    server.add_function(return_all)
    return server


def flight_client():
    client = flight.FlightClient(("localhost", 8815))
    return client


# Define a scalar function
@udf(input_types=["INT", "INT"], result_type="INT")
def add(x, y):
    return x + y


@udf(input_types=["INT"], result_type="INT")
def wait(x):
    time.sleep(0.01)
    return 0


@udf(input_types=["INT"], result_type="INT", io_threads=32)
def wait_concurrent(x):
    time.sleep(0.01)
    return 0


@udf(
    input_types=[
        "null",
        "boolean",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float32",
        "float64",
        "decimal",
        "date32",
        "time64",
        "timestamp",
        "interval",
        "string",
        "large_string",
        "binary",
        "large_binary",
        "json",
        "int[]",
        "struct<a:int, b:string>",
    ],
    result_type="""struct<
        null: null,
        boolean: boolean,
        int8: int8,
        int16: int16,
        int32: int32,
        int64: int64,
        uint8: uint8,
        uint16: uint16,
        uint32: uint32,
        uint64: uint64,
        float32: float32,
        float64: float64,
        decimal: decimal,
        date32: date32,
        time64: time64,
        timestamp: timestamp,
        interval: interval,
        string: string,
        large_string: large_string,
        binary: binary,
        large_binary: large_binary,
        json: json,
        list: int[],
        struct: struct<a:int, b:string>,
    >""",
)
def return_all(
    null,
    bool,
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f32,
    f64,
    decimal,
    date,
    time,
    timestamp,
    interval,
    string,
    large_string,
    binary,
    large_binary,
    json,
    list,
    struct,
):
    return {
        "null": null,
        "boolean": bool,
        "int8": i8,
        "int16": i16,
        "int32": i32,
        "int64": i64,
        "uint8": u8,
        "uint16": u16,
        "uint32": u32,
        "uint64": u64,
        "float32": f32,
        "float64": f64,
        "decimal": decimal,
        "date32": date,
        "time64": time,
        "timestamp": timestamp,
        "interval": interval,
        "string": string,
        "large_string": large_string,
        "binary": binary,
        "large_binary": large_binary,
        "json": json,
        "list": list,
        "struct": struct,
    }


def test_simple():
    LEN = 64
    data = pa.Table.from_arrays(
        [pa.array(range(0, LEN)), pa.array(range(0, LEN))], names=["x", "y"]
    )

    batches = data.to_batches(max_chunksize=512)

    with flight_client() as client, flight_server() as server:
        flight_info = flight.FlightDescriptor.for_path(b"add")
        writer, reader = client.do_exchange(descriptor=flight_info)
        with writer:
            writer.begin(schema=data.schema)
            for batch in batches:
                writer.write_batch(batch)
            writer.done_writing()

            chunk = reader.read_chunk()
            assert len(chunk.data) == LEN
            assert chunk.data.column("add").equals(
                pa.array(range(0, LEN * 2, 2), type=pa.int32())
            )


def test_io_concurrency():
    LEN = 64
    data = pa.Table.from_arrays([pa.array(range(0, LEN))], names=["x"])
    batches = data.to_batches(max_chunksize=512)

    with flight_client() as client, flight_server() as server:
        # Single-threaded function takes a long time
        flight_info = flight.FlightDescriptor.for_path(b"wait")
        writer, reader = client.do_exchange(descriptor=flight_info)
        with writer:
            writer.begin(schema=data.schema)
            for batch in batches:
                writer.write_batch(batch)
            writer.done_writing()
            start_time = time.time()

            total_len = 0
            for chunk in reader:
                total_len += len(chunk.data)

            assert total_len == LEN

            elapsed_time = time.time() - start_time  # ~0.64s
            assert elapsed_time > 0.5

        # Multi-threaded I/O bound function will take a much shorter time
        flight_info = flight.FlightDescriptor.for_path(b"wait_concurrent")
        writer, reader = client.do_exchange(descriptor=flight_info)
        with writer:
            writer.begin(schema=data.schema)
            for batch in batches:
                writer.write_batch(batch)
            writer.done_writing()
            start_time = time.time()

            total_len = 0
            for chunk in reader:
                total_len += len(chunk.data)

            assert total_len == LEN

            elapsed_time = time.time() - start_time
            assert elapsed_time < 0.25


def test_all_types():
    arrays = [
        pa.array([None], type=pa.null()),
        pa.array([True], type=pa.bool_()),
        pa.array([1], type=pa.int8()),
        pa.array([2], type=pa.int16()),
        pa.array([3], type=pa.int32()),
        pa.array([4], type=pa.int64()),
        pa.array([5], type=pa.uint8()),
        pa.array([6], type=pa.uint16()),
        pa.array([7], type=pa.uint32()),
        pa.array([8], type=pa.uint64()),
        pa.array([9], type=pa.float32()),
        pa.array([10], type=pa.float64()),
        pa.ExtensionArray.from_storage(
            DecimalType(),
            pa.array(["12345678901234567890.1234567890"], type=pa.string()),
        ),
        pa.array([datetime.date(2023, 6, 1)], type=pa.date32()),
        pa.array([datetime.time(1, 2, 3, 456789)], type=pa.time64("us")),
        pa.array(
            [datetime.datetime(2023, 6, 1, 1, 2, 3, 456789)],
            type=pa.timestamp("us"),
        ),
        pa.array([(1, 2, 3)], type=pa.month_day_nano_interval()),
        pa.array(["string"], type=pa.string()),
        pa.array(["large_string"], type=pa.large_string()),
        pa.array(["binary"], type=pa.binary()),
        pa.array(["large_binary"], type=pa.large_binary()),
        pa.ExtensionArray.from_storage(
            JsonType(), pa.array(['{ "key": 1 }'], type=pa.string())
        ),
        pa.array([[1]], type=pa.list_(pa.int32())),
        pa.array(
            [{"a": 1, "b": "string"}],
            type=pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.string())]),
        ),
    ]
    batch = pa.RecordBatch.from_arrays(arrays, names=["" for _ in arrays])

    with flight_client() as client, flight_server() as server:
        flight_info = flight.FlightDescriptor.for_path(b"return_all")
        writer, reader = client.do_exchange(descriptor=flight_info)
        with writer:
            writer.begin(schema=batch.schema)
            writer.write_batch(batch)
            writer.done_writing()

            chunk = reader.read_chunk()
            assert [v.as_py() for _, v in chunk.data.column(0)[0].items()] == [
                None,
                True,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9.0,
                10.0,
                Decimal("12345678901234567890.1234567890"),
                datetime.date(2023, 6, 1),
                datetime.time(1, 2, 3, 456789),
                datetime.datetime(2023, 6, 1, 1, 2, 3, 456789),
                (1, 2, 3),
                "string",
                "large_string",
                b"binary",
                b"large_binary",
                {"key": 1},
                [1],
                {"a": 1, "b": "string"},
            ]
