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

import socket
import struct
import time
from typing import Iterator, List, Optional, Tuple, Any
from decimal import Decimal

from arrow_udf import udf, udtf, UdfServer


@udf(input_types=[], result_type="INT")
def int_42() -> int:
    return 42


@udf(input_types=["INT"], result_type="INT")
def sleep(s: int) -> int:
    time.sleep(s)
    return 0


@udf(input_types=["INT", "INT"], result_type="INT")
def gcd(x: int, y: int) -> int:
    while y != 0:
        (x, y) = (y, x % y)
    return x


@udf(name="gcd3", input_types=["INT", "INT", "INT"], result_type="INT")
def gcd3(x: int, y: int, z: int) -> int:
    return gcd(gcd(x, y), z)


@udf(
    input_types=["BINARY"],
    result_type="STRUCT<src_addr: STRING, dst_addr: STRING, src_port: INT16, dst_port: INT16>",
)
def extract_tcp_info(tcp_packet: bytes):
    src_addr, dst_addr = struct.unpack("!4s4s", tcp_packet[12:20])
    src_port, dst_port = struct.unpack("!HH", tcp_packet[20:24])
    src_addr = socket.inet_ntoa(src_addr)
    dst_addr = socket.inet_ntoa(dst_addr)
    return {
        "src_addr": src_addr,
        "dst_addr": dst_addr,
        "src_port": src_port,
        "dst_port": dst_port,
    }


@udtf(name="range", input_types="INT", result_types="INT")
def series(n: int) -> Iterator[int]:
    if n is None:
        return
    for i in range(n):
        yield i


@udtf(input_types="STRING", result_types=["STRING", "INT"])
def split(string: str) -> Iterator[Tuple[str, int]]:
    for s in string.split(" "):
        yield s, len(s)


@udf(input_types="STRING", result_type="DECIMAL")
def hex_to_dec(hex: Optional[str]) -> Optional[Decimal]:
    if not hex:
        return None

    hex = hex.strip()
    dec = Decimal(0)

    while hex:
        chunk = hex[:16]
        chunk_value = int(hex[:16], 16)
        dec = dec * (1 << (4 * len(chunk))) + chunk_value
        hex = hex[16:]
    return dec


@udf(input_types=["FLOAT64"], result_type="DECIMAL")
def float_to_decimal(f: float) -> Decimal:
    return Decimal(f)


@udf(input_types=["DECIMAL", "DECIMAL"], result_type="DECIMAL")
def decimal_add(a: Decimal, b: Decimal) -> Decimal:
    return a + b


@udf(input_types=["STRING[]", "INT"], result_type="STRING")
def array_access(list: List[str], idx: int) -> Optional[str]:
    if idx == 0 or idx > len(list):
        return None
    return list[idx - 1]


@udf(input_types=["JSON", "INT"], result_type="JSON")
def json_array_access(json: Any, i: int) -> Any:
    if not json:
        return None
    return json[i]


@udf(input_types=["JSON[]"], result_type="JSON")
def json_concat(list: List[Any]) -> Any:
    if not list:
        return None
    return list


@udf(input_types="JSON[]", result_type="JSON[]")
def json_array_identity(list: List[Any]) -> List[Any]:
    return list


ALL_TYPES = "BOOLEAN,SMALLINT,INT,BIGINT,FLOAT32,FLOAT64,DECIMAL,DATE,TIME,TIMESTAMP,INTERVAL,STRING,BYTEA,JSON".split(
    ","
) + [
    "STRUCT<a:INT,b:INT>"
]


@udf(
    input_types=[
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
    ],
    result_type="""struct<
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
    >""",
)
def return_all(
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
):
    return {
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
    }


if __name__ == "__main__":
    server = UdfServer(location="localhost:8815")
    server.add_function(int_42)
    server.add_function(sleep)
    server.add_function(gcd)
    server.add_function(gcd3)
    server.add_function(series)
    server.add_function(split)
    server.add_function(extract_tcp_info)
    server.add_function(hex_to_dec)
    server.add_function(float_to_decimal)
    server.add_function(decimal_add)
    server.add_function(array_access)
    server.add_function(json_array_access)
    server.add_function(json_concat)
    server.add_function(json_array_identity)
    server.add_function(return_all)
    server.serve()
