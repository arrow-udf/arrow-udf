# Copyright 2025 RisingWave Labs
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

import decimal
import datetime
from typing import (
    Dict,
    Any,
    TypedDict,
    Annotated,
)

import pyarrow as pa
import arrow_udf


# Annotate original types with Arrow data types and normalized names.
# The original types are those returned by `pyarrow.Scalar.as_py()`.

Null = Annotated[None, pa.null(), "null"]
Bool = Annotated[bool, pa.bool_(), "bool"]
Int8 = Annotated[int, pa.int8(), "int8"]
Int16 = Annotated[int, pa.int16(), "int16"]
Int32 = Annotated[int, pa.int32(), "int32"]
Int64 = Annotated[int, pa.int64(), "int64"]
UInt8 = Annotated[int, pa.uint8(), "uint8"]
UInt16 = Annotated[int, pa.uint16(), "uint16"]
UInt32 = Annotated[int, pa.uint32(), "uint32"]
UInt64 = Annotated[int, pa.uint64(), "uint64"]
Float32 = Annotated[float, pa.float32(), "float32"]
Float64 = Annotated[float, pa.float64(), "float64"]
Decimal = Annotated[decimal.Decimal, arrow_udf.DecimalType(), "decimal"]
Date32 = Annotated[datetime.datetime, pa.date32(), "date32"]
Time64 = Annotated[datetime.timedelta, pa.time64("us"), "time64"]
Timestamp = Annotated[datetime.datetime, pa.timestamp("us"), "timestamp"]
Interval = Annotated[pa.MonthDayNano, pa.month_day_nano_interval(), "interval"]
String = Annotated[str, pa.string(), "string"]
LargeString = Annotated[str, pa.large_string(), "large_string"]
Json = Annotated[Dict[str, Any], arrow_udf.JsonType(), "json"]
Binary = Annotated[bytes, pa.binary(), "binary"]
LargeBinary = Annotated[bytes, pa.large_binary(), "large_binary"]

StructType = TypedDict
"""
`StructType` is an alias of `typing.TypedDict`.
User can use this to define a custom struct type. For example::

    from arrow_udf.typing import StructType, String, Int32, Json

    class TcpInfo(StructType):
        src_addr: String
        dst_addr: String
        src_port: Int32
        dst_port: Int32

    @server.udf
    def foo(x: Json) -> TcpInfo: ...
"""
