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

from typing import *
import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
import inspect
import traceback
import json
from concurrent.futures import ThreadPoolExecutor
import concurrent
from decimal import Decimal
import signal


class UserDefinedFunction:
    """
    Base interface for user-defined function.
    """

    _name: str
    _input_schema: pa.Schema
    _result_schema: pa.Schema
    _io_threads: Optional[int]
    _executor: Optional[ThreadPoolExecutor]

    def eval_batch(self, batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        """
        Apply the function on a batch of inputs.
        """
        return iter([])


class ScalarFunction(UserDefinedFunction):
    """
    Base interface for user-defined scalar function. A user-defined scalar functions maps zero, one,
    or multiple scalar values to a new scalar value.
    """

    def __init__(self, *args, **kwargs):
        self._io_threads = kwargs.pop("io_threads")
        self._executor = (
            ThreadPoolExecutor(max_workers=self._io_threads)
            if self._io_threads is not None
            else None
        )
        super().__init__(*args, **kwargs)

    def eval(self, *args) -> Any:
        """
        Method which defines the logic of the scalar function.
        """
        pass

    def eval_batch(self, batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        # parse value from json string for jsonb columns
        inputs = [[v.as_py() for v in array] for array in batch]
        inputs = [
            _process_func(pa.list_(type), False)(array)
            for array, type in zip(inputs, self._input_schema.types)
        ]
        if self._executor is not None:
            # evaluate the function for each row
            tasks = [
                self._executor.submit(self._func, *[col[i] for col in inputs])
                for i in range(batch.num_rows)
            ]
            column = [
                future.result() for future in concurrent.futures.as_completed(tasks)
            ]
        else:
            # evaluate the function for each row
            column = [
                self.eval(*[col[i] for col in inputs]) for i in range(batch.num_rows)
            ]

        column = _process_func(pa.list_(self._result_schema.types[0]), True)(column)

        array = pa.array(column, type=self._result_schema.types[0])
        yield pa.RecordBatch.from_arrays([array], schema=self._result_schema)


def _process_func(type: pa.DataType, output: bool) -> Callable:
    """Return a function to process input or output value."""
    if pa.types.is_list(type):
        func = _process_func(type.value_type, output)
        return lambda array: [(func(v) if v is not None else None) for v in array]

    if pa.types.is_struct(type):
        funcs = [_process_func(field.type, output) for field in type]
        if output:
            return lambda tup: tuple(
                (func(v) if v is not None else None) for v, func in zip(tup, funcs)
            )
        else:
            # the input value of struct type is a dict
            # we convert it into tuple here
            return lambda map: tuple(
                (func(v) if v is not None else None)
                for v, func in zip(map.values(), funcs)
            )

    if type.equals(JSONB):
        if output:
            return lambda v: json.dumps(v)
        else:
            return lambda v: json.loads(v)

    if type.equals(UNCONSTRAINED_DECIMAL):
        if output:

            def decimal_to_str(v):
                if not isinstance(v, Decimal):
                    raise ValueError(f"Expected Decimal, got {v}")
                # use `f` format to avoid scientific notation, e.g. `1e10`
                return format(v, "f").encode("utf-8")

            return decimal_to_str
        else:
            return lambda v: Decimal(v.decode("utf-8"))

    return lambda v: v


class TableFunction(UserDefinedFunction):
    """
    Base interface for user-defined table function. A user-defined table functions maps zero, one,
    or multiple scalar values to a new table value.
    """

    BATCH_SIZE = 1024

    def eval(self, *args) -> Iterator:
        """
        Method which defines the logic of the table function.
        """
        yield

    def eval_batch(self, batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        class RecordBatchBuilder:
            """A utility class for constructing Arrow RecordBatch by row."""

            schema: pa.Schema
            columns: List[List]

            def __init__(self, schema: pa.Schema):
                self.schema = schema
                self.columns = [[] for _ in self.schema.types]

            def len(self) -> int:
                """Returns the number of rows in the RecordBatch being built."""
                return len(self.columns[0])

            def append(self, index: int, value: Any):
                """Appends a new row to the RecordBatch being built."""
                self.columns[0].append(index)
                self.columns[1].append(value)

            def build(self) -> pa.RecordBatch:
                """Builds the RecordBatch from the accumulated data and clears the state."""
                # Convert the columns to arrow arrays
                arrays = [
                    pa.array(col, type)
                    for col, type in zip(self.columns, self.schema.types)
                ]
                # Reset columns
                self.columns = [[] for _ in self.schema.types]
                return pa.RecordBatch.from_arrays(arrays, schema=self.schema)

        builder = RecordBatchBuilder(self._result_schema)

        # Iterate through rows in the input RecordBatch
        for row_index in range(batch.num_rows):
            row = tuple(column[row_index].as_py() for column in batch)
            for result in self.eval(*row):
                builder.append(row_index, result)
                if builder.len() == self.BATCH_SIZE:
                    yield builder.build()
        if builder.len() != 0:
            yield builder.build()


class UserDefinedScalarFunctionWrapper(ScalarFunction):
    """
    Base Wrapper for Python user-defined scalar function.
    """

    _func: Callable

    def __init__(self, func, input_types, result_type, name=None, io_threads=None):
        self._func = func
        self._input_schema = pa.schema(
            zip(
                inspect.getfullargspec(func)[0],
                [_to_data_type(t) for t in _to_list(input_types)],
            )
        )
        self._result_schema = pa.schema([("output", _to_data_type(result_type))])
        self._name = name or (
            func.__name__ if hasattr(func, "__name__") else func.__class__.__name__
        )
        super().__init__(io_threads=io_threads)

    def __call__(self, *args):
        return self._func(*args)

    def eval(self, *args):
        return self._func(*args)


class UserDefinedTableFunctionWrapper(TableFunction):
    """
    Base Wrapper for Python user-defined table function.
    """

    _func: Callable

    def __init__(self, func, input_types, result_types, name=None):
        self._func = func
        self._name = name or (
            func.__name__ if hasattr(func, "__name__") else func.__class__.__name__
        )
        self._input_schema = pa.schema(
            zip(
                inspect.getfullargspec(func)[0],
                [_to_data_type(t) for t in _to_list(input_types)],
            )
        )
        self._result_schema = pa.schema(
            [
                ("row_index", pa.int32()),
                (
                    self._name,
                    pa.struct([("", _to_data_type(t)) for t in result_types])
                    if isinstance(result_types, list)
                    else _to_data_type(result_types),
                ),
            ]
        )

    def __call__(self, *args):
        return self._func(*args)

    def eval(self, *args):
        return self._func(*args)


def _to_list(x):
    if isinstance(x, list):
        return x
    else:
        return [x]


def udf(
    input_types: Union[List[Union[str, pa.DataType]], Union[str, pa.DataType]],
    result_type: Union[str, pa.DataType],
    name: Optional[str] = None,
    io_threads: Optional[int] = None,
) -> Callable:
    """
    Annotation for creating a user-defined scalar function.

    Parameters:
    - input_types: A list of strings or Arrow data types that specifies the input data types.
    - result_type: A string or an Arrow data type that specifies the return value type.
    - name: An optional string specifying the function name. If not provided, the original name will be used.
    - io_threads: Number of I/O threads used per data chunk for I/O bound functions.

    Example:
    ```
    @udf(input_types=['INT', 'INT'], result_type='INT')
    def gcd(x, y):
        while y != 0:
            (x, y) = (y, x % y)
        return x
    ```

    I/O bound Example:
    ```
    @udf(input_types=['INT'], result_type='INT', io_threads=64)
    def external_api(x):
        response = requests.get(my_endpoint + '?param=' + x)
        return response["data"]
    ```
    """

    if io_threads is not None and io_threads > 1:
        return lambda f: UserDefinedScalarFunctionWrapper(
            f, input_types, result_type, name, io_threads=io_threads
        )
    else:
        return lambda f: UserDefinedScalarFunctionWrapper(
            f, input_types, result_type, name
        )


def udtf(
    input_types: Union[List[Union[str, pa.DataType]], Union[str, pa.DataType]],
    result_types: Union[List[Union[str, pa.DataType]], Union[str, pa.DataType]],
    name: Optional[str] = None,
) -> Callable:
    """
    Annotation for creating a user-defined table function.

    Parameters:
    - input_types: A list of strings or Arrow data types that specifies the input data types.
    - result_types A list of strings or Arrow data types that specifies the return value types.
    - name: An optional string specifying the function name. If not provided, the original name will be used.

    Example:
    ```
    @udtf(input_types='INT', result_types='INT')
    def series(n):
        for i in range(n):
            yield i
    ```
    """

    return lambda f: UserDefinedTableFunctionWrapper(f, input_types, result_types, name)


class UdfServer(pa.flight.FlightServerBase):
    """
    A server that provides user-defined functions to clients.

    Example:
    ```
    server = UdfServer(location="0.0.0.0:8815")
    server.add_function(my_udf)
    server.serve()
    ```
    """

    # UDF server based on Apache Arrow Flight protocol.
    # Reference: https://arrow.apache.org/cookbook/py/flight.html#simple-parquet-storage-service-with-arrow-flight

    _location: str
    _functions: Dict[str, UserDefinedFunction]

    def __init__(self, location="0.0.0.0:8815", **kwargs):
        super(UdfServer, self).__init__("grpc://" + location, **kwargs)
        self._location = location
        self._functions = {}

    def get_flight_info(self, context, descriptor):
        """Return the result schema of a function."""
        udf = self._functions[descriptor.path[0].decode("utf-8")]
        # return the concatenation of input and output schema
        full_schema = pa.schema(list(udf._input_schema) + list(udf._result_schema))
        # we use `total_records` to indicate the number of input arguments
        return pa.flight.FlightInfo(
            schema=full_schema,
            descriptor=descriptor,
            endpoints=[],
            total_records=len(udf._input_schema),
            total_bytes=0,
        )

    def add_function(self, udf: UserDefinedFunction):
        """Add a function to the server."""
        name = udf._name
        if name in self._functions:
            raise ValueError("Function already exists: " + name)

        input_types = ",".join(
            [_data_type_to_string(t) for t in udf._input_schema.types]
        )
        if isinstance(udf, TableFunction):
            output_type = udf._result_schema.types[-1]
            if isinstance(output_type, pa.StructType):
                output_type = ",".join(
                    f"field_{i} {_data_type_to_string(field.type)}"
                    for i, field in enumerate(output_type)
                )
                output_type = f"TABLE({output_type})"
            else:
                output_type = _data_type_to_string(output_type)
                output_type = f"TABLE(output {output_type})"
        else:
            output_type = _data_type_to_string(udf._result_schema.types[-1])

        sql = f"CREATE FUNCTION {name}({input_types}) RETURNS {output_type} AS '{name}' USING LINK 'http://{self._location}';"
        print(f"added function: {name}, corresponding SQL:\n{sql}\n")
        self._functions[name] = udf

    def do_exchange(self, context, descriptor, reader, writer):
        """Call a function from the client."""
        udf = self._functions[descriptor.path[0].decode("utf-8")]
        writer.begin(udf._result_schema)
        try:
            for batch in reader:
                # print(pa.Table.from_batches([batch.data]))
                for output_batch in udf.eval_batch(batch.data):
                    writer.write_batch(output_batch)
        except Exception as e:
            print(traceback.print_exc())
            raise e

    def serve(self):
        """
        Block until the server shuts down.

        This method only returns if shutdown() is called or a signal (SIGINT, SIGTERM) received.
        """
        print(
            "Note: You can use arbitrary function names and struct field names in CREATE FUNCTION statements."
            f"\n\nlistening on {self._location}"
        )
        signal.signal(signal.SIGTERM, lambda s, f: self.shutdown())
        super(UdfServer, self).serve()


def _to_data_type(t: Union[str, pa.DataType]) -> pa.DataType:
    """
    Convert a SQL data type string or `pyarrow.DataType` to `pyarrow.DataType`.
    """
    if isinstance(t, str):
        return _string_to_data_type(t)
    else:
        return t


# we use `large_binary` to represent unconstrained decimal type
UNCONSTRAINED_DECIMAL = pa.large_binary()
JSONB = pa.large_string()


def _string_to_data_type(type_str: str):
    """
    Convert a SQL data type string to `pyarrow.DataType`.
    """
    type_str = type_str.upper()
    if type_str.endswith("[]"):
        return pa.list_(_string_to_data_type(type_str[:-2]))
    elif type_str in ("BOOLEAN", "BOOL"):
        return pa.bool_()
    elif type_str in ("SMALLINT", "INT2"):
        return pa.int16()
    elif type_str in ("INT", "INTEGER", "INT4"):
        return pa.int32()
    elif type_str in ("BIGINT", "INT8"):
        return pa.int64()
    elif type_str in ("FLOAT4", "REAL"):
        return pa.float32()
    elif type_str in ("FLOAT8", "DOUBLE PRECISION"):
        return pa.float64()
    elif type_str.startswith("DECIMAL") or type_str.startswith("NUMERIC"):
        if type_str == "DECIMAL" or type_str == "NUMERIC":
            return UNCONSTRAINED_DECIMAL
        rest = type_str[8:-1]  # remove "DECIMAL(" and ")"
        if "," in rest:
            precision, scale = rest.split(",")
            return pa.decimal128(int(precision), int(scale))
        else:
            return pa.decimal128(int(rest), 0)
    elif type_str in ("DATE"):
        return pa.date32()
    elif type_str in ("TIME", "TIME WITHOUT TIME ZONE"):
        return pa.time64("us")
    elif type_str in ("TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE"):
        return pa.timestamp("us")
    elif type_str.startswith("INTERVAL"):
        return pa.month_day_nano_interval()
    elif type_str in ("VARCHAR"):
        return pa.string()
    elif type_str in ("JSONB"):
        return JSONB
    elif type_str in ("BYTEA"):
        return pa.binary()
    elif type_str.startswith("STRUCT"):
        # extract 'STRUCT<INT, VARCHAR, STRUCT<INT>, ...>'
        type_list = type_str[7:-1]  # strip "STRUCT<>"
        fields = []
        elements = []
        start = 0
        depth = 0
        for i, c in enumerate(type_list):
            if c == "<":
                depth += 1
            elif c == ">":
                depth -= 1
            elif c == "," and depth == 0:
                type_str = type_list[start:i].strip()
                fields.append(pa.field("", _string_to_data_type(type_str)))
                start = i + 1
        type_str = type_list[start:].strip()
        fields.append(pa.field("", _string_to_data_type(type_str)))
        return pa.struct(fields)

    raise ValueError(f"Unsupported type: {type_str}")


def _data_type_to_string(t: pa.DataType) -> str:
    """
    Convert a `pyarrow.DataType` to a SQL data type string.
    """
    if isinstance(t, pa.ListType):
        return _data_type_to_string(t.value_type) + "[]"
    elif t.equals(pa.bool_()):
        return "BOOLEAN"
    elif t.equals(pa.int16()):
        return "SMALLINT"
    elif t.equals(pa.int32()):
        return "INT"
    elif t.equals(pa.int64()):
        return "BIGINT"
    elif t.equals(pa.float32()):
        return "FLOAT4"
    elif t.equals(pa.float64()):
        return "FLOAT8"
    elif t.equals(UNCONSTRAINED_DECIMAL):
        return "DECIMAL"
    elif pa.types.is_decimal(t):
        return f"DECIMAL({t.precision},{t.scale})"
    elif t.equals(pa.date32()):
        return "DATE"
    elif t.equals(pa.time64("us")):
        return "TIME"
    elif t.equals(pa.timestamp("us")):
        return "TIMESTAMP"
    elif t.equals(pa.month_day_nano_interval()):
        return "INTERVAL"
    elif t.equals(pa.string()):
        return "VARCHAR"
    elif t.equals(JSONB):
        return "JSONB"
    elif t.equals(pa.binary()):
        return "BYTEA"
    elif isinstance(t, pa.StructType):
        return (
            "STRUCT<"
            + ",".join(
                f"f{i+1} {_data_type_to_string(field.type)}"
                for i, field in enumerate(t)
            )
            + ">"
        )
    else:
        raise ValueError(f"Unsupported type: {t}")
