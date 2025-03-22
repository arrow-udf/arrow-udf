# Arrow UDF Python Server

## Installation

```sh
pip install arrow-udf
```

## Usage

Define functions in a Python file:

```python
# udf.py
from arrow_udf import udf, udtf, UdfServer
import struct
import socket

# Define a scalar function
@udf(input_types=['INT', 'INT'], result_type='INT')
def gcd(x, y):
    while y != 0:
        (x, y) = (y, x % y)
    return x

# Define a scalar function that returns multiple values (within a struct)
@udf(input_types=['BINARY'], result_type='STRUCT<src_addr: STRING, dst_addr: STRING, src_port: INT16, dst_port: INT16>')
def extract_tcp_info(tcp_packet: bytes):
    src_addr, dst_addr = struct.unpack('!4s4s', tcp_packet[12:20])
    src_port, dst_port = struct.unpack('!HH', tcp_packet[20:24])
    src_addr = socket.inet_ntoa(src_addr)
    dst_addr = socket.inet_ntoa(dst_addr)
    return {
        'src_addr': src_addr,
        'dst_addr': dst_addr,
        'src_port': src_port,
        'dst_port': dst_port,
    }

# Define a table function
@udtf(input_types='INT', result_types='INT')
def series(n):
    for i in range(n):
        yield i

# Start a UDF server
if __name__ == '__main__':
    server = UdfServer(location="0.0.0.0:8815")
    server.add_function(gcd)
    server.add_function(extract_tcp_info)
    server.add_function(series)
    server.serve()
```

Start the UDF server:

```sh
python3 udf.py
```

## Data Types

| Arrow Type           | Python Type                    |
| -------------------- | ------------------------------ |
| `boolean`            | `bool`                         |
| `int8`               | `int`                          |
| `int16`              | `int`                          |
| `int32`              | `int`                          |
| `int64`              | `int`                          |
| `uint8`              | `int`                          |
| `uint16`             | `int`                          |
| `uint32`             | `int`                          |
| `uint64`             | `int`                          |
| `float32`            | `float`                        |
| `float32`            | `float`                        |
| `date32`             | `datetime.date`                |
| `time64`             | `datetime.time`                |
| `timestamp`          | `datetime.datetime`            |
| `interval`           | `MonthDayNano` / `(int, int, int)` (fields can be obtained by `months()`, `days()` and `nanoseconds()` from `MonthDayNano`) |
| `string`             | `str`                          |
| `binary`             | `bytes`                        |
| `large_string`       | `str`                          |
| `large_binary`       | `bytes`                        |

Extension types:

| Data type   | Metadata            | Python Type           |
| ----------- | ------------------- | --------------------- |
| `decimal`   | `arrowudf.decimal`  | `decimal.Decimal`     |
| `json`      | `arrowudf.json`     | `any`                 |
