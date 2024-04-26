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

package com.risingwave.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Unit test for UDF server. */
public class TestUdfServer {
    private static UdfClient client;
    private static UdfServer server;
    private static BufferAllocator allocator = new RootAllocator();

    @BeforeAll
    public static void setup() throws IOException {
        server = new UdfServer("localhost", 0);
        server.addFunction("gcd", new Gcd());
        server.addFunction("return_all", new ReturnAll());
        server.addFunction("series", new Series());
        server.start();

        client = new UdfClient("localhost", server.getPort());
    }

    @AfterAll
    public static void teardown() throws InterruptedException {
        client.close();
        server.close();
    }

    public static class Gcd implements ScalarFunction {
        public int eval(int a, int b) {
            while (b != 0) {
                int temp = b;
                b = a % b;
                a = temp;
            }
            return a;
        }
    }

    @Test
    public void gcd() throws Exception {
        var c0 = new IntVector("", allocator);
        c0.allocateNew(1);
        c0.set(0, 15);
        c0.setValueCount(1);

        var c1 = new IntVector("", allocator);
        c1.allocateNew(1);
        c1.set(0, 12);
        c1.setValueCount(1);

        var input = VectorSchemaRoot.of(c0, c1);

        try (var stream = client.call("gcd", input)) {
            var output = stream.getRoot();
            assertTrue(stream.next());
            assertEquals("3", output.contentToTSVString().trim());
        }
    }

    public static class ReturnAll implements ScalarFunction {
        public static class Row {
            public Boolean bool;
            public Byte i8;
            public Short i16;
            public Integer i32;
            public Long i64;
            public @DataTypeHint("uint8") Byte u8;
            public Character u16;
            public @DataTypeHint("uint32") Integer u32;
            public @DataTypeHint("uint64") Long u64;
            public Float f32;
            public Double f64;
            public BigDecimal decimal;
            public LocalDate date;
            public LocalTime time;
            public LocalDateTime timestamp;
            public PeriodDuration interval;
            public String str;
            public byte[] bytes;
            public @DataTypeHint("json") String json;
            public Struct struct;
        }

        public static class Struct {
            public Integer f1;
            public Integer f2;

            public String toString() {
                return String.format("(%d, %d)", f1, f2);
            }
        }

        public Row eval(
                Boolean bool,
                Byte i8,
                Short i16,
                Integer i32,
                Long i64,
                @DataTypeHint("uint8") Byte u8,
                Character u16,
                @DataTypeHint("uint32") Integer u32,
                @DataTypeHint("uint64") Long u64,
                Float f32,
                Double f64,
                BigDecimal decimal,
                LocalDate date,
                LocalTime time,
                LocalDateTime timestamp,
                PeriodDuration interval,
                String str,
                byte[] bytes,
                @DataTypeHint("json") String json,
                Struct struct) {
            var row = new Row();
            row.bool = bool;
            row.i8 = i8;
            row.i16 = i16;
            row.i32 = i32;
            row.i64 = i64;
            row.u8 = u8;
            row.u16 = u16;
            row.u32 = u32;
            row.u64 = u64;
            row.f32 = f32;
            row.f64 = f64;
            row.decimal = decimal;
            row.date = date;
            row.time = time;
            row.timestamp = timestamp;
            row.interval = interval;
            row.str = str;
            row.bytes = bytes;
            row.json = json;
            row.struct = struct;
            return row;
        }
    }

    @Test
    public void all_types() throws Exception {
        var c0 = new BitVector("", allocator);
        c0.allocateNew(2);
        c0.set(0, 1);
        c0.setValueCount(2);

        var c1 = new TinyIntVector("", allocator);
        c1.allocateNew(2);
        c1.set(0, 1);
        c1.setValueCount(2);

        var c2 = new SmallIntVector("", allocator);
        c2.allocateNew(2);
        c2.set(0, 1);
        c2.setValueCount(2);

        var c3 = new IntVector("", allocator);
        c3.allocateNew(2);
        c3.set(0, 1);
        c3.setValueCount(2);

        var c4 = new BigIntVector("", allocator);
        c4.allocateNew(2);
        c4.set(0, 1);
        c4.setValueCount(2);

        var c5 = new UInt1Vector("", allocator);
        c5.allocateNew(2);
        c5.set(0, 1);
        c5.setValueCount(2);

        var c6 = new UInt2Vector("", allocator);
        c6.allocateNew(2);
        c6.set(0, 1);
        c6.setValueCount(2);

        var c7 = new UInt4Vector("", allocator);
        c7.allocateNew(2);
        c7.set(0, 1);
        c7.setValueCount(2);

        var c8 = new UInt8Vector("", allocator);
        c8.allocateNew(2);
        c8.set(0, 1);
        c8.setValueCount(2);

        var c9 = new Float4Vector("", allocator);
        c9.allocateNew(2);
        c9.set(0, 1);
        c9.setValueCount(2);

        var c10 = new Float8Vector("", allocator);
        c10.allocateNew(2);
        c10.set(0, 1);
        c10.setValueCount(2);

        var c11 = new LargeVarBinaryVector("", allocator);
        c11.allocateNew(2);
        c11.set(0, "1.234".getBytes());
        c11.setValueCount(2);

        var c12 = new DateDayVector("", allocator);
        c12.allocateNew(2);
        c12.set(0, (int) LocalDate.of(2023, 1, 1).toEpochDay());
        c12.setValueCount(2);

        var c13 = new TimeMicroVector("", allocator);
        c13.allocateNew(2);
        c13.set(0, LocalTime.of(1, 2, 3).toNanoOfDay() / 1000);
        c13.setValueCount(2);

        var c14 = new TimeStampMicroVector("", allocator);
        c14.allocateNew(2);
        var ts = LocalDateTime.of(2023, 1, 1, 1, 2, 3);
        c14.set(
                0,
                ts.toLocalDate().toEpochDay() * 24 * 3600 * 1000000
                        + ts.toLocalTime().toNanoOfDay() / 1000);
        c14.setValueCount(2);

        var c15 =
                new IntervalMonthDayNanoVector(
                        "",
                        FieldType.nullable(MinorType.INTERVALMONTHDAYNANO.getType()),
                        allocator);
        c15.allocateNew(2);
        c15.set(0, 1000, 2000, 3000);
        c15.setValueCount(2);

        var c16 = new VarCharVector("", allocator);
        c16.allocateNew(2);
        c16.set(0, "string".getBytes());
        c16.setValueCount(2);

        var c17 = new VarBinaryVector("", allocator);
        c17.allocateNew(2);
        c17.set(0, "bytes".getBytes());
        c17.setValueCount(2);

        var c18 = new LargeVarCharVector("", allocator);
        c18.allocateNew(2);
        c18.set(0, "{ key: 1 }".getBytes());
        c18.setValueCount(2);

        var c19 =
                new StructVector(
                        "", allocator, FieldType.nullable(ArrowType.Struct.INSTANCE), null);
        c19.allocateNew();
        var f1 = c19.addOrGet("f1", FieldType.nullable(MinorType.INT.getType()), IntVector.class);
        var f2 = c19.addOrGet("f2", FieldType.nullable(MinorType.INT.getType()), IntVector.class);
        f1.allocateNew(2);
        f2.allocateNew(2);
        f1.set(0, 1);
        f2.set(0, 2);
        c19.setIndexDefined(0);
        c19.setValueCount(2);

        var input =
                VectorSchemaRoot.of(
                        c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19);

        try (var stream = client.call("return_all", input)) {
            var output = stream.getRoot();
            assertTrue(stream.next());
            assertEquals(
                    "{\"bool\":true,\"i8\":1,\"i16\":1,\"i32\":1,\"i64\":1,\"u8\":1,\"u16\":\"\\u0001\",\"u32\":1,\"u64\":1,\"f32\":1.0,\"f64\":1.0,\"decimal\":\"MS4yMzQ=\",\"date\":19358,\"time\":3723000000,\"timestamp\":[2023,1,1,1,2,3],\"interval\":{\"period\":\"P1000M2000D\",\"duration\":0.000003000,\"units\":[\"YEARS\",\"MONTHS\",\"DAYS\",\"SECONDS\",\"NANOS\"]},\"str\":\"string\",\"bytes\":\"Ynl0ZXM=\",\"json\":\"{ key: 1 }\",\"struct\":{\"f1\":1,\"f2\":2}}\n{}",
                    output.contentToTSVString().trim());
        }
    }

    public static class Series implements TableFunction {
        public Iterator<Integer> eval(int n) {
            return IntStream.range(0, n).iterator();
        }
    }

    @Test
    public void series() throws Exception {
        var c0 = new IntVector("", allocator);
        c0.allocateNew(3);
        c0.set(0, 0);
        c0.set(1, 1);
        c0.set(2, 2);
        c0.setValueCount(3);

        var input = VectorSchemaRoot.of(c0);

        try (var stream = client.call("series", input)) {
            var output = stream.getRoot();
            assertTrue(stream.next());
            assertEquals("row_index\t\n1\t0\n2\t0\n2\t1\n", output.contentToTSVString());
        }
    }
}
