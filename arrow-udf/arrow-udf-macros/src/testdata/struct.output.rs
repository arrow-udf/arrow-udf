#[export_name = "arrowudt_RGF0YT1udWxsOm51bGwsYm9vbGVhbjpib29sZWFuLGludDg6aW50OCxpbnQxNjppbnQxNixpbnQzMjppbnQzMixpbnQ2NDppbnQ2NCx1aW50ODp1aW50OCx1aW50MTY6dWludDE2LHVpbnQzMjp1aW50MzIsdWludDY0OnVpbnQ2NCxmbG9hdDMyOmZsb2F0MzIsZmxvYXQ2NDpmbG9hdDY0LGRlY2ltYWw6ZGVjaW1hbCxkYXRlOmRhdGUzMix0aW1lOnRpbWU2NCx0aW1lc3RhbXA6dGltZXN0YW1wLGludGVydmFsOmludGVydmFsLGpzb246anNvbixzdHJpbmc6c3RyaW5nLGJpbmFyeTpiaW5hcnksc3RyaW5nX2FycmF5OnN0cmluZ1tdLHN0cnVjdF86c3RydWN0IEtleVZhbHVl"]
static DATA_METADATA: () = ();
impl ::arrow_udf::types::StructType for Data {
    fn fields() -> ::arrow_udf::codegen::arrow_schema::Fields {
        use ::arrow_udf::codegen::arrow_schema::{self, Field, TimeUnit, IntervalUnit};
        vec![
            arrow_schema::Field::new("null", arrow_schema::DataType::Null, true),
            arrow_schema::Field::new("boolean", arrow_schema::DataType::Boolean, true),
            arrow_schema::Field::new("int8", arrow_schema::DataType::Int8, true),
            arrow_schema::Field::new("int16", arrow_schema::DataType::Int16, true),
            arrow_schema::Field::new("int32", arrow_schema::DataType::Int32, true),
            arrow_schema::Field::new("int64", arrow_schema::DataType::Int64, true),
            arrow_schema::Field::new("uint8", arrow_schema::DataType::UInt8, true),
            arrow_schema::Field::new("uint16", arrow_schema::DataType::UInt16, true),
            arrow_schema::Field::new("uint32", arrow_schema::DataType::UInt32, true),
            arrow_schema::Field::new("uint64", arrow_schema::DataType::UInt64, true),
            arrow_schema::Field::new("float32", arrow_schema::DataType::Float32, true),
            arrow_schema::Field::new("float64", arrow_schema::DataType::Float64, true),
            arrow_schema::Field::new("decimal", arrow_schema::DataType::Utf8, true)
            .with_metadata([("ARROW:extension:name".into(), "arrowudf.decimal".into())]
            .into()), arrow_schema::Field::new("date", arrow_schema::DataType::Date32,
            true), arrow_schema::Field::new("time",
            arrow_schema::DataType::Time64(TimeUnit::Microsecond), true),
            arrow_schema::Field::new("timestamp",
            arrow_schema::DataType::Timestamp(TimeUnit::Microsecond, None), true),
            arrow_schema::Field::new("interval",
            arrow_schema::DataType::Interval(IntervalUnit::MonthDayNano), true),
            arrow_schema::Field::new("json", arrow_schema::DataType::Utf8, true)
            .with_metadata([("ARROW:extension:name".into(), "arrowudf.json".into())]
            .into()), arrow_schema::Field::new("string", arrow_schema::DataType::Utf8,
            true), arrow_schema::Field::new("binary", arrow_schema::DataType::Binary,
            true), arrow_schema::Field::new("string_array",
            arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new("item",
            arrow_schema::DataType::Utf8, true))), true),
            arrow_schema::Field::new("struct_",
            arrow_schema::DataType::Struct(KeyValue::fields()), true)
        ]
            .into()
    }
    fn append_to(
        self,
        builder: &mut ::arrow_udf::codegen::arrow_array::builder::StructBuilder,
    ) {
        use ::arrow_udf::codegen::arrow_array::builder::*;
        {
            let builder = builder.field_builder::<NullBuilder>(0usize).unwrap();
            let v = self.null;
            builder.append_empty_value()
        }
        {
            let builder = builder.field_builder::<BooleanBuilder>(1usize).unwrap();
            let v = self.boolean;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<Int8Builder>(2usize).unwrap();
            let v = self.int8;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<Int16Builder>(3usize).unwrap();
            let v = self.int16;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<Int32Builder>(4usize).unwrap();
            let v = self.int32;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<Int64Builder>(5usize).unwrap();
            let v = self.int64;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<UInt8Builder>(6usize).unwrap();
            let v = self.uint8;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<UInt16Builder>(7usize).unwrap();
            let v = self.uint16;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<UInt32Builder>(8usize).unwrap();
            let v = self.uint32;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<UInt64Builder>(9usize).unwrap();
            let v = self.uint64;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<Float32Builder>(10usize).unwrap();
            let v = self.float32;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<Float64Builder>(11usize).unwrap();
            let v = self.float64;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<StringBuilder>(12usize).unwrap();
            let v = self.decimal;
            builder.append_value(v.to_string())
        }
        {
            let builder = builder.field_builder::<Date32Builder>(13usize).unwrap();
            let v = self.date;
            builder.append_value(arrow_array::types::Date32Type::from_naive_date(v))
        }
        {
            let builder = builder
                .field_builder::<Time64MicrosecondBuilder>(14usize)
                .unwrap();
            let v = self.time;
            builder.append_value(arrow_array::temporal_conversions::time_to_time64us(v))
        }
        {
            let builder = builder
                .field_builder::<TimestampMicrosecondBuilder>(15usize)
                .unwrap();
            let v = self.timestamp;
            builder.append_value(v.and_utc().timestamp_micros())
        }
        {
            let builder = builder
                .field_builder::<IntervalMonthDayNanoBuilder>(16usize)
                .unwrap();
            let v = self.interval;
            builder
                .append_value({
                    let v: arrow_udf::types::Interval = v.into();
                    arrow_array::types::IntervalMonthDayNanoType::make_value(
                        v.months,
                        v.days,
                        v.nanos,
                    )
                })
        }
        {
            let builder = builder.field_builder::<StringBuilder>(17usize).unwrap();
            let v = self.json;
            {
                use std::fmt::Write;
                write!(builder, "{}", v).expect("write json");
                builder.append_value("");
            }
        }
        {
            let builder = builder.field_builder::<StringBuilder>(18usize).unwrap();
            let v = self.string;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<BinaryBuilder>(19usize).unwrap();
            let v = self.binary;
            builder.append_value(v)
        }
        {
            let builder = builder
                .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(20usize)
                .unwrap();
            let v = self.string_array;
            {
                let value_builder = builder
                    .values()
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .expect("downcast list value builder");
                value_builder.extend(v.into_iter().map(Some));
                builder.append(true);
            }
        }
        {
            let builder = builder.field_builder::<StructBuilder>(21usize).unwrap();
            let v = self.struct_;
            {
                v.append_to(builder);
            }
        }
        builder.append(true);
    }
    fn append_null(
        builder: &mut ::arrow_udf::codegen::arrow_array::builder::StructBuilder,
    ) {
        use ::arrow_udf::codegen::arrow_array::builder::*;
        {
            let builder = builder.field_builder::<NullBuilder>(0usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<BooleanBuilder>(1usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<Int8Builder>(2usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<Int16Builder>(3usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<Int32Builder>(4usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<Int64Builder>(5usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<UInt8Builder>(6usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<UInt16Builder>(7usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<UInt32Builder>(8usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<UInt64Builder>(9usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<Float32Builder>(10usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<Float64Builder>(11usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<StringBuilder>(12usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<Date32Builder>(13usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder
                .field_builder::<Time64MicrosecondBuilder>(14usize)
                .unwrap();
            builder.append_null()
        }
        {
            let builder = builder
                .field_builder::<TimestampMicrosecondBuilder>(15usize)
                .unwrap();
            builder.append_null()
        }
        {
            let builder = builder
                .field_builder::<IntervalMonthDayNanoBuilder>(16usize)
                .unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<StringBuilder>(17usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<StringBuilder>(18usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<BinaryBuilder>(19usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder
                .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(20usize)
                .unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<StructBuilder>(21usize).unwrap();
            KeyValue::append_null(builder)
        }
        builder.append_null();
    }
}
