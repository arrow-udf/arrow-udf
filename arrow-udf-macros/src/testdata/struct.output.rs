#[export_name = "arrowudt_RGF0YT1hOnZvaWQsYjpib29sZWFuLGM6aW50MixkOmludDQsZTppbnQ4LGY6ZmxvYXQ0LGc6ZmxvYXQ4LGg6ZGVjaW1hbCxpOmRhdGUsajp0aW1lLGs6dGltZXN0YW1wLGw6aW50ZXJ2YWwsbTpqc29uLG46dmFyY2hhcixvOmJ5dGVhLHA6dmFyY2hhcltdLHE6c3RydWN0IEtleVZhbHVl"]
static DATA_METADATA: () = ();
impl ::arrow_udf::types::StructType for Data {
    fn fields() -> ::arrow_udf::codegen::arrow_schema::Fields {
        use ::arrow_udf::codegen::arrow_schema::{self, Field, TimeUnit, IntervalUnit};
        let fields: Vec<Field> = vec![
            Field::new("a", arrow_schema::DataType::Null, true), Field::new("b",
            arrow_schema::DataType::Boolean, true), Field::new("c",
            arrow_schema::DataType::Int16, true), Field::new("d",
            arrow_schema::DataType::Int32, true), Field::new("e",
            arrow_schema::DataType::Int64, true), Field::new("f",
            arrow_schema::DataType::Float32, true), Field::new("g",
            arrow_schema::DataType::Float64, true), Field::new("h",
            arrow_schema::DataType::LargeBinary, true), Field::new("i",
            arrow_schema::DataType::Date32, true), Field::new("j",
            arrow_schema::DataType::Time64(TimeUnit::Microsecond), true), Field::new("k",
            arrow_schema::DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("l", arrow_schema::DataType::Interval(IntervalUnit::MonthDayNano),
            true), Field::new("m", arrow_schema::DataType::LargeUtf8, true),
            Field::new("n", arrow_schema::DataType::Utf8, true), Field::new("o",
            arrow_schema::DataType::Binary, true), Field::new("p",
            arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new("item",
            arrow_schema::DataType::Utf8, true))), true), Field::new("q",
            arrow_schema::DataType::Struct(KeyValue::fields()), true),
        ];
        fields.into()
    }
    fn append_to(
        self,
        builder: &mut ::arrow_udf::codegen::arrow_array::builder::StructBuilder,
    ) {
        use ::arrow_udf::codegen::arrow_array::builder::*;
        {
            let builder = builder.field_builder::<NullBuilder>(0usize).unwrap();
            let v = self.a;
            builder.append_empty_value()
        }
        {
            let builder = builder.field_builder::<BooleanBuilder>(1usize).unwrap();
            let v = self.b;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<Int16Builder>(2usize).unwrap();
            let v = self.c;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<Int32Builder>(3usize).unwrap();
            let v = self.d;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<Int64Builder>(4usize).unwrap();
            let v = self.e;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<Float32Builder>(5usize).unwrap();
            let v = self.f;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<Float64Builder>(6usize).unwrap();
            let v = self.g;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<LargeBinaryBuilder>(7usize).unwrap();
            let v = self.h;
            builder.append_value(v.to_string())
        }
        {
            let builder = builder.field_builder::<Date32Builder>(8usize).unwrap();
            let v = self.i;
            builder.append_value(arrow_array::types::Date32Type::from_naive_date(v))
        }
        {
            let builder = builder
                .field_builder::<Time64MicrosecondBuilder>(9usize)
                .unwrap();
            let v = self.j;
            builder.append_value(arrow_array::temporal_conversions::time_to_time64us(v))
        }
        {
            let builder = builder
                .field_builder::<TimestampMicrosecondBuilder>(10usize)
                .unwrap();
            let v = self.k;
            builder.append_value(v.and_utc().timestamp_micros())
        }
        {
            let builder = builder
                .field_builder::<IntervalMonthDayNanoBuilder>(11usize)
                .unwrap();
            let v = self.l;
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
            let builder = builder.field_builder::<LargeStringBuilder>(12usize).unwrap();
            let v = self.m;
            {
                use std::fmt::Write;
                write!(builder, "{}", v).expect("write json");
                builder.append_value("");
            }
        }
        {
            let builder = builder.field_builder::<StringBuilder>(13usize).unwrap();
            let v = self.n;
            builder.append_value(v)
        }
        {
            let builder = builder.field_builder::<BinaryBuilder>(14usize).unwrap();
            let v = self.o;
            builder.append_value(v)
        }
        {
            let builder = builder
                .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(15usize)
                .unwrap();
            let v = self.p;
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
            let builder = builder.field_builder::<StructBuilder>(16usize).unwrap();
            let v = self.q;
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
            let builder = builder.field_builder::<Int16Builder>(2usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<Int32Builder>(3usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<Int64Builder>(4usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<Float32Builder>(5usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<Float64Builder>(6usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<LargeBinaryBuilder>(7usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<Date32Builder>(8usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder
                .field_builder::<Time64MicrosecondBuilder>(9usize)
                .unwrap();
            builder.append_null()
        }
        {
            let builder = builder
                .field_builder::<TimestampMicrosecondBuilder>(10usize)
                .unwrap();
            builder.append_null()
        }
        {
            let builder = builder
                .field_builder::<IntervalMonthDayNanoBuilder>(11usize)
                .unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<LargeStringBuilder>(12usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<StringBuilder>(13usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<BinaryBuilder>(14usize).unwrap();
            builder.append_null()
        }
        {
            let builder = builder
                .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(15usize)
                .unwrap();
            builder.append_null()
        }
        {
            let builder = builder.field_builder::<StructBuilder>(16usize).unwrap();
            KeyValue::append_null(builder)
        }
        builder.append_null();
    }
}
