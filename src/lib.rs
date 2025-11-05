use std::fs::File;
use std::io;
use std::path::Path;

use io::Read;
use io::Seek;

use io::BufReader;

use arrow::array::ArrayRef;
use arrow::array::BooleanBuilder;
use arrow::array::Float64Builder;
use arrow::array::Int64Builder;
use arrow::array::StringBuilder;
use arrow::array::TimestampNanosecondBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::TimeUnit;

use arrow::record_batch::RecordBatch;

use calamine::Data;
use calamine::DataType as CalamineDataType;
use calamine::Range;
use calamine::Reader;
use chrono::NaiveDateTime;

use calamine::Xlsx;

pub trait ExcelReader {
    type ReadSeek: Read + Seek;
    type InternalReader: Reader<Self::ReadSeek>;

    fn internal_reader(&mut self) -> &mut Self::InternalReader;

    fn sheet_range(&mut self, sheet_name: &str) -> Result<Range<Data>, io::Error> {
        self.internal_reader()
            .worksheet_range(sheet_name)
            .map_err(|e| format!("unable to get the range: {e:#?}"))
            .map_err(io::Error::other)
    }
}

pub struct ExcelRange(pub Range<Data>);

impl ExcelRange {
    pub fn to_rows(&self) -> impl Iterator<Item = &[Data]> {
        self.0.rows()
    }

    pub fn to_batch(
        &self,
        schema: SchemaRef,
        rows_per_batch: usize,
    ) -> Result<impl Iterator<Item = Result<RecordBatch, io::Error>>, io::Error> {
        let rows: Vec<Vec<Data>> = self.0.rows().map(|r| r.to_vec()).collect();
        let schema_captured = schema.clone();

        let mut chunks = Vec::new();
        for chunk in rows.chunks(rows_per_batch) {
            chunks.push(chunk.to_vec());
        }

        Ok(chunks.into_iter().map(move |chunk| {
            let schema = schema_captured.clone();
            let mut arrays = Vec::with_capacity(schema.fields().len());

            for (i, field) in schema.fields().iter().enumerate() {
                let array: ArrayRef = match field.data_type() {
                    DataType::Int64 => {
                        let mut builder = Int64Builder::with_capacity(chunk.len());
                        for row in &chunk {
                            match row.get(i) {
                                Some(Data::Int(v)) => builder.append_value(*v),
                                Some(Data::Float(v)) => builder.append_value(*v as i64),
                                Some(Data::String(s)) => {
                                    if let Ok(v) = s.parse::<i64>() {
                                        builder.append_value(v)
                                    } else {
                                        builder.append_null()
                                    }
                                }
                                _ => builder.append_null(),
                            }
                        }
                        std::sync::Arc::new(builder.finish())
                    }
                    DataType::Float64 => {
                        let mut builder = Float64Builder::with_capacity(chunk.len());
                        for row in &chunk {
                            match row.get(i) {
                                Some(Data::Float(v)) => builder.append_value(*v),
                                Some(Data::Int(v)) => builder.append_value(*v as f64),
                                Some(Data::String(s)) => {
                                    if let Ok(v) = s.parse::<f64>() {
                                        builder.append_value(v)
                                    } else {
                                        builder.append_null()
                                    }
                                }
                                _ => builder.append_null(),
                            }
                        }
                        std::sync::Arc::new(builder.finish())
                    }
                    DataType::Utf8 => {
                        let mut builder =
                            StringBuilder::with_capacity(chunk.len(), chunk.len() * 10);
                        for row in &chunk {
                            match row.get(i) {
                                Some(Data::String(s)) => builder.append_value(s),
                                Some(Data::Int(v)) => builder.append_value(v.to_string()),
                                Some(Data::Float(v)) => builder.append_value(v.to_string()),
                                Some(Data::Bool(v)) => builder.append_value(v.to_string()),
                                _ => builder.append_null(),
                            }
                        }
                        std::sync::Arc::new(builder.finish())
                    }
                    DataType::Boolean => {
                        let mut builder = BooleanBuilder::with_capacity(chunk.len());
                        for row in &chunk {
                            match row.get(i) {
                                Some(Data::Bool(v)) => builder.append_value(*v),
                                _ => builder.append_null(),
                            }
                        }
                        std::sync::Arc::new(builder.finish())
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                        let mut builder = TimestampNanosecondBuilder::with_capacity(chunk.len());
                        for row in &chunk {
                            match row.get(i) {
                                Some(cell) => {
                                    if let Some(ndt) = cell.as_datetime() {
                                        if let Some(nanos) = ndt.and_utc().timestamp_nanos_opt() {
                                            builder.append_value(nanos);
                                        } else {
                                            builder.append_null();
                                        }
                                    } else if let Some(s) = cell.get_string() {
                                        if let Ok(ndt) =
                                            NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                                        {
                                            if let Some(nanos) = ndt.and_utc().timestamp_nanos_opt()
                                            {
                                                builder.append_value(nanos);
                                            } else {
                                                builder.append_null();
                                            }
                                        } else {
                                            builder.append_null();
                                        }
                                    } else {
                                        builder.append_null();
                                    }
                                }
                                None => builder.append_null(),
                            }
                        }
                        std::sync::Arc::new(builder.finish())
                    }
                    // TODO: Add other types like DateTime
                    _ => {
                        return Err(io::Error::other(format!(
                            "unsupported data type: {}",
                            field.data_type()
                        )));
                    }
                };
                arrays.push(array);
            }

            RecordBatch::try_new(schema, arrays)
                .map_err(|e| io::Error::other(format!("failed to create record batch: {}", e)))
        }))
    }
}

pub struct XlsxFileReader(pub Xlsx<BufReader<File>>);

impl XlsxFileReader {
    pub fn new<P>(path2x: P) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let xr: Xlsx<_> = calamine::open_workbook(path2x).map_err(io::Error::other)?;
        Ok(Self(xr))
    }
}

impl ExcelReader for XlsxFileReader {
    type ReadSeek = BufReader<File>;
    type InternalReader = Xlsx<BufReader<File>>;

    fn internal_reader(&mut self) -> &mut Self::InternalReader {
        &mut self.0
    }
}
