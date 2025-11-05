use std::io::{self, Error, stdout};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use calamine::Data;
use clap::Parser;

use rs_x2arrow_ipc_stream::{ExcelRange, ExcelReader, XlsxFileReader};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input Excel file
    #[arg(short, long)]
    input: String,

    /// Sheet name
    #[arg(short, long)]
    sheet: String,

    /// Number of rows per batch
    #[arg(long, default_value_t = 1024)]
    batch_size: usize,
}

fn infer_schema(header: &[Data]) -> SchemaRef {
    let fields: Vec<Field> = header
        .iter()
        .map(|cell| {
            let data_type = match cell {
                Data::Int(_) => DataType::Int64,
                Data::Float(_) => DataType::Float64,
                Data::String(_) => DataType::Utf8,
                Data::Bool(_) => DataType::Boolean,
                Data::DateTime(_) => DataType::Timestamp(TimeUnit::Nanosecond, None),
                _ => DataType::Utf8, // Default to Utf8 for other types
            };
            Field::new(cell.to_string(), data_type, true)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

pub fn main() -> Result<(), Error> {
    let args = Args::parse();

    let mut excel_reader = XlsxFileReader::new(args.input)?;
    let range = excel_reader.sheet_range(&args.sheet)?;

    let mut rows_iter = range.rows();
    let header = rows_iter
        .next()
        .ok_or_else(|| Error::other("Empty sheet"))?;
    let schema = infer_schema(header);

    let (height, width) = range.get_size();
    if height <= 1 {
        // Only header or empty sheet
        return Ok(());
    }

    let data_range = range.range((1, 0), (height as u32 - 1, width as u32 - 1));

    let excel_range = ExcelRange(data_range);
    let batches = excel_range.to_batch(schema.clone(), args.batch_size)?;

    let stdout = stdout();
    let mut writer = StreamWriter::try_new(stdout, &schema).map_err(io::Error::other)?;

    for batch in batches {
        let batch = batch?;
        writer.write(&batch).map_err(io::Error::other)?;
    }

    writer.finish().map_err(io::Error::other)?;

    Ok(())
}
