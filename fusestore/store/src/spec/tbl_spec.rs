// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::convert::TryInto;
use std::path::Path;
use std::sync::Arc;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_arrow::arrow_flight::FlightData;
use common_arrow::parquet::arrow::ArrowWriter;
use common_arrow::parquet::file::writer::InMemoryWriteableCursor;
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tonic::metadata::MetadataMap;
use tonic::Request;
use tonic::Streaming;

use crate::io::FS;
use crate::meta::TableMeta;
use crate::meta::TableSnapshot;

const TBL_SPEC_V1: u8 = 1u8;

pub struct TableSpec {
    fs: Arc<dyn FS>
}

impl TableSpec {
    pub fn new(fs: Arc<dyn FS>) -> Self {
        TableSpec { fs }
    }

    pub fn new_() -> Self {
        todo!()
    }
    pub async fn create_table(&self, meta: &TableMeta) -> Result<TableSnapshot> {
        let io = &self.fs;

        // file location : /${db_name}/${table_name}/meta_v0.json
        let path = Path::new(&meta.db_name).join(&meta.tbl_name);
        let sequence = 0u64;
        let file_name = TableSpec::meta_file_name(sequence);

        let content = serde_json::to_string(&meta)?;
        io.put_if_absent(&path, &file_name, &content.as_bytes())
            .await?;

        Ok(TableSnapshot {
            spec_version: TBL_SPEC_V1,
            sequence,
            meta_uri: format!("/{}/{}", path.to_string_lossy(), file_name),
            table_name: meta.db_name.clone(),
            db_name: meta.db_name.clone(),
            table_uuid: meta.table_uuid
        })
    }

    async fn append_data(
        &self,
        mut stream: std::pin::Pin<Box<dyn futures::Stream<Item = FlightData>>>
    ) -> Result<Vec<String>> {
        if let Some(flight_data) = stream.next().await {
            flight_data.flight_descriptor;

            let data_schema = DataSchema::try_from(&flight_data)?;
            let meta = data_schema.metadata();

            let db_name = meta.get("_DB_NAME").context("_DB_NAME must exist")?;
            let tbl_name = meta.get("_TBL_NAME").context("_TBL_NAME must exist")?;
            let tbl_snapshot_id = meta.get("_TBL_SNAPSHOT_ID");
            let path = Path::new(&db_name).join(&tbl_name);

            let schema_ref = Arc::new(data_schema);

            let mut block_stream = stream.map(move |flight_data| {
                let batch = flight_data_to_arrow_batch(&flight_data, schema_ref.clone(), &[])?;
                batch.try_into().map_err(anyhow::Error::new)
            });

            let mut appended_files: Vec<String> = Vec::new();

            while let Some(block) = block_stream.next().await {
                let buffer = write_in_memory(block?)?;
                let data_file_name = uuid::Uuid::new_v4().to_simple().to_string() + ".parquet";
                // TODO partitioning, schema validation, etc.
                let data_path = path.join(data_file_name);
                // get an output stream for $db/$tbl/$uuid.parquet
                let mut writer = self.fs.writer(&data_path).await?;
                writer.write_all(&buffer).await?;
                writer.flush().await?;
                std::mem::drop(writer);
                appended_files.push(data_path.to_string_lossy().to_string()); // NB
            }

            Ok(appended_files)
        } else {
            bail!("empty stream")
        }
    }

    fn meta_file_name(ver: u64) -> String {
        format!("meta_v{}.json", ver)
    }
}

pub fn write_in_memory(block: DataBlock) -> Result<Vec<u8>> {
    let cursor = InMemoryWriteableCursor::default();
    {
        let cursor = cursor.clone();
        let batch: RecordBatch = block.try_into()?;
        let mut writer = ArrowWriter::try_new(cursor, batch.schema(), None)?;
        writer.write(&batch)?;
        let _ = writer.close()?;
    }
    Ok(cursor.into_inner().context("todo")?)
}

#[cfg(test)]
mod test {
    use std::io::Cursor;
    use std::path::PathBuf;

    use common_arrow::arrow::datatypes::DataType;
    use common_arrow::arrow::ipc::writer::IpcWriteOptions;
    use common_arrow::arrow_flight::utils::flight_data_from_arrow_batch;
    use common_arrow::arrow_flight::utils::flight_data_from_arrow_schema;
    use common_arrow::arrow_flight::utils::ipc_message_from_arrow_schema;
    use common_arrow::parquet::arrow::ArrowReader;
    use common_arrow::parquet::arrow::ParquetFileArrowReader;
    use common_arrow::parquet::file::reader::SerializedFileReader;
    use common_arrow::parquet::file::serialized_reader::SliceableCursor;
    use common_datavalues::DataField;
    use common_datavalues::DataSchema;
    use common_datavalues::Int64Array;
    use common_datavalues::StringArray;
    use prost::encoding::check_wire_type;

    use super::*;

    #[test]
    fn test_in_memory_write() -> anyhow::Result<()> {
        let schema = Arc::new(DataSchema::new(vec![
            DataField::new("col_i", DataType::Int64, false),
            DataField::new("col_s", DataType::Utf8, false),
        ]));

        let col0 = Arc::new(Int64Array::from(vec![0, 1, 2]));
        let col1 = Arc::new(StringArray::from(vec!["str1", "str2", "str3"]));
        let block = DataBlock::create(schema.clone(), vec![col0.clone(), col1.clone()]);

        let buffer = write_in_memory(block)?;

        let cursor = SliceableCursor::new(buffer);
        let reader = SerializedFileReader::new(cursor)?;

        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));

        let arrow_schema = arrow_reader.get_schema()?;
        assert_eq!(&arrow_schema, schema.as_ref());

        let mut records = arrow_reader.get_record_reader(1024)?;
        if let Some(r) = records.next() {
            let batch = r?;
            assert_eq!(batch.schema(), schema);
            assert_eq!(
                batch.column(0),
                (&(col0 as std::sync::Arc<dyn common_arrow::arrow::array::Array>))
            );
            assert_eq!(
                batch.column(1),
                (&(col1 as std::sync::Arc<dyn common_arrow::arrow::array::Array>))
            );
            Ok(())
        } else {
            bail!("empty record set?")
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_append() -> anyhow::Result<()> {
        //let db_name = meta.get("_DB_NAME").context("_DB_NAME must exist")?;
        //let tbl_name = meta.get("_TBL_NAME").context("_TBL_NAME must exist")?;
        let meta = [
            ("_TBL_NAME".to_string(), "tbl".to_string()),
            ("_DB_NAME".to_string(), "db".to_string())
        ]
        .iter()
        .cloned()
        .collect();
        let schema = DataSchema::new_with_metadata(
            vec![
                DataField::new("col_i", DataType::Int64, false),
                DataField::new("col_s", DataType::Utf8, false),
            ],
            meta
        );

        let schema_ref = Arc::new(schema);

        let col0 = Arc::new(Int64Array::from(vec![0, 1, 2]));
        let col1 = Arc::new(StringArray::from(vec!["str1", "str2", "str3"]));
        let block = DataBlock::create(schema_ref.clone(), vec![col0.clone(), col1.clone()]);
        let batch = block.try_into()?;

        let fs = crate::poc::file_io::Namtso::new(PathBuf::from("/tmp"), PathBuf::from("/tmp"));
        let mut spec = TableSpec::new(Arc::new(fs));
        let default_ipc_write_opt = IpcWriteOptions::default();
        let flight = flight_data_from_arrow_schema(schema_ref.as_ref(), &default_ipc_write_opt);

        let req = futures::stream::iter(vec![
            flight,
            flight_data_from_arrow_batch(&batch, &default_ipc_write_opt).1,
        ]);
        let r = spec.append_data(Box::pin(req)).await;
        println!("r is {:?}", r);
        assert!(r.is_ok());
        Ok(())
    }
}
