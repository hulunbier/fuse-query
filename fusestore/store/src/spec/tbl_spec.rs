// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::{Stream, StreamExt};
use prost::bytes::BufMut;
use tokio::io::AsyncWriteExt;
use tonic::{Request, Streaming};

use common_arrow::arrow_flight::FlightData;
use common_datablocks::DataBlock;

use crate::io::FileIO;
use crate::meta::TableMeta;

pub struct TblSpec {
    fs: Box<dyn FileIO>,
}

impl TblSpec {
    fn meta_name(ver: u64) -> String {
        format!("meta_v{}.json", ver)
    }

    pub async fn create_table(&self, meta: &TableMeta) -> Result<()> {
        let path = std::path::Path::new(&meta.db).join(&meta.name);
        let content = serde_json::to_string(meta)?;

        let io = &self.fs;
        // path layout : ${db_name}/${table_name}/meta_v0.json
        io.put_if_absence(&path, &TblSpec::meta_name(0u64), &content.as_bytes()).await
    }

    pub async fn insert(&self, meta: &TableMeta, data: Request<Streaming<FlightData>>) -> Result<()> {
        let req_meta = data.metadata();

        let db_name = req_meta.get("FUSE_DB").context("missing database name")?
            .to_str().context("invalid database name encoding")?.to_string();
        let tbl = req_meta.get("FUSE_TBL").context("missing table name")?
            .to_str().context("invalid table name encoding")?.to_string();
        let tbl_snapshot_id = req_meta.get("FUSE_TBL_SNAPSHOT").context("missing snapshot id")?
            .to_str().context("invalid snapshot id encoding")?.to_string();

        let mut stream = data.into_inner();
        let io = &self.fs;

        let data_file_name = uuid::Uuid::new_v4().to_simple().to_string() + ".json";
        let path = Path::new(&db_name).join(tbl).join(data_file_name);
        let mut writer = io.writer(&path).await?;

        while let Some(value) = stream.next().await {
            let flight = value.context("malformed stream")?;
            writer.write_all(&flight.data_body).await.context("output stream failure")?;
        }

        writer.flush().await.context("flushing failure")?;

        Ok(())
    }
}


