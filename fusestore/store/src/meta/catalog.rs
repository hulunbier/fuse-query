// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;
use async_trait::async_trait;

use common_datavalues::DataSchema;

use crate::meta::{DatabaseMeta, TableMeta};

struct SnapshotId;

#[async_trait]
pub trait Catalog: Send + Sync {
    async fn list_databases(&self) -> Result<Vec<DatabaseMeta>>;
    async fn list_tables(&self, db_name: &str) -> Result<Vec<TableMeta>>;
    async fn get_table(&self, db_name: &str, tbl_name: &str) -> Result<Option<TableMeta>>;

    async fn create_db(&self, db_name: DatabaseMeta) -> Result<()>;
    async fn create_table(&self, schema: &TableMeta) -> Result<()>;
}