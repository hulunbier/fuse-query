// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use sqlparser::dialect::keywords::Keyword::TBLPROPERTIES;
use tokio::io::{AsyncRead, AsyncWrite};

use common_datavalues::DataSchema;
use common_infallible::RwLock;

use crate::io::FileIO;
use crate::meta::{Catalog, DatabaseMeta, TableMeta};
use crate::spec::{DatabaseSpec, TblSpec};

pub struct LibAlexandria {
    databases: Box<RwLock<HashMap<String, DatabaseMeta>>>,
    tables: Box<RwLock<HashMap<String, TableMeta>>>,

    tbl_spec: TblSpec,
}

impl LibAlexandria {
    pub fn new() -> Self {
        todo!()
    }

    fn add_db(&self, db_meta: DatabaseMeta) {
        self.databases.write().insert(db_meta.name.clone(), db_meta);
    }
    fn add_table(&self, tbl_meta: TableMeta) {
        let key = format!("{}/{}", tbl_meta.db, tbl_meta.name);
        self.tables.write().insert(key, tbl_meta);
    }
}

#[async_trait]
impl Catalog for LibAlexandria {
    async fn list_databases(&self) -> Result<Vec<DatabaseMeta>> {
        todo!()
    }

    async fn list_tables(&self, db_name: &str) -> Result<Vec<TableMeta>> {
        todo!()
    }

    async fn get_table(&self, db_name: &str, tbl_name: &str) -> Result<Option<TableMeta>> {
        todo!()
    }

    async fn create_db(&self, meta: DatabaseMeta) -> Result<()> {
        todo!()
        //DatabaseSpec::create_database(self.file_io.as_ref(), &meta).await?;
        //self.add_db(meta);
        //Ok(())
    }

    async fn create_table(&self, schema: &TableMeta) -> Result<()> {
        self.tbl_spec.create_table(schema).await
    }
}