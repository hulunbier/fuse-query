// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-Lise-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::sync::Mutex;

use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow_flight;
use common_arrow::arrow_flight::FlightData;
use common_flights::CreateDatabaseAction;
use common_flights::CreateDatabaseActionResult;
use common_flights::CreateTableAction;
use common_flights::CreateTableActionResult;
use common_flights::GetTableAction;
use common_flights::GetTableActionResult;
use common_flights::StoreDoAction;
use common_flights::StoreDoActionResult;
#[allow(unused_imports)]
use log::error;
#[allow(unused_imports)]
use log::info;
use tokio::sync::mpsc::Sender;
use tonic::Status;

use crate::engine::MemEngine;
use crate::fs::IFileSystem;
use crate::protobuf::CmdCreateDatabase;
use crate::protobuf::CmdCreateTable;
use crate::protobuf::Db;
use crate::protobuf::Table;

pub struct ActionHandler {
    // TODO zbr's proposol
    // catalog: Box<dyn Catalog>,
    // tbl_spec: TableSpec,
    // db_spec: DatabaseSpec,
    // TODO delegate table/database RW to fs
    meta: Arc<Mutex<MemEngine>>,
    fs: Arc<dyn IFileSystem>
}

impl ActionHandler {
    pub fn create(fs: Arc<dyn IFileSystem>) -> Self {
        ActionHandler {
            meta: MemEngine::create(),
            fs
        }
    }

    /// Handle pull-file requests, which is used internally for replicating data copies.
    /// In FuseStore impl there is no internal file id etc, thus replication use the same `key` in communication with FuseQuery as in internal replication.
    pub async fn do_pull_file(
        &self,
        key: String,
        tx: Sender<Result<FlightData, tonic::Status>>
    ) -> Result<(), Status> {
        // TODO: stream read if the file is too large.
        let buf = self
            .fs
            .read_all(key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        tx.send(Ok(FlightData {
            data_body: buf,
            ..Default::default()
        }))
        .await
        .map_err(|e| Status::internal(format!("{:?}", e)))
    }

    pub async fn execute(&self, action: StoreDoAction) -> Result<StoreDoActionResult, Status> {
        match action {
            StoreDoAction::ReadPlan(_) => Err(Status::internal("Store read plan unimplemented")),
            StoreDoAction::CreateDatabase(a) => self.create_db(a).await,
            StoreDoAction::CreateTable(a) => self.create_table(a).await,
            StoreDoAction::GetTable(a) => self.get_table(a).await
        }
    }

    async fn create_db(&self, act: CreateDatabaseAction) -> Result<StoreDoActionResult, Status> {
        let plan = act.plan;
        let mut meta = self.meta.lock().unwrap();

        let cmd = CmdCreateDatabase {
            db_name: plan.db,
            db: Some(Db {
                // meta fills it
                db_id: -1,
                ver: -1,
                table_name_to_id: HashMap::new(),
                tables: HashMap::new()
            })
        };

        let database_id = meta
            .create_database(cmd, plan.if_not_exists)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(StoreDoActionResult::CreateDatabase(
            CreateDatabaseActionResult { database_id }
        ))
    }

    async fn create_table(&self, act: CreateTableAction) -> Result<StoreDoActionResult, Status> {
        let plan = act.plan;
        let db_name = plan.db;
        let table_name = plan.table;

        info!("create table: {:}: {:?}", db_name, table_name);

        let mut meta = self.meta.lock().unwrap();

        let options = common_arrow::arrow::ipc::writer::IpcWriteOptions::default();
        let flight_data =
            arrow_flight::utils::flight_data_from_arrow_schema(&plan.schema, &options);

        let table = Table {
            // the storage engine fills the id.
            table_id: -1,
            ver: -1,
            schema: flight_data.data_header,
            options: plan.options,

            // TODO
            placement_policy: vec![]
        };

        let cmd = CmdCreateTable {
            db_name,
            table_name,
            table: Some(table)
        };

        let table_id = meta.create_table(cmd, plan.if_not_exists)?;

        Ok(StoreDoActionResult::CreateTable(CreateTableActionResult {
            table_id
        }))
    }

    async fn get_table(&self, act: GetTableAction) -> Result<StoreDoActionResult, Status> {
        let db_name = act.db;
        let table_name = act.table;

        info!("create table: {:}: {:?}", db_name, table_name);

        let mut meta = self.meta.lock().unwrap();

        let table = meta.get_table(db_name.clone(), table_name.clone())?;

        let schema = Schema::try_from(&FlightData {
            data_header: table.schema,
            ..Default::default()
        })
        .map_err(|e| Status::internal(format!("invalid schema: {:}", e.to_string())))?;

        let rst = StoreDoActionResult::GetTable(GetTableActionResult {
            table_id: table.table_id,
            db: db_name,
            name: table_name,
            schema: Arc::new(schema)
        });

        Ok(rst)
    }
}
