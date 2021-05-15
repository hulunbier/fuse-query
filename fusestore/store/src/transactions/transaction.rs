// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use thiserror::Error;

#[derive(Debug, PartialOrd, PartialEq, Ord, Eq, Copy, Clone)]
struct SnapshotId(u64);

enum IsolationLevel {}

/// todo
struct Transaction {
    /// Snapshot id which this transaction started with
    base_snapshot_id: SnapshotId,
    /// Isolation level
    isolation_level: IsolationLevel,
    /// operations belong to this transaction
    ops: Vec<Operation>
}

/// Operation is categorized into three types:
/// - Append
/// - Removal
/// - SchemaCreation
/// - SchemaEvolution
/// - ApplicationSpecific
#[derive(Debug)]
enum Operation {
    Append(Append),
    Remove(Removal),
    Schema(SchemaEvolution)
}

#[derive(Debug)]
struct Append {}

#[derive(Debug)]
struct Removal {}

#[derive(Debug)]
struct SchemaCreation {}

#[derive(Debug)]
struct SchemaEvolution {}

#[derive(Error, Debug)]
enum TransactionError {}

type Result<T> = std::result::Result<T, TransactionError>;

impl Transaction {
    fn new() -> Self {
        todo!()
    }
    fn append_data(&mut self, path: &str) -> Result<()> {
        todo!()
    }
    fn remove_data(&mut self) {
        todo!()
    }

    fn commit(self) -> std::result::Result<(), (Transaction, TransactionError)> {
        todo!()
    }
    fn rollback(self) -> Result<()> {
        todo!()
    }

    fn append_log() {
        todo!()
    }
}

struct Table {}

struct Schema;
struct Filter;
struct Part;

struct TableCreation {}
struct TableSchemaEvolution {}

impl TableCreation {
    fn schema(&self) -> Schema {
        todo!()
    }
}

trait Apply<Ctx> {
    type R;
    fn apply(&self, ctx: Ctx) -> Self::R;
}

trait Context {
    fn with_tx<F, R>(&mut self, f: F) -> R
    where F: Fn(&mut Transaction) -> R;
}

impl<C> Apply<&mut C> for TableCreation
where C: Context
{
    type R = Result<()>;

    fn apply(&self, ctx: &mut C) -> Result<()> {
        ctx.with_tx(|tx| {
            let schema = self.schema();
            //let table_meta = new_table_meta(schema);
        });
        Ok(())
    }
}

impl Table {
    pub fn create(tx: &mut Transaction, schema: &Schema) -> Result<Table> {
        todo!()
    }

    pub fn load_table() -> Result<Table> {
        todo!()
    }

    pub fn update_schema(&mut self, tx: &mut Transaction, new_schema: &Schema) -> Result<()> {
        todo!()
    }

    pub fn filter_parts(&mut self, tx: &mut Transaction, filter: &Filter) -> Result<Vec<Part>> {
        todo!()
    }

    pub fn load_data(&mut self, tx: &mut Transaction, part: &[&Part]) {
        todo!()
    }

    pub fn append_data(&mut self, tx: &mut Transaction) -> Result<()> {
        todo!()
    }

    pub fn rm_data(&mut self, tx: &mut Transaction) -> Result<()> {
        todo!()
    }
}

/// "A proof-read" of tx API

mod example {
    use common_arrow::arrow_flight::FlightData;
    use tokio_stream::StreamExt;
    use tonic::Status;
    use tonic::Streaming;

    use super::*;
    use crate::meta::TableMeta;

    fn create_table(tbl_meta: &TableMeta) -> Result<()> {
        let mut tx = Transaction::new();
        let schema = Schema {};
        let mut table = Table::create(&mut tx, &schema)?;
        table.append_data(&mut tx)?;
        Ok(())
    }

    fn insert_data(path: &str) -> Result<()> {
        let mut tx = Transaction::new();
        let schema = Schema {};
        let mut table = Table::create(&mut tx, &schema)?;
        table.append_data(&mut tx)?;
        Ok(())
    }
}
