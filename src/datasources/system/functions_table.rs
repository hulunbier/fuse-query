// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;

use crate::datablocks::DataBlock;
use crate::datasources::{ITable, Partition, Statistics};
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::datavalues::{DataField, DataSchema, DataSchemaRef, DataType, StringArray};
use crate::error::FuseQueryResult;
use crate::functions::FunctionFactory;
use crate::planners::{PlanNode, ReadDataSourcePlan};
use crate::sessions::FuseQueryContextRef;

pub struct FunctionsTable {
    schema: DataSchemaRef,
}

impl FunctionsTable {
    pub fn create() -> Self {
        FunctionsTable {
            schema: Arc::new(DataSchema::new(vec![DataField::new(
                "name",
                DataType::Utf8,
                false,
            )])),
        }
    }
}

#[async_trait]
impl ITable for FunctionsTable {
    fn name(&self) -> &str {
        "functions"
    }

    fn engine(&self) -> &str {
        "SystemFunctions"
    }

    fn schema(&self) -> FuseQueryResult<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn read_plan(
        &self,
        _ctx: FuseQueryContextRef,
        _push_down_plan: PlanNode,
    ) -> FuseQueryResult<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: "system".to_string(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            partitions: vec![Partition {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: "(Read from system.functions table)".to_string(),
        })
    }

    async fn read(&self, _ctx: FuseQueryContextRef) -> FuseQueryResult<SendableDataBlockStream> {
        let func_names = FunctionFactory::registered_names();
        let names: Vec<&str> = func_names.iter().map(|x| x.as_ref()).collect();
        let block = DataBlock::create(
            self.schema.clone(),
            vec![Arc::new(StringArray::from(names))],
        );
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
