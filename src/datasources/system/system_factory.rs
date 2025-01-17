// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use crate::datasources::datasource::DatabaseHashMap;
use crate::datasources::{system, ITable};
use crate::error::FuseQueryResult;

pub struct SystemFactory;

impl SystemFactory {
    pub fn create() -> Self {
        Self
    }

    pub fn get_tables(&self) -> FuseQueryResult<DatabaseHashMap> {
        let tables: Vec<Arc<dyn ITable>> = vec![
            Arc::new(system::OneTable::create()),
            Arc::new(system::FunctionsTable::create()),
            Arc::new(system::SettingsTable::create()),
            Arc::new(system::NumbersTable::create("numbers")),
            Arc::new(system::NumbersTable::create("numbers_mt")),
            Arc::new(system::TablesTable::create()),
            Arc::new(system::ClustersTable::create()),
        ];

        let mut hashmap: DatabaseHashMap = HashMap::default();
        hashmap.insert("system", tables);
        Ok(hashmap)
    }
}
