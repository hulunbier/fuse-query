// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::time::{Instant, SystemTime};

use uuid::Uuid;

use common_planners::{TableEngineType, TableOptions};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct TableMeta {
    pub spec_version: u8,
    pub table_version: u64,

    pub table_uuid: u128,

    pub name: String,
    pub db: String,
    pub engine: TableEngineType,
    pub options: TableOptions,

    pub snapshot_base_seq: u64,
    pub create_ts: SystemTime, //TODO chrono
}

impl TableMeta {
    pub fn new() -> Self {
        todo!()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct TableSnapshot {
    pub spec_version: u8,
    pub sequence: u64,
    pub meta_uri: String,
}



