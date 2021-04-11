// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_planners::{CreateDatabasePlan, DatabaseEngineType};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct DatabaseMeta {
    pub name: String,
    pub engine: DatabaseEngineType,
    pub options: HashMap<String, String>,
}

impl From<&CreateDatabasePlan> for DatabaseMeta {
    fn from(plan: &CreateDatabasePlan) -> Self {
        DatabaseMeta {
            name: plan.db.clone(),
            engine: plan.engine.clone(),
            options: plan.options.clone(),
        }
    }
}

impl From<CreateDatabasePlan> for DatabaseMeta {
    fn from(plan: CreateDatabasePlan) -> Self {
        DatabaseMeta {
            name: plan.db,
            engine: plan.engine,
            options: plan.options,
        }
    }
}

