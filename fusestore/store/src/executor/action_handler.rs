// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-Lise-Identifier: Apache-2.0.

use anyhow::Result;
use async_trait::async_trait;

use common_datavalues::DataSchema;
use common_flights::store_do_action::StoreDoAction;

use crate::meta::{Catalog, DatabaseMeta};

pub struct ActionHandler {
    catalog: Box<dyn Catalog>,
}

impl ActionHandler {
    pub fn new() -> Self {
        todo!()
        //ActionHandler {
        //}
    }
    pub async fn execute(&self, action: &StoreDoAction) -> Result<()> {
        match action {
            StoreDoAction::CreateDatabase(act) => {
                let plan = &act.plan;
                self.catalog.create_db(DatabaseMeta::from(plan)).await
            }
            StoreDoAction::CreateTable(act) => {
                let plan = &act.plan;
                // TODO converts plan to catalog operation, encapsulating all the plan info.
                let meta = todo!();
                self.catalog.create_table(meta).await
            }
        }
    }
}