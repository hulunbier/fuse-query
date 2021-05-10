// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

pub enum IsolationLevel {
    SERIALIZABLE,
    SNAPSHOT
}

struct TableTransaction {}

impl TableTransaction {
    fn commit(&self) {}
}

fn with_transaction(tx: &mut TableTransaction) {}
