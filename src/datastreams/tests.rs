// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_chunk_stream() {
    use std::sync::Arc;

    use futures::stream::StreamExt;

    use crate::datablocks::*;
    use crate::datastreams::*;
    use crate::datavalues::*;

    let mut s1 = DataBlockStream::create(
        Arc::new(DataSchema::empty()),
        None,
        vec![DataBlock::empty(), DataBlock::empty(), DataBlock::empty()],
    );
    while let Some(_) = s1.next().await {}
}
