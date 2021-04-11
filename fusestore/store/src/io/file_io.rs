// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-Lise-Identifier: Apache-2.0.

use anyhow::Result;
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};
use futures::AsyncReadExt;
use std::path::Path;

pub trait AsyncSeekableRead: AsyncRead + AsyncSeek {}

#[async_trait]
pub trait FileIO: Send + Sync {

    //TODO depends on std::path::Path is no a good idea
    async fn put_if_absence(&self, path: &Path, name: &str, content: &[u8]) -> Result<()>;
    async fn writer(&self, path: &Path) -> Result<Box<dyn AsyncWrite + Unpin>>;
    async fn reader(&self, path: &Path) -> Result<Box<dyn AsyncRead>>;
    async fn seekable_reader(&self, path: &Path) -> Result<Box<dyn AsyncSeekableRead>>;

}
