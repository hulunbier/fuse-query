// Copyright 2020 The VectorQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[allow(clippy::all)]
pub mod protobuf {
    // tonic::include_proto!("store_meta");
    include!(concat!(env!("OUT_DIR"), concat!("/store_meta.rs")));
}

#[cfg(test)]
#[macro_use]
pub mod tests;

pub mod api;
pub mod configs;
pub mod dfs;
pub mod engine;
pub mod executor;
pub mod fs;
pub mod localfs;
pub mod meta_service;
pub mod metrics;

#[allow(clippy::all)]
pub mod protobuf {
    // tonic::include_proto!("store_meta");
    include!(concat!(env!("OUT_DIR"), concat!("/store_meta.rs")));
}

mod io;
mod meta;
mod poc;
mod spec;
