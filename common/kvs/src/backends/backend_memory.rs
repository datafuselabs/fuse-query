// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_exception::Result;

use crate::backends::Lock;
use crate::backends::StateBackend;

#[allow(dead_code)]
pub struct MemoryBackend {
    addr: String,
}

impl MemoryBackend {
    pub fn create(addr: String) -> Self {
        Self { addr }
    }
}

#[async_trait]
impl StateBackend for MemoryBackend {
    async fn get(&self, _key: String) -> Result<Option<String>> {
        todo!()
    }

    async fn get_from_prefix(&self, _prefix: String) -> Result<Vec<(String, String)>> {
        todo!()
    }

    async fn put(&self, _key: String, _value: String) -> Result<()> {
        todo!()
    }

    async fn remove(&self, _key: String) -> Result<()> {
        todo!()
    }

    async fn lock(&self, _key: String) -> Result<Box<dyn Lock>> {
        todo!()
    }
}
