// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_exception::Result;
use common_planners::InsertIntoPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_streams::SendableDataBlockStream;

use crate::sessions::FuseQueryContextRef;

#[async_trait::async_trait]
pub trait TableStorage: Sync + Send {
    // Get the read source plan.
    async fn sread_plan(
        &self,
        ctx: FuseQueryContextRef,
        scan: &ScanPlan,
        partitions: usize,
    ) -> Result<ReadDataSourcePlan>;

    // Read block data from the underling.
    async fn sread(
        &self,
        ctx: FuseQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream>;

    // append_data
    async fn sappend_data(
        &self,
        _ctx: FuseQueryContextRef,
        _insert_plan: InsertIntoPlan,
    ) -> Result<()>;
}
