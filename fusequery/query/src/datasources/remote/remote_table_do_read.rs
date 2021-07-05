// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_store_api::ReadAction;
use common_store_api::StorageApi;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::datasources::remote::remote_table::RemoteTable;
use crate::datasources::remote::StoreClientProvider;
use crate::sessions::FuseQueryContextRef;

impl RemoteTable {
    #[inline]
    pub(super) async fn do_read(
        store_client_provider: StoreClientProvider,
        ctx: FuseQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let client = store_client_provider.try_get_client().await?;
        let db = source_plan.db.clone();
        let tbl = source_plan.table.clone();
        let schema = source_plan.schema.clone();
        let progress_callback = ctx.progress_callback();

        let iter = {
            let schema = schema.clone();
            std::iter::from_fn(move || match ctx.try_get_partitions(1) {
                Err(_) => None,
                Ok(parts) if parts.is_empty() => None,
                Ok(parts) => Some(ReadAction {
                    part: parts[0].clone(),
                    push_down: PlanNode::ReadSource(ReadDataSourcePlan {
                        db: db.clone(),
                        table: tbl.clone(),
                        schema: schema.clone(),
                        remote: true,
                        ..ReadDataSourcePlan::empty()
                    }),
                }),
            })
        };

        let parts = futures::stream::iter(iter);
        let streams = parts.then(move |parts| {
            let mut client = client.clone();
            let schema = schema.clone();
            async move {
                let r = client.read_partition(schema, &parts).await;
                r.unwrap_or_else(|e| {
                    Box::pin(futures::stream::once(async move {
                        Err(ErrorCode::CannotReadFile(format!(
                            "get partition failure. partition [{:?}], error {}",
                            &parts, e
                        )))
                    }))
                })
            }
        });

        let stream = ProgressStream::try_create(Box::pin(streams.flatten()), progress_callback?)?;
        Ok(Box::pin(stream))
    }
}
