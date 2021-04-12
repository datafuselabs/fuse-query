// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_partial_groupby() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_planners::*;
    use common_planners::{self};
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::pipelines::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    // sum(number)+1, avg(number)
    let aggr_exprs = vec![add(sum(col("number")), lit(2u64)), avg(col("number"))];
    let group_exprs = vec![modular(col("number"), lit(3u64)), col("number")];
    let aggr_partial = PlanBuilder::create(test_source.number_schema_for_test()?)
        .aggregate_partial(aggr_exprs.clone(), group_exprs.clone())?
        .build()?;

    // Pipeline.
    let mut pipeline = Pipeline::create();
    let source = test_source.number_source_transform_for_test(10)?;
    pipeline.add_source(Arc::new(source))?;
    pipeline.add_simple_transform(|| {
        Ok(Box::new(GroupByPartialTransform::create(
            aggr_partial.schema(),
            aggr_exprs.clone(),
            group_exprs.clone(),
        )))
    })?;
    pipeline.merge_processor()?;

    // Result.
    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 2);

    let expected = vec![
        "+-------------------+--------+",
        "| modulo(number, 3) | number |",
        "+-------------------+--------+",
        "| 0                 | 0      |",
        "| 0                 | 3      |",
        "| 0                 | 6      |",
        "| 0                 | 9      |",
        "| 1                 | 1      |",
        "| 1                 | 4      |",
        "| 1                 | 7      |",
        "| 2                 | 2      |",
        "| 2                 | 5      |",
        "| 2                 | 8      |",
        "+-------------------+--------+",
    ];
    crate::assert_blocks_sorted_eq!(expected, result.as_slice());

    Ok(())
}