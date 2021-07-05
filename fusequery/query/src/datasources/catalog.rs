// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::Arc;
use common_exception::Result;
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;

use crate::datasources::datasource::TableStorage;
use crate::datasources::Database;
use crate::datasources::Table;
use crate::datasources::TableFunction;

#[async_trait::async_trait]
pub trait Catalog: Send + Sync {
    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>>;
    fn get_databases(&self) -> Result<Vec<String>>;
    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>>;
    fn get_all_tables(&self) -> Result<Vec<(String, Arc<dyn Table>)>>;
    fn get_table_function(&self, name: &str) -> Result<Arc<dyn TableFunction>>;

    async fn create_database(&self, plan: CreateDatabasePlan) -> Result<()>;
    async fn drop_database(&self, plan: DropDatabasePlan) -> Result<()>;

    fn get_storage(
        &self,
        is_remote: bool,
        db_name: &str,
        tbl_name: &str,
    ) -> Result<Arc<dyn TableStorage>>;
}
