// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_infallible::RwLock;
use common_runtime::tokio::sync::mpsc::Sender;
use common_runtime::tokio::sync::*;
use common_streams::AbortStream;
use tokio_stream::StreamExt;

use crate::api::rpc::flight_scatter::FlightScatter;
use crate::api::rpc::flight_scatter_broadcast::BroadcastFlightScatter;
use crate::api::rpc::flight_scatter_hash::HashFlightScatter;
use crate::api::FlightAction;
use crate::pipelines::processors::Pipeline;
use crate::pipelines::processors::PipelineBuilder;
use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionRef;

struct StreamInfo {
    #[allow(unused)]
    schema: DataSchemaRef,
    tx: mpsc::Sender<Result<DataBlock>>,
    rx: mpsc::Receiver<Result<DataBlock>>,
}

pub struct FuseQueryFlightDispatcher {
    streams: Arc<RwLock<HashMap<String, StreamInfo>>>,
    stages_notify: Arc<RwLock<HashMap<String, Arc<Notify>>>>,
    abort: Arc<AtomicBool>,
}

impl FuseQueryFlightDispatcher {
    pub fn create() -> FuseQueryFlightDispatcher {
        FuseQueryFlightDispatcher {
            streams: Arc::new(RwLock::new(HashMap::new())),
            stages_notify: Arc::new(RwLock::new(HashMap::new())),
            abort: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Reject new session if is aborted.
    pub fn abort(&self) {
        self.abort.store(true, Ordering::Relaxed)
    }

    pub fn is_aborted(&self) -> bool {
        self.abort.load(Ordering::Relaxed)
    }

    pub fn get_stream(
        &self,
        query_id: &str,
        stage_id: &str,
        stream: &str,
    ) -> Result<mpsc::Receiver<Result<DataBlock>>> {
        let stage_name = format!("{}/{}", query_id, stage_id);
        if let Some(notify) = self.stages_notify.write().remove(&stage_name) {
            notify.notify_waiters();
        }

        let stream_name = format!("{}/{}", stage_name, stream);
        match self.streams.write().remove(&stream_name) {
            Some(stream_info) => Ok(stream_info.rx),
            None => Err(ErrorCode::NotFoundStream("Stream is not found")),
        }
    }

    pub fn broadcast_action(&self, session: SessionRef, action: FlightAction) -> Result<()> {
        let query_id = action.get_query_id();
        let stage_id = action.get_stage_id();
        let action_sinks = action.get_sinks();
        let data_schema = action.get_plan().schema();
        self.create_stage_streams(&query_id, &stage_id, &data_schema, &action_sinks);

        match action.get_sinks().len() {
            0 => Err(ErrorCode::LogicalError("")),
            1 => self.one_sink_action(session, &action),
            _ => self.action_with_scatter::<BroadcastFlightScatter>(session, &action),
        }
    }

    pub fn shuffle_action(&self, session: SessionRef, action: FlightAction) -> Result<()> {
        let query_id = action.get_query_id();
        let stage_id = action.get_stage_id();
        let action_sinks = action.get_sinks();
        let data_schema = action.get_plan().schema();
        self.create_stage_streams(&query_id, &stage_id, &data_schema, &action_sinks);

        match action.get_sinks().len() {
            0 => Err(ErrorCode::LogicalError("")),
            1 => self.one_sink_action(session, &action),
            _ => self.action_with_scatter::<HashFlightScatter>(session, &action),
        }
    }

    fn one_sink_action(&self, session: SessionRef, action: &FlightAction) -> Result<()> {
        let query_context = session.create_context();
        let action_context = FuseQueryContext::new(query_context.clone());
        let pipeline_builder = PipelineBuilder::create(action_context.clone());
        let pipeline = pipeline_builder.build(&action.get_plan())?;

        let action_sinks = action.get_sinks();
        let action_query_id = action.get_query_id();
        let action_stage_id = action.get_stage_id();

        assert_eq!(action_sinks.len(), 1);
        let stage_name = format!("{}/{}", action_query_id, action_stage_id);
        let stages_notify = self.stages_notify.clone();

        let stream_name = format!("{}/{}", stage_name, action_sinks[0]);
        let tx_ref = self.streams.read().get(&stream_name).map(|x| x.tx.clone());
        let tx = tx_ref.ok_or_else(|| ErrorCode::NotFoundStream("Not found stream"))?;

        query_context.execute_task(async move {
            let _session = session;
            let action_context = action_context;
            wait_start(stage_name, stages_notify).await;
            let abortable_stream = Self::execute(pipeline, &action_context).await;

            match abortable_stream {
                Err(error) => {
                    tx.send(Err(error)).await.ok();
                }
                Ok(mut abortable_stream) => {
                    while let Some(item) = abortable_stream.next().await {
                        if let Err(error) = tx.send(item).await {
                            log::error!(
                                "Cannot push data when run_action_without_scatters. {}",
                                error
                            );
                            break;
                        }
                    }
                }
            };
        })?;
        Ok(())
    }

    fn action_with_scatter<T>(&self, session: SessionRef, action: &FlightAction) -> Result<()>
    where T: FlightScatter + Send + 'static {
        let query_context = session.create_context();
        let action_context = FuseQueryContext::new(query_context.clone());
        let pipeline_builder = PipelineBuilder::create(action_context.clone());
        let pipeline = pipeline_builder.build(&action.get_plan())?;

        let action_query_id = action.get_query_id();
        let action_stage_id = action.get_stage_id();

        let sinks_tx = {
            let action_sinks = action.get_sinks();

            assert!(action_sinks.len() > 1);
            let mut sinks_tx = Vec::with_capacity(action_sinks.len());

            for sink in &action_sinks {
                let stream_name = format!("{}/{}/{}", action_query_id, action_stage_id, sink);
                match self.streams.read().get(&stream_name) {
                    Some(stream) => sinks_tx.push(stream.tx.clone()),
                    None => {
                        return Err(ErrorCode::NotFoundStream(format!(
                            "Not found stream {}",
                            stream_name
                        )))
                    }
                }
            }

            Result::Ok(sinks_tx)
        }?;

        let stage_name = format!("{}/{}", action_query_id, action_stage_id);
        let stages_notify = self.stages_notify.clone();

        let flight_scatter = T::try_create(
            action.get_plan().schema(),
            action.get_scatter_expression(),
            action.get_sinks().len(),
        )?;

        query_context.execute_task(async move {
            let _session = session;
            let action_context = action_context;
            wait_start(stage_name, stages_notify).await;

            let sinks_tx_ref = &sinks_tx;
            let forward_blocks = async move {
                let mut abortable_stream = Self::execute(pipeline, &action_context).await?;
                while let Some(item) = abortable_stream.next().await {
                    let forward_blocks = flight_scatter.execute(&item?)?;

                    assert_eq!(forward_blocks.len(), sinks_tx_ref.len());

                    for (index, forward_block) in forward_blocks.iter().enumerate() {
                        let tx: &Sender<Result<DataBlock>> = &sinks_tx_ref[index];
                        tx.send(Ok(forward_block.clone()))
                            .await
                            .map_err_to_code(ErrorCode::LogicalError, || {
                                "Cannot push data when run_action"
                            })?;
                    }
                }

                Result::Ok(())
            };

            if let Err(error) = forward_blocks.await {
                for tx in &sinks_tx {
                    // Ignore send error
                    let send_error_message = tx.send(Err(error.clone()));
                    let _ = send_error_message.await;
                }
            }
        })?;

        Ok(())
    }

    async fn execute(mut pipeline: Pipeline, ctx: &FuseQueryContextRef) -> Result<AbortStream> {
        let data_stream = pipeline.execute().await?;
        ctx.try_create_abortable(data_stream)
    }

    fn create_stage_streams(
        &self,
        query_id: &str,
        stage_id: &str,
        schema: &DataSchemaRef,
        streams_name: &[String],
    ) {
        let stage_name = format!("{}/{}", query_id, stage_id);
        self.stages_notify
            .write()
            .insert(stage_name.clone(), Arc::new(Notify::new()));

        let mut streams = self.streams.write();

        for stream_name in streams_name {
            let (tx, rx) = mpsc::channel(5);
            let stream_name = format!("{}/{}", stage_name, stream_name);

            streams.insert(stream_name, StreamInfo {
                schema: schema.clone(),
                tx,
                rx,
            });
        }
    }
}

async fn wait_start(stage_name: String, stages_notify: Arc<RwLock<HashMap<String, Arc<Notify>>>>) {
    let notify = {
        let stages_notify = stages_notify.read();
        stages_notify.get(&stage_name).map(Arc::clone)
    };

    if let Some(notify) = notify {
        notify.notified().await;
    }
}
