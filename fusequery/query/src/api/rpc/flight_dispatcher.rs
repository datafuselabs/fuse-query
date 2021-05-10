use std::collections::HashMap;

use tokio::runtime::Runtime;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use common_arrow::arrow_flight::FlightData;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::PlanNode;

use common_datavalues::DataSchemaRef;
use common_arrow::arrow::datatypes::SchemaRef;
use crate::pipelines::processors::{Pipeline, PipelineBuilder};
use crate::configs::Config;
use crate::clusters::ClusterRef;
use crate::sessions::SessionRef;
use std::sync::Arc;
use tokio_stream::StreamExt;
use std::convert::TryInto;
use tonic::Status;
use tokio::sync::mpsc::error::SendError;

pub struct PrepareStageInfo(String, String, PlanNode, Vec<String>);

pub struct StreamInfo(pub DataSchemaRef, pub String, pub Vec<String>);

pub enum Request {
    GetSchema(String, Sender<Result<DataSchemaRef>>),
    GetStream(String, Sender<Result<Receiver<Result<FlightData>>>>),
    PrepareQueryStage(PrepareStageInfo, Sender<Result<()>>),
    GetStreamInfo(String, Sender<Result<StreamInfo>>),
}

pub struct QueryInfo {
    pub runtime: Runtime,
}

pub struct FlightStreamInfo {
    schema: DataSchemaRef,
    stream_data_receiver: Receiver<Result<FlightData>>,
    launcher_sender: Sender<()>,
}

pub struct DispatcherState {
    queries: HashMap<String, QueryInfo>,
    streams: HashMap<String, FlightStreamInfo>,
}

struct ServerState {
    conf: Config,
    cluster: ClusterRef,
    session_manager: SessionRef,
}

pub struct FlightDispatcher {
    state: Arc<ServerState>
}

impl FlightDispatcher {
    pub fn run(&self) -> Sender<Request> {
        let state = self.state.clone();
        let (sender, mut receiver) = channel::<Request>(100);
        tokio::spawn(async move { Self::dispatch(state.clone(), receiver).await });
        sender.clone()
    }

    #[inline(always)]
    async fn dispatch(state: Arc<ServerState>, mut receiver: Receiver<Request>) {
        let mut dispatcher_state = DispatcherState::create();
        while let Some(request) = receiver.recv().await {
            match request {
                Request::GetStream(id, stream_receiver) => {
                    match dispatcher_state.streams.remove(&id) {
                        Some(stream_info) => {
                            stream_info.launcher_sender.send(()).await;
                            stream_receiver.send(Ok(stream_info.stream_data_receiver)).await
                        },
                        None => stream_receiver.send(Err(ErrorCodes::NotFoundStream(format!("Stream {} is not found", id)))).await,
                    };
                },
                Request::GetSchema(id, schema_receiver) => {
                    match dispatcher_state.streams.get(&id) {
                        Some(stream_info) => schema_receiver.send(Ok(stream_info.schema.clone())).await,
                        None => schema_receiver.send(Err(ErrorCodes::NotFoundStream(format!("Stream {} is not found", id)))).await,
                    };
                }
                Request::PrepareQueryStage(info, response_sender) => {
                    let mut pipeline = Self::create_plan_pipeline(&*state, &info.2);
                    response_sender.send(Self::prepare_stage(&mut dispatcher_state, &info, pipeline)).await;
                }
                Request::GetStreamInfo(id, stream_info_receiver) => {
                    match dispatcher_state.streams.get(&id) {
                        Some(stream_info) => {
                            stream_info_receiver.send(Ok(StreamInfo(stream_info.schema.clone(), id, vec![]))).await
                        },
                        None => stream_info_receiver.send(Err(ErrorCodes::NotFoundStream(format!("Stream {} is not found", id)))).await,
                    };
                }
            };
        }
        // TODO: shutdown
    }

    fn prepare_stage(state: &mut DispatcherState, info: &PrepareStageInfo, mut pipeline: Result<Pipeline>) -> Result<()> {
        if !state.queries.contains_key(&info.0) {
            fn build_runtime(max_threads: u64) -> Result<Runtime> {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_io()
                    .worker_threads(max_threads as usize)
                    .build()
                    .map_err(|tokio_error| ErrorCodes::TokioError(format!("{}", tokio_error)))
            }

            match build_runtime(8) {
                Err(e) => return Err(e),
                Ok(runtime) => state.queries.insert(info.0.clone(), QueryInfo { runtime })
            };
        }

        let mut streams_data_sender = vec![];
        let (launcher_sender, mut launcher_receiver) = channel(info.3.len());
        for stream_name in &info.3 {
            let stream_full_name = format!("{}/{}/{}", info.0, info.1, stream_name);
            let (sender, stream_info) = FlightStreamInfo::create(&info.2.schema(), &launcher_sender);
            streams_data_sender.push(sender);
            state.streams.insert(stream_full_name, stream_info);
        }

        let query_info = state.queries.get_mut(&info.0).expect("No exists query info");

        query_info.runtime.spawn(async move {
            let _ = launcher_receiver.recv().await;

            let _ = Self::receive_data_and_push(pipeline, streams_data_sender).await;

            // for stream_data_sender in streams_data_sender {
            //     stream_data_sender.send(last_result.clone()).await;
            // }
        });

        Ok(())
    }

    fn create_plan_pipeline(state: &ServerState, plan: &PlanNode) -> Result<Pipeline> {
        state.session_manager.clone()
            .try_create_context()
            .and_then(|ctx| ctx.with_cluster(state.cluster.clone()))
            .and_then(|ctx| {
                ctx.set_max_threads(state.conf.num_cpus);
                PipelineBuilder::create(ctx.clone(), plan.clone()).build()
            })
    }

    // We need to always use the inline function to ensure that async/await state machine is simple enough
    #[inline(always)]
    async fn receive_data_and_push(action_pipeline: Result<Pipeline>, senders: Vec<Sender<Result<FlightData>>>) -> Result<()> {
        use common_arrow::arrow::ipc::writer::IpcWriteOptions;
        use common_arrow::arrow_flight::utils::flight_data_from_arrow_batch;

        let options = IpcWriteOptions::default();
        let mut pipeline = action_pipeline?;
        let mut pipeline_stream = pipeline.execute().await?;

        if senders.len() == 1 {
            while let Some(item) = pipeline_stream.next().await {
                let block = item?;

                let record_batch = block.try_into()?;
                let (dicts, values) = flight_data_from_arrow_batch(&record_batch, &options);
                let normalized_flight_data = dicts.into_iter().chain(std::iter::once(values));
                for flight_data in normalized_flight_data {
                    senders[0].send(Ok(flight_data)).await;
                }
            }
        } else {
            while let Some(item) = pipeline_stream.next().await {
                let block = item?;

                // TODO: split block and push to sender
                let record_batch = block.try_into()?;
                let (dicts, values) = flight_data_from_arrow_batch(&record_batch, &options);
                let normalized_flight_data = dicts.into_iter().chain(std::iter::once(values));
                for flight_data in normalized_flight_data {
                    // TODO: push to sender
                    senders[0].send(Ok(flight_data)).await;
                }
            }
        }

        Ok(())
    }

    pub fn new(conf: Config, cluster: ClusterRef, session_manager: SessionRef) -> FlightDispatcher {
        FlightDispatcher {
            state: Arc::new(ServerState {
                conf: conf,
                cluster: cluster,
                session_manager: session_manager,
            })
        }
    }
}

impl DispatcherState {
    pub fn create() -> DispatcherState {
        DispatcherState {
            queries: HashMap::new(),
            streams: HashMap::new(),
        }
    }
}

impl FlightStreamInfo {
    pub fn create(schema: &SchemaRef, launcher_sender: &Sender<()>) -> (Sender<Result<FlightData>>, FlightStreamInfo) {
        let (sender, mut receive) = channel(100);
        (sender, FlightStreamInfo {
            schema: schema.clone(),
            stream_data_receiver: receive,
            launcher_sender: launcher_sender.clone(),
        })
    }
}

impl PrepareStageInfo {
    pub fn create(query_id: String, stage_id: String, plan_node: PlanNode, shuffle_to: Vec<String>) -> PrepareStageInfo {
        PrepareStageInfo(query_id, stage_id, plan_node, shuffle_to)
    }
}
