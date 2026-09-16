//! Read-only Arrow Flight: discovery plus JSON-ticket DoGet over the same
//! shard as REST. DuckDB supplies Arrow batches and the empty schema.

use crate::{
    filter,
    store::{Error, Store},
};
use arrow_flight::{
    encode::FlightDataEncoderBuilder, flight_service_server::FlightService, Action, ActionType,
    Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::{Stream, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::{pin::Pin, sync::Arc};
use tonic::{Request, Response, Status};

type FlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;
const READ_ONLY: &str = "read-only service: use ListFlights, GetFlightInfo, GetSchema or DoGet";
const DEFAULT_COLUMNS: [&str; 4] = ["id", "geometry", "properties", "source_id"];

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ShardTicket {
    collection: String,
    bbox: Option<Vec<f64>>,
    columns: Option<Vec<String>>,
    limit: Option<u32>,
    #[serde(default)]
    offset: u32,
    sources: Option<Vec<i64>>,
}

impl From<Error> for Status {
    fn from(e: Error) -> Self {
        match e {
            Error::Invalid(msg) => Status::invalid_argument(msg),
            Error::NotFound(msg) => Status::not_found(msg),
            Error::Overloaded => Status::resource_exhausted("server busy; retry"),
            Error::Backend(msg) => {
                tracing::error!(error = %msg, "Flight query failed");
                Status::internal("shard query failed")
            }
        }
    }
}

#[derive(Clone)]
pub struct ShardFlight {
    pub store: Arc<Store>,
}

struct Planned {
    predicate: String,
    projection: String,
}

impl ShardFlight {
    fn plan(&self, ticket: &ShardTicket, header: Option<Vec<i64>>) -> Result<Planned, Status> {
        self.store.collection(&ticket.collection)?;
        let columns = ticket
            .columns
            .clone()
            .unwrap_or_else(|| DEFAULT_COLUMNS.iter().map(|s| s.to_string()).collect());
        if columns.is_empty() {
            return Err(Status::invalid_argument("columns must not be empty"));
        }
        let mut select = Vec::with_capacity(columns.len());
        for (i, column) in columns.iter().enumerate() {
            if columns[..i].contains(column) {
                return Err(Status::invalid_argument("duplicate column"));
            }
            select.push(match column.as_str() {
                "id" => "id",
                "geometry" => "ST_AsWKB(geom) AS geometry",
                "properties" => "properties::VARCHAR AS properties",
                "source_id" => "source_id",
                // Build-time derivatives when the shard carries them;
                // otherwise the equivalent computed expression.
                "x" | "y" | "name" => self.store.derived_or(column),
                _ => {
                    return Err(Status::invalid_argument(format!(
                        "unknown column: {column}"
                    )))
                }
            });
        }
        let bbox = ticket
            .bbox
            .as_deref()
            .map(filter::bbox)
            .transpose()
            .map_err(Status::invalid_argument)?;
        let limit = ticket.limit.unwrap_or(10_000);
        if limit > 100_000 {
            return Err(Status::invalid_argument("limit must be <= 100000"));
        }
        let sources = filter::sources(ticket.sources.clone(), header);
        let predicate = Store::predicate(&ticket.collection, bbox, &sources);
        Ok(Planned {
            predicate,
            projection: select.join(", "),
        })
    }

    fn descriptor(descriptor: &FlightDescriptor) -> Result<ShardTicket, Status> {
        match descriptor.r#type {
            1 if descriptor.path.len() == 1 => Ok(ShardTicket {
                collection: descriptor.path[0].clone(),
                bbox: None,
                columns: None,
                limit: None,
                offset: 0,
                sources: None,
            }),
            2 => serde_json::from_slice(&descriptor.cmd)
                .map_err(|e| Status::invalid_argument(e.to_string())),
            _ => Err(Status::invalid_argument(
                "descriptor must be a one-part collection path or a JSON command",
            )),
        }
    }

    async fn info(&self, descriptor: FlightDescriptor) -> Result<FlightInfo, Status> {
        let ticket = Self::descriptor(&descriptor)?;
        let planned = self.plan(&ticket, None)?;
        let schema = self
            .store
            .arrow("FALSE".into(), planned.projection, 1, 0)
            .await?;
        Ok(FlightInfo::new()
            .try_with_schema(&schema.schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(descriptor)
            .with_endpoint(
                FlightEndpoint::new()
                    .with_ticket(Ticket::new(serde_json::to_vec(&ticket).unwrap())),
            ))
    }
}

#[tonic::async_trait]
impl FlightService for ShardFlight {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    type ListFlightsStream = FlightStream<FlightInfo>;
    type DoGetStream = FlightStream<FlightData>;
    type DoPutStream = FlightStream<PutResult>;
    type DoActionStream = FlightStream<arrow_flight::Result>;
    type ListActionsStream = FlightStream<ActionType>;
    type DoExchangeStream = FlightStream<FlightData>;

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        if !request.get_ref().expression.is_empty() {
            return Err(Status::invalid_argument("criteria are not supported"));
        }
        let mut flights = Vec::new();
        for collection in &self.store.collections {
            flights.push(Ok(self
                .info(FlightDescriptor::new_path(vec![collection.clone()]))
                .await?));
        }
        Ok(Response::new(Box::pin(futures::stream::iter(flights))))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Ok(Response::new(self.info(request.into_inner()).await?))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let ticket = Self::descriptor(request.get_ref())?;
        let planned = self.plan(&ticket, None)?;
        let result = self
            .store
            .arrow("FALSE".into(), planned.projection, 1, 0)
            .await?;
        let schema = SchemaAsIpc::new(&result.schema, &Default::default())
            .try_into()
            .map_err(|e: duckdb::arrow::error::ArrowError| Status::internal(e.to_string()))?;
        Ok(Response::new(schema))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket: ShardTicket = serde_json::from_slice(&request.get_ref().ticket)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let header = request
            .metadata()
            .get("x-source-ids")
            .map(|value| {
                let raw = value
                    .to_str()
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                filter::parse_sources(raw).map_err(Status::invalid_argument)
            })
            .transpose()?;
        // Validate before streaming; SQL includes the full request.
        // Batches stream through per-stream and process-wide byte budgets;
        // the guard lives in the response stream so premature drop marks
        // cancellation and interrupts the query, while normal completion
        // clears the handle before the connection is reused. The consumer
        // releases both budgets on read.
        let planned = self.plan(&ticket, header)?;
        let batches = self
            .store
            .arrow_stream(
                planned.predicate,
                planned.projection,
                ticket.limit.unwrap_or(10_000),
                ticket.offset,
            )
            .await?;
        let schema = batches.schema.clone();
        let input = futures::stream::unfold(
            (
                batches.batches,
                batches.guard,
                batches.buffered,
                batches.global_buffered,
            ),
            |(mut rx, guard, buffered, global)| async move {
                match rx.recv().await {
                    Some(Ok(batch)) => {
                        let size = batch.get_array_memory_size();
                        buffered.fetch_sub(size, std::sync::atomic::Ordering::AcqRel);
                        global.fetch_sub(size, std::sync::atomic::Ordering::AcqRel);
                        Some((
                            Ok(batch)
                                as Result<
                                    duckdb::arrow::record_batch::RecordBatch,
                                    arrow_flight::error::FlightError,
                                >,
                            (rx, guard, buffered, global),
                        ))
                    }
                    Some(Err(e)) => Some((
                        Err(arrow_flight::error::FlightError::from_external_error(
                            Box::new(e),
                        )),
                        (rx, guard, buffered, global),
                    )),
                    None => None,
                }
            },
        );
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(input);
        Ok(Response::new(Box::pin(stream.map_err(Status::from))))
    }

    async fn handshake(
        &self,
        _: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(READ_ONLY))
    }
    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(READ_ONLY))
    }
    async fn do_put(
        &self,
        _: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(READ_ONLY))
    }
    async fn do_exchange(
        &self,
        _: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(READ_ONLY))
    }
    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented(READ_ONLY))
    }
    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(Box::pin(futures::stream::empty())))
    }
}
