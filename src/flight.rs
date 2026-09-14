//! Raw Arrow Flight fast path (no ADBC layer).
//!
//! One shard (HTTP or local Parquet) scanned by DuckDB, Arrow record batches
//! streamed to the caller. Lineage comes from request metadata
//! (`x-source-ids` dev stand-in, JWT claim next) union the ticket's own
//! list, and is injected into SQL. An empty set matches nothing.
//!
//! Wire protocol (v0): `DoGet` with a JSON ticket:
//! `{"collection":"buildings","bbox":[minx,miny,maxx,maxy],
//!   "columns":["id","x"],"limit":10000,"sources":[1,2]}`.
//! Clients call `DoGet` directly; discovery RPCs graduate later.
//!
//! Overload protection mirrors the Poem path: repeated tickets share a moka
//! batch cache, concurrent identical tickets coalesce in the guard, and only
//! 16 DuckDB queries run at once (overflow sheds `resource_exhausted`).

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::{Stream, TryStreamExt};
use moka::future::Cache;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
};
use tonic::{Request, Response, Status};

const COLLECTIONS: [&str; 2] = ["buildings", "ag_fields"];
const COLUMNS: [&str; 5] = ["id", "x", "y", "name", "source_id"];
const MAX_LIMIT: u32 = 100_000;
const BATCH_ROWS: usize = 8192;

/// Fixed fast schema: plain doubles for position so bbox filters prune via
/// Parquet stats with no extensions. Sort shard files by `x` for max effect.
/// (GEOMETRY + ST_Intersects graduates in with the hot `.duckdb` shards.)
#[derive(serde::Deserialize, Debug)]
struct ShardTicket {
    collection: String,
    bbox: Option<[f64; 4]>,
    columns: Option<Vec<String>>,
    limit: Option<u32>,
    sources: Option<Vec<i64>>,
}

#[derive(Clone)]
pub struct ShardFlight {
    duck: Arc<Mutex<duckdb::Connection>>,
    source: Arc<String>,
    guard: crate::guard::RequestGuard<Vec<RecordBatch>>,
    batches: Cache<String, Vec<RecordBatch>>,
}

impl ShardFlight {
    pub fn open(source: &str) -> Result<Self, String> {
        let duck = duckdb::Connection::open_in_memory().map_err(|e| format!("duckdb: {e}"))?;
        // Best effort: only remote http(s):// sources need httpfs; local
        // paths work regardless.
        if let Err(e) = duck.execute_batch("INSTALL httpfs; LOAD httpfs") {
            tracing::warn!(error = %e, "httpfs unavailable; remote sources will fail");
        }
        Ok(Self {
            duck: Arc::new(Mutex::new(duck)),
            source: Arc::new(source.to_string()),
            // Bulk queries run long: roomy slots, patient waits.
            guard: crate::guard::RequestGuard::new(
                16,
                std::time::Duration::from_secs(30),
                std::time::Duration::from_secs(10),
            ),
            batches: Cache::builder()
                .max_capacity(256)
                .time_to_live(std::time::Duration::from_secs(300))
                .build(),
        })
    }

    /// Validate the ticket and build (SQL, projection, schema).
    /// Only allowlisted identifiers ever reach SQL; the source URL is
    /// operator config (CLI), never caller input.
    fn plan(
        &self,
        ticket: &ShardTicket,
        meta_sources: &[i64],
    ) -> Result<(String, Vec<String>, Arc<Schema>), Status> {
        if !COLLECTIONS.contains(&ticket.collection.as_str()) {
            return Err(Status::not_found(format!(
                "unknown collection: {}",
                ticket.collection
            )));
        }
        let cols: Vec<String> = match &ticket.columns {
            Some(requested) => {
                for name in requested {
                    if !COLUMNS.contains(&name.as_str()) {
                        return Err(Status::invalid_argument(format!("unknown column: {name}")));
                    }
                }
                requested.clone()
            }
            None => COLUMNS.iter().map(|s| s.to_string()).collect(),
        };
        if cols.is_empty() {
            return Err(Status::invalid_argument("columns must not be empty"));
        }
        let mut sources = ticket.sources.clone().unwrap_or_default();
        sources.extend_from_slice(meta_sources);
        sources.sort_unstable();
        sources.dedup();

        if let Some(bbox) = ticket.bbox {
            crate::filter::check_bbox(bbox).map_err(Status::invalid_argument)?;
        }
        let pred = crate::filter::lineage_predicate(&sources);
        let bbox_sql = ticket
            .bbox
            .map(|b| {
                // Inlined numerics: planner-known constants, stats pruning eligible.
                format!(
                    " AND x BETWEEN {} AND {} AND y BETWEEN {} AND {}",
                    b[0], b[2], b[1], b[3]
                )
            })
            .unwrap_or_default();
        let limit = ticket.limit.unwrap_or(10_000).min(MAX_LIMIT);
        let select = cols.join(", ");
        let sql = format!(
            "SELECT {select} FROM read_parquet('{}') WHERE {pred}{bbox_sql} LIMIT {limit}",
            self.source
        );
        let schema = Arc::new(Schema::new(
            cols.iter()
                .map(|name| match name.as_str() {
                    "x" | "y" => Field::new(name, DataType::Float64, true),
                    "source_id" => Field::new(name, DataType::Int64, true),
                    _ => Field::new(name, DataType::Utf8, true),
                })
                .collect::<Vec<_>>(),
        ));
        Ok((sql, cols, schema))
    }

    /// Run the query on the DuckDB connection and chunk rows into batches.
    /// Synchronous throughout: callers run this under `spawn_blocking`.
    fn run_query(
        duck: &Mutex<duckdb::Connection>,
        sql: &str,
        cols: &[String],
    ) -> Result<(Arc<Schema>, Vec<RecordBatch>), Status> {
        enum Col {
            Str(Vec<Option<String>>),
            F64(Vec<Option<f64>>),
            I64(Vec<Option<i64>>),
        }
        let conn = duck.lock().map_err(|_| Status::internal("duckdb lock"))?;
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut builders: Vec<Col> = cols
            .iter()
            .map(|name| match name.as_str() {
                "x" | "y" => Col::F64(Vec::new()),
                "source_id" => Col::I64(Vec::new()),
                _ => Col::Str(Vec::new()),
            })
            .collect();
        let mut rows = stmt
            .query_map([], |row| {
                for (index, _) in cols.iter().enumerate() {
                    match &mut builders[index] {
                        Col::Str(values) => values.push(row.get(index)?),
                        Col::F64(values) => values.push(row.get(index)?),
                        Col::I64(values) => values.push(row.get(index)?),
                    }
                }
                Ok(())
            })
            .map_err(|e| Status::internal(e.to_string()))?;
        while rows
            .next()
            .transpose()
            .map_err(|e| Status::internal(e.to_string()))?
            .is_some()
        {}
        drop(rows);

        let schema = Arc::new(Schema::new(
            cols.iter()
                .map(|name| match name.as_str() {
                    "x" | "y" => Field::new(name, DataType::Float64, true),
                    "source_id" => Field::new(name, DataType::Int64, true),
                    _ => Field::new(name, DataType::Utf8, true),
                })
                .collect::<Vec<_>>(),
        ));
        let mut batches = Vec::new();
        loop {
            let remaining = match builders.first() {
                Some(Col::Str(values)) => values.len(),
                Some(Col::F64(values)) => values.len(),
                Some(Col::I64(values)) => values.len(),
                None => 0,
            };
            if remaining == 0 {
                break;
            }
            let take = remaining.min(BATCH_ROWS);
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(builders.len());
            for builder in builders.iter_mut() {
                let array: ArrayRef = match builder {
                    Col::Str(values) => {
                        Arc::new(StringArray::from(values.drain(..take).collect::<Vec<_>>()))
                    }
                    Col::F64(values) => {
                        Arc::new(Float64Array::from(values.drain(..take).collect::<Vec<_>>()))
                    }
                    Col::I64(values) => {
                        Arc::new(Int64Array::from(values.drain(..take).collect::<Vec<_>>()))
                    }
                };
                arrays.push(array);
            }
            batches.push(
                RecordBatch::try_new(schema.clone(), arrays)
                    .map_err(|e| Status::internal(e.to_string()))?,
            );
        }
        Ok((schema, batches))
    }

    fn stream_out(
        batches: Vec<RecordBatch>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // No explicit schema attached: the encoder emits it with the first
        // batch. Empty results stream zero messages (documented behavior).
        let stream = FlightDataEncoderBuilder::new().build(futures::stream::iter(
            batches
                .into_iter()
                .map(Ok::<_, arrow_flight::error::FlightError>),
        ));
        Ok(Response::new(
            Box::pin(stream.map_err(Status::from)) as <ShardFlight as FlightService>::DoGetStream
        ))
    }
}

const UNIMPLEMENTED: &str = "iron-feather v0: use DoGet with a JSON ticket";

#[tonic::async_trait]
impl FlightService for ShardFlight {
    type HandshakeStream = Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send>>;
    type DoActionStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream = Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send>>;
    type DoExchangeStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(UNIMPLEMENTED))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented(UNIMPLEMENTED))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(UNIMPLEMENTED))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(UNIMPLEMENTED))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented(UNIMPLEMENTED))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket: ShardTicket = serde_json::from_slice(&request.get_ref().ticket)
            .map_err(|e| Status::invalid_argument(format!("bad ticket: {e}")))?;
        let meta_sources = request
            .metadata()
            .get("x-source-ids")
            .and_then(|value| value.to_str().ok())
            .map(|raw| crate::filter::parse_source_list(Some(raw)))
            .unwrap_or_default();
        let mut sources = ticket.sources.clone().unwrap_or_default();
        sources.extend_from_slice(&meta_sources);
        let key = format!(
            "flight:{}:{}:{}:{}",
            ticket.collection,
            ticket
                .bbox
                .map(|bbox| format!("{bbox:?}"))
                .unwrap_or_default(),
            ticket.columns.clone().unwrap_or_default().join(","),
            crate::filter::visibility_fingerprint("flight", "dev", &sources),
        );
        if let Some(batches) = self.batches.get(&key).await {
            return Self::stream_out(batches);
        }
        let (sql, cols, _) = self.plan(&ticket, &meta_sources)?;
        let duck = self.duck.clone();
        let cached = self
            .guard
            .run(key.clone(), move || {
                let duck = duck.clone();
                let sql = sql.clone();
                let cols = cols.clone();
                async move {
                    tokio::task::spawn_blocking(move || {
                        Self::run_query(&duck, &sql, &cols)
                            .map(|(_, batches)| batches)
                            .map_err(|status| crate::guard::GuardError::Backend(status.to_string()))
                    })
                    .await
                    .map_err(|e| crate::guard::GuardError::Backend(format!("join: {e}")))?
                }
            })
            .await
            .map_err(|e| match e {
                crate::guard::GuardError::Overloaded => Status::resource_exhausted("shedding load"),
                crate::guard::GuardError::Missing => Status::not_found("row absent"),
                crate::guard::GuardError::Backend(message) => Status::internal(message),
            })?;
        let batches = (*cached).clone();
        self.batches.insert(key, batches.clone()).await;
        Self::stream_out(batches)
    }

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(UNIMPLEMENTED))
    }

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(UNIMPLEMENTED))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented(UNIMPLEMENTED))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented(UNIMPLEMENTED))
    }
}

pub async fn serve(listen: String, service: ShardFlight) -> Result<(), Box<dyn std::error::Error>> {
    let addr: std::net::SocketAddr = listen.parse()?;
    tracing::info!(%addr, "arrow flight up");
    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static FIXTURE_SEQ: AtomicU64 = AtomicU64::new(0);

    /// Tiny fast-schema Parquet: id, x, y, source_id, name.
    fn tiny_parquet() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "iron-feather-test-{}-{}",
            std::process::id(),
            FIXTURE_SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tiny.parquet");
        let duck = duckdb::Connection::open_in_memory().unwrap();
        duck.execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES \
             ('a', 1.0, 2.0, CAST(1 AS BIGINT), 'one'), \
             ('b', 9.0, 9.0, CAST(2 AS BIGINT), 'two')) \
             t(id, x, y, source_id, name)) TO '{}' (FORMAT PARQUET)",
            path.display()
        ))
        .unwrap();
        path
    }

    fn service_for(path: &std::path::Path) -> ShardFlight {
        ShardFlight::open(path.to_str().unwrap()).unwrap()
    }

    fn test_service() -> ShardFlight {
        ShardFlight {
            duck: Arc::new(Mutex::new(duckdb::Connection::open_in_memory().unwrap())),
            source: Arc::new(String::new()),
            guard: crate::guard::RequestGuard::new(
                4,
                std::time::Duration::from_secs(5),
                std::time::Duration::from_secs(1),
            ),
            batches: moka::future::Cache::builder().max_capacity(16).build(),
        }
    }

    #[test]
    fn plan_rejects_unknown_collection_and_column() {
        let svc = test_service();
        let bad_collection = ShardTicket {
            collection: "nope".to_string(),
            bbox: None,
            columns: None,
            limit: None,
            sources: None,
        };
        let err = svc.plan(&bad_collection, &[]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);

        let bad_column = ShardTicket {
            collection: "buildings".to_string(),
            bbox: None,
            columns: Some(vec!["; DROP TABLE features; --".to_string()]),
            limit: None,
            sources: None,
        };
        let err = svc.plan(&bad_column, &[]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn round_trip_applies_bbox_and_lineage() {
        let path = tiny_parquet();
        let svc = service_for(&path);

        // bbox hits only row a; lineage allows source 1 only.
        let ticket = ShardTicket {
            collection: "buildings".to_string(),
            bbox: Some([0.0, 0.0, 5.0, 5.0]),
            columns: None,
            limit: None,
            sources: Some(vec![1]),
        };
        let (sql, cols, schema) = svc.plan(&ticket, &[]).unwrap();
        assert!(sql.contains("source_id IN (1)"), "{sql}");
        assert_eq!(schema.fields().len(), 5);
        let (_, batches) = ShardFlight::run_query(&svc.duck, &sql, &cols).unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1);
        assert_eq!(batches[0].num_columns(), 5);

        // Secure default: no lineage anywhere => zero rows, not an error.
        let ticket = ShardTicket {
            collection: "buildings".to_string(),
            bbox: None,
            columns: Some(vec!["id".to_string()]),
            limit: None,
            sources: None,
        };
        let (sql, cols, _) = svc.plan(&ticket, &[]).unwrap();
        assert!(sql.contains("1 = 0"), "{sql}");
        let (_, batches) = ShardFlight::run_query(&svc.duck, &sql, &cols).unwrap();
        assert!(batches.iter().map(RecordBatch::num_rows).sum::<usize>() == 0);

        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }
}
