//! Shared fixtures and helpers for the test modules: a tiny deterministic
//! edge-case shard plus HTTP/Flight call helpers.
use crate::{
    db::{self, NeoConnection},
    filter,
    flight::ShardFlight,
    store::Store,
};
use arrow::{array::StringArray, datatypes::Schema};
use arrow_flight::{decode::FlightRecordBatchStream, flight_service_server::FlightService, Ticket};
use duckdb_neo::Parameters;
use futures::TryStreamExt;
use poem::test::TestResponse;
use serde_json::Value;
use std::sync::Arc;
use tonic::Request;
pub(crate) struct Fixture {
    pub(crate) _dir: tempfile::TempDir,
    pub(crate) store: Arc<Store>,
    pub(crate) catalog: String,
}

pub(crate) fn install_extensions() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let db = db::open_memory().unwrap();
        let conn = db.connect().unwrap();
        db::execute_all(
            &conn,
            &["INSTALL spatial", "INSTALL ducklake", "INSTALL httpfs"],
        )
        .unwrap()
    });
}

impl Fixture {
    pub(crate) fn new(connections: usize) -> Self {
        Self::with_queue(connections, 0, std::time::Duration::ZERO, connections)
    }

    pub(crate) fn with_queue(
        connections: usize,
        max_waiters: usize,
        max_wait: std::time::Duration,
        bulk_limit: usize,
    ) -> Self {
        install_extensions();
        let dir = tempfile::tempdir().unwrap();
        let catalog = dir.path().join("shard.ducklake");
        let files = dir.path().join("files");
        std::fs::create_dir_all(&files).unwrap();
        let db = db::open_memory().unwrap();
        let conn: NeoConnection = db.connect().unwrap();
        db::execute_all(&conn, &["LOAD spatial", "LOAD ducklake", "LOAD httpfs"]).unwrap();
        let attach = format!(
            "ATTACH {} AS lake (DATA_PATH {})",
            filter::quote(&format!("ducklake:{}", catalog.to_str().unwrap())),
            filter::quote(&format!("{}/", files.to_str().unwrap())),
        );
        db::execute_all(&conn, &[attach.as_str(), "USE lake"]).unwrap();
        // One statement per execute: the v2 API takes exactly one.
        for statement in [
            "CREATE TABLE features(id VARCHAR, layer VARCHAR, source_id BIGINT, geom GEOMETRY, properties JSON,
              sortkey BIGINT, xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE, cx DOUBLE, cy DOUBLE, name VARCHAR)",
            "INSERT INTO features VALUES
              ('way:1', 'buildings', 1, ST_GeomFromText('POLYGON ((0 0,10 0,10 10,0 10,0 0))'), '{\"name\":\"Café\",\"height\":12}', 1, 0, 0, 10, 10, 5, 5, 'Café'),
              ('way:2', 'buildings', 2, ST_Point(12,12), '{\"name\":null}', 2, 12, 12, 12, 12, 12, 12, NULL),
              ('relation:1', 'buildings', 1, ST_GeomFromText('POLYGON ((-10 -10,-5 -10,-5 -5,-10 -5,-10 -10))'), '{}', 3, -10, -10, -5, -5, -7.5, -7.5, NULL),
              ('way:3', 'buildings', 1, ST_Point(179,2), '{}', 4, 179, 2, 179, 2, 179, 2, NULL),
              ('way:4', 'buildings', 1, ST_Point(-179,2), '{}', 5, -179, 2, -179, 2, -179, 2, NULL),
              ('way:5', 'buildings', 3, ST_Point(20,60), '{}', 6, 20, 60, 20, 60, 20, 60, NULL),
              ('way:road1', 'roads', 1, ST_GeomFromText('LINESTRING (0 0,20 20)'), '{}', 7, 0, 0, 20, 20, 10, 10, NULL)",
            "CREATE TABLE collections AS SELECT DISTINCT layer AS id FROM features",
            "CALL ducklake_flush_inlined_data('lake')",
        ] {
            conn.execute(statement, Parameters::None).unwrap();
        }
        drop(conn);
        drop(db);
        let catalog_path = catalog.to_str().unwrap().to_string();
        Self {
            store: Arc::new(
                Store::open(
                    &catalog_path,
                    connections,
                    max_waiters,
                    max_wait,
                    bulk_limit,
                    1,
                    0,
                    std::time::Duration::ZERO,
                )
                .unwrap(),
            ),
            catalog: catalog_path,
            _dir: dir,
        }
    }
}

pub(crate) async fn body(response: TestResponse) -> Value {
    let status = response.0.status();
    let text = response.0.into_body().into_string().await.unwrap();
    assert_eq!(status, 200, "body was: {text}");
    serde_json::from_str(&text).unwrap()
}

pub(crate) fn ticket(value: Value) -> Request<Ticket> {
    Request::new(Ticket::new(serde_json::to_vec(&value).unwrap()))
}

pub(crate) async fn flight_ids(
    service: &ShardFlight,
    request: Request<Ticket>,
) -> (Schema, Vec<String>) {
    let stream = service
        .do_get(request)
        .await
        .unwrap()
        .into_inner()
        .map_err(arrow_flight::error::FlightError::from);
    let mut batches = FlightRecordBatchStream::new_from_flight_data(stream);
    let mut ids = Vec::new();
    while let Some(batch) = batches.try_next().await.unwrap() {
        let column = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        ids.extend(column.iter().map(|s| s.unwrap().to_string()));
    }
    (batches.schema().unwrap().as_ref().clone(), ids)
}
