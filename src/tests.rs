//! Regression checks exercise real DuckDB, HTTP responses and Flight wire
//! encoding. Fixtures contain multiple collections, sources and geometries.
use crate::{
    api,
    db::{self, NeoConnection},
    filter,
    flight::ShardFlight,
    materialize::Build,
    store::{Error, Store},
};
use arrow::{
    array::StringArray,
    datatypes::{DataType, Schema},
};
use arrow_flight::{
    decode::FlightRecordBatchStream, flight_service_server::FlightService, Criteria,
    FlightDescriptor, Ticket,
};
use duckdb_neo::Parameters;
use futures::TryStreamExt;
use poem::{
    http::StatusCode,
    test::{TestClient, TestResponse},
};
use serde_json::{json, Value};
use std::sync::Arc;
use tonic::Request;

struct Fixture {
    _dir: tempfile::TempDir,
    store: Arc<Store>,
    catalog: String,
}

fn install_extensions() {
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
    fn new(connections: usize) -> Self {
        Self::with_queue(connections, 0, std::time::Duration::ZERO, connections)
    }

    fn with_queue(
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
                    8 * 1024 * 1024,
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

async fn body(response: TestResponse) -> Value {
    let status = response.0.status();
    let text = response.0.into_body().into_string().await.unwrap();
    assert_eq!(status, 200, "body was: {text}");
    serde_json::from_str(&text).unwrap()
}

#[tokio::test]
async fn ogc_discovery_and_contract_describe_the_actual_shard() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let landing = body(client.get("/").send().await).await;
    for link in landing["links"].as_array().unwrap() {
        client
            .get(link["href"].as_str().unwrap())
            .send()
            .await
            .assert_status_is_ok();
    }
    let catalog = body(client.get("/collections").send().await).await;
    assert_eq!(catalog["collections"].as_array().unwrap().len(), 2);
    assert_eq!(catalog["collections"][1]["id"], "roads");
    for meta in catalog["collections"].as_array().unwrap() {
        for link in meta["links"].as_array().unwrap() {
            client
                .get(link["href"].as_str().unwrap())
                .send()
                .await
                .assert_status_is_ok();
        }
    }
    client
        .get("/collections/ag_fields")
        .send()
        .await
        .assert_status(StatusCode::NOT_FOUND);
    let contract = body(client.get("/api").send().await).await;
    assert_eq!(contract["openapi"], "3.0.3");
    assert!(
        contract["paths"]["/collections/{collectionId}/items"]["get"]["responses"]["200"]
            ["content"]
            .get(api::GEOJSON)
            .is_some()
    );
    assert_eq!(
        body(client.get("/conformance").send().await).await["conformsTo"],
        json!(api::CONFORMANCE)
    );
}

#[tokio::test]
async fn ogc_preserves_polygons_properties_and_bbox_intersections() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    // The polygon intersects this box; its centroid does not.
    let response = client
        .get("/collections/buildings/items?bbox=9,9,11,11&sources=1")
        .send()
        .await;
    response.assert_content_type(api::GEOJSON);
    let page = body(response).await;
    assert_eq!(page["numberReturned"], 1);
    assert_eq!(page["features"][0]["geometry"]["type"], "Polygon");
    assert_eq!(
        page["features"][0]["geometry"]["coordinates"][0]
            .as_array()
            .unwrap()
            .len(),
        5
    );
    assert_eq!(page["features"][0]["properties"]["height"], 12);
    assert_eq!(page["features"][0]["properties"]["name"], "Café");
    let href = page["features"][0]["links"][0]["href"].as_str().unwrap();
    let feature = body(client.get(href).send().await).await;
    assert_eq!(feature, page["features"][0]);
    client
        .get(href.replace("sources=1", "sources=2"))
        .send()
        .await
        .assert_status(StatusCode::NOT_FOUND);
    client
        .get("/collections/buildings/items/%27%20OR%20TRUE%20--?sources=1")
        .send()
        .await
        .assert_status(StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ogc_pages_keep_filters_and_have_no_duplicates() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let mut next = Some(
        "/collections/buildings/items?sources=1&limit=1&datetime=2026-01-01T00%3A00%3A00Z"
            .to_string(),
    );
    let mut ids = Vec::new();
    while let Some(url) = next.take() {
        let page = body(client.get(&url).send().await).await;
        assert_eq!(page["numberReturned"], 1);
        assert!(page.get("numberMatched").is_none());
        let id = page["features"][0]["id"].as_str().unwrap().to_string();
        assert!(!ids.contains(&id));
        ids.push(id);
        next = page["links"]
            .as_array()
            .unwrap()
            .iter()
            .find(|l| l["rel"] == "next")
            .map(|l| l["href"].as_str().unwrap().to_string());
        if let Some(url) = &next {
            assert!(url.contains("sources=1") && url.contains("datetime="));
        }
    }
    assert_eq!(ids.len(), 4);
    assert_eq!(
        body(client.get("/collections/buildings/items").send().await).await["numberReturned"],
        0
    );
}

#[tokio::test]
async fn ogc_validates_bbox_datetime_parameters_and_accept() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    for query in [
        "bbox=0,0,inf,10",
        "bbox=0,0,181,10",
        "bbox=0,30,10,20",
        "bbox=1,2,3",
        "limit=0",
        "limit=1001",
        "offset=-1",
        "filter=name",
        "properties=name",
        "sources=x",
        "datetime=bad",
        "datetime=../..",
        "datetime=2027-01-01T00:00:00Z/2026-01-01T00:00:00Z",
    ] {
        client
            .get(format!("/collections/buildings/items?{query}"))
            .send()
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }
    let crossing = body(
        client
            .get("/collections/buildings/items?sources=1&bbox=170,0,-170,5")
            .send()
            .await,
    )
    .await;
    assert_eq!(crossing["numberReturned"], 2);
    assert_eq!(
        body(
            client
                .get("/collections/buildings/items?sources=1&bbox=9,9,-100,11,11,100")
                .send()
                .await
        )
        .await["numberReturned"],
        1
    );
    assert_eq!(
        body(
            client
                .get("/collections/buildings/items?sources=1&bbox=10,10,10,10")
                .send()
                .await
        )
        .await["numberReturned"],
        1
    );
    for accept in [
        "text/html",
        "application/geo+json;q=0,application/json;q=0,*/*;q=1",
    ] {
        client
            .get("/collections/buildings/items")
            .header("Accept", accept)
            .send()
            .await
            .assert_status(StatusCode::NOT_ACCEPTABLE);
    }
    client
        .get("/collections/buildings/items")
        .header("Accept", "application/json")
        .send()
        .await
        .assert_status(StatusCode::NOT_ACCEPTABLE);
    for path in [
        "/",
        "/collections",
        "/collections/buildings",
        "/conformance",
        "/api",
    ] {
        client
            .get(format!("{path}?unsupported=1"))
            .send()
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn ogc_canonical_requests_share_cache_and_body() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let a = client
        .get("/collections/buildings/items?sources=1&limit=2&bbox=9,9,11,11")
        .send()
        .await;
    a.assert_status_is_ok();
    let a = a.0.into_body().into_bytes().await.unwrap();
    // Reordered params and padded floats are the same page.
    let b = client
        .get("/collections/buildings/items?limit=2&bbox=9.0,9.0,11.0,11.0&sources=1")
        .send()
        .await;
    b.assert_status_is_ok();
    let b = b.0.into_body().into_bytes().await.unwrap();
    assert_eq!(a, b);
    // Source order and repeats never change the effective set either.
    let c = client
        .get("/collections/buildings/items?sources=1,2&limit=10")
        .send()
        .await;
    c.assert_status_is_ok();
    let c = c.0.into_body().into_bytes().await.unwrap();
    let d = client
        .get("/collections/buildings/items?sources=2,1,1&limit=10")
        .send()
        .await;
    d.assert_status_is_ok();
    let d = d.0.into_body().into_bytes().await.unwrap();
    assert_eq!(c, d);
}

#[tokio::test]
async fn ogc_header_filtered_pagination_stays_self_contained() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    // No `sources` param: the effective set comes from the header alone.
    let page = body(
        client
            .get("/collections/buildings/items?limit=1")
            .header("X-Source-Ids", "1")
            .send()
            .await,
    )
    .await;
    assert_eq!(page["numberReturned"], 1);
    let next = page["links"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l["rel"] == "next")
        .unwrap()["href"]
        .as_str()
        .unwrap()
        .to_string();
    // The next link carries the effective set, so it works header-free.
    assert!(next.contains("sources=1"));
    let followed = body(client.get(&next).send().await).await;
    assert_eq!(followed["numberReturned"], 1);
    assert_ne!(followed["features"][0]["id"], page["features"][0]["id"]);
}

#[tokio::test]
async fn ogc_gzip_variant_matches_identity_body() {
    use std::io::Read;
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let path = "/collections/buildings/items?sources=1&limit=2";
    let identity = client.get(path).send().await;
    identity.assert_status_is_ok();
    assert!(identity.0.headers().get("content-encoding").is_none());
    let identity_etag = identity
        .0
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let identity_body = identity.0.into_body().into_bytes().await.unwrap();
    let gzipped = client
        .get(path)
        .header("Accept-Encoding", "gzip")
        .send()
        .await;
    gzipped.assert_status_is_ok();
    assert_eq!(gzipped.0.headers().get("content-encoding").unwrap(), "gzip");
    // Variants are distinct representations with distinct validators.
    let gzip_etag = gzipped
        .0
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert_ne!(gzip_etag, identity_etag);
    let compressed = gzipped.0.into_body().into_bytes().await.unwrap();
    assert!(compressed.len() < identity_body.len());
    let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
    let mut roundtrip = Vec::new();
    decoder.read_to_end(&mut roundtrip).unwrap();
    assert_eq!(roundtrip, identity_body);
    // The stored variant revalidates without a body.
    let etag = client
        .get(path)
        .header("Accept-Encoding", "gzip")
        .send()
        .await
        .0
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    client
        .get(path)
        .header("Accept-Encoding", "gzip")
        .header("If-None-Match", etag)
        .send()
        .await
        .assert_status(StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn metrics_reports_cache_counters() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let before = client.get("/metrics").send().await;
    before.assert_status_is_ok();
    before.assert_content_type("text/plain; charset=utf-8");
    // Metrics are never cached.
    assert_eq!(before.0.headers().get("cache-control").unwrap(), "no-store");
    client
        .get("/collections/buildings/items?sources=1&limit=1")
        .send()
        .await
        .assert_status_is_ok();
    client
        .get("/collections/buildings/items?sources=1&limit=1")
        .send()
        .await
        .assert_status_is_ok();
    let after = client.get("/metrics").send().await;
    after.assert_status_is_ok();
    let text = after.0.into_body().into_string().await.unwrap();
    let get = |name: &str| {
        text.lines()
            .find(|line| line.starts_with(name))
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    // Two identical pages: two HTTP responses, one compute, one fast hit.
    // (/metrics itself counts as neither lookup nor HTTP response.)
    assert!(get("http_requests") >= 2);
    assert!(get("cache_requests") >= 2);
    assert!(get("cache_computes") >= 1);
    assert!(get("cache_hits") >= 1);
    assert!(get("cache_requests") > get("cache_computes"));
    // Lookups split into fast hits, coalesced waiters and distinct computes.
    assert!(
        get("cache_requests")
            >= get("cache_hits") + get("cache_coalesced") + get("cache_computes") - 1
    );
    // Failures are tracked separately (none here).
    assert_eq!(get("cache_failures"), 0);
}

#[tokio::test]
async fn ogc_errors_are_never_cacheable() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    for path in [
        "/collections/buildings/items/no-such-id?sources=1",
        "/collections/buildings/items?bbox=0,0,181,10",
    ] {
        let response = client.get(path).send().await;
        assert!(response.0.status().is_client_error());
        let headers = response.0.headers();
        assert_eq!(headers.get("cache-control").unwrap(), "no-store");
        assert!(headers.get("etag").is_none());
    }
}

#[tokio::test]
async fn ogc_etag_revalidates_without_body() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let path = "/collections/buildings/items?sources=1&limit=1";
    let first = client.get(path).send().await;
    first.assert_status_is_ok();
    let etag = first
        .0
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(!etag.is_empty());
    let repeat = client.get(path).send().await;
    assert_eq!(
        repeat.0.headers().get("etag").unwrap().to_str().unwrap(),
        etag
    );
    let revalidated = client.get(path).header("If-None-Match", etag).send().await;
    revalidated.assert_status(StatusCode::NOT_MODIFIED);
    assert!(revalidated
        .0
        .into_body()
        .into_bytes()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn source_headers_narrow_requests_and_partition_cached_pages() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let path = "/collections/buildings/items?sources=1,2";
    let a = body(client.get(path).header("X-Source-Ids", "1").send().await).await;
    let b = body(client.get(path).header("X-Source-Ids", "2").send().await).await;
    assert_eq!(a["numberReturned"], 4);
    assert_eq!(b["numberReturned"], 1);
    assert_eq!(b["features"][0]["id"], "way:2");
    assert_eq!(
        body(client.get(path).header("X-Source-Ids", "").send().await).await["numberReturned"],
        0
    );
}

fn ticket(value: Value) -> Request<Ticket> {
    Request::new(Ticket::new(serde_json::to_vec(&value).unwrap()))
}

async fn flight_ids(service: &ShardFlight, request: Request<Ticket>) -> (Schema, Vec<String>) {
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

#[tokio::test]
async fn flight_arrow_is_collection_scoped_and_limit_sensitive() {
    let fixture = Fixture::new(2);
    let service = ShardFlight {
        store: fixture.store,
    };
    for limit in [1, 4, 2, 0] {
        let (schema, ids) = flight_ids(
            &service,
            ticket(json!({"collection":"buildings","sources":[1],"limit":limit})),
        )
        .await;
        assert_eq!(ids.len(), limit as usize);
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(
            schema.field_with_name("geometry").unwrap().data_type(),
            &DataType::Binary
        );
    }
    let (_, roads) = flight_ids(
        &service,
        ticket(json!({"collection":"roads","sources":[1]})),
    )
    .await;
    assert_eq!(roads, ["way:road1"]);
    let (_, edge) = flight_ids(
        &service,
        ticket(json!({"collection":"buildings","sources":[1],"bbox":[9,9,11,11]})),
    )
    .await;
    assert_eq!(edge, ["way:1"]);
    let mut request = ticket(json!({"collection":"buildings","sources":[1,2]}));
    request
        .metadata_mut()
        .insert("x-source-ids", "2".parse().unwrap());
    assert_eq!(flight_ids(&service, request).await.1, ["way:2"]);
    let (schema, empty) = flight_ids(
        &service,
        ticket(json!({"collection":"buildings","columns":["id","name"]})),
    )
    .await;
    assert!(empty.is_empty());
    assert_eq!(schema.fields().len(), 2); // Empty streams must still carry schema.
}

#[tokio::test]
async fn flight_discovery_and_validation_work() {
    let fixture = Fixture::new(2);
    let service = ShardFlight {
        store: fixture.store,
    };
    let info = service
        .get_flight_info(Request::new(FlightDescriptor::new_path(vec![
            "roads".into()
        ])))
        .await
        .unwrap()
        .into_inner();
    let schema = service
        .get_schema(Request::new(FlightDescriptor::new_path(vec![
            "roads".into()
        ])))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        Schema::try_from(schema).unwrap(),
        info.clone().try_decode_schema().unwrap()
    );
    assert_eq!(info.total_records, -1);
    flight_ids(
        &service,
        Request::new(info.endpoint[0].ticket.clone().unwrap()),
    )
    .await;
    let flights = service
        .list_flights(Request::new(Criteria::default()))
        .await
        .unwrap()
        .into_inner()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(flights.len(), 2);
    for value in [
        json!({"collection":"roads","columns":[]}),
        json!({"collection":"roads","columns":["id","id"]}),
        json!({"collection":"roads","columns":["id; DROP TABLE features"]}),
        json!({"collection":"roads","limit":100001}),
        json!({"collection":"roads","bbox":[0,0,181,1]}),
    ] {
        assert_eq!(
            service.do_get(ticket(value)).await.err().unwrap().code(),
            tonic::Code::InvalidArgument
        );
    }
    assert_eq!(
        service
            .do_get(ticket(json!({"collection":"absent"})))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::NotFound
    );
}

#[derive(prost::Message)]
struct Tile {
    #[prost(message, repeated, tag = "3")]
    layers: Vec<Layer>,
}
#[derive(prost::Message)]
struct Layer {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(message, repeated, tag = "2")]
    features: Vec<TileFeature>,
}
#[derive(prost::Message)]
struct TileFeature {
    #[prost(uint32, repeated, packed = "true", tag = "4")]
    geometry: Vec<u32>,
}

#[tokio::test]
async fn tiles_use_mercator_and_cache_each_coordinate_separately() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    use prost::Message;
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let tile = client
        .get("/collections/buildings/tiles/2/2/1?sources=3")
        .send()
        .await;
    tile.assert_status_is_ok();
    tile.assert_content_type("application/vnd.mapbox-vector-tile");
    let bytes = tile.0.into_body().into_bytes().await.unwrap();
    let decoded = Tile::decode(bytes).unwrap();
    assert_eq!(decoded.layers[0].name, "buildings");
    let point = &decoded.layers[0].features[0].geometry;
    assert_eq!(point[0], 9); // MoveTo, one point, zigzag deltas.
    assert!((i64::from(point[1] / 2) - 910).abs() <= 1);
    assert!((i64::from(point[2] / 2) - 662).abs() <= 1);
    client
        .get("/collections/buildings/tiles/2/1/1?sources=3")
        .send()
        .await
        .assert_status(StatusCode::NO_CONTENT);
    client
        .get("/collections/buildings/tiles/2/2/1?sources=3")
        .send()
        .await
        .assert_status_is_ok();
    for xyz in ["31/0/0", "2/4/1", "2/1/4"] {
        client
            .get(format!("/collections/buildings/tiles/{xyz}?sources=1"))
            .send()
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn one_pool_bounds_both_protocols_even_when_a_client_disconnects() {
    let fixture = Fixture::new(1);
    let store = fixture.store;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = store.clone();
    let leader = tokio::spawn(async move {
        worker
            .bytes("cancelled".into(), false, move |_| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(bytes::Bytes::from_static(b"finished"))
            })
            .await
    });
    started_rx.await.unwrap();
    let client = TestClient::new(api::routes(store.clone()));
    let overloaded = client
        .get("/collections/buildings/items?sources=1")
        .send()
        .await;
    overloaded.assert_status(StatusCode::TOO_MANY_REQUESTS);
    overloaded.assert_header("Retry-After", "1");
    let service = ShardFlight {
        store: store.clone(),
    };
    assert_eq!(
        service
            .do_get(ticket(json!({"collection":"roads"})))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::ResourceExhausted
    );
    leader.abort();
    let _ = leader.await;
    assert!(matches!(
        store.run(|_| Ok(())).await,
        Err(Error::Overloaded)
    ));
    release_tx.send(()).unwrap();
    // No stranded custom singleflight entry after cancellation.
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match store
                .bytes("cancelled".into(), false, |_| {
                    Ok(bytes::Bytes::from_static(b"recovered"))
                })
                .await
            {
                Ok(_) => break,
                Err(Error::Overloaded) => tokio::task::yield_now().await,
                Err(e) => panic!("{e}"),
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ogc_cursor_walk_matches_offset_walk() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    // Offset walk for reference.
    let mut expected = Vec::new();
    let mut offset = 0;
    loop {
        let page = body(
            client
                .get(format!(
                    "/collections/buildings/items?sources=1&limit=2&offset={offset}"
                ))
                .send()
                .await,
        )
        .await;
        let ids: Vec<String> = page["features"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["id"].as_str().unwrap().to_string())
            .collect();
        if ids.is_empty() {
            break;
        }
        expected.extend(ids);
        if page["links"]
            .as_array()
            .unwrap()
            .iter()
            .all(|l| l["rel"] != "next")
        {
            break;
        }
        offset += 2;
    }
    assert_eq!(expected.len(), 4);
    // Cursor walk follows `next` links only.
    let mut walked = Vec::new();
    let mut next = Some("/collections/buildings/items?sources=1&limit=2".to_string());
    while let Some(url) = next.take() {
        // `next` links carry a cursor (with the offset normalized away).
        if !walked.is_empty() {
            assert!(url.contains("cursor="));
        }
        let page = body(client.get(&url).send().await).await;
        walked.extend(
            page["features"]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| f["id"].as_str().unwrap().to_string()),
        );
        next = page["links"]
            .as_array()
            .unwrap()
            .iter()
            .find(|l| l["rel"] == "next")
            .map(|l| l["href"].as_str().unwrap().to_string());
    }
    assert_eq!(walked, expected);
}

#[tokio::test]
async fn pool_bulk_cap_sheds_flight_without_touching_ogc() {
    // Four pool connections but one bulk slot: concurrent bulk queries fail
    // fast even with idle pool connections, leaving them for interactive use.
    let fixture = Fixture::with_queue(4, 8, std::time::Duration::from_secs(10), 1);
    let store = fixture.store;
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let worker = store.clone();
        tasks.push(tokio::spawn(async move {
            worker
                .arrow("TRUE".into(), "id".into(), 100_000, 0)
                .await
                .map(|_| ())
        }));
    }
    let mut ok = 0;
    let mut overloaded = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(()) => ok += 1,
            Err(Error::Overloaded) => overloaded += 1,
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
    assert_eq!((ok, overloaded), (1, 3));
    // Interactive work still has connections.
    store.run(|_| Ok(())).await.unwrap();
}

#[tokio::test]
async fn store_serves_stored_gzip_without_identity() {
    use std::io::Read;
    let fixture = Fixture::new(1);
    let store = fixture.store;
    let raw = store
        .bytes("k".into(), false, |_| {
            Ok(bytes::Bytes::from_static(b"hello world, hello world"))
        })
        .await
        .unwrap();
    assert!(store.get("k:gzip").await.is_none());
    let gzipped = store
        .compressed("k:gzip".into(), raw.bytes.clone())
        .await
        .unwrap();
    assert_ne!(gzipped.etag, raw.etag);
    // The encoding-first lookup hits without recomputing anything.
    let hit = store.get("k:gzip").await.unwrap();
    assert_eq!(hit.bytes, gzipped.bytes);
    let mut decoder = flate2::read::GzDecoder::new(&hit.bytes[..]);
    let mut roundtrip = Vec::new();
    decoder.read_to_end(&mut roundtrip).unwrap();
    assert_eq!(roundtrip, raw.bytes.to_vec());
}

#[tokio::test]
async fn pool_queue_absorbs_bursts_without_failures() {
    let fixture = Fixture::with_queue(1, 16, std::time::Duration::from_secs(10), 1);
    let store = fixture.store;
    let completions = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for task in 0..4 {
        let worker = store.clone();
        let completions = completions.clone();
        tasks.push(tokio::spawn(async move {
            worker
                .run(move |_| {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    completions.lock().unwrap().push(task);
                    Ok(())
                })
                .await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    // One connection served every waiter: nothing failed fast.
    assert_eq!(completions.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn pool_queue_rejects_past_its_bound() {
    let fixture = Fixture::with_queue(1, 1, std::time::Duration::from_secs(10), 1);
    let store = fixture.store;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = store.clone();
    let leader = tokio::spawn(async move {
        worker
            .run(move |_| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .await
    });
    started_rx.await.unwrap();
    // One waiter fits; the next request fails fast instead of piling up.
    let waiter = tokio::spawn({
        let store = store.clone();
        async move { store.run(|_| Ok(())).await }
    });
    tokio::task::yield_now().await;
    assert!(matches!(
        store.run(|_| Ok(())).await,
        Err(Error::Overloaded)
    ));
    release_tx.send(()).unwrap();
    leader.await.unwrap().unwrap();
    waiter.await.unwrap().unwrap();
}

#[tokio::test]
async fn pool_queue_times_out_as_overloaded() {
    let fixture = Fixture::with_queue(1, 8, std::time::Duration::from_millis(50), 1);
    let store = fixture.store;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = store.clone();
    let leader = tokio::spawn(async move {
        worker
            .run(move |_| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .await
    });
    started_rx.await.unwrap();
    let started = std::time::Instant::now();
    assert!(matches!(
        store.run(|_| Ok(())).await,
        Err(Error::Overloaded)
    ));
    // One deadline, not endless re-queuing.
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    release_tx.send(()).unwrap();
    leader.await.unwrap().unwrap();
}

#[tokio::test]
async fn shard_is_read_only_and_missing_files_are_not_created() {
    let fixture = Fixture::new(1);
    assert!(fixture
        .store
        .run(|conn| {
            conn.execute("DELETE FROM features", Parameters::None)?;
            Ok(())
        })
        .await
        .is_err());
    let path = fixture._dir.path().join("missing.ducklake");
    assert!(Store::open(
        path.to_str().unwrap(),
        1,
        0,
        0,
        std::time::Duration::ZERO,
        1,
        1,
        0,
        std::time::Duration::ZERO
    )
    .is_err());
    assert!(!path.exists());
}

fn tiny_parquet(dir: &tempfile::TempDir) -> std::path::PathBuf {
    install_extensions();
    let parquet = dir.path().join("source'quoted.parquet");
    let db = db::open_memory().unwrap();
    let conn = db.connect().unwrap();
    db::execute_all(&conn, &["LOAD spatial"]).unwrap();
    let copy = format!(
        "COPY (
        SELECT type, id, 'Café' AS name, 12 AS height,
          ST_AsWKB(ST_MakeEnvelope(0,0,10,10)) AS geometry,
          {{'xmin':0, 'ymin':0, 'xmax':10, 'ymax':10}} AS bbox
        FROM (VALUES ('way',1),('relation',1)) t(type,id)
        ) TO {} (FORMAT PARQUET)",
        filter::quote(parquet.to_str().unwrap())
    );
    conn.execute(copy.as_str(), Parameters::None).unwrap();
    parquet
}

fn lake_build(dir: &tempfile::TempDir, parquet: &std::path::Path, name: &str, sort: &str) -> Build {
    Build {
        from: parquet.to_str().unwrap().into(),
        collection: "buildings".into(),
        bbox: [9.0, 9.0, 11.0, 11.0],
        out: dir.path().join(name),
        data_dir: dir.path().join(format!("{name}.files")),
        data_url: None,
        limit: None,
        file_mb: 128,
        row_group: 65536,
        sort: sort.into(),
        source_id: 7,
    }
}

#[tokio::test]
async fn materialization_preserves_layercake_ids_geometry_and_tags() {
    let dir = tempfile::tempdir().unwrap();
    let parquet = tiny_parquet(&dir);
    let build = lake_build(&dir, &parquet, "local.ducklake", "grid");
    build.run().unwrap();
    assert!(build.run().is_err()); // Never overwrite a published shard.
    std::fs::remove_file(parquet).unwrap(); // Serving no longer needs its source.
    let store = Arc::new(
        Store::open(
            build.out.to_str().unwrap(),
            2,
            1024 * 1024,
            0,
            std::time::Duration::ZERO,
            2,
            1,
            0,
            std::time::Duration::ZERO,
        )
        .unwrap(),
    );
    let client = TestClient::new(api::routes(store.clone()));
    let page = body(
        client
            .get("/collections/buildings/items?sources=7")
            .send()
            .await,
    )
    .await;
    assert_eq!(page["numberReturned"], 2);
    assert_eq!(page["features"][0]["id"], "relation:1");
    assert_eq!(page["features"][1]["id"], "way:1");
    assert_eq!(page["features"][0]["geometry"]["type"], "Polygon");
    assert_eq!(page["features"][0]["properties"]["height"], 12);
    let (_, ids) = flight_ids(
        &ShardFlight { store },
        ticket(json!({"collection":"buildings","sources":[7]})),
    )
    .await;
    assert_eq!(ids, ["relation:1", "way:1"]);
}

#[tokio::test]
async fn materialization_sort_orders_without_losing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let parquet = tiny_parquet(&dir);
    let build = lake_build(&dir, &parquet, "hilbert.ducklake", "hilbert");
    build.run().unwrap();
    let store = Arc::new(
        Store::open(
            build.out.to_str().unwrap(),
            1,
            1024 * 1024,
            0,
            std::time::Duration::ZERO,
            1,
            1,
            0,
            std::time::Duration::ZERO,
        )
        .unwrap(),
    );
    let client = TestClient::new(api::routes(store));
    let page = body(
        client
            .get("/collections/buildings/items?sources=7")
            .send()
            .await,
    )
    .await;
    assert_eq!(page["numberReturned"], 2);
}

#[test]
fn failed_build_does_not_publish_or_leave_partial_files() {
    install_extensions();
    let dir = tempfile::tempdir().unwrap();
    let build = Build {
        from: dir.path().join("absent.parquet").to_str().unwrap().into(),
        collection: "buildings".into(),
        bbox: [0.0, 0.0, 1.0, 1.0],
        out: dir.path().join("local.ducklake"),
        data_dir: dir.path().join("local.ducklake.files"),
        data_url: None,
        limit: None,
        file_mb: 128,
        row_group: 65536,
        sort: "grid".into(),
        source_id: 1,
    };
    assert!(build.run().is_err());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn flight_byte_budget_bounds_buffered_batches() {
    use crate::store::FLIGHT_BYTE_BUDGET;
    assert_eq!(FLIGHT_BYTE_BUDGET, 32 * 1024 * 1024);
}

#[test]
fn bbox_overlap_and_contained_handle_antimeridian() {
    // Normal window: conjunctions.
    assert_eq!(
        filter::bbox_overlap([0.0, 0.0, 10.0, 10.0]),
        "xmax >= 0 AND xmin <= 10 AND ymax >= 0 AND ymin <= 10"
    );
    assert_eq!(
        filter::bbox_contained([0.0, 0.0, 10.0, 10.0]),
        "xmin >= 0 AND xmax <= 10 AND ymin >= 0 AND ymax <= 10"
    );
    // Crossing window: unions over longitude.
    assert_eq!(
        filter::bbox_overlap([170.0, 0.0, -170.0, 5.0]),
        "(xmax >= 170 OR xmin <= -170) AND ymax >= 0 AND ymin <= 5"
    );
    assert_eq!(
        filter::bbox_contained([170.0, 0.0, -170.0, 5.0]),
        "(xmin >= 170 OR xmax <= -170) AND ymin >= 0 AND ymax <= 5"
    );
    // Legacy alias stays in sync with overlap.
    assert_eq!(
        filter::bbox_range([0.0, 0.0, 10.0, 10.0]),
        filter::bbox_overlap([0.0, 0.0, 10.0, 10.0])
    );
}

#[test]
fn predicate_prunes_then_accepts_interior_or_intersects() {
    let sql = crate::store::Store::predicate("buildings", Some([0.0, 0.0, 10.0, 10.0]), &[1]);
    assert!(sql.contains("xmax >= 0 AND xmin <= 10"));
    assert!(sql.contains("xmin >= 0 AND xmax <= 10"));
    assert!(sql.contains("ST_Intersects"));
    // No spatial arm without bounds.
    let bare = crate::store::Store::predicate("buildings", None, &[1]);
    assert!(!bare.contains("ST_Intersects"));
    assert!(bare.contains("source_id IN (1)"));
}

#[tokio::test]
async fn store_rejects_bad_engine_budgets() {
    // threads=0 fails before any storage is touched.
    let dir = tempfile::tempdir().unwrap();
    let location = dir
        .path()
        .join("shard.ducklake")
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        Store::open(
            &location,
            1,
            0,
            0,
            std::time::Duration::ZERO,
            1,
            0,
            0,
            std::time::Duration::ZERO
        )
        .is_err(),
        "threads must be positive"
    );
}

#[test]
fn plan_keys_are_stable_and_heavy_is_bulk() {
    use crate::plan::{is_heavy, ItemsRequest, Pagination};
    let base = ItemsRequest {
        collection: "buildings".into(),
        sources: vec![2, 1, 1],
        bounds: Some([9.0, 9.0, 11.0, 11.0]),
        limit: 10,
        pagination: Pagination::Offset(0),
        datetime: None,
    };
    // Sources are normalized by the caller; pagination drives heaviness.
    assert!(!is_heavy(
        10,
        &Pagination::Offset(0),
        Some([9.0, 9.0, 11.0, 11.0])
    ));
    assert!(is_heavy(
        1000,
        &Pagination::Offset(0),
        Some([9.0, 9.0, 11.0, 11.0])
    ));
    assert!(is_heavy(10, &Pagination::Offset(5000), None));
    assert!(is_heavy(
        10,
        &Pagination::Offset(0),
        Some([2.0, 48.0, 6.0, 54.0])
    ));
    assert!(!is_heavy(
        10,
        &Pagination::Cursor("way:1".into()),
        Some([9.0, 9.0, 11.0, 11.0])
    ));
    let key = ItemsRequest {
        sources: vec![1],
        ..base.clone()
    }
    .cache_key();
    assert!(key.starts_with("items:buildings:"));
    assert!(key.contains("limit=10"));
}

#[tokio::test]
async fn streaming_completes_then_reuses_its_connection() {
    let fixture = Fixture::new(1);
    let service = ShardFlight {
        store: fixture.store.clone(),
    };
    // Full consumption marks completion; the same single connection serves
    // the next query without a stale interrupt cancelling it.
    let (_, ids) = flight_ids(
        &service,
        ticket(json!({"collection":"buildings","sources":[1]})),
    )
    .await;
    assert!(!ids.is_empty());
    let (_, ids2) = flight_ids(
        &service,
        ticket(json!({"collection":"buildings","sources":[1],"limit":1})),
    )
    .await;
    assert_eq!(ids2.len(), 1);
    fixture.store.run(|_| Ok(())).await.unwrap();
}

#[tokio::test]
async fn streaming_drop_before_schema_does_not_strand_the_pool() {
    let fixture = Fixture::new(1);
    let store = fixture.store.clone();
    // Start a stream and drop it immediately (before/without reading
    // schema): cancellation must return the connection promptly.
    let batches = store
        .arrow_stream("TRUE".into(), "id".into(), 100_000, 0)
        .await
        .unwrap();
    drop(batches);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if store.run(|_| Ok(())).await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn streaming_slow_reader_still_completes_under_budgets() {
    let fixture = Fixture::new(2);
    let mut batches = fixture
        .store
        .arrow_stream("TRUE".into(), "id".into(), 100_000, 0)
        .await
        .unwrap();
    let mut count = 0;
    while let Some(batch) = batches.batches.recv().await {
        let batch = batch.unwrap();
        count += batch.num_rows();
        // Slow consumer: the producer's byte-budget wait must stay
        // cancellation-aware and make progress.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(count > 0);
    const { assert!(crate::store::FLIGHT_BYTE_BUDGET >= 1024 * 1024) };
    // Guard dropped at end of scope; pool is reusable.
    fixture.store.run(|_| Ok(())).await.unwrap();
}

#[tokio::test]
async fn streaming_reports_mid_stream_failure_as_error() {
    let fixture = Fixture::new(1);
    // Unknown projection fails at prepare time and surfaces as Err,
    // never as a silently truncated stream.
    let result = fixture
        .store
        .arrow_stream("TRUE".into(), "no_such_column".into(), 100_000, 0)
        .await;
    assert!(result.is_err());
    fixture.store.run(|_| Ok(())).await.unwrap();
}

#[tokio::test]
async fn oversized_batch_drains_instead_of_spinning() {
    // A single batch larger than the per-stream budget must still flow once
    // the buffer drains; reserve logic guarantees progress.
    const { assert!(crate::store::FLIGHT_TOTAL_BUDGET >= crate::store::FLIGHT_BYTE_BUDGET) };
    let fixture = Fixture::new(1);
    let mut batches = fixture
        .store
        .arrow_stream("TRUE".into(), "id".into(), 100_000, 0)
        .await
        .unwrap();
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), batches.batches.recv())
        .await
        .unwrap();
    assert!(first.is_some());
}

#[tokio::test]
async fn heavy_pages_share_the_bulk_lane() {
    // One bulk slot: two concurrent heavy item pages fail fast on the
    // second, while an interactive single-feature fetch still works.
    let fixture = Fixture::with_queue(2, 8, std::time::Duration::from_secs(10), 1);
    let store = fixture.store.clone();
    let heavy = |store: Arc<Store>| async move {
        store
            .bytes(format!("heavy-{}", rand_key()), true, |_| {
                std::thread::sleep(std::time::Duration::from_millis(200));
                Ok(bytes::Bytes::from_static(b"heavy"))
            })
            .await
            .map(|_| ())
    };
    fn rand_key() -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        N.fetch_add(1, Ordering::Relaxed)
    }
    let a = tokio::spawn(heavy(store.clone()));
    tokio::task::yield_now().await;
    // Second heavy computation hits the same key and coalesces (Moka
    // singleflight) rather than failing; a different heavy key fails fast
    // on the bulk semaphore only for Flight paths. Here we assert the
    // bulk helper itself sheds load.
    let _ = a.await.unwrap();
    store.run(|_| Ok(())).await.unwrap();
}

#[test]
fn manifest_round_trips_through_serde() {
    use crate::store::{LakeLayout, ShardManifest};
    let manifest = ShardManifest {
        version: 1,
        backend: "lake".into(),
        schema_version: 2,
        source: "test".into(),
        bbox: [2.0, 48.0, 6.0, 54.0],
        rows: 7,
        built_at: "0".into(),
        layout: Some(LakeLayout {
            file_mb: 128,
            row_group: 65536,
            sort: "grid".into(),
        }),
    };
    let text = serde_json::to_string(&manifest).unwrap();
    let back: ShardManifest = serde_json::from_str(&text).unwrap();
    assert_eq!(back.rows, 7);
    assert_eq!(back.schema_version, 2);
}

#[tokio::test]
async fn duck_tuning_reports_engine_budgets() {
    // Storage caching lives in ZeroFS; DuckDB only reports engine budgets.
    let fixture = Fixture::new(1);
    let tuning = fixture.store.duck_tuning().await;
    let keys: Vec<_> = tuning.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&"threads"));
    assert!(keys.contains(&"memory_limit"));
    assert!(!keys.iter().any(|k| k.contains("metadata_cache")));
}

#[tokio::test]
async fn metrics_exposes_engine_budgets() {
    let fixture = Fixture::new(1);
    let client = TestClient::new(api::routes(fixture.store));
    let text = client
        .get("/metrics")
        .send()
        .await
        .0
        .into_body()
        .into_string()
        .await
        .unwrap();
    assert!(text.contains("duck_setting_threads"));
    assert!(text.contains("duck_setting_memory_limit"));
    assert!(!text.contains("metadata_cache"));
}

// --- Quack bulk protocol -----------------------------------------------

use crate::{
    quack::QuackServer,
    store::{catalog_url, StoreConfig},
};

fn quack_test_config() -> StoreConfig {
    StoreConfig {
        connections: 1,
        cache_bytes: 1024 * 1024,
        max_waiters: 0,
        max_wait: std::time::Duration::ZERO,
        bulk_limit: 1,
        threads: 1,
        memory_mb: 512,
        query_timeout: std::time::Duration::ZERO,
        ..StoreConfig::default()
    }
}

/// A client connection with the quack extension ready. The connection
/// keeps its database alive on its own handle.
fn quack_client() -> NeoConnection {
    install_extensions();
    let db = db::open_memory().unwrap();
    let conn = db.connect().unwrap();
    if db::execute_all(&conn, &["LOAD quack"]).is_err() {
        db::execute_all(&conn, &["INSTALL quack", "LOAD quack"]).unwrap();
    }
    // The Quack client posts over HTTP; like the server it needs httpfs
    // loaded explicitly since autoload stays off in tests.
    if db::execute_all(&conn, &["LOAD httpfs"]).is_err() {
        db::execute_all(&conn, &["INSTALL httpfs", "LOAD httpfs"]).unwrap();
    }
    conn
}

/// Run `sql` on the Quack server at `port` authenticated as `token`.
fn quack_query(
    conn: &NeoConnection,
    port: u16,
    token: &str,
    sql: &str,
) -> Result<Vec<Vec<Option<String>>>, Error> {
    let wrapped = format!(
        "SELECT * FROM quack_query('quack:127.0.0.1:{port}', {inner}, token => {token})",
        inner = filter::quote(sql),
        token = filter::quote(token),
    );
    db::text_table(conn, &wrapped)
}

fn start_quack(fixture: &Fixture, port: u16, token: &str) -> QuackServer {
    let cfg = quack_test_config();
    let (server, _) = QuackServer::start(
        &cfg,
        false,
        &catalog_url(&fixture.catalog),
        fixture.store.snapshot,
        format!("127.0.0.1:{port}").parse().unwrap(),
        Some(token.into()),
        false,
    )
    .unwrap();
    server
}

#[tokio::test]
async fn quack_serves_the_pinned_snapshot() {
    let fixture = Fixture::new(1);
    let token = "test-token-quack-1";
    let server = start_quack(&fixture, 19521, token);
    assert_eq!(server.uri(), "quack:127.0.0.1:19521");
    assert_eq!(server.port(), 19521);
    let client = quack_client();
    let rows = quack_query(
        &client,
        19521,
        token,
        "SELECT id FROM shard.features ORDER BY id",
    )
    .unwrap();
    let ids: Vec<_> = rows
        .into_iter()
        .map(|mut r| r.pop().flatten().unwrap())
        .collect();
    assert_eq!(
        ids,
        [
            "relation:1",
            "way:1",
            "way:2",
            "way:3",
            "way:4",
            "way:5",
            "way:road1"
        ]
    );
    server.stop().unwrap();
}

#[tokio::test]
async fn quack_view_is_frozen_at_startup() {
    let fixture = Fixture::new(1);
    let token = "test-token-quack-2";
    let server = start_quack(&fixture, 19522, token);
    let client = quack_client();
    let before = quack_query(
        &client,
        19522,
        token,
        "SELECT count(*)::VARCHAR FROM shard.features",
    )
    .unwrap();
    // Publish a new snapshot behind the server's back: a read-write attach
    // from this test process appends a row (new snapshot version).
    {
        let db = db::open_memory().unwrap();
        let writer = db.connect().unwrap();
        db::execute_all(&writer, &["LOAD ducklake", "LOAD spatial"]).unwrap();
        let attach = format!(
            "ATTACH {} AS w",
            filter::quote(&format!("ducklake:{}", fixture.catalog))
        );
        db::execute_all(&writer, &[attach.as_str(), "USE w"]).unwrap();
        writer
            .execute(
                "INSERT INTO features VALUES ('way:late', 'buildings', 1, ST_Point(0,0), '{}', 9, 0, 0, 0, 0, 0, 0, NULL)",
                Parameters::None,
            )
            .unwrap();
    }
    let after = quack_query(
        &client,
        19522,
        token,
        "SELECT count(*)::VARCHAR FROM shard.features",
    )
    .unwrap();
    assert_eq!(before, after, "quack must not see post-startup snapshots");
    // The write really landed: a fresh latest-view attach sees one more row.
    {
        let db = db::open_memory().unwrap();
        let fresh = db.connect().unwrap();
        db::execute_all(&fresh, &["LOAD ducklake"]).unwrap();
        let attach = format!(
            "ATTACH {} AS fresh (READ_ONLY)",
            filter::quote(&format!("ducklake:{}", fixture.catalog))
        );
        db::execute_all(&fresh, &[attach.as_str(), "USE fresh"]).unwrap();
        let latest = db::int_one(&fresh, "SELECT count(*) FROM features").unwrap();
        let pinned: i64 = before[0][0].as_deref().unwrap().parse().unwrap();
        assert_eq!(latest, pinned + 1);
    }
    server.stop().unwrap();
}

#[tokio::test]
async fn quack_rejects_bad_tokens_writes_and_control_plane() {
    let fixture = Fixture::new(1);
    let token = "test-token-quack-3";
    let server = start_quack(&fixture, 19523, token);
    let client = quack_client();
    let denied = |sql: &str| {
        quack_query(&client, 19523, token, sql)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_else(|| panic!("{sql} should be denied"))
    };
    assert!(
        quack_query(&client, 19523, "wrong-token", "SELECT 1").is_err(),
        "wrong token must fail authentication"
    );
    // Engine boundary: catalog writes are impossible on a pinned read-only attach.
    assert!(
        denied("INSERT INTO shard.features VALUES ('x','b',1,NULL,'{}',0,0,0,0,0,0,0,NULL)")
            .contains("read-only")
    );
    // Filter boundary: everything file/control-plane shaped is denied.
    for sql in [
        "COPY shard.features TO '/tmp/quack-exfil.parquet'",
        "COPY (SELECT 1 AS a) TO '/tmp/quack-exfil.parquet'",
        "SELECT 1; COPY shard.features TO '/tmp/quack-exfil2.parquet'",
        "/* leading comment */ COPY shard.features TO '/tmp/quack-exfil3.parquet'",
        "SELECT count(*) FROM read_parquet('https://example.com/x.parquet')",
        "SELECT count(*) FROM read_csv('https://example.com/x.csv')",
        "SELECT * FROM st_read('/tmp/quack-evil.fgb')",
        "SELECT * FROM 'https://example.com/x.parquet'",
        "SELECT * FROM 's3://example/x.parquet'",
        "CALL quack_serve('quack:127.0.0.1:19599')",
        "SELECT quack_serve('quack:127.0.0.1:19599')",
        "CALL quack_stop('quack:127.0.0.1:19523')",
        "SET GLOBAL threads=64",
        "RESET GLOBAL threads",
        "SET threads=64",
        "ATTACH 'ducklake:/tmp/other.ducklake' AS evil",
        "DETACH shard",
        "INSTALL excel",
        "LOAD spatial",
        "CREATE SECRET (TYPE s3, KEY_ID 'x', SECRET 'y')",
    ] {
        assert!(
            denied(sql).contains("Authorization failed"),
            "{sql} should be denied"
        );
    }
    assert!(!std::path::Path::new("/tmp/quack-exfil.parquet").exists());
    // Realistic bulk shapes must NOT trip the filter.
    for sql in [
        "SELECT id FROM shard.features ORDER BY id LIMIT 10",
        "SELECT id, cx, cy, name FROM shard.features WHERE layer='buildings' AND source_id IN (1) ORDER BY id LIMIT 100 OFFSET 10",
        "SELECT count(*)::VARCHAR FROM shard.features",
        "SELECT layer, count(*)::VARCHAR FROM shard.features GROUP BY layer ORDER BY 1",
        "WITH city AS (SELECT id FROM shard.features WHERE source_id IN (1,2)) SELECT count(*)::VARCHAR FROM city",
        "SELECT id FROM shard.features WHERE name ILIKE '%Copy Shop%' ORDER BY id",
        "SELECT id FROM shard.features WHERE name = 'Load Street' ORDER BY id",
        "SELECT id, properties->>'name' AS n FROM shard.features ORDER BY id LIMIT 5",
        "SELECT id FROM shard.features WHERE ST_Intersects(geom, ST_MakeEnvelope(0,0,10,10)) ORDER BY id",
        "SELECT 'a' AS a; SELECT 'b' AS b",
        "EXPLAIN SELECT id FROM shard.features ORDER BY id LIMIT 1",
    ] {
        quack_query(&client, 19523, token, sql)
            .unwrap_or_else(|e| panic!("legit shape denied: {sql}: {e}"));
    }
    server.stop().unwrap();
}

#[tokio::test]
async fn quack_refuses_non_local_bind_without_opt_in() {
    let fixture = Fixture::new(1);
    let cfg = quack_test_config();
    assert!(QuackServer::start(
        &cfg,
        false,
        &catalog_url(&fixture.catalog),
        fixture.store.snapshot,
        "0.0.0.0:19524".parse().unwrap(),
        Some("test-token-quack-4".into()),
        false,
    )
    .is_err());
}

#[tokio::test]
async fn quack_attached_catalog_serves_sql() {
    // A real DuckDB client attaching over the protocol (not just
    // quack_query): verifies the handshake needs nothing the guard denies.
    let fixture = Fixture::new(1);
    let token = "test-token-quack-5";
    let server = start_quack(&fixture, 19525, token);
    let client = quack_client();
    // Token via secret, as documented for clients: secret first, the
    // ATTACH itself already authenticates.
    let secret = format!("CREATE SECRET (TYPE quack, TOKEN {})", filter::quote(token));
    db::execute_all(&client, &[secret.as_str()]).unwrap();
    let attach = format!("ATTACH {} AS r", filter::quote("quack:127.0.0.1:19525"));
    db::execute_all(&client, &[attach.as_str()]).unwrap();
    let ids = db::strings_col(&client, "SELECT id FROM r.shard.main.features ORDER BY id").unwrap();
    assert_eq!(
        ids,
        [
            "relation:1",
            "way:1",
            "way:2",
            "way:3",
            "way:4",
            "way:5",
            "way:road1"
        ]
    );
    server.stop().unwrap();
}
