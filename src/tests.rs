//! Regression checks exercise real DuckDB, HTTP responses and Flight wire
//! encoding. Fixtures contain multiple collections, sources and geometries.
use crate::{
    api, filter,
    flight::ShardFlight,
    materialize::Build,
    store::{Error, Store},
};
use arrow_flight::{
    decode::FlightRecordBatchStream, flight_service_server::FlightService, Criteria,
    FlightDescriptor, Ticket,
};
use duckdb::{
    arrow::{array::StringArray, datatypes::Schema},
    Connection,
};
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
}

fn install_spatial() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        Connection::open_in_memory()
            .unwrap()
            .execute_batch("INSTALL spatial")
            .unwrap()
    });
}

impl Fixture {
    fn new(connections: usize) -> Self {
        install_spatial();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard.duckdb");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("LOAD spatial;
            CREATE TABLE features(id VARCHAR, layer VARCHAR, source_id BIGINT, geom GEOMETRY, properties JSON);
            INSERT INTO features VALUES
              ('way:1', 'buildings', 1, ST_GeomFromText('POLYGON ((0 0,10 0,10 10,0 10,0 0))'), '{\"name\":\"Café\",\"height\":12}'),
              ('way:2', 'buildings', 2, ST_Point(12,12), '{\"name\":null}'),
              ('relation:1', 'buildings', 1, ST_GeomFromText('POLYGON ((-10 -10,-5 -10,-5 -5,-10 -5,-10 -10))'), '{}'),
              ('way:3', 'buildings', 1, ST_Point(179,2), '{}'),
              ('way:4', 'buildings', 1, ST_Point(-179,2), '{}'),
              ('way:5', 'buildings', 3, ST_Point(20,60), '{}'),
              ('way:road1', 'roads', 1, ST_GeomFromText('LINESTRING (0 0,20 20)'), '{}');
            CREATE UNIQUE INDEX feature_id ON features(id);
            CREATE INDEX feature_geom ON features USING RTREE(geom);
            CREATE TABLE collections AS SELECT DISTINCT layer AS id FROM features;
            CHECKPOINT;").unwrap();
        drop(conn);
        Self {
            store: Arc::new(
                Store::open(path.to_str().unwrap(), connections, 8 * 1024 * 1024).unwrap(),
            ),
            _dir: dir,
        }
    }
}

async fn body(response: TestResponse) -> Value {
    response.assert_status_is_ok();
    response.0.into_body().into_json().await.unwrap()
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
async fn flight_native_arrow_is_collection_scoped_and_limit_sensitive() {
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
            &duckdb::arrow::datatypes::DataType::Binary
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
async fn flight_discovery_and_validation_work_after_cache_hits() {
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
            .bytes("cancelled".into(), move |_| {
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
                .bytes("cancelled".into(), |_| {
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
async fn shard_is_read_only_and_missing_files_are_not_created() {
    let fixture = Fixture::new(1);
    assert!(fixture
        .store
        .run(|conn| {
            conn.execute("DELETE FROM features", [])?;
            Ok(())
        })
        .await
        .is_err());
    let path = fixture._dir.path().join("missing.duckdb");
    assert!(Store::open(path.to_str().unwrap(), 1, 0).is_err());
    assert!(!path.exists());
}

#[tokio::test]
async fn materialization_preserves_layercake_ids_geometry_and_tags() {
    install_spatial();
    let dir = tempfile::tempdir().unwrap();
    let parquet = dir.path().join("source'quoted.parquet");
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        "LOAD spatial; COPY (
        SELECT type, id, 'Café' AS name, 12 AS height,
          ST_AsWKB(ST_MakeEnvelope(0,0,10,10)) AS geometry,
          {{'xmin':0, 'ymin':0, 'xmax':10, 'ymax':10}} AS bbox
        FROM (VALUES ('way',1),('relation',1)) t(type,id)
        ) TO {} (FORMAT PARQUET)",
        filter::quote(parquet.to_str().unwrap())
    ))
    .unwrap();
    let build = Build {
        from: parquet.to_str().unwrap().into(),
        collection: "buildings".into(),
        bbox: [9.0, 9.0, 11.0, 11.0],
        out: dir.path().join("local.duckdb"),
        limit: None,
        source_id: 7,
    };
    build.run().unwrap();
    assert!(build.run().is_err()); // Never overwrite a published shard.
    std::fs::remove_file(parquet).unwrap(); // Serving no longer needs its source.
    let store = Arc::new(Store::open(build.out.to_str().unwrap(), 2, 1024 * 1024).unwrap());
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

#[test]
fn failed_build_does_not_publish_or_leave_partial_files() {
    install_spatial();
    let dir = tempfile::tempdir().unwrap();
    let build = Build {
        from: dir.path().join("absent.parquet").to_str().unwrap().into(),
        collection: "buildings".into(),
        bbox: [0.0, 0.0, 1.0, 1.0],
        out: dir.path().join("local.duckdb"),
        limit: None,
        source_id: 1,
    };
    assert!(build.run().is_err());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}
