//! Catalog builds: materialization, sorting, failure atomicity, manifest.
use super::common::{body, flight_ids, install_extensions, ticket};
use crate::{api, db, filter, flight::ShardFlight, materialize::Build, store::Store};
use duckdb_neo::Parameters;
use poem::test::TestClient;
use serde_json::json;
use std::sync::Arc;
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
        content_address: false,
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
async fn content_addressed_builds_converge_on_identical_keys() {
    // Same input built twice must yield identical data-file names, so an
    // additive publish uploads nothing new and shared caches stay warm.
    let build_names = |dir: &tempfile::TempDir, name: &str| {
        let mut build = lake_build(dir, &tiny_parquet(dir), name, "grid");
        build.content_address = true;
        build.run().unwrap();
        let mut names: Vec<String> = Vec::new();
        let mut stack = vec![dir.path().join(format!("{name}.files"))];
        while let Some(d) = stack.pop() {
            for entry in std::fs::read_dir(d).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    stack.push(entry.path());
                } else {
                    names.push(entry.file_name().to_string_lossy().into_owned());
                }
            }
        }
        names.sort();
        names
    };
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let names_a = build_names(&dir_a, "a.ducklake");
    let names_b = build_names(&dir_b, "b.ducklake");
    assert!(!names_a.is_empty());
    assert_eq!(names_a, names_b);
    assert!(names_a.iter().all(|n| {
        n.len() == 64 + ".parquet".len()
            && n.ends_with(".parquet")
            && n[..64].chars().all(|c| c.is_ascii_hexdigit())
    }));
    // The renamed snapshot still serves.
    let store = Arc::new(
        Store::open(
            dir_a.path().join("a.ducklake").to_str().unwrap(),
            1,
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
        content_address: false,
    };
    assert!(build.run().is_err());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
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
