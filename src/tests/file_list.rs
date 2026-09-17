//! Serving-index pruning: agreement with catalog reads, subset selection,
//! and fail-closed fallback (deletes, stale or mismatched documents).
use super::common::{body, install_extensions, Fixture};
use crate::{api, db, filter, store::Store};
use duckdb_neo::Parameters;
use poem::test::TestClient;
use std::sync::Arc;

fn catalog_ids(catalog: &str) -> Vec<String> {
    install_extensions();
    let db = db::open_memory().unwrap();
    let conn = db.connect().unwrap();
    db::execute_all(&conn, &["LOAD spatial", "LOAD ducklake"]).unwrap();
    let attach = format!(
        "ATTACH {} AS c (READ_ONLY)",
        filter::quote(&format!("ducklake:{catalog}")),
    );
    db::execute_all(&conn, &[attach.as_str(), "USE c"]).unwrap();
    db::strings_col(
        &conn,
        "SELECT id FROM features WHERE layer='buildings' AND source_id IN (1) ORDER BY id",
    )
    .unwrap()
}

/// Open the fixture catalog with a freshly generated serving index.
/// Mirrors what `build` publishes: footer stats over the local data dir.
fn open_indexed(catalog: &str, files_dir: &str) -> Arc<Store> {
    let doc = crate::materialize::generate_index(catalog, files_dir).unwrap();
    let text = serde_json::to_string(&doc).unwrap();
    Arc::new(
        Store::open_config(crate::store::StoreConfig {
            location: catalog.to_string(),
            index_json: Some(text),
            connections: 2,
            max_waiters: 0,
            max_wait: std::time::Duration::ZERO,
            bulk_limit: 2,
            threads: 1,
            memory_mb: 0,
            query_timeout: std::time::Duration::ZERO,
            ..crate::store::StoreConfig::default()
        })
        .unwrap(),
    )
}

async fn served_ids(store: &Arc<Store>, path: &str) -> Vec<String> {
    let client = TestClient::new(api::routes(store.clone()));
    let page = body(client.get(path).send().await).await;
    page["features"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn index_prunes_and_agrees_with_catalog_reads() {
    let fixture = Fixture::new(2);
    let files = fixture._dir.path().join("files");
    let store = open_indexed(&fixture.catalog, files.to_str().unwrap());
    // The harness fixture holds a single data file, so pruning cannot
    // drop anything here; it must still agree exactly with catalog reads.
    // (Multi-file pruning is proven by the serving-index unit tests and
    // the 25M-row benchmark.) An out-of-extent bbox matches nothing
    // without opening data files.
    let tight = store.read_source(Some([0.0, 0.0, 10.0, 10.0]));
    let all = store.read_source(None);
    assert_eq!(tight, all);
    // Pages agree exactly with catalog reads.
    let served = served_ids(&store, "/collections/buildings/items?sources=1&limit=1000").await;
    let mut catalog = catalog_ids(&fixture.catalog);
    catalog.sort();
    assert_eq!(served, catalog);
    // A bbox outside all data matches nothing without opening data files.
    let empty = served_ids(
        &store,
        "/collections/buildings/items?bbox=50,50,51,51&sources=1&limit=10",
    )
    .await;
    assert!(empty.is_empty());
}

#[tokio::test]
async fn index_with_wrong_commit_is_ignored() {
    let fixture = Fixture::new(2);
    let files = fixture._dir.path().join("files");
    let mut doc =
        crate::materialize::generate_index(&fixture.catalog, files.to_str().unwrap()).unwrap();
    doc.ducklake_commit += 100;
    let store = Arc::new(
        Store::open_config(crate::store::StoreConfig {
            location: fixture.catalog.clone(),
            index_json: Some(serde_json::to_string(&doc).unwrap()),
            connections: 1,
            max_waiters: 0,
            max_wait: std::time::Duration::ZERO,
            bulk_limit: 1,
            threads: 1,
            memory_mb: 0,
            query_timeout: std::time::Duration::ZERO,
            ..crate::store::StoreConfig::default()
        })
        .unwrap(),
    );
    // Falls back to the unpruned frozen file list, still correct.
    assert_eq!(
        store.read_source(Some([0.0, 0.0, 10.0, 10.0])),
        store.read_source(None)
    );
    let served = served_ids(&store, "/collections/buildings/items?sources=1&limit=1000").await;
    let mut catalog = catalog_ids(&fixture.catalog);
    catalog.sort();
    assert_eq!(served, catalog);
}

#[tokio::test]
async fn index_with_missing_file_is_ignored() {
    let fixture = Fixture::new(2);
    let files = fixture._dir.path().join("files");
    let mut doc =
        crate::materialize::generate_index(&fixture.catalog, files.to_str().unwrap()).unwrap();
    // Corrupt one path: the file set no longer matches the catalog, so the
    // document must be ignored (fail closed to the unpruned fallback).
    doc.files[0].path = "main/features/does-not-exist.parquet".to_string();
    let store = Arc::new(
        Store::open_config(crate::store::StoreConfig {
            location: fixture.catalog.clone(),
            index_json: Some(serde_json::to_string(&doc).unwrap()),
            connections: 1,
            max_waiters: 0,
            max_wait: std::time::Duration::ZERO,
            bulk_limit: 1,
            threads: 1,
            memory_mb: 0,
            query_timeout: std::time::Duration::ZERO,
            ..crate::store::StoreConfig::default()
        })
        .unwrap(),
    );
    assert_eq!(
        store.read_source(Some([0.0, 0.0, 10.0, 10.0])),
        store.read_source(None)
    );
}

/// A catalog with a row delete must fall back to catalog reads (which
/// still filter the deleted row); the file list would resurrect it.
#[tokio::test]
async fn file_list_falls_back_when_deletes_exist() {
    install_extensions();
    let dir = tempfile::tempdir().unwrap();
    let catalog = dir.path().join("shard.ducklake");
    let files = dir.path().join("files");
    std::fs::create_dir_all(&files).unwrap();
    let db = db::open_memory().unwrap();
    let conn = db.connect().unwrap();
    db::execute_all(&conn, &["LOAD spatial", "LOAD ducklake"]).unwrap();
    let attach = format!(
        "ATTACH {} AS lake (DATA_PATH {})",
        filter::quote(&format!("ducklake:{}", catalog.to_str().unwrap())),
        filter::quote(&format!("{}/", files.to_str().unwrap())),
    );
    db::execute_all(&conn, &[attach.as_str(), "USE lake"]).unwrap();
    for statement in [
        "CREATE TABLE features(id VARCHAR, layer VARCHAR, source_id BIGINT, geom GEOMETRY, properties JSON,
          sortkey BIGINT, xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE, cx DOUBLE, cy DOUBLE, name VARCHAR)",
        "INSERT INTO features VALUES
          ('way:1', 'buildings', 1, ST_Point(1,1), '{}', 1, 1, 1, 1, 1, 1, 1, NULL),
          ('way:2', 'buildings', 1, ST_Point(2,2), '{}', 2, 2, 2, 2, 2, 2, 2, NULL)",
        "CREATE TABLE collections AS SELECT DISTINCT layer AS id FROM features",
        "CALL ducklake_flush_inlined_data('lake')",
        "DELETE FROM features WHERE id = 'way:2'",
    ] {
        conn.execute(statement, Parameters::None).unwrap();
    }
    drop(conn);
    drop(db);
    let store = Arc::new(
        Store::open(
            catalog.to_str().unwrap(),
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
    assert_eq!(store.read_source(None), "features");
    let served = served_ids(&store, "/collections/buildings/items?sources=1&limit=1000").await;
    assert_eq!(served, vec!["way:1".to_string()]);
}
