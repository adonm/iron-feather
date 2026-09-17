//! Frozen file-list serving: agreement with catalog reads plus fail-closed
//! fallback (deletes force catalog mode, which still filters them).
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

#[tokio::test]
async fn file_list_serves_identical_pages_to_catalog_reads() {
    let fixture = Fixture::new(2);
    assert!(
        fixture.store.table_from().starts_with("read_parquet(["),
        "harness catalog should resolve to a file list, got: {}",
        fixture.store.table_from()
    );
    let client = TestClient::new(api::routes(fixture.store));
    let page = body(
        client
            .get("/collections/buildings/items?sources=1&limit=1000")
            .send()
            .await,
    )
    .await;
    let served: Vec<String> = page["features"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["id"].as_str().unwrap().to_string())
        .collect();
    let mut catalog = catalog_ids(&fixture.catalog);
    catalog.sort();
    assert_eq!(served, catalog);
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
    assert_eq!(store.table_from(), "features");
    let client = TestClient::new(api::routes(store));
    let page = body(
        client
            .get("/collections/buildings/items?sources=1&limit=1000")
            .send()
            .await,
    )
    .await;
    let served: Vec<String> = page["features"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(served, vec!["way:1".to_string()]);
}
