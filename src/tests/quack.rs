//! Quack bulk protocol: pinned snapshots, auth, statement filter.
use super::common::{install_extensions, Fixture};
use crate::{
    db::{self, NeoConnection},
    filter,
    quack::QuackServer,
    store::{catalog_url, Error, StoreConfig},
};
use duckdb_neo::Parameters;
// --- Quack bulk protocol -----------------------------------------------

fn quack_test_config() -> StoreConfig {
    StoreConfig {
        connections: 1,
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
