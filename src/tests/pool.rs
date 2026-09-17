//! Pool admission, overload shedding, metrics and engine budgets.
use super::common::{ticket, Fixture};
use crate::{
    api,
    flight::ShardFlight,
    store::{Error, Store, StoreConfig},
};
use arrow_flight::flight_service_server::FlightService;
use duckdb_neo::Parameters;
use poem::{http::StatusCode, test::TestClient};
use serde_json::json;
use std::sync::Arc;
#[tokio::test]
async fn metrics_reports_requests_and_engine_budgets() {
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
    // Two identical pages: two HTTP responses, each executed (no app cache).
    // (/metrics itself counts as neither.)
    assert!(get("http_requests") >= 2);
    assert!(text.contains("duck_setting_threads"));
    assert!(text.contains("duck_setting_memory_limit"));
    assert!(!text.contains("cache_"));
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
            .run_bytes(false, move |_| {
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
    // The pool recovers after cancellation: a fresh query runs at once.
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match store
                .run_bytes(false, |_| Ok(bytes::Bytes::from_static(b"recovered")))
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
async fn gzip_body_round_trips_with_distinct_etag() {
    use crate::store::{gzip_body, CachedBody};
    use std::io::Read;
    let raw = CachedBody::with_bytes(bytes::Bytes::from_static(b"hello world, hello world"));
    let gzipped = gzip_body(raw.bytes.clone()).await.unwrap();
    assert_ne!(gzipped.etag, raw.etag);
    let mut decoder = flate2::read::GzDecoder::new(&gzipped.bytes[..]);
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
        std::time::Duration::ZERO,
        1,
        1,
        0,
        std::time::Duration::ZERO
    )
    .is_err());
    assert!(!path.exists());
}

#[tokio::test]
async fn data_path_override_redirects_relative_reads() {
    // The published catalog stores relative data paths plus a
    // zone-independent DATA_PATH; readers override it per zone. Prove the
    // override is honored (not ignored): an override to an empty dir must
    // fail data reads, and an override to a copied dir must succeed after
    // the original is removed.
    let fixture = Fixture::new(1);
    let catalog = fixture.catalog.clone();
    let files = fixture._dir.path().join("files");
    assert!(files.join("main").exists());
    let open = |data_path_override: Option<String>| {
        Store::open_config(StoreConfig {
            location: catalog.clone(),
            data_path_override,
            connections: 1,
            max_waiters: 0,
            max_wait: std::time::Duration::ZERO,
            bulk_limit: 1,
            threads: 1,
            memory_mb: 0,
            query_timeout: std::time::Duration::ZERO,
            ..StoreConfig::default()
        })
    };
    // Sanity: stored DATA_PATH serves without an override.
    let plain = open(None).unwrap();
    // Force a real data read (count(*) can be answered from metadata).
    let len = plain
        .run(|conn| {
            crate::db::text_table(conn, "SELECT id FROM features ORDER BY id LIMIT 1")
                .map(|rows| rows.len())
        })
        .await
        .unwrap();
    assert_eq!(len, 1);
    // Override to an empty dir must fail (proves the override is honored
    // rather than ignored): even the open-time probe needs data files.
    let empty = fixture._dir.path().join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    let empty_override = format!("{}/", empty.to_str().unwrap());
    assert!(open(Some(empty_override)).is_err());
    // Copy data aside, remove the original, override to the copy: reads
    // must succeed via the override alone.
    let copy = fixture._dir.path().join("files-copy");
    copy_dir(&files, &copy);
    std::fs::remove_dir_all(&files).unwrap();
    let copy_override = format!("{}/", copy.to_str().unwrap());
    let redirected = open(Some(copy_override)).unwrap();
    let len = redirected
        .run(|conn| {
            crate::db::text_table(conn, "SELECT id FROM features ORDER BY id LIMIT 1")
                .map(|rows| rows.len())
        })
        .await
        .unwrap();
    assert_eq!(len, 1);
}

fn copy_dir(source: &std::path::Path, dest: &std::path::Path) {
    std::fs::create_dir_all(dest).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = dest.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
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

#[tokio::test]
async fn heavy_pages_share_the_bulk_lane() {
    // One bulk slot: a second concurrent heavy query fails fast while an
    // interactive query on the free connection still works.
    let fixture = Fixture::with_queue(2, 8, std::time::Duration::from_secs(10), 1);
    let store = fixture.store.clone();
    let heavy = |store: Arc<Store>| async move {
        store
            .run_bytes(true, |_| {
                std::thread::sleep(std::time::Duration::from_millis(200));
                Ok(bytes::Bytes::from_static(b"heavy"))
            })
            .await
            .map(|_| ())
    };
    let a = tokio::spawn(heavy(store.clone()));
    // The first query holds the single bulk slot for its whole 200 ms.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    // The bulk semaphore is held: this fails fast without consuming a
    // pool connection, while interactive work still has connections.
    assert!(matches!(heavy(store.clone()).await, Err(Error::Overloaded)));
    store.run(|_| Ok(())).await.unwrap();
    a.await.unwrap().unwrap();
}

#[tokio::test]
async fn duck_tuning_reports_engine_budgets() {
    // Storage caching lives in Cachey; DuckDB only reports engine budgets.
    let fixture = Fixture::new(1);
    let tuning = fixture.store.duck_tuning();
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
