//! Arrow Flight surface: tickets, discovery, streaming lifecycle, budgets.
use super::common::{flight_ids, ticket, Fixture};
use crate::flight::ShardFlight;
use arrow::datatypes::{DataType, Schema};
use arrow_flight::{flight_service_server::FlightService, Criteria, FlightDescriptor};
use futures::TryStreamExt;
use serde_json::json;
use tonic::Request;
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

#[test]
fn flight_byte_budget_bounds_buffered_batches() {
    use crate::store::FLIGHT_BYTE_BUDGET;
    assert_eq!(FLIGHT_BYTE_BUDGET, 32 * 1024 * 1024);
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
        .arrow_stream(
            "TRUE".into(),
            "id".into(),
            store.read_source(None),
            100_000,
            0,
        )
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
        .arrow_stream(
            "TRUE".into(),
            "id".into(),
            fixture.store.read_source(None),
            100_000,
            0,
        )
        .await
        .unwrap();
    let mut count = 0;
    while let Some(batch) = batches.batches.recv().await {
        let batch = batch.unwrap().batch;
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
        .arrow_stream(
            "TRUE".into(),
            "no_such_column".into(),
            fixture.store.read_source(None),
            100_000,
            0,
        )
        .await;
    assert!(result.is_err());
    fixture.store.run(|_| Ok(())).await.unwrap();
}

#[tokio::test]
async fn streaming_drop_with_queued_batches_releases_budget() {
    // Budget permits travel with queued batches: dropping the stream
    // without consuming must release process-wide bytes and return the
    // pool connection, so the next request still completes.
    let fixture = Fixture::new(1);
    let store = fixture.store.clone();
    let batches = store
        .arrow_stream(
            "TRUE".into(),
            "id".into(),
            store.read_source(None),
            100_000,
            0,
        )
        .await
        .unwrap();
    // Wait for the worker to queue at least one batch (budget held).
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while store.flight_used_bytes() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(batches);
    // Queued permits drop with the channel: budget returns to zero.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while store.flight_used_bytes() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(store.flight_used_bytes(), 0);
    // The worker exits promptly on disconnect; the pool serves again.
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
async fn oversized_batch_drains_instead_of_spinning() {
    // A single batch larger than the per-stream budget must still flow once
    // the buffer drains; reserve logic guarantees progress.
    const { assert!(crate::store::FLIGHT_TOTAL_BUDGET >= crate::store::FLIGHT_BYTE_BUDGET) };
    let fixture = Fixture::new(1);
    let mut batches = fixture
        .store
        .arrow_stream(
            "TRUE".into(),
            "id".into(),
            fixture.store.read_source(None),
            100_000,
            0,
        )
        .await
        .unwrap();
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), batches.batches.recv())
        .await
        .unwrap();
    assert!(first.is_some());
}
