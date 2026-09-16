//! Regression coverage for the hand-rolled stable-v2-API surface:
//! chunk iteration with typed reads, cross-thread interrupt, and the
//! Arrow C stream export declared locally (absent from duckdb-rs's
//! pregenerated v2 bindings).
use arrow::record_batch::RecordBatchReader;
use duckdb_neo::{environment::Environment, environment::StorageLocation, Parameters};

// `duckdb_v2_result_to_arrow_stream` ships in the pinned engine but is
// absent from duckdb-rs's pregenerated v2 bindings, so declare the stable
// signature directly (verified against .deps/duckdb/duckdb_v2.h).
unsafe extern "C" {
    fn duckdb_v2_result_to_arrow_stream(
        result: *mut libduckdb_sys::v2::duckdb_v2_result_handle,
        batch_size: u64,
        out_stream: *mut arrow::ffi_stream::FFI_ArrowArrayStream,
        err: *mut libduckdb_sys::v2::duckdb_v2_error_info_handle,
    ) -> libduckdb_sys::v2::DUCKDB_V2_ERROR;
}

#[test]
fn neo_query_chunks_and_values() {
    let env = Environment::new().unwrap();
    let db = env.open(StorageLocation::InMemory).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id VARCHAR, n BIGINT)", Parameters::None)
        .unwrap();
    conn.execute(
        "INSERT INTO t VALUES ('a', 1), ('b', NULL)",
        Parameters::None,
    )
    .unwrap();
    let mut rows = Vec::new();
    for chunk in conn
        .query("SELECT id, n FROM t ORDER BY id", Parameters::None)
        .unwrap()
    {
        let chunk = chunk.unwrap();
        let ids = chunk.get_vector_at::<String>(0).unwrap();
        let ns = chunk.get_vector_at::<i64>(1).unwrap();
        for i in 0..chunk.row_count().unwrap() {
            rows.push((
                ids.get(i).unwrap().map(|s| s.to_string()),
                ns.get(i).unwrap().copied(),
            ));
        }
    }
    assert_eq!(
        rows,
        vec![(Some("a".into()), Some(1)), (Some("b".into()), None)]
    );
}

#[test]
fn neo_cross_thread_interrupt_cancels() {
    use duckdb_neo::query_result::QueryResultStep;
    use std::sync::{Arc, Barrier};

    let env = Environment::new().unwrap();
    let db = env.open(StorageLocation::InMemory).unwrap();
    let conn = db.connect().unwrap();
    // Raw handle is a pointer: the documented cross-thread cancel entry point.
    let handle: libduckdb_sys::v2::duckdb_v2_connection_handle = *conn;
    let gate = Arc::new(Barrier::new(2));
    let worker_gate = gate.clone();
    let worker = std::thread::spawn(move || {
        let mut result = conn
            .query(
                "SELECT count(*) FROM range(10000000000) t(i)",
                Parameters::None,
            )
            .unwrap();
        worker_gate.wait();
        loop {
            match result.step().unwrap() {
                QueryResultStep::Canceled => return "canceled",
                QueryResultStep::Finished => return "finished",
                QueryResultStep::Chunk(_) => {}
                QueryResultStep::Waiting => result.wait().unwrap(),
            }
        }
    });
    gate.wait();
    std::thread::sleep(std::time::Duration::from_millis(200));
    let code =
        unsafe { libduckdb_sys::v2::duckdb_v2_connection_interrupt(handle, std::ptr::null_mut()) };
    assert_eq!(
        code as u32,
        libduckdb_sys::v2::DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE as u32
    );
    assert_eq!(worker.join().unwrap(), "canceled");
}

#[test]
fn neo_arrow_stream_exports_batches() {
    let env = Environment::new().unwrap();
    let db = env.open(StorageLocation::InMemory).unwrap();
    let conn = db.connect().unwrap();
    let mut result = conn
        .query(
            "SELECT 1::INTEGER AS a, 'x' AS b UNION ALL SELECT 2, 'y'",
            Parameters::None,
        )
        .unwrap();
    let mut stream = arrow::ffi_stream::FFI_ArrowArrayStream::empty();
    let code = unsafe {
        duckdb_v2_result_to_arrow_stream(
            &mut result.handle,
            1024,
            &mut stream,
            std::ptr::null_mut(),
        )
    };
    assert_eq!(
        code as u32,
        libduckdb_sys::v2::DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE as u32
    );
    let reader = arrow::ffi_stream::ArrowArrayStreamReader::try_new(stream).unwrap();
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 2);
    let mut rows = 0;
    for batch in reader {
        rows += batch.unwrap().num_rows();
    }
    assert_eq!(rows, 2);
}
