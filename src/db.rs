//! All DuckDB access through the stable v2 C API (`duckdb-neo`).
//!
//! The pool in [`crate::store`] owns [`Connection`]s; workers borrow them
//! exclusively while a query runs. Cross-thread cancellation uses the raw
//! connection handle (a pointer, always safe to interrupt from any thread)
//! through [`interrupt`]. Arrow export uses
//! `duckdb_v2_result_to_arrow_stream`, which ships in the pinned engine but
//! is missing from duckdb-rs's pregenerated v2 bindings, so its stable
//! signature is declared here directly (verified against
//! `.deps/duckdb/duckdb_v2.h`).

use crate::store::Error;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use duckdb_neo::{
    environment::{Environment, StorageLocation},
    query_result::QueryResult,
    Parameters,
};

pub use duckdb_neo::connection::Connection as NeoConnection;
pub use duckdb_neo::database::Database as NeoDatabase;

unsafe extern "C" {
    fn duckdb_v2_result_to_arrow_stream(
        result: *mut libduckdb_sys::v2::duckdb_v2_result_handle,
        batch_size: u64,
        out_stream: *mut FFI_ArrowArrayStream,
        err: *mut libduckdb_sys::v2::duckdb_v2_error_info_handle,
    ) -> libduckdb_sys::v2::DUCKDB_V2_ERROR;
}

fn v2_ok(code: libduckdb_sys::v2::DUCKDB_V2_ERROR) -> bool {
    code as u32 == libduckdb_sys::v2::DUCKDB_V2_ERROR::DUCKDB_V2_ERROR_NONE as u32
}

impl From<duckdb_neo::error::Error> for Error {
    fn from(e: duckdb_neo::error::Error) -> Self {
        Self::Backend(e.to_string())
    }
}

/// Open an in-memory database. Storage always lives in the attached
/// DuckLake catalog, never in this database file.
pub fn open_memory() -> Result<NeoDatabase, Error> {
    let env = Environment::new()?;
    Ok(env.open(StorageLocation::InMemory)?)
}

/// Run each statement in order. The v2 API takes exactly one statement per
/// call, so callers pass pre-split statements instead of `;`-joined batches.
pub fn execute_all(conn: &NeoConnection, statements: &[&str]) -> Result<(), Error> {
    for sql in statements {
        conn.execute(*sql, Parameters::None)?;
    }
    Ok(())
}

/// Copy of the interrupt path for a handle retained across threads
/// (stable ABI: the handle is just a pointer).
///
/// # Safety
/// The handle must come from a live [`NeoConnection`].
pub unsafe fn interrupt_handle(handle: libduckdb_sys::v2::duckdb_v2_connection_handle) {
    unsafe {
        libduckdb_sys::v2::duckdb_v2_connection_interrupt(handle, std::ptr::null_mut());
    }
}

/// First column of every row as text.
pub fn strings_col(conn: &NeoConnection, sql: &str) -> Result<Vec<String>, Error> {
    let mut out = Vec::new();
    for chunk in conn.query(sql, Parameters::None)? {
        let chunk = chunk?;
        let col = chunk.get_vector_at::<String>(0)?;
        for i in 0..chunk.row_count()? {
            out.push(col.get(i)?.unwrap_or_default().to_string());
        }
    }
    Ok(out)
}

/// Every column of every row as nullable text. Callers only use this for
/// VARCHAR projections (ids, GeoJSON, properties JSON).
pub fn text_table(conn: &NeoConnection, sql: &str) -> Result<Vec<Vec<Option<String>>>, Error> {
    let mut out = Vec::new();
    for chunk in conn.query(sql, Parameters::None)? {
        let chunk = chunk?;
        let ncols = chunk.vectors_count()?;
        let mut cols = Vec::with_capacity(ncols);
        for c in 0..ncols {
            cols.push(chunk.get_vector_at::<String>(c)?);
        }
        for i in 0..chunk.row_count()? {
            let mut row = Vec::with_capacity(ncols);
            for col in &cols {
                row.push(col.get(i)?.map(|s| s.to_string()));
            }
            out.push(row);
        }
    }
    Ok(out)
}

/// Single BIGINT cell. Errors on zero or multiple rows.
pub fn int_one(conn: &NeoConnection, sql: &str) -> Result<i64, Error> {
    let mut result = conn.query(sql, Parameters::None)?;
    let mut value = None;
    let mut rows = 0;
    while let Some(chunk) = result.next().transpose()? {
        let col = chunk.get_vector_at::<i64>(0)?;
        for i in 0..chunk.row_count()? {
            rows += 1;
            value = col.get(i)?.copied();
        }
    }
    match (rows, value) {
        (1, Some(v)) => Ok(v),
        (1, None) => Err(Error::Backend("unexpected NULL".into())),
        _ => Err(Error::Backend(format!("expected 1 row, got {rows}"))),
    }
}

/// Single nullable BLOB cell; `None` when the query returns no rows (used
/// for empty-tile detection alongside `ST_AsMVT` NULLs).
pub fn blob_one(conn: &NeoConnection, sql: &str) -> Result<Option<Vec<u8>>, Error> {
    use duckdb_neo::types::BlobValue;
    let mut result = conn.query(sql, Parameters::None)?;
    let mut value = None;
    while let Some(chunk) = result.next().transpose()? {
        let col = chunk.get_vector_at::<BlobValue>(0)?;
        for i in 0..chunk.row_count()? {
            value = col.get(i)?.map(|b| b.to_vec());
        }
    }
    Ok(value)
}

/// Export a fresh result as an Arrow C stream reader. The reader must be
/// consumed (or dropped, releasing its C stream) before the connection runs
/// another query.
pub fn arrow_stream(result: &mut QueryResult<'_>) -> Result<ArrowArrayStreamReader, Error> {
    let mut stream = FFI_ArrowArrayStream::empty();
    let code = unsafe {
        duckdb_v2_result_to_arrow_stream(&mut result.handle, 0, &mut stream, std::ptr::null_mut())
    };
    if !v2_ok(code) {
        return Err(Error::Backend("arrow stream export failed".into()));
    }
    ArrowArrayStreamReader::try_new(stream).map_err(|e| Error::Backend(e.to_string()))
}
