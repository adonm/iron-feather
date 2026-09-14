//! Minimal Flight client proving the standalone fast path.
//!
//! ```sh
//! just run-serve # in another shell, with --shard-source pointing at parquet
//! cargo run --locked --features serve --example flight_client
//! ```

use arrow_flight::{flight_service_client::FlightServiceClient, Ticket};
use futures::{StreamExt, TryStreamExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:50051")
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);
    let ticket = serde_json::json!({
        "collection": "buildings",
        "bbox": [-87.35, 13.95, -87.05, 14.2],
        "columns": ["id", "x", "y", "name"],
        "limit": 1000,
        "sources": [1],
    });
    let request = tonic::Request::new(Ticket {
        ticket: ticket.to_string().into_bytes().into(),
    });
    let stream = client
        .do_get(request)
        .await?
        .into_inner()
        .map_err(arrow_flight::error::FlightError::from);
    let mut batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(stream);
    let mut total = 0usize;
    while let Some(batch) = batches.try_next().await? {
        total += batch.num_rows();
    }
    println!("streamed {total} rows");
    Ok(())
}
