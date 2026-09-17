//! Minimal Flight client reading the same shard as the OGC API.
//!
//! ```sh
//! just run fixtures/osm.ducklake # in another shell
//! cargo run --locked --example flight_client
//! cargo run --locked --example flight_client -- --addr http://127.0.0.1:5211
//! ```

use arrow_flight::{flight_service_client::FlightServiceClient, Ticket};
use futures::TryStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut addr = "http://127.0.0.1:50051".to_string();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--addr" {
            addr = args.next().ok_or("--addr needs a value")?;
        }
    }
    let channel = tonic::transport::Endpoint::from_shared(addr)?
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);
    let ticket = serde_json::json!({
        "collection": "buildings",
        "columns": ["id", "geometry", "properties"],
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
