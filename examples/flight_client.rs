//! Minimal Flight client reading the same shard as the OGC API.
//!
//! ```sh
//! just run fixtures/osm.ducklake # in another shell
//! cargo run --locked --example flight_client
//! cargo run --locked --example flight_client -- --addr http://127.0.0.1:5211
//! cargo run --locked --example flight_client -- --addr http://127.0.0.1:5211 --limit 10 --out /tmp/flight.tsv
//! ```
//!
//! With `--out`, writes one TSV row per feature (`id`, WKB hex, properties
//! JSON) for byte-level cross-protocol checks against OGC responses.

use arrow_flight::{flight_service_client::FlightServiceClient, Ticket};
use futures::TryStreamExt;

fn hex(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(ALPHABET[(b >> 4) as usize] as char);
        out.push(ALPHABET[(b & 15) as usize] as char);
    }
    out
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut addr = "http://127.0.0.1:50051".to_string();
    let mut limit = 1000u32;
    let mut bbox: Option<Vec<f64>> = None;
    let mut out: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => addr = args.next().ok_or("--addr needs a value")?,
            "--limit" => limit = args.next().ok_or("--limit needs a value")?.parse()?,
            "--bbox" => {
                let raw = args.next().ok_or("--bbox needs a value")?;
                bbox = Some(
                    raw.split(',')
                        .map(|v| v.trim().parse::<f64>())
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|_| "bbox must be numbers")?,
                );
            }
            "--out" => out = Some(args.next().ok_or("--out needs a value")?),
            other => return Err(format!("unknown arg: {other}").into()),
        }
    }
    let channel = tonic::transport::Endpoint::from_shared(addr)?
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);
    let mut ticket = serde_json::json!({
        "collection": "buildings",
        "columns": ["id", "geometry", "properties"],
        "limit": limit,
        "sources": [1],
    });
    if let Some(bbox) = bbox {
        ticket["bbox"] = serde_json::json!(bbox);
    }
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
    let mut tsv = String::new();
    while let Some(batch) = batches.try_next().await? {
        total += batch.num_rows();
        if out.is_some() {
            use arrow::array::{Array, BinaryArray, StringArray};
            let ids = batch
                .column_by_name("id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or("missing id column")?;
            let geoms = batch
                .column_by_name("geometry")
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or("missing geometry column")?;
            let props = batch
                .column_by_name("properties")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or("missing properties column")?;
            for i in 0..batch.num_rows() {
                let id = ids.value(i);
                let wkb = hex(geoms.value(i));
                let prop = props.value(i).replace(['\t', '\n'], " ");
                tsv.push_str(&format!("{id}\t{wkb}\t{prop}\n"));
            }
        }
    }
    println!("streamed {total} rows");
    if let Some(path) = out {
        std::fs::write(path, tsv)?;
    }
    Ok(())
}
