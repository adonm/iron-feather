//! Build local fixtures from an OSM-derived HTTP Parquet slice (Overture).
//!
//! ```sh
//! just fixture-osm   # fixtures/osm-buildings.parquet + fixtures/demo.duckdb
//! ```
//!
//! Lineage here is synthetic (`source_id` derived from author count) purely
//! so the dev loop exercises lineage filtering end to end. Real pipelines
//! carry their own source/policy ids.

use clap::Parser;

const DEFAULT_FROM: &str = "https://overturemaps-us-west-2.s3.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building/part-00000-66390f89-3dae-58d7-a8f9-dd8538b7141a-c000.zstd.parquet,https://overturemaps-us-west-2.s3.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building/part-00001-c5857db4-1f51-5c2f-886e-417d7f5f7e10-c000.zstd.parquet,https://overturemaps-us-west-2.s3.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building/part-00002-b2a8ae88-c777-52d0-9f2d-683e782daa86-c000.zstd.parquet,https://overturemaps-us-west-2.s3.amazonaws.com/release/2026-08-19.0/theme=buildings/type=building/part-00003-a1f0c648-352b-5719-a1d8-32f28042e244-c000.zstd.parquet";

#[derive(Parser, Debug)]
#[command(
    name = "iron-feather-fixture",
    about = "Slice OSM buildings into the fast schema + a demo DuckDB shard"
)]
struct Args {
    /// Comma-separated explicit part URLs (plain HTTPS has no listing/glob).
    #[arg(long, env = "IRON_FEATHER_FIXTURE_FROM", default_value = DEFAULT_FROM)]
    from: String,
    #[arg(long, default_value_t = -87.35)]
    minx: f64,
    #[arg(long, default_value_t = 13.95)]
    miny: f64,
    #[arg(long, default_value_t = -87.05)]
    maxx: f64,
    #[arg(long, default_value_t = 14.2)]
    maxy: f64,
    #[arg(long, default_value_t = 20_000)]
    limit: u32,
    #[arg(long, default_value = "fixtures")]
    out_dir: std::path::PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.out_dir)?;
    let parquet = args.out_dir.join("osm-buildings.parquet");
    let shard = args.out_dir.join("demo.duckdb");
    let (minx, miny, maxx, maxy) = (args.minx, args.miny, args.maxx, args.maxy);

    let duck = duckdb::Connection::open_in_memory()?;
    duck.execute_batch("INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;")?;

    let inputs = args
        .from
        .split(',')
        .map(|url| format!("'{}'", url.trim()))
        .collect::<Vec<_>>()
        .join(",");
    let (ex0, ey0, ex1, ey1, total): (f64, f64, f64, f64, i64) = duck.query_row(
        &format!(
            "SELECT min(bbox.xmin), min(bbox.ymin), max(bbox.xmax), max(bbox.ymax), COUNT(*) \
             FROM read_parquet([{inputs}])"
        ),
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    println!("inputs extent [{ex0},{ey0},{ex1},{ey1}] rows={total}");
    let slice = format!(
        "SELECT id, (bbox.xmin + bbox.xmax) / 2 AS x, (bbox.ymin + bbox.ymax) / 2 AS y, \
         CAST(ABS(hash(id)) % 3 + 1 AS BIGINT) AS source_id, \
         CAST(COALESCE(CAST(names['common'] AS VARCHAR), id) AS VARCHAR) AS name \
         FROM read_parquet([{inputs}]) \
         WHERE bbox.xmin BETWEEN {minx} AND {maxx} AND bbox.xmax BETWEEN {minx} AND {maxx} \
         AND bbox.ymin BETWEEN {miny} AND {maxy} AND bbox.ymax BETWEEN {miny} AND {maxy} \
         LIMIT {}",
        args.limit
    );
    let available: i64 = duck.query_row(&format!("SELECT COUNT(*) FROM ({slice})"), [], |row| {
        row.get(0)
    })?;
    println!("slice rows available: {available}");
    duck.execute_batch(&format!(
        "COPY ({slice}) TO '{}' (FORMAT PARQUET)",
        parquet.display()
    ))?;
    let probe: bool = duck.query_row(
        "SELECT ST_Intersects(ST_Point(-87.2, 14.08), ST_MakeEnvelope(-87.35, 13.95, -87.05, 14.2))",
        [],
        |row| row.get(0),
    )?;
    println!("spatial probe intersects={probe}");

    if shard.exists() {
        std::fs::remove_file(&shard)?;
    }
    duck.execute_batch(&format!(
        "ATTACH '{}' AS demo; CREATE TABLE demo.main.features AS \
         SELECT id, 'buildings' AS layer, source_id, ST_Point(x, y) AS geom, name \
         FROM read_parquet('{}')",
        shard.display(),
        parquet.display()
    ))?;

    let shape_count: i64 = duck.query_row(
        "SELECT COUNT(*) FROM demo.main.features WHERE layer = 'buildings' AND source_id IN (1) \
         AND ST_Intersects(geom, ST_MakeEnvelope(-87.35, 13.95, -87.05, 14.2))",
        [],
        |row| row.get(0),
    )?;
    let sample: String = duck.query_row(
        "SELECT ST_AsText(geom) FROM demo.main.features LIMIT 1",
        [],
        |row| row.get(0),
    )?;
    println!("serve-shape count={shape_count} sample={sample}");
    let mut dist =
        duck.prepare("SELECT source_id, COUNT(*) FROM demo.main.features GROUP BY 1 ORDER BY 1")?;
    let dist_rows = dist.query_map([], |row| {
        let id: i64 = row.get(0)?;
        let n: i64 = row.get(1)?;
        Ok((id, n))
    })?;
    for row in dist_rows {
        let (id, n) = row?;
        println!("source distribution: {id} => {n}");
    }
    println!("wrote {} and {}", parquet.display(), shard.display());
    Ok(())
}
