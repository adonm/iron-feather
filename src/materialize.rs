//! Remote I/O happens here, once. Serve only the completed local shard.
use crate::{filter, store::ShardManifest};
use clap::Args;
use duckdb::Connection;
use std::path::PathBuf;

#[derive(Args, Debug)]
pub struct Build {
    /// Layercake GeoParquet URL or local file, with id, type, geometry and bbox.
    #[arg(
        long,
        default_value = "https://data.openstreetmap.us/layercake/buildings.parquet"
    )]
    pub from: String,
    #[arg(long, default_value = "buildings")]
    pub collection: String,
    /// CRS84 west,south,east,north. Required to bound remote work.
    #[arg(long, allow_hyphen_values = true, value_parser = filter::parse_bbox)]
    pub bbox: [f64; 4],
    #[arg(long, default_value = "fixtures/osm.duckdb")]
    pub out: PathBuf,
    /// Optional cap for development slices; omit to materialize the whole bbox.
    #[arg(long)]
    pub limit: Option<u32>,
    /// Order heap rows by Hilbert value before indexing, so spatially close
    /// rows share storage blocks. One-time build cost for fewer range reads.
    #[arg(long)]
    pub hilbert: bool,
    #[arg(long, default_value_t = 1)]
    pub source_id: i64,
}

impl Build {
    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !filter::collection_id(&self.collection) {
            return Err("invalid collection id".into());
        }
        if self.out.exists() {
            return Err(format!("{} exists; build a new shard path", self.out.display()).into());
        }
        let parent = self
            .out
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."));
        std::fs::create_dir_all(parent)?;
        let staging = tempfile::Builder::new()
            .prefix(".iron-feather-")
            .tempdir_in(parent)?;
        let database = staging.path().join("shard.duckdb");
        let rows = self.write(&database)?;
        // Versioned manifest travels atomically with the shard so servers
        // can verify the snapshot and clients can reason about layout.
        let built_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().to_string())
            .unwrap_or_default();
        let manifest = ShardManifest {
            version: 1,
            backend: "native".to_string(),
            schema_version: 2,
            source: self.from.clone(),
            bbox: self.bbox,
            rows,
            built_at,
            layout: None,
        };
        let manifest_staging = staging.path().join("shard.manifest.json");
        std::fs::write(&manifest_staging, serde_json::to_vec_pretty(&manifest)?)?;
        // Publish the completed, closed database on the same filesystem,
        // without overwriting a concurrently created shard. TempDir cleans up.
        std::fs::hard_link(&database, &self.out)?;
        let manifest_out = format!("{}.manifest.json", self.out.display());
        // Manifest is advisory; a concurrent build winning the shard path
        // still leaves a matching manifest behind.
        let _ = std::fs::hard_link(&manifest_staging, &manifest_out);
        Ok(())
    }

    fn write(&self, path: &std::path::Path) -> Result<i64, Box<dyn std::error::Error>> {
        let conn = Connection::open(path)?;
        conn.execute_batch("INSTALL spatial; LOAD spatial;")?;
        if self.from.starts_with("https://")
            || self.from.starts_with("http://")
            || self.from.starts_with("s3://")
        {
            conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;
        }
        let input = format!("read_parquet({})", filter::quote(&self.from));
        let columns = conn
            .prepare(&format!("DESCRIBE SELECT * FROM {input}"))?
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        let geom_type = columns
            .iter()
            .find(|(name, _)| name == "geometry")
            .ok_or("source is missing geometry")?;
        let geometry = if geom_type.1.starts_with("GEOMETRY") {
            "geometry"
        } else {
            "ST_GeomFromWKB(geometry)"
        };
        // Preserve both current flat columns and older Layercake tags structs.
        let properties = columns
            .iter()
            .filter(|(name, _)| !["geometry", "bbox"].contains(&name.as_str()))
            .map(|(name, _)| {
                let id = format!("\"{}\"", name.replace('"', "\"\""));
                format!("{id} := {id}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let [w, s, e, n] = self.bbox;
        let longitude = if w <= e {
            format!("bbox.xmin <= {e} AND bbox.xmax >= {w}")
        } else {
            format!("(bbox.xmax >= {w} OR bbox.xmin <= {e})")
        };
        let limit = self.limit.map(|n| format!("LIMIT {n}")).unwrap_or_default();
        let order = if self.hilbert {
            format!("ORDER BY ST_Hilbert({geometry})")
        } else {
            String::new()
        };
        // Build-time derivatives: centroids and display names are read on
        // every Flight x/y/name projection and many tile paths. Pay once at
        // build instead of per row per request. Original geometry and full
        // properties stay authoritative.
        let create = format!(
            "CREATE TABLE features AS SELECT type || ':' || id::VARCHAR AS id, {}::VARCHAR AS layer, \
             {}::BIGINT AS source_id, {geometry} AS geom, to_json(struct_pack({properties})) AS properties, \
             ST_X(ST_Centroid({geometry})) AS cx, ST_Y(ST_Centroid({geometry})) AS cy, \
             coalesce(json_extract_string(to_json(struct_pack({properties})), '$.name'), json_extract_string(to_json(struct_pack({properties})), '$.tags.name')) AS name \
             FROM {input} WHERE {longitude} AND bbox.ymin <= {n} AND bbox.ymax >= {s} \
             AND {} {order} {limit};",
            filter::quote(&self.collection), self.source_id,
            filter::spatial_predicate(self.bbox).replace("geom,", &format!("{geometry},")));
        conn.execute_batch(&create)?;
        conn.execute_batch(&format!(
            "CREATE UNIQUE INDEX feature_id ON features(id); \
             CREATE INDEX feature_geom ON features USING RTREE(geom); \
             CREATE TABLE collections AS SELECT DISTINCT layer AS id FROM features ORDER BY id; \
             CREATE TABLE provenance AS SELECT {} AS source, current_timestamp AS built_at; \
             ANALYZE; CHECKPOINT;",
            filter::quote(&self.from)
        ))?;
        let count: i64 = conn.query_row("SELECT count(*) FROM features", [], |r| r.get(0))?;
        println!("materialized {count} features into {}", self.out.display());
        Ok(count)
    }
}
