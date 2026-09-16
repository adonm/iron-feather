//! Remote I/O happens here, once: Layercake GeoParquet becomes a versioned
//! DuckLake snapshot (small catalog plus spatially clustered Parquet) ready
//! to serve straight from S3.
use crate::{db, filter, store::ShardManifest};
use clap::Args;
use duckdb_neo::Parameters;
use std::path::{Path, PathBuf};

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
    /// Output DuckLake catalog path.
    #[arg(long, default_value = "fixtures/osm.ducklake")]
    pub out: PathBuf,
    /// Local directory receiving the Parquet data files.
    #[arg(long, default_value = "fixtures/osm.files")]
    pub data_dir: PathBuf,
    /// URL prefix the Parquet files will be served from. Defaults to the
    /// data dir so a local build serves immediately; pass the S3/HTTPS
    /// prefix when publishing, then sync the data dir there.
    #[arg(long)]
    pub data_url: Option<String>,
    /// Optional cap for development slices; omit to materialize the whole bbox.
    #[arg(long)]
    pub limit: Option<u32>,
    /// Target Parquet file size in MiB.
    #[arg(long, default_value_t = 128)]
    pub file_mb: u64,
    /// Parquet row-group size in rows.
    #[arg(long, default_value_t = 65536)]
    pub row_group: u64,
    /// Row clustering for tight per-file bbox statistics: grid (default),
    /// hilbert, or none (preserve source insertion order).
    #[arg(long, default_value = "grid")]
    pub sort: String,
    #[arg(long, default_value_t = 1)]
    pub source_id: i64,
}

fn publish_tree(source: &Path, dest: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let target = dest.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            std::fs::create_dir_all(&target)?;
            publish_tree(&entry.path(), &target)?;
        } else if !target.exists() {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

impl Build {
    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !filter::collection_id(&self.collection) {
            return Err("invalid collection id".into());
        }
        if !["grid", "hilbert", "none"].contains(&self.sort.as_str()) {
            return Err("sort must be grid, hilbert, or none".into());
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
        let staging_catalog = staging.path().join("catalog.ducklake");
        let staging_data = staging.path().join("files");
        std::fs::create_dir_all(&staging_data)?;
        let data_url = self.data_url.clone().unwrap_or_else(|| {
            let mut url = self.data_dir.to_string_lossy().into_owned();
            if !url.ends_with('/') {
                url.push('/');
            }
            url
        });
        let rows = self.write(&staging_catalog, &staging_data, &data_url)?;
        // Publish data files first, then the catalog that references them.
        // Existing published files are never deleted: another snapshot may
        // still reference them. Layout nesting (schema/table dirs) is
        // preserved so the catalog's relative paths keep resolving.
        std::fs::create_dir_all(&self.data_dir)?;
        publish_tree(&staging_data, &self.data_dir)?;
        std::fs::rename(&staging_catalog, &self.out)?;
        // Versioned manifest travels atomically with the shard so servers
        // can verify the snapshot and clients can reason about layout.
        let built_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().to_string())
            .unwrap_or_default();
        let manifest = ShardManifest {
            version: 1,
            backend: "lake".to_string(),
            schema_version: 2,
            source: self.from.clone(),
            bbox: self.bbox,
            rows,
            built_at,
            layout: Some(crate::store::LakeLayout {
                file_mb: self.file_mb,
                row_group: self.row_group,
                sort: self.sort.clone(),
            }),
        };
        let manifest_out = format!("{}.manifest.json", self.out.display());
        std::fs::write(&manifest_out, serde_json::to_vec_pretty(&manifest)?)?;
        println!(
            "materialized {rows} features into {} (+ {})",
            self.out.display(),
            self.data_dir.display()
        );
        Ok(())
    }

    fn write(
        &self,
        catalog: &Path,
        staging_data: &Path,
        data_url: &str,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let db = db::open_memory()?;
        let conn = db.connect()?;
        conn.execute("INSTALL spatial", Parameters::None)?;
        conn.execute("LOAD spatial", Parameters::None)?;
        conn.execute("INSTALL ducklake", Parameters::None)?;
        conn.execute("LOAD ducklake", Parameters::None)?;
        if self.from.starts_with("https://")
            || self.from.starts_with("http://")
            || self.from.starts_with("s3://")
        {
            conn.execute("INSTALL httpfs", Parameters::None)?;
            conn.execute("LOAD httpfs", Parameters::None)?;
        }
        let input = format!("read_parquet({})", filter::quote(&self.from));
        let columns: Vec<(String, String)> =
            db::text_table(&conn, &format!("DESCRIBE SELECT * FROM {input}"))?
                .into_iter()
                .map(|mut row| {
                    let mut cells = row.drain(..);
                    (
                        cells.next().flatten().unwrap_or_default(),
                        cells.next().flatten().unwrap_or_default(),
                    )
                })
                .collect();
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
        // Clustering for tight per-file bbox statistics: grid cells from the
        // region origin keep nearby rows in the same files; Hilbert is the
        // alternative when the source order is already scattered.
        let (sort_ddl, sort_select): (Option<&str>, String) = match self.sort.as_str() {
            "grid" => (
                Some("ALTER TABLE features SET SORTED BY (sortkey ASC, id ASC)"),
                format!(
                    "((ST_XMin({geometry}) - {}) * 100)::BIGINT * 1000 + ((ST_YMin({geometry}) - {}) * 100)::BIGINT,",
                    w.floor() as i64,
                    s.floor() as i64
                ),
            ),
            "hilbert" => (
                Some("ALTER TABLE features SET SORTED BY (sortkey ASC, id ASC)"),
                format!(
                    "ST_Hilbert({geometry}, ST_Extent(ST_MakeEnvelope({w}, {s}, {e}, {n}))),"
                ),
            ),
            // Preserve source insertion order.
            _ => (None, "0,".to_string()),
        };
        // Build-time derivatives: centroids and display names are read on
        // every x/y/name projection and many tile paths. Pay once at build
        // instead of per row per request. Original geometry and full
        // properties stay authoritative.
        let catalog_sql = filter::quote(&format!("ducklake:{}", catalog.to_string_lossy()));
        let staging_sql = filter::quote(&format!("{}/", staging_data.to_string_lossy()));
        let data_url_sql = filter::quote(&if data_url.ends_with('/') {
            data_url.to_string()
        } else {
            format!("{data_url}/")
        });
        let mut setup = vec![
            format!("ATTACH {catalog_sql} AS lake (DATA_PATH {data_url_sql})"),
            "DETACH lake".to_string(),
            format!(
                "ATTACH {catalog_sql} AS lake (DATA_PATH {staging_sql}, OVERRIDE_DATA_PATH true)"
            ),
            "USE lake".to_string(),
            format!(
                "CALL lake.set_option('target_file_size', '{}MB')",
                self.file_mb
            ),
            format!(
                "CALL lake.set_option('parquet_row_group_size', {})",
                self.row_group
            ),
            "CALL lake.set_option('parquet_compression', 'zstd')".to_string(),
            "CALL lake.set_option('parquet_compression_level', 3)".to_string(),
            "CREATE TABLE features( \
               id VARCHAR, layer VARCHAR, source_id BIGINT, geom GEOMETRY, properties JSON, \
               sortkey BIGINT, xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE, \
               cx DOUBLE, cy DOUBLE, name VARCHAR)"
                .to_string(),
            "CREATE TABLE collections(id VARCHAR)".to_string(),
        ];
        if let Some(ddl) = sort_ddl {
            setup.push(ddl.to_string());
        }
        let setup_refs: Vec<&str> = setup.iter().map(String::as_str).collect();
        db::execute_all(&conn, &setup_refs)?;
        let insert = format!(
            "INSERT INTO features \
             SELECT type || ':' || id::VARCHAR, {}::VARCHAR, {}::BIGINT, {geometry}, \
              to_json(struct_pack({properties})), {sort_select} \
              ST_XMin({geometry}), ST_YMin({geometry}), ST_XMax({geometry}), ST_YMax({geometry}), \
              ST_X(ST_Centroid({geometry})), ST_Y(ST_Centroid({geometry})), \
              coalesce(json_extract_string(to_json(struct_pack({properties})), '$.name'), json_extract_string(to_json(struct_pack({properties})), '$.tags.name')) \
             FROM {input} WHERE {longitude} AND bbox.ymin <= {n} AND bbox.ymax >= {s} \
             AND {} {limit};",
            filter::quote(&self.collection), self.source_id,
            filter::spatial_predicate(self.bbox).replace("geom,", &format!("{geometry},")));
        conn.execute(insert.as_str(), Parameters::None)?;
        db::execute_all(
            &conn,
            &[
                "INSERT INTO collections SELECT DISTINCT layer AS id FROM features ORDER BY id",
                "CALL ducklake_flush_inlined_data('lake')",
            ],
        )?;
        let count = db::int_one(&conn, "SELECT count(*) FROM features")?;
        Ok(count)
    }
}
