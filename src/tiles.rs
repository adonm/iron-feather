//! XYZ MVT extension. Spatial filtering is CRS84; tile encoding is Web Mercator.
use crate::{
    api::{self, SourceQuery},
    filter,
    store::{Error, Store},
};
use bytes::Bytes;
use poem::{
    handler,
    http::StatusCode,
    web::{Data, Path, Query},
    Request, Response,
};
use std::sync::Arc;

#[handler]
pub async fn tile(
    Path((collection, z, x, y)): Path<(String, u8, u32, u32)>,
    Query(query): Query<SourceQuery>,
    req: &Request,
    Data(store): Data<&Arc<Store>>,
) -> Result<Response, Error> {
    store.collection(&collection)?;
    if z > 30 || x >= (1u32 << z) || y >= (1u32 << z) {
        return Err(Error::Invalid("tile coordinates outside XYZ matrix".into()));
    }
    let sources = api::source_ids(req, query.sources.as_deref())?;
    let bbox = martin_tile_utils::xyz_to_bbox(z, x, y, x, y);
    let half = std::f64::consts::PI * 6_378_137.0;
    let span = 2.0 * half / f64::from(1u32 << z);
    let west = -half + f64::from(x) * span;
    let north = half - f64::from(y) * span;
    let candidate_sql = format!(
        "SELECT id FROM features WHERE {} ORDER BY id LIMIT 5000",
        filter::predicate(&collection, Some(bbox), &sources)
    );
    let key = format!("tile:{z}:{x}:{y}:{collection}:{sources:?}");
    let bytes = store
        .bytes(key, move |conn| {
            let ids = conn
                .prepare(&candidate_sql)?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            if ids.is_empty() {
                return Ok(Bytes::new());
            }
            let ids = ids
                .iter()
                .map(|id| filter::quote(id))
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "SELECT ST_AsMVT(t, {}) FROM (SELECT id, ST_AsMVTGeom(\
                 ST_Transform(geom, 'EPSG:4326', 'EPSG:3857', always_xy := true), \
                 ST_Extent(ST_MakeEnvelope({west}, {}, {}, {north})), 4096, 64, true) AS geom \
                 FROM features WHERE id IN ({ids}) ORDER BY id) t \
                 WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)",
                filter::quote(&collection),
                north - span,
                west + span
            );
            let tile: Option<Vec<u8>> = conn.query_row(&sql, [], |r| r.get(0))?;
            Ok(Bytes::from(tile.unwrap_or_default()))
        })
        .await?;
    if bytes.is_empty() {
        Ok(Response::builder().status(StatusCode::NO_CONTENT).finish())
    } else {
        Ok(api::response(bytes, "application/vnd.mapbox-vector-tile"))
    }
}
