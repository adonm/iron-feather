//! XYZ MVT extension. Spatial filtering is CRS84; tile encoding is Web Mercator.
use crate::{
    api::{self, SourceQuery},
    db, filter, plan,
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
    // Key derives from the normalized inputs; SQL builds on miss so hits
    // pay validation + lookup only. Tiles hold up to 5k features and run
    // Mercator transforms, so they share the bulk lane with Flight.
    let key = plan::tile_key(z, x, y, &collection, &sources);
    let body = store
        .bytes(key, true, move |conn| {
            // Predicates build inside the worker: the cached path never
            // formats SQL.
            let fetch = Store::predicate(&collection, Some(bbox), &sources);
            // Empty tiles stay 204: check cheaply before paying for the
            // transform + encode.
            let empty = db::strings_col(
                conn,
                &format!("SELECT 'x' FROM features WHERE {fetch} LIMIT 1"),
            )?;
            if empty.is_empty() {
                return Ok(Bytes::new());
            }
            // Page first, transform second: the inner scan selects raw
            // id/geom for the page, the outer runs Mercator + clip only
            // over those rows instead of every scanned match.
            let sql = format!(
                "SELECT ST_AsMVT(t, {}) FROM (SELECT id, ST_AsMVTGeom(\
                 ST_Transform(page.geom, 'EPSG:4326', 'EPSG:3857', always_xy := true), \
                 ST_Extent(ST_MakeEnvelope({west}, {}, {}, {north})), 4096, 64, true) AS geom \
                 FROM (SELECT id, geom FROM features WHERE {fetch} ORDER BY id LIMIT 5000) AS page) t \
                 WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)",
                filter::quote(&collection),
                north - span,
                west + span
            );
            let tile = db::blob_one(conn, &sql)?.unwrap_or_default();
            Ok(Bytes::from(tile))
        })
        .await?;
    if body.bytes.is_empty() {
        Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header(poem::http::header::CACHE_CONTROL, "public, max-age=60")
            .header(
                poem::http::header::VARY,
                "Accept, Accept-Encoding, X-Source-Ids",
            )
            .finish())
    } else {
        Ok(api::conditional_response(
            req,
            &body,
            "application/vnd.mapbox-vector-tile",
        ))
    }
}
