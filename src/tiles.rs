//! Plain-Poem routes: health and vector tiles.
//!
//! Tiles stay outside the OpenAPI impl so the MVT content type is exact
//! (`application/vnd.mapbox-vector-tile`). Envelope math comes from Martin
//! (`martin-tile-utils`) as a library.

use poem::{
    handler,
    http::{header, StatusCode},
    web::{Data, Path},
    Request, Response,
};

use crate::{filter::parse_source_list, AppState};

#[handler]
pub fn healthz() -> &'static str {
    "ok"
}

#[handler]
pub async fn tile(
    Path((collection, z, x, y)): Path<(String, u8, u32, u32)>,
    req: &Request,
    Data(state): Data<&AppState>,
) -> Response {
    // Mirrors martin_tile_utils::MAX_ZOOM (30); xyz_to_bbox panics above it.
    if z > 30 {
        return status_only(StatusCode::BAD_REQUEST);
    }
    // WGS84 [min_lng, min_lat, max_lng, max_lat], via Martin as a lib.
    let bbox = martin_tile_utils::xyz_to_bbox(z, x, y, x, y);
    let header_sources = req
        .headers()
        .get("x-source-ids")
        .and_then(|value| value.to_str().ok());
    let sources = parse_source_list(header_sources);

    match state.store.tile(&collection, bbox, z, &sources).await {
        Ok(Some(bytes)) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/vnd.mapbox-vector-tile")
            .body(bytes),
        Ok(None) => status_only(StatusCode::NO_CONTENT),
        Err(error) => {
            tracing::warn!(error = ?error, collection = %collection, zoom = z, "tile request failed");
            match error {
                crate::store::StoreError::NotFound(_) => status_only(StatusCode::NOT_FOUND),
                crate::store::StoreError::Overloaded => Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .header(header::RETRY_AFTER, "1")
                    .body(()),
                crate::store::StoreError::Backend(_) => status_only(StatusCode::BAD_GATEWAY),
            }
        }
    }
}

fn status_only(status: StatusCode) -> Response {
    Response::builder().status(status).body(())
}
