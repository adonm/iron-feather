//! OGC API surface: discovery, pages, validation, encoding, tiles.
use super::common::{body, Fixture};
use crate::api;
use poem::{http::StatusCode, test::TestClient};
use serde_json::json;
#[tokio::test]
async fn ogc_discovery_and_contract_describe_the_actual_shard() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let landing = body(client.get("/").send().await).await;
    for link in landing["links"].as_array().unwrap() {
        client
            .get(link["href"].as_str().unwrap())
            .send()
            .await
            .assert_status_is_ok();
    }
    let catalog = body(client.get("/collections").send().await).await;
    assert_eq!(catalog["collections"].as_array().unwrap().len(), 2);
    assert_eq!(catalog["collections"][1]["id"], "roads");
    for meta in catalog["collections"].as_array().unwrap() {
        for link in meta["links"].as_array().unwrap() {
            client
                .get(link["href"].as_str().unwrap())
                .send()
                .await
                .assert_status_is_ok();
        }
    }
    client
        .get("/collections/ag_fields")
        .send()
        .await
        .assert_status(StatusCode::NOT_FOUND);
    let contract = body(client.get("/api").send().await).await;
    assert_eq!(contract["openapi"], "3.0.3");
    assert!(
        contract["paths"]["/collections/{collectionId}/items"]["get"]["responses"]["200"]
            ["content"]
            .get(api::GEOJSON)
            .is_some()
    );
    assert_eq!(
        body(client.get("/conformance").send().await).await["conformsTo"],
        json!(api::CONFORMANCE)
    );
}

#[tokio::test]
async fn ogc_preserves_polygons_properties_and_bbox_intersections() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    // The polygon intersects this box; its centroid does not.
    let response = client
        .get("/collections/buildings/items?bbox=9,9,11,11&sources=1")
        .send()
        .await;
    response.assert_content_type(api::GEOJSON);
    let page = body(response).await;
    assert_eq!(page["numberReturned"], 1);
    assert_eq!(page["features"][0]["geometry"]["type"], "Polygon");
    assert_eq!(
        page["features"][0]["geometry"]["coordinates"][0]
            .as_array()
            .unwrap()
            .len(),
        5
    );
    assert_eq!(page["features"][0]["properties"]["height"], 12);
    assert_eq!(page["features"][0]["properties"]["name"], "Café");
    let href = page["features"][0]["links"][0]["href"].as_str().unwrap();
    let feature = body(client.get(href).send().await).await;
    assert_eq!(feature, page["features"][0]);
    client
        .get(href.replace("sources=1", "sources=2"))
        .send()
        .await
        .assert_status(StatusCode::NOT_FOUND);
    client
        .get("/collections/buildings/items/%27%20OR%20TRUE%20--?sources=1")
        .send()
        .await
        .assert_status(StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ogc_pages_keep_filters_and_have_no_duplicates() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let mut next = Some(
        "/collections/buildings/items?sources=1&limit=1&datetime=2026-01-01T00%3A00%3A00Z"
            .to_string(),
    );
    let mut ids = Vec::new();
    while let Some(url) = next.take() {
        let page = body(client.get(&url).send().await).await;
        assert_eq!(page["numberReturned"], 1);
        assert!(page.get("numberMatched").is_none());
        let id = page["features"][0]["id"].as_str().unwrap().to_string();
        assert!(!ids.contains(&id));
        ids.push(id);
        next = page["links"]
            .as_array()
            .unwrap()
            .iter()
            .find(|l| l["rel"] == "next")
            .map(|l| l["href"].as_str().unwrap().to_string());
        if let Some(url) = &next {
            assert!(url.contains("sources=1") && url.contains("datetime="));
        }
    }
    assert_eq!(ids.len(), 4);
    assert_eq!(
        body(client.get("/collections/buildings/items").send().await).await["numberReturned"],
        0
    );
}

#[tokio::test]
async fn ogc_validates_bbox_datetime_parameters_and_accept() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    for query in [
        "bbox=0,0,inf,10",
        "bbox=0,0,181,10",
        "bbox=0,30,10,20",
        "bbox=1,2,3",
        "limit=0",
        "limit=1001",
        "offset=-1",
        "filter=name",
        "properties=name",
        "sources=x",
        "datetime=bad",
        "datetime=../..",
        "datetime=2027-01-01T00:00:00Z/2026-01-01T00:00:00Z",
    ] {
        client
            .get(format!("/collections/buildings/items?{query}"))
            .send()
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }
    let crossing = body(
        client
            .get("/collections/buildings/items?sources=1&bbox=170,0,-170,5")
            .send()
            .await,
    )
    .await;
    assert_eq!(crossing["numberReturned"], 2);
    assert_eq!(
        body(
            client
                .get("/collections/buildings/items?sources=1&bbox=9,9,-100,11,11,100")
                .send()
                .await
        )
        .await["numberReturned"],
        1
    );
    assert_eq!(
        body(
            client
                .get("/collections/buildings/items?sources=1&bbox=10,10,10,10")
                .send()
                .await
        )
        .await["numberReturned"],
        1
    );
    for accept in [
        "text/html",
        "application/geo+json;q=0,application/json;q=0,*/*;q=1",
    ] {
        client
            .get("/collections/buildings/items")
            .header("Accept", accept)
            .send()
            .await
            .assert_status(StatusCode::NOT_ACCEPTABLE);
    }
    client
        .get("/collections/buildings/items")
        .header("Accept", "application/json")
        .send()
        .await
        .assert_status(StatusCode::NOT_ACCEPTABLE);
    for path in [
        "/",
        "/collections",
        "/collections/buildings",
        "/conformance",
        "/api",
    ] {
        client
            .get(format!("{path}?unsupported=1"))
            .send()
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn ogc_canonical_requests_share_body() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let a = client
        .get("/collections/buildings/items?sources=1&limit=2&bbox=9,9,11,11")
        .send()
        .await;
    a.assert_status_is_ok();
    let a = a.0.into_body().into_bytes().await.unwrap();
    // Reordered params and padded floats are the same page.
    let b = client
        .get("/collections/buildings/items?limit=2&bbox=9.0,9.0,11.0,11.0&sources=1")
        .send()
        .await;
    b.assert_status_is_ok();
    let b = b.0.into_body().into_bytes().await.unwrap();
    assert_eq!(a, b);
    // Source order and repeats never change the effective set either.
    let c = client
        .get("/collections/buildings/items?sources=1,2&limit=10")
        .send()
        .await;
    c.assert_status_is_ok();
    let c = c.0.into_body().into_bytes().await.unwrap();
    let d = client
        .get("/collections/buildings/items?sources=2,1,1&limit=10")
        .send()
        .await;
    d.assert_status_is_ok();
    let d = d.0.into_body().into_bytes().await.unwrap();
    assert_eq!(c, d);
}

#[tokio::test]
async fn ogc_header_filtered_pagination_stays_self_contained() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    // No `sources` param: the effective set comes from the header alone.
    let page = body(
        client
            .get("/collections/buildings/items?limit=1")
            .header("X-Source-Ids", "1")
            .send()
            .await,
    )
    .await;
    assert_eq!(page["numberReturned"], 1);
    let next = page["links"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l["rel"] == "next")
        .unwrap()["href"]
        .as_str()
        .unwrap()
        .to_string();
    // The next link carries the effective set, so it works header-free.
    assert!(next.contains("sources=1"));
    let followed = body(client.get(&next).send().await).await;
    assert_eq!(followed["numberReturned"], 1);
    assert_ne!(followed["features"][0]["id"], page["features"][0]["id"]);
}

#[tokio::test]
async fn ogc_gzip_variant_matches_identity_body() {
    use std::io::Read;
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let path = "/collections/buildings/items?sources=1&limit=2";
    let identity = client.get(path).send().await;
    identity.assert_status_is_ok();
    assert!(identity.0.headers().get("content-encoding").is_none());
    let identity_etag = identity
        .0
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let identity_body = identity.0.into_body().into_bytes().await.unwrap();
    let gzipped = client
        .get(path)
        .header("Accept-Encoding", "gzip")
        .send()
        .await;
    gzipped.assert_status_is_ok();
    assert_eq!(gzipped.0.headers().get("content-encoding").unwrap(), "gzip");
    // Variants are distinct representations with distinct validators.
    let gzip_etag = gzipped
        .0
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert_ne!(gzip_etag, identity_etag);
    let compressed = gzipped.0.into_body().into_bytes().await.unwrap();
    assert!(compressed.len() < identity_body.len());
    let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
    let mut roundtrip = Vec::new();
    decoder.read_to_end(&mut roundtrip).unwrap();
    assert_eq!(roundtrip, identity_body);
    // The stored variant revalidates without a body.
    let etag = client
        .get(path)
        .header("Accept-Encoding", "gzip")
        .send()
        .await
        .0
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    client
        .get(path)
        .header("Accept-Encoding", "gzip")
        .header("If-None-Match", etag)
        .send()
        .await
        .assert_status(StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn ogc_errors_are_never_cacheable() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    for path in [
        "/collections/buildings/items/no-such-id?sources=1",
        "/collections/buildings/items?bbox=0,0,181,10",
    ] {
        let response = client.get(path).send().await;
        assert!(response.0.status().is_client_error());
        let headers = response.0.headers();
        assert_eq!(headers.get("cache-control").unwrap(), "no-store");
        assert!(headers.get("etag").is_none());
    }
}

#[tokio::test]
async fn ogc_etag_revalidates_without_body() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let path = "/collections/buildings/items?sources=1&limit=1";
    let first = client.get(path).send().await;
    first.assert_status_is_ok();
    let etag = first
        .0
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(!etag.is_empty());
    let repeat = client.get(path).send().await;
    assert_eq!(
        repeat.0.headers().get("etag").unwrap().to_str().unwrap(),
        etag
    );
    let revalidated = client.get(path).header("If-None-Match", etag).send().await;
    revalidated.assert_status(StatusCode::NOT_MODIFIED);
    assert!(revalidated
        .0
        .into_body()
        .into_bytes()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn source_headers_narrow_requests_and_partition_cached_pages() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let path = "/collections/buildings/items?sources=1,2";
    let a = body(client.get(path).header("X-Source-Ids", "1").send().await).await;
    let b = body(client.get(path).header("X-Source-Ids", "2").send().await).await;
    assert_eq!(a["numberReturned"], 4);
    assert_eq!(b["numberReturned"], 1);
    assert_eq!(b["features"][0]["id"], "way:2");
    assert_eq!(
        body(client.get(path).header("X-Source-Ids", "").send().await).await["numberReturned"],
        0
    );
}

#[derive(prost::Message)]
struct Tile {
    #[prost(message, repeated, tag = "3")]
    layers: Vec<Layer>,
}
#[derive(prost::Message)]
struct Layer {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(message, repeated, tag = "2")]
    features: Vec<TileFeature>,
}
#[derive(prost::Message)]
struct TileFeature {
    #[prost(uint32, repeated, packed = "true", tag = "4")]
    geometry: Vec<u32>,
}

#[tokio::test]
async fn tiles_use_mercator_and_isolate_coordinates() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    use prost::Message;
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    let tile = client
        .get("/collections/buildings/tiles/2/2/1?sources=3")
        .send()
        .await;
    tile.assert_status_is_ok();
    tile.assert_content_type("application/vnd.mapbox-vector-tile");
    let bytes = tile.0.into_body().into_bytes().await.unwrap();
    let decoded = Tile::decode(bytes).unwrap();
    assert_eq!(decoded.layers[0].name, "buildings");
    let point = &decoded.layers[0].features[0].geometry;
    assert_eq!(point[0], 9); // MoveTo, one point, zigzag deltas.
    assert!((i64::from(point[1] / 2) - 910).abs() <= 1);
    assert!((i64::from(point[2] / 2) - 662).abs() <= 1);
    client
        .get("/collections/buildings/tiles/2/1/1?sources=3")
        .send()
        .await
        .assert_status(StatusCode::NO_CONTENT);
    client
        .get("/collections/buildings/tiles/2/2/1?sources=3")
        .send()
        .await
        .assert_status_is_ok();
    for xyz in ["31/0/0", "2/4/1", "2/1/4"] {
        client
            .get(format!("/collections/buildings/tiles/{xyz}?sources=1"))
            .send()
            .await
            .assert_status(StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn ogc_cursor_walk_matches_offset_walk() {
    let fixture = Fixture::new(2);
    let client = TestClient::new(api::routes(fixture.store));
    // Offset walk for reference.
    let mut expected = Vec::new();
    let mut offset = 0;
    loop {
        let page = body(
            client
                .get(format!(
                    "/collections/buildings/items?sources=1&limit=2&offset={offset}"
                ))
                .send()
                .await,
        )
        .await;
        let ids: Vec<String> = page["features"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["id"].as_str().unwrap().to_string())
            .collect();
        if ids.is_empty() {
            break;
        }
        expected.extend(ids);
        if page["links"]
            .as_array()
            .unwrap()
            .iter()
            .all(|l| l["rel"] != "next")
        {
            break;
        }
        offset += 2;
    }
    assert_eq!(expected.len(), 4);
    // Cursor walk follows `next` links only.
    let mut walked = Vec::new();
    let mut next = Some("/collections/buildings/items?sources=1&limit=2".to_string());
    while let Some(url) = next.take() {
        // `next` links carry a cursor (with the offset normalized away).
        if !walked.is_empty() {
            assert!(url.contains("cursor="));
        }
        let page = body(client.get(&url).send().await).await;
        walked.extend(
            page["features"]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| f["id"].as_str().unwrap().to_string()),
        );
        next = page["links"]
            .as_array()
            .unwrap()
            .iter()
            .find(|l| l["rel"] == "next")
            .map(|l| l["href"].as_str().unwrap().to_string());
    }
    assert_eq!(walked, expected);
}
