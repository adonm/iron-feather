//! Shared request validation and SQL literals. No caller-supplied SQL.

pub fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn collection_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
}

/// CRS84 bbox: 2D or 3D (height is irrelevant to our 2D features).
/// West > east crosses the antimeridian; zero-width boxes are valid.
pub fn bbox(values: &[f64]) -> Result<[f64; 4], String> {
    let b = match values {
        [w, s, e, n] => [*w, *s, *e, *n],
        [w, s, low, e, n, high] if low <= high => [*w, *s, *e, *n],
        _ => return Err("bbox requires 4 or 6 ordered numbers".into()),
    };
    if !values.iter().all(|v| v.is_finite())
        || !(-180.0..=180.0).contains(&b[0])
        || !(-180.0..=180.0).contains(&b[2])
        || !(-90.0..=90.0).contains(&b[1])
        || !(-90.0..=90.0).contains(&b[3])
        || b[1] > b[3]
    {
        return Err("bbox must be finite CRS84 bounds with south <= north".into());
    }
    Ok(b)
}

pub fn parse_bbox(raw: &str) -> Result<[f64; 4], String> {
    let values = raw
        .split(',')
        .map(|v| v.trim().parse::<f64>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| "bbox must contain numbers")?;
    bbox(&values)
}

pub fn spatial_predicate(b: [f64; 4]) -> String {
    let envelope = |w, e| {
        format!(
            "ST_Intersects(geom, ST_MakeEnvelope({w}, {}, {e}, {}))",
            b[1], b[3]
        )
    };
    if b[0] > b[2] {
        format!("({} OR {})", envelope(b[0], 180.0), envelope(-180.0, b[2]))
    } else {
        envelope(b[0], b[2])
    }
}

pub fn parse_sources(raw: &str) -> Result<Vec<i64>, String> {
    if raw.is_empty() {
        return Ok(vec![]);
    }
    raw.split(',')
        .map(|v| {
            v.trim()
                .parse()
                .map_err(|_| "sources must be integers".into())
        })
        .collect()
}

/// A request may narrow the header's source set, never broaden it.
/// These are data filters, not an authentication mechanism.
pub fn sources(requested: Option<Vec<i64>>, header: Option<Vec<i64>>) -> Vec<i64> {
    let mut ids = match (requested, header) {
        (Some(ids), Some(allowed)) => ids.into_iter().filter(|id| allowed.contains(id)).collect(),
        (Some(ids), None) | (None, Some(ids)) => ids,
        (None, None) => vec![],
    };
    ids.sort_unstable();
    ids.dedup();
    ids
}

pub fn predicate(collection: &str, bounds: Option<[f64; 4]>, sources: &[i64]) -> String {
    let lineage = if sources.is_empty() {
        "FALSE".into()
    } else {
        format!(
            "source_id IN ({})",
            sources
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    let mut sql = format!("layer = {} AND {lineage}", quote(collection));
    if let Some(b) = bounds {
        sql.push_str(&format!(" AND {}", spatial_predicate(b)));
    }
    sql
}

/// Layercake's edit timestamp isn't a feature's temporal extent. Features
/// without temporal geometry match every valid OGC datetime filter.
pub fn datetime(raw: &str) -> Result<(), String> {
    let parse = |s: &str| {
        chrono::DateTime::parse_from_rfc3339(s).map_err(|_| "invalid RFC3339 datetime".to_string())
    };
    if let Some((start, end)) = raw.split_once('/') {
        let start = if start == ".." || start.is_empty() {
            None
        } else {
            Some(parse(start)?)
        };
        let end = if end == ".." || end.is_empty() {
            None
        } else {
            Some(parse(end)?)
        };
        if start.is_none() && end.is_none() || matches!((start, end), (Some(a), Some(b)) if a > b) {
            return Err("datetime requires a nonempty, ordered interval".into());
        }
    } else {
        parse(raw)?;
    }
    Ok(())
}
