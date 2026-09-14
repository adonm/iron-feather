//! Request parsing shared by Features, tiles, and (later) Flight.
//!
//! Pure functions: `just test` covers them with no backend.
//!
//! Lineage rule (enforced everywhere): `source_ids` come from auth
//! (JWT/OIDC policy in prod, `?sources=` dev stand-in). They NEVER come from
//! the user `filter` expression. An empty set matches nothing.

/// Parse `bbox=minx,miny,maxx,maxy` into WGS84 bounds.
pub fn parse_bbox(raw: &str) -> Result<[f64; 4], String> {
    let parts: Vec<&str> = raw.split(',').collect();
    if parts.len() != 4 {
        return Err(format!("bbox must have 4 numbers, got {}", parts.len()));
    }
    let mut out = [0.0f64; 4];
    for (i, part) in parts.iter().enumerate() {
        out[i] = part
            .trim()
            .parse::<f64>()
            .map_err(|_| format!("bbox[{i}] is not a number: {part}"))?;
    }
    if !(out[0] < out[2] && out[1] < out[3]) {
        return Err("bbox requires minx<maxx and miny<maxy".to_string());
    }
    Ok(out)
}

/// Parse a comma-separated source-id allowlist into a sorted, deduped set.
pub fn parse_source_list(raw: Option<&str>) -> Vec<i64> {
    let mut ids: Vec<i64> = raw
        .unwrap_or_default()
        .split(',')
        .filter_map(|part| part.trim().parse().ok())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// SQL fragment enforcing lineage. Empty set matches nothing (secure default).
pub fn lineage_predicate(source_ids: &[i64]) -> String {
    if source_ids.is_empty() {
        return "1 = 0".to_string();
    }
    let list = source_ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("source_id IN ({list})")
}

/// Cache-key segment binding data version + policy. Full responses are only
/// ever shared between callers with identical effective visibility.
pub fn visibility_fingerprint(
    serving_version: &str,
    policy_version: &str,
    source_ids: &[i64],
) -> String {
    let mut ids = source_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();
    format!("{serving_version}:{policy_version}:{ids:?}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bbox_ok() {
        assert_eq!(
            parse_bbox("-180,-90,180,90"),
            Ok([-180.0, -90.0, 180.0, 90.0])
        );
    }

    #[test]
    fn bbox_rejects_shape_and_order() {
        assert!(parse_bbox("1,2,3").is_err());
        assert!(parse_bbox("3,2,1,0").is_err());
        assert!(parse_bbox("a,b,c,d").is_err());
    }

    #[test]
    fn lineage_empty_matches_nothing() {
        assert_eq!(lineage_predicate(&[]), "1 = 0");
        assert_eq!(lineage_predicate(&[7, 3]), "source_id IN (7,3)");
    }

    #[test]
    fn sources_sorted_and_deduped() {
        assert_eq!(parse_source_list(Some("3,1,3,x")), vec![1, 3]);
        assert!(parse_source_list(None).is_empty());
    }

    #[test]
    fn fingerprint_stable_regardless_of_order() {
        let a = visibility_fingerprint("v3", "p9", &[3, 1]);
        let b = visibility_fingerprint("v3", "p9", &[1, 3, 1]);
        assert_eq!(a, b);
        assert_ne!(a, visibility_fingerprint("v3", "p10", &[1, 3]));
    }
}

/// Validate an already-decoded bbox (JSON tickets carry numbers, not strings).
pub fn check_bbox(bbox: [f64; 4]) -> Result<[f64; 4], String> {
    if bbox.iter().all(|v| v.is_finite()) && bbox[0] < bbox[2] && bbox[1] < bbox[3] {
        Ok(bbox)
    } else {
        Err("bbox requires finite minx<maxx and miny<maxy".to_string())
    }
}

#[cfg(test)]
mod bbox_tests {
    use super::check_bbox;

    #[test]
    fn check_bbox_ok_and_rejects() {
        assert!(check_bbox([-180.0, -90.0, 180.0, 90.0]).is_ok());
        assert!(check_bbox([0.0, 0.0, 0.0, 1.0]).is_err());
        assert!(check_bbox([f64::NAN, 0.0, 1.0, 1.0]).is_err());
    }
}
