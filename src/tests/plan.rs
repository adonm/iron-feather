//! Query planning classifications and predicate SQL shapes.
use crate::filter;
#[test]
fn bbox_overlap_and_contained_handle_antimeridian() {
    // Normal window: conjunctions.
    assert_eq!(
        filter::bbox_overlap([0.0, 0.0, 10.0, 10.0]),
        "xmax >= 0 AND xmin <= 10 AND ymax >= 0 AND ymin <= 10"
    );
    assert_eq!(
        filter::bbox_contained([0.0, 0.0, 10.0, 10.0]),
        "xmin >= 0 AND xmax <= 10 AND ymin >= 0 AND ymax <= 10"
    );
    // Crossing window: unions over longitude.
    assert_eq!(
        filter::bbox_overlap([170.0, 0.0, -170.0, 5.0]),
        "(xmax >= 170 OR xmin <= -170) AND ymax >= 0 AND ymin <= 5"
    );
    assert_eq!(
        filter::bbox_contained([170.0, 0.0, -170.0, 5.0]),
        "(xmin >= 170 OR xmax <= -170) AND ymin >= 0 AND ymax <= 5"
    );
    // Legacy alias stays in sync with overlap.
    assert_eq!(
        filter::bbox_range([0.0, 0.0, 10.0, 10.0]),
        filter::bbox_overlap([0.0, 0.0, 10.0, 10.0])
    );
}

#[test]
fn predicate_prunes_then_accepts_interior_or_intersects() {
    let sql = crate::store::Store::predicate("buildings", Some([0.0, 0.0, 10.0, 10.0]), &[1]);
    assert!(sql.contains("xmax >= 0 AND xmin <= 10"));
    assert!(sql.contains("xmin >= 0 AND xmax <= 10"));
    assert!(sql.contains("ST_Intersects"));
    // No spatial arm without bounds.
    let bare = crate::store::Store::predicate("buildings", None, &[1]);
    assert!(!bare.contains("ST_Intersects"));
    assert!(bare.contains("source_id IN (1)"));
}

#[test]
fn plan_heavy_pages_share_the_bulk_lane() {
    use crate::plan::{is_heavy, Pagination};
    // Pagination and page size drive heaviness.
    assert!(!is_heavy(
        10,
        &Pagination::Offset(0),
        Some([9.0, 9.0, 11.0, 11.0])
    ));
    assert!(is_heavy(
        1000,
        &Pagination::Offset(0),
        Some([9.0, 9.0, 11.0, 11.0])
    ));
    assert!(is_heavy(10, &Pagination::Offset(5000), None));
    assert!(is_heavy(
        10,
        &Pagination::Offset(0),
        Some([2.0, 48.0, 6.0, 54.0])
    ));
    assert!(!is_heavy(
        10,
        &Pagination::Cursor("way:1".into()),
        Some([9.0, 9.0, 11.0, 11.0])
    ));
}
