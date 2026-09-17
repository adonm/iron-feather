//! Publish-time serving index: per-collection spatial partitions over the
//! exact files live at a DuckLake commit.
//!
//! Disposable OGC workers pin one commit but must not interpret the whole
//! catalog per query, and must not open every data file on a large lake.
//! The writer therefore compiles each commit into this index (one JSON
//! sidecar next to the catalog). Serving intersects the request bbox
//! against partition bboxes in memory and hands DuckDB only the candidate
//! files. Unbounded requests read all files; bboxes outside the extent
//! read none (a footer-only probe). Anything doubtful fails closed to the
//! existing catalog-table / full-file-list behavior.
//!
//! Partition bboxes are conservative supersets of member file bboxes, and
//! every file overlapping a cell is listed in it, so pruning can only drop
//! files whose bbox provably misses the query. Row ids are intentionally
//! absent: ids are uncorrelated with the geographic sort order, so id
//! ranges would not prune. Single-item reads use the full file list (same
//! as today); DuckDB footer statistics still prune inside it.

use std::collections::HashMap;

use crate::{db::NeoConnection, filter};

pub const INDEX_VERSION: u32 = 1;

/// Sidecar location for a catalog: `<catalog>.serving.json`, whether the
/// catalog is a local path or an http(s) URL.
pub fn index_path_for(location: &str) -> String {
    format!("{location}.serving.json")
}

/// One data file: DATA-base-relative path (schema/table prefix and
/// filename, exactly as resolved for `read_parquet`), plus its bbox.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct IndexedFile {
    pub path: String,
    pub bbox: [f64; 4],
}

/// A coarse grid cell: files listed here are exactly those whose bbox
/// overlaps the cell. Files spanning cells repeat; callers dedupe.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Partition {
    pub bbox: [f64; 4],
    /// Indices into [`ServingIndex::files`], in file order.
    pub files: Vec<usize>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ServingIndex {
    pub version: u32,
    /// DuckLake `max(snapshot_id)` the index was compiled from. Serving
    /// pins this commit; any other pinned snapshot ignores the index.
    pub ducklake_commit: i64,
    /// Uniform grid over the data extent: `[cell_width_deg, cell_height_deg]`.
    pub cell: [f64; 2],
    pub files: Vec<IndexedFile>,
    pub partitions: Vec<Partition>,
}

/// Split a CRS84 bbox into non-wrapping lobes. Normal boxes yield one
/// lobe; antimeridian boxes yield two. Empty (inverted-latitude) boxes
/// yield none and therefore match nothing.
fn lobes(b: [f64; 4]) -> Vec<[f64; 4]> {
    let [w, s, e, n] = b;
    // NaN bounds match nothing; filter validation already rejects them at
    // the API boundary, but the index must stay total on garbage input.
    if !matches!(
        s.partial_cmp(&n),
        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
    ) {
        return Vec::new();
    }
    if w <= e {
        vec![[w, s, e, n]]
    } else {
        vec![[w, s, 180.0, n], [-180.0, s, e, n]]
    }
}

/// Closed-interval overlap on one axis.
fn overlaps_1d(a0: f64, a1: f64, b0: f64, b1: f64) -> bool {
    a0 <= b1 && b0 <= a1
}

/// Conservative bbox intersection across the antimeridian: true unless the
/// boxes provably miss on every lobe pair.
pub fn bboxes_overlap(a: [f64; 4], b: [f64; 4]) -> bool {
    lobes(a).iter().any(|l| {
        lobes(b)
            .iter()
            .any(|m| overlaps_1d(l[0], l[2], m[0], m[2]) && overlaps_1d(l[1], l[3], m[1], m[3]))
    })
}

/// File indices whose bbox may intersect `bounds`, in file order.
/// `None` bounds (unbounded requests) select every file.
pub fn prune_files(index: &ServingIndex, bounds: Option<[f64; 4]>) -> Vec<usize> {
    let Some(q) = bounds else {
        return (0..index.files.len()).collect();
    };
    let mut seen = vec![false; index.files.len()];
    let mut out = Vec::new();
    for part in &index.partitions {
        if !bboxes_overlap(part.bbox, q) {
            continue;
        }
        for &i in &part.files {
            if !seen[i] && bboxes_overlap(index.files[i].bbox, q) {
                seen[i] = true;
                out.push(i);
            }
        }
    }
    out
}

/// Assign files to a uniform grid over `extent`: each file joins every
/// cell its bbox overlaps. One axis gets `cells_per_axis` cells; degenerate
/// extents collapse to a single cell.
pub fn partition_files(
    extent: [f64; 4],
    bboxes: &[[f64; 4]],
    cells_per_axis: usize,
) -> (Vec<Partition>, [f64; 2]) {
    let [w, s, e, n] = extent;
    let nx = cells_per_axis.max(1);
    let ny = cells_per_axis.max(1);
    let cw = ((e - w) / nx as f64).max(f64::MIN_POSITIVE);
    let ch = ((n - s) / ny as f64).max(f64::MIN_POSITIVE);
    let mut grid: HashMap<(usize, usize), Vec<usize>> = HashMap::new();
    for (i, b) in bboxes.iter().enumerate() {
        let x0 = (((b[0] - w) / cw).floor() as usize).min(nx - 1);
        let x1 = (((b[2] - w) / cw).floor() as usize).min(nx - 1);
        let y0 = (((b[1] - s) / ch).floor() as usize).min(ny - 1);
        let y1 = (((b[3] - s) / ch).floor() as usize).min(ny - 1);
        for x in x0.min(x1)..=x0.max(x1) {
            for y in y0.min(y1)..=y0.max(y1) {
                grid.entry((x, y)).or_default().push(i);
            }
        }
    }
    let mut cells: Vec<((usize, usize), Vec<usize>)> = grid.into_iter().collect();
    cells.sort_by_key(|(k, _)| *k);
    let partitions = cells
        .into_iter()
        .map(|((x, y), mut files)| {
            files.sort_unstable();
            files.dedup();
            Partition {
                bbox: [
                    w + x as f64 * cw,
                    s + y as f64 * ch,
                    w + (x + 1) as f64 * cw,
                    s + (y + 1) as f64 * ch,
                ],
                files,
            }
        })
        .collect();
    (partitions, [cw, ch])
}

/// Footer-only bbox per file, in catalog file order. Geometry has no
/// Parquet statistics; the builder's explicit bbox columns do.
fn file_bboxes(conn: &NeoConnection, urls: &[String]) -> Result<Vec<[f64; 4]>, String> {
    let mut out = Vec::with_capacity(urls.len());
    for url in urls {
        let rows = crate::db::text_table(
            conn,
            &format!(
                "SELECT min(xmin)::VARCHAR, max(xmax)::VARCHAR, min(ymin)::VARCHAR, max(ymax)::VARCHAR \
                 FROM read_parquet({})",
                filter::quote(url),
            ),
        )
        .map_err(|e| format!("file stats for {url}: {e}"))?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| format!("no stats for {url}"))?;
        let mut vals = [f64::NAN; 4];
        for (i, cell) in row.into_iter().enumerate().take(4) {
            vals[i] = cell
                .as_deref()
                .unwrap_or("NaN")
                .parse::<f64>()
                .map_err(|_| format!("unparseable stats for {url}"))?;
        }
        if vals.iter().any(|v| !v.is_finite()) {
            return Err(format!("non-finite bbox stats for {url}"));
        }
        out.push([vals[0], vals[2], vals[1], vals[3]]);
    }
    Ok(out)
}

/// Compile a serving index over already-resolved absolute file URLs.
/// `relpaths` are the matching DATA-base-relative paths in the same order.
/// The grid extent is the union of file bboxes, so no build-bbox input is
/// needed and repacks of existing catalogs work identically.
pub fn build_index(
    conn: &NeoConnection,
    snapshot: i64,
    absolute_urls: &[String],
    relpaths: &[String],
) -> Result<ServingIndex, String> {
    if absolute_urls.len() != relpaths.len() {
        return Err("file URL list and relative path list disagree".into());
    }
    // Absolute paths bake one machine's disk layout into the document and
    // would misresolve everywhere else: index only portable relative layouts.
    if relpaths
        .iter()
        .any(|p| p.contains("://") || p.starts_with('/'))
    {
        return Err("absolute data file paths cannot be indexed".into());
    }
    let stats = file_bboxes(conn, absolute_urls)?;
    let files: Vec<IndexedFile> = relpaths
        .iter()
        .zip(stats.iter())
        .map(|(path, bbox)| IndexedFile {
            path: path.clone(),
            bbox: *bbox,
        })
        .collect();
    let bboxes: Vec<[f64; 4]> = files.iter().map(|f| f.bbox).collect();
    let extent = bboxes.iter().fold(None, |acc: Option<[f64; 4]>, b| {
        Some(match acc {
            None => *b,
            Some([w, s, e, n]) => [w.min(b[0]), s.min(b[1]), e.max(b[2]), n.max(b[3])],
        })
    });
    let Some(extent) = extent else {
        return Err("no files to index".into());
    };
    // ~sqrt(files) cells per axis keeps cells near one file on average
    // while bounding duplication from spanning files.
    let per_axis = (absolute_urls.len() as f64).sqrt().ceil() as usize;
    let (partitions, cell) = partition_files(extent, &bboxes, per_axis);
    Ok(ServingIndex {
        version: INDEX_VERSION,
        ducklake_commit: snapshot,
        cell,
        files,
        partitions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlap_handles_antimeridian_conservatively() {
        assert!(bboxes_overlap(
            [0.0, 0.0, 10.0, 10.0],
            [5.0, 5.0, 15.0, 15.0]
        ));
        assert!(!bboxes_overlap(
            [0.0, 0.0, 10.0, 10.0],
            [11.0, 0.0, 20.0, 10.0]
        ));
        // Touching edges overlap (closed intervals, matches SQL >= / <=).
        assert!(bboxes_overlap(
            [0.0, 0.0, 10.0, 10.0],
            [10.0, 10.0, 20.0, 20.0]
        ));
        // Antimeridian query spans both lobes.
        assert!(bboxes_overlap(
            [170.0, 0.0, -170.0, 10.0],
            [175.0, 0.0, 179.0, 10.0]
        ));
        assert!(bboxes_overlap(
            [170.0, 0.0, -170.0, 10.0],
            [-179.0, 0.0, -175.0, 10.0]
        ));
        assert!(!bboxes_overlap(
            [170.0, 0.0, -170.0, 10.0],
            [0.0, 0.0, 10.0, 10.0]
        ));
        // Inverted latitude matches nothing.
        assert!(!bboxes_overlap(
            [0.0, 10.0, 10.0, 0.0],
            [0.0, 0.0, 10.0, 10.0]
        ));
    }

    fn toy_index() -> ServingIndex {
        // Three files: west, middle (spans the middle cell boundary), east.
        let files = vec![
            IndexedFile {
                path: "a.parquet".into(),
                bbox: [0.0, 0.0, 3.0, 10.0],
            },
            IndexedFile {
                path: "b.parquet".into(),
                bbox: [2.0, 0.0, 8.0, 10.0],
            },
            IndexedFile {
                path: "c.parquet".into(),
                bbox: [7.0, 0.0, 10.0, 10.0],
            },
        ];
        let bboxes: Vec<[f64; 4]> = files.iter().map(|f| f.bbox).collect();
        let (partitions, cell) = partition_files([0.0, 0.0, 10.0, 10.0], &bboxes, 2);
        ServingIndex {
            version: INDEX_VERSION,
            ducklake_commit: 6,
            cell,
            files,
            partitions,
        }
    }

    #[test]
    fn prune_selects_only_overlapping_files() {
        let idx = toy_index();
        // West-only query: spanning file b is correctly excluded by the
        // file-level recheck (its bbox starts at x=2).
        assert_eq!(prune_files(&idx, Some([0.0, 0.0, 1.0, 10.0])), vec![0]);
        assert_eq!(prune_files(&idx, Some([9.0, 0.0, 10.0, 10.0])), vec![2]);
        // A query spanning the boundary needs both sides.
        assert_eq!(
            prune_files(&idx, Some([2.5, 0.0, 7.5, 10.0])),
            vec![0, 1, 2]
        );
        // Outside everything: empty.
        assert!(prune_files(&idx, Some([20.0, 20.0, 30.0, 30.0])).is_empty());
        // Unbounded: everything, in order.
        assert_eq!(prune_files(&idx, None), vec![0, 1, 2]);
    }

    #[test]
    fn index_json_round_trips() {
        let idx = toy_index();
        let text = serde_json::to_string(&idx).unwrap();
        assert_eq!(serde_json::from_str::<ServingIndex>(&text).unwrap(), idx);
    }
}
