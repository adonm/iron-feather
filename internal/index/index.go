// Package index ports src/index.rs: the publish-time serving index over the
// exact files live at a DuckLake commit. Serving intersects the request bbox
// against partition bboxes in memory and hands DuckDB only candidate files.
package index

import "math"

const Version uint32 = 1

// IndexPathFor mirrors index::index_path_for.
func IndexPathFor(location string) string { return location + ".serving.json" }

// IndexedFile is a DATA-relative path plus its bbox.
type IndexedFile struct {
	Path string     `json:"path"`
	BBox [4]float64 `json:"bbox"`
}

// Partition lists the files overlapping one grid cell, in file order.
type Partition struct {
	BBox  [4]float64 `json:"bbox"`
	Files []int      `json:"files"`
}

// ServingIndex is the sidecar document beside the catalog.
type ServingIndex struct {
	Version        uint32        `json:"version"`
	DuckLakeCommit int64         `json:"ducklake_commit"`
	Cell           [2]float64    `json:"cell"`
	Files          []IndexedFile `json:"files"`
	Partitions     []Partition   `json:"partitions"`
}

func lobes(b [4]float64) [][4]float64 {
	w, s, e, n := b[0], b[1], b[2], b[3]
	if math.IsNaN(s) || math.IsNaN(n) || s > n {
		return nil
	}
	if w <= e {
		return [][4]float64{{w, s, e, n}}
	}
	return [][4]float64{{w, s, 180, n}, {-180, s, e, n}}
}

func overlaps1D(a0, a1, b0, b1 float64) bool { return a0 <= b1 && b0 <= a1 }

// BBoxesOverlap is conservative across the antimeridian.
func BBoxesOverlap(a, b [4]float64) bool {
	for _, l := range lobes(a) {
		for _, m := range lobes(b) {
			if overlaps1D(l[0], l[2], m[0], m[2]) && overlaps1D(l[1], l[3], m[1], m[3]) {
				return true
			}
		}
	}
	return false
}

// PruneFiles returns file indices whose bbox may intersect bounds, in file
// order. Nil bounds select every file.
func PruneFiles(idx *ServingIndex, bounds *[4]float64) []int {
	if bounds == nil {
		out := make([]int, len(idx.Files))
		for i := range idx.Files {
			out[i] = i
		}
		return out
	}
	seen := make([]bool, len(idx.Files))
	var out []int
	for _, part := range idx.Partitions {
		if !BBoxesOverlap(part.BBox, *bounds) {
			continue
		}
		for _, i := range part.Files {
			if !seen[i] && BBoxesOverlap(idx.Files[i].BBox, *bounds) {
				seen[i] = true
				out = append(out, i)
			}
		}
	}
	return out
}

// PartitionFiles assigns files to a uniform grid over extent.
func PartitionFiles(extent [4]float64, bboxes [][4]float64, cellsPerAxis int) ([]Partition, [2]float64) {
	w, s, e, n := extent[0], extent[1], extent[2], extent[3]
	nx := max(cellsPerAxis, 1)
	ny := max(cellsPerAxis, 1)
	cw := max((e-w)/float64(nx), math.SmallestNonzeroFloat64)
	ch := max((n-s)/float64(ny), math.SmallestNonzeroFloat64)
	partitions := make([]Partition, 0, nx*ny)
	for iy := 0; iy < ny; iy++ {
		for ix := 0; ix < nx; ix++ {
			pw := [4]float64{w + float64(ix)*cw, s + float64(iy)*ch, w + float64(ix+1)*cw, s + float64(iy+1)*ch}
			var files []int
			for i, b := range bboxes {
				if BBoxesOverlap(pw, b) {
					files = append(files, i)
				}
			}
			if len(files) > 0 {
				partitions = append(partitions, Partition{BBox: pw, Files: files})
			}
		}
	}
	return partitions, [2]float64{cw, ch}
}
