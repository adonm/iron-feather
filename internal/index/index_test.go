package index

import "testing"

func TestPruneFiles(t *testing.T) {
	idx := &ServingIndex{
		Version: 1,
		Files: []IndexedFile{
			{Path: "a.parquet", BBox: [4]float64{0, 0, 1, 1}},
			{Path: "b.parquet", BBox: [4]float64{5, 5, 6, 6}},
		},
		Partitions: []Partition{
			{BBox: [4]float64{0, 0, 1, 1}, Files: []int{0}},
			{BBox: [4]float64{5, 5, 6, 6}, Files: []int{1}},
		},
	}
	q := [4]float64{0, 0, 1, 1}
	got := PruneFiles(idx, &q)
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("prune wrong: %v", got)
	}
	if got := PruneFiles(idx, nil); len(got) != 2 {
		t.Fatalf("unbounded must select all: %v", got)
	}
	far := [4]float64{50, 50, 51, 51}
	if got := PruneFiles(idx, &far); len(got) != 0 {
		t.Fatalf("far bbox must prune all: %v", got)
	}
}

func TestAntimeridianOverlap(t *testing.T) {
	a := [4]float64{170, 10, -170, 20}
	b := [4]float64{-175, 12, -165, 18}
	if !BBoxesOverlap(a, b) {
		t.Fatal("antimeridian overlap missed")
	}
	c := [4]float64{0, 0, 1, 1}
	if BBoxesOverlap(a, c) {
		t.Fatal("false overlap")
	}
}
