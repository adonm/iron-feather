package plan

import "testing"

func TestIsHeavy(t *testing.T) {
	if !IsHeavy(101, Pagination{}, nil) {
		t.Fatal("limit>100 must be heavy")
	}
	if !IsHeavy(10, Pagination{Offset: 1000}, nil) {
		t.Fatal("offset>=1000 must be heavy")
	}
	b := &[4]float64{0, 0, 4, 2} // 8deg²
	if !IsHeavy(10, Pagination{}, b) {
		t.Fatal("broad slice must be heavy")
	}
	if IsHeavy(10, Pagination{}, nil) {
		t.Fatal("small interactive page must not be heavy")
	}
}

func TestPagePartsCursor(t *testing.T) {
	c := "way:5"
	where, tail := PageParts("layer = 'b' AND TRUE", 10, Pagination{Cursor: &c})
	if where != "layer = 'b' AND TRUE AND id > 'way:5'" {
		t.Fatalf("cursor where wrong: %q", where)
	}
	if tail != "ORDER BY id LIMIT 11" {
		t.Fatalf("cursor tail wrong: %q", tail)
	}
}
