package filter

import "testing"

func TestCollectionID(t *testing.T) {
	if !CollectionID("buildings") || !CollectionID("a-b_c9") {
		t.Fatal("valid ids rejected")
	}
	for _, bad := range []string{"", "has space", "semi;colon", "quote'"} {
		if CollectionID(bad) {
			t.Fatalf("invalid id accepted: %q", bad)
		}
	}
	long := make([]byte, 129)
	for i := range long {
		long[i] = 'a'
	}
	if CollectionID(string(long)) {
		t.Fatal("129-char id accepted")
	}
}

func TestBBox(t *testing.T) {
	if _, err := ParseBBox("13.35,52.48,13.45,52.55"); err != nil {
		t.Fatal(err)
	}
	// Antimeridian crossing is valid.
	if _, err := ParseBBox("170,10,-170,20"); err != nil {
		t.Fatal(err)
	}
	// 3D bounds over 2D data.
	if _, err := BBox([]float64{13, 52, 0, 14, 53, 10}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"1,2,3", "a,b,c,d", "0,50,10,40", "0,200,10,20", "NaN,0,1,1"} {
		if _, err := ParseBBox(bad); err == nil {
			t.Fatalf("bad bbox accepted: %q", bad)
		}
	}
}

func TestSourcesIntersection(t *testing.T) {
	got := Sources([]int64{1, 2, 3}, []int64{2, 3, 4}, true, true)
	if len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("intersection wrong: %v", got)
	}
	if len(Sources(nil, nil, false, false)) != 0 {
		t.Fatal("missing both must be empty")
	}
}

func TestPredicateEmptySources(t *testing.T) {
	if got := Predicate("buildings", nil, nil); got != "layer = 'buildings' AND FALSE" {
		t.Fatalf("empty sources must be FALSE: %q", got)
	}
}

func TestDatetimeValidation(t *testing.T) {
	for _, ok := range []string{"2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z/2024-02-01T00:00:00Z", "../2024-02-01T00:00:00Z"} {
		if err := ValidateDatetime(ok); err != nil {
			t.Fatalf("valid datetime rejected %q: %v", ok, err)
		}
	}
	for _, bad := range []string{"not-a-date", "2024-02-01T00:00:00Z/2024-01-01T00:00:00Z", "../.."} {
		if err := ValidateDatetime(bad); err == nil {
			t.Fatalf("bad datetime accepted: %q", bad)
		}
	}
}
