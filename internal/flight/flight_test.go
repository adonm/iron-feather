package flight

import "testing"

func TestTicketValidation(t *testing.T) {
	tk := Ticket{Collection: "buildings"}
	cols, limit, offset, err := tk.Validate()
	if err != nil || limit != 10000 || offset != 0 || len(cols) != 4 {
		t.Fatalf("defaults wrong: %v %d %d %v", cols, limit, offset, err)
	}
	bad := Ticket{Columns: []string{"nope"}}
	if _, _, _, err := bad.Validate(); err == nil {
		t.Fatal("unknown column accepted")
	}
	dup := Ticket{Columns: []string{"id", "id"}}
	if _, _, _, err := dup.Validate(); err == nil {
		t.Fatal("duplicate column accepted")
	}
	over := 100001
	if _, _, _, err := (Ticket{Limit: &over}).Validate(); err == nil {
		t.Fatal("over-limit accepted")
	}
}
