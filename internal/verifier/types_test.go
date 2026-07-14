package verifier

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestCollectionTask_NS(t *testing.T) {
	task := CollectionTask{DBName: "mydb", ColName: "users"}
	if got := task.NS(); got != "mydb.users" {
		t.Errorf("NS() = %q, want %q", got, "mydb.users")
	}
}

func TestCollectionTask_IsSplit(t *testing.T) {
	cases := []struct {
		name       string
		rangeTotal int
		want       bool
	}{
		{"zero value (not split)", 0, false},
		{"explicit single range", 1, false},
		{"two ranges", 2, true},
		{"many ranges", 8, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			task := CollectionTask{DBName: "db", ColName: "col", RangeTotal: c.rangeTotal}
			if got := task.IsSplit(); got != c.want {
				t.Errorf("IsSplit() with RangeTotal=%d = %v, want %v", c.rangeTotal, got, c.want)
			}
		})
	}
}

func TestCollectionTask_CheckpointKey_Unsplit(t *testing.T) {
	task := CollectionTask{DBName: "mydb", ColName: "users"}
	if got := task.CheckpointKey(); got != "mydb.users" {
		t.Errorf("CheckpointKey() for an unsplit task = %q, want bare ns %q (backward compat with existing checkpoints.json)", got, "mydb.users")
	}
}

func TestCollectionTask_CheckpointKey_Split(t *testing.T) {
	task := CollectionTask{DBName: "mydb", ColName: "users", RangeIndex: 2, RangeTotal: 8}
	got := task.CheckpointKey()
	want := "mydb.users#r2/8"
	if got != want {
		t.Errorf("CheckpointKey() for a split task = %q, want %q", got, want)
	}
}

func TestCollectionTask_CheckpointKey_DistinctPerRange(t *testing.T) {
	// Two ranges of the same namespace must never collide on the same
	// checkpoint key - a collision would make --resume overwrite one
	// range's saved position with another's (see docs/TESTING.md's
	// checkpoint findings and internal/utils/checkpoint.go).
	seen := make(map[string]bool)
	total := 8
	for i := 0; i < total; i++ {
		task := CollectionTask{DBName: "mydb", ColName: "orders", RangeIndex: i, RangeTotal: total}
		key := task.CheckpointKey()
		if seen[key] {
			t.Fatalf("range %d produced a checkpoint key already used by another range: %q", i, key)
		}
		seen[key] = true
	}
}

func TestIDRangeFilter_Unsplit(t *testing.T) {
	task := CollectionTask{DBName: "db", ColName: "col"}
	filter := idRangeFilter(task)
	if len(filter) != 0 {
		t.Errorf("idRangeFilter() for an unsplit task = %v, want empty filter", filter)
	}
}

func TestIDRangeFilter_Bounded(t *testing.T) {
	task := CollectionTask{DBName: "db", ColName: "col", RangeTotal: 2, RangeMin: 10, RangeMax: 20}
	filter := idRangeFilter(task)
	cond, ok := filter["_id"].(bson.M)
	if !ok {
		t.Fatalf("idRangeFilter() = %v, want an _id condition", filter)
	}
	if cond["$gte"] != 10 || cond["$lt"] != 20 {
		t.Errorf("idRangeFilter() _id condition = %v, want $gte=10 $lt=20", cond)
	}
}

func TestIDRangeFilter_UnboundedSide(t *testing.T) {
	// First range: no lower bound. Last range: no upper bound. Neither
	// $gte nor $lt should appear for the side that's nil.
	first := idRangeFilter(CollectionTask{DBName: "db", ColName: "col", RangeTotal: 2, RangeMax: 20})
	cond := first["_id"].(bson.M)
	if _, has := cond["$gte"]; has {
		t.Errorf("first range should have no $gte, got %v", cond)
	}
	if cond["$lt"] != 20 {
		t.Errorf("first range $lt = %v, want 20", cond["$lt"])
	}

	last := idRangeFilter(CollectionTask{DBName: "db", ColName: "col", RangeTotal: 2, RangeMin: 20})
	cond = last["_id"].(bson.M)
	if _, has := cond["$lt"]; has {
		t.Errorf("last range should have no $lt, got %v", cond)
	}
	if cond["$gte"] != 20 {
		t.Errorf("last range $gte = %v, want 20", cond["$gte"])
	}
}
