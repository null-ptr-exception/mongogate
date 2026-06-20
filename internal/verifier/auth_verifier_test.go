package verifier

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestNormalizePrivileges_SameContentSameString(t *testing.T) {
	privs := bson.A{
		bson.M{"resource": bson.M{"db": "mydb", "collection": ""}, "actions": bson.A{"find", "insert"}},
	}
	a := normalizePrivileges(privs)
	b := normalizePrivileges(privs)
	if a != b {
		t.Errorf("identical privilege lists should normalize the same: %q vs %q", a, b)
	}
}

func TestNormalizePrivileges_ActionOrderDoesNotMatter(t *testing.T) {
	a := normalizePrivileges(bson.A{
		bson.M{"resource": bson.M{"db": "mydb", "collection": ""}, "actions": bson.A{"find", "insert"}},
	})
	b := normalizePrivileges(bson.A{
		bson.M{"resource": bson.M{"db": "mydb", "collection": ""}, "actions": bson.A{"insert", "find"}},
	})
	if a != b {
		t.Errorf("action order within a privilege should not matter: %q vs %q", a, b)
	}
}

func TestNormalizePrivileges_DifferentActionsDifferentString(t *testing.T) {
	a := normalizePrivileges(bson.A{
		bson.M{"resource": bson.M{"db": "mydb", "collection": ""}, "actions": bson.A{"find"}},
	})
	b := normalizePrivileges(bson.A{
		bson.M{"resource": bson.M{"db": "mydb", "collection": ""}, "actions": bson.A{"find", "remove"}},
	})
	if a == b {
		t.Error("a role granted extra actions on target should not normalize the same as source")
	}
}

func TestNormalizePrivileges_DifferentResourceDifferentString(t *testing.T) {
	a := normalizePrivileges(bson.A{
		bson.M{"resource": bson.M{"db": "mydb", "collection": ""}, "actions": bson.A{"find"}},
	})
	b := normalizePrivileges(bson.A{
		bson.M{"resource": bson.M{"db": "otherdb", "collection": ""}, "actions": bson.A{"find"}},
	})
	if a == b {
		t.Error("a role scoped to a different resource should not normalize the same")
	}
}
