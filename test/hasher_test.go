package utils_test

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/null-ptr-exception/mongogate/internal/utils"
)

var opts = utils.DefaultHashOptions

// ── Basic DocHash tests ──

func TestDocHash_SameDocSameHash(t *testing.T) {
	doc := bson.M{"name": "alice", "age": int32(30)}
	h1 := utils.DocHash(doc, opts)
	h2 := utils.DocHash(doc, opts)
	if h1 != h2 {
		t.Errorf("the same document should hash the same: %s vs %s", h1, h2)
	}
}

func TestDocHash_ExcludesID(t *testing.T) {
	doc1 := bson.M{"_id": primitive.NewObjectID(), "x": int32(1)}
	doc2 := bson.M{"_id": primitive.NewObjectID(), "x": int32(1)}
	if utils.DocHash(doc1, opts) != utils.DocHash(doc2, opts) {
		t.Error("different _id but same content should hash the same")
	}
}

func TestDocHash_KeyOrderStable(t *testing.T) {
	doc1 := bson.M{"a": int32(1), "b": int32(2), "c": int32(3)}
	doc2 := bson.M{"c": int32(3), "a": int32(1), "b": int32(2)}
	if utils.DocHash(doc1, opts) != utils.DocHash(doc2, opts) {
		t.Error("different key order but same content should hash the same")
	}
}

// ── DateTime normalization ──

func TestDocHash_DatetimeUTC(t *testing.T) {
	utc := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	taipei := time.Date(2024, 1, 1, 8, 0, 0, 0, time.FixedZone("Asia/Taipei", 8*3600))

	doc1 := bson.M{"ts": primitive.NewDateTimeFromTime(utc)}
	doc2 := bson.M{"ts": primitive.NewDateTimeFromTime(taipei)}

	// Same instant, different timezone representation: hash should match.
	if utils.DocHash(doc1, opts) != utils.DocHash(doc2, opts) {
		t.Error("UTC and +08:00 at the same instant should hash the same (datetime normalization)")
	}
}

// ── Float precision ──

func TestDocHash_FloatPrecision(t *testing.T) {
	doc1 := bson.M{"price": float64(0.1 + 0.2)}           // 0.30000000000000004
	doc2 := bson.M{"price": float64(0.3)}
	// After fixing precision to 10 decimal places these should match.
	if utils.DocHash(doc1, opts) != utils.DocHash(doc2, opts) {
		t.Error("after float precision normalization, 0.1+0.2 should equal 0.3")
	}
}

// ── Decimal128 trailing zeros ──

func TestDocHash_Decimal128TrailingZero(t *testing.T) {
	d1, _ := primitive.ParseDecimal128("123.4500")
	d2, _ := primitive.ParseDecimal128("123.45")
	doc1 := bson.M{"amount": d1}
	doc2 := bson.M{"amount": d2}
	if utils.DocHash(doc1, opts) != utils.DocHash(doc2, opts) {
		t.Error("Decimal128 normalization should make these hash the same")
	}
}

// ── Binary subtype ──

func TestDocHash_BinarySubtypeDiff(t *testing.T) {
	bin1 := primitive.Binary{Subtype: 0x00, Data: []byte("hello")}
	bin2 := primitive.Binary{Subtype: 0x05, Data: []byte("hello")}
	doc1 := bson.M{"data": bin1}
	doc2 := bson.M{"data": bin2}

	// check_binary_subtype=true: different subtype should hash differently.
	optsStrict := opts
	optsStrict.CheckBinarySubtype = true
	if utils.DocHash(doc1, optsStrict) == utils.DocHash(doc2, optsStrict) {
		t.Error("different subtype in strict mode should hash differently")
	}

	// check_binary_subtype=false: content-only comparison should match.
	optsLax := opts
	optsLax.CheckBinarySubtype = false
	if utils.DocHash(doc1, optsLax) != utils.DocHash(doc2, optsLax) {
		t.Error("check_binary_subtype=false with identical content should hash the same")
	}
}

// ── ObjectId strict check ──

func TestDeepCompare_ObjectIDDegraded(t *testing.T) {
	oid := primitive.NewObjectID()
	src := bson.M{"ref": oid}
	tgt := bson.M{"ref": oid.Hex()} // CDC turned the ObjectId into a string

	passed, diffs := utils.DeepCompare(src, tgt, opts)
	if passed {
		t.Error("ObjectId degraded to string should be reported as a diff")
	}
	if len(diffs) == 0 || diffs[0].IssueType != "OBJECTID_DEGRADED" {
		t.Errorf("issue type should be OBJECTID_DEGRADED, got: %v", diffs)
	}
}

// ── Array ordering ──

func TestDocHash_ArrayOrder(t *testing.T) {
	doc1 := bson.M{"tags": primitive.A{"a", "b", "c"}}
	doc2 := bson.M{"tags": primitive.A{"c", "a", "b"}}

	// sort_arrays=false: different order should hash differently.
	optsNoSort := opts
	optsNoSort.SortArrays = false
	if utils.DocHash(doc1, optsNoSort) == utils.DocHash(doc2, optsNoSort) {
		t.Error("sort_arrays=false with different order should hash differently")
	}

	// sort_arrays=true: same values regardless of order should hash the same.
	optsSort := opts
	optsSort.SortArrays = true
	if utils.DocHash(doc1, optsSort) != utils.DocHash(doc2, optsSort) {
		t.Error("sort_arrays=true with same values should hash the same")
	}
}

// ── DeepCompare issue classification ──

func TestDeepCompare_MissingField(t *testing.T) {
	src := bson.M{"a": int32(1), "b": int32(2)}
	tgt := bson.M{"a": int32(1)}
	passed, diffs := utils.DeepCompare(src, tgt, opts)
	if passed { t.Error("a missing field should fail") }
	found := false
	for _, d := range diffs {
		if d.IssueType == "MISSING_FIELD" && d.Path == "b" {
			found = true
		}
	}
	if !found { t.Errorf("expected MISSING_FIELD for b, got: %v", diffs) }
}

func TestDeepCompare_TypeMismatch(t *testing.T) {
	src := bson.M{"x": int32(1)}
	tgt := bson.M{"x": int64(1)}
	passed, diffs := utils.DeepCompare(src, tgt, opts)
	if passed { t.Error("a type mismatch should fail") }
	found := false
	for _, d := range diffs {
		if d.IssueType == "TYPE_MISMATCH" { found = true }
	}
	if !found { t.Errorf("expected TYPE_MISMATCH, got: %v", diffs) }
}

func TestDeepCompare_ExtraField(t *testing.T) {
	src := bson.M{"a": int32(1)}
	tgt := bson.M{"a": int32(1), "extra": "surprise"}
	passed, diffs := utils.DeepCompare(src, tgt, opts)
	if passed { t.Error("an extra field in target should fail") }
	found := false
	for _, d := range diffs {
		if d.IssueType == "EXTRA_FIELD" && d.Path == "extra" { found = true }
	}
	if !found { t.Errorf("expected EXTRA_FIELD for extra, got: %v", diffs) }
}

func TestDeepCompare_NestedDoc(t *testing.T) {
	src := bson.M{"addr": bson.M{"city": "Taipei", "zip": "100"}}
	tgt := bson.M{"addr": bson.M{"city": "Taipei", "zip": "200"}}
	passed, diffs := utils.DeepCompare(src, tgt, opts)
	if passed { t.Error("a nested field diff should fail") }
	found := false
	for _, d := range diffs {
		if d.Path == "addr.zip" { found = true }
	}
	if !found { t.Errorf("expected a diff at addr.zip, got: %v", diffs) }
}

func TestDeepCompare_Identical(t *testing.T) {
	doc := bson.M{
		"name":  "test",
		"count": int32(42),
		"tags":  primitive.A{"x", "y"},
	}
	passed, diffs := utils.DeepCompare(doc, doc, opts)
	if !passed { t.Errorf("identical documents should pass, diffs: %v", diffs) }
}
