package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// HashOptions controls hash normalization behavior, tuned to the quirks of
// whichever migration tool produced the target data.
type HashOptions struct {
	NormalizeDatetime       bool // normalize to UTC (strongly recommended; CDC through Kafka often shifts timezones)
	NormalizeFloatPrecision int  // fixed decimal precision for floats (0 = disabled)
	NormalizeDecimal128     bool // normalize Decimal128 by stripping trailing zeros
	SortArrays              bool // sort arrays (only enable when business logic doesn't depend on order)
	CheckBinarySubtype      bool // also compare BinData subtype (CDC sometimes drops it)
	StrictObjectID          bool // ObjectId must not degrade to string (common when CDC goes through JSON)
}

// DefaultHashOptions is a safe set of defaults.
var DefaultHashOptions = HashOptions{
	NormalizeDatetime:       true,
	NormalizeFloatPrecision: 10,
	NormalizeDecimal128:     true,
	SortArrays:              false, // preserve order by default; business logic may depend on it
	CheckBinarySubtype:      true,
	StrictObjectID:          true,
}

// DocHash computes a document's SHA256 hash, excluding _id.
func DocHash(doc bson.M, opts HashOptions) string {
	d := make(map[string]interface{})
	for k, v := range doc {
		if k == "_id" {
			continue
		}
		d[k] = normalizeValue(v, opts)
	}
	data, _ := json.Marshal(stableSortedMap(d))
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// ──────────────────────────────────────────
// Normalization core
// ──────────────────────────────────────────

func normalizeValue(v interface{}, opts HashOptions) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.(type) {

	// ── ObjectId ──
	// CDC through JSON can turn this into a string; StrictObjectID=true makes
	// DeepCompare flag it. For hashing we always render as a hex string so
	// both sides are comparable.
	case primitive.ObjectID:
		return "oid:" + val.Hex()

	// ── string (may be an ObjectId that CDC serialized as a string) ──
	case string:
		return val

	// ── datetime: normalize to UTC with a fixed format ──
	case primitive.DateTime:
		if opts.NormalizeDatetime {
			return "dt:" + val.Time().UTC().Format(time.RFC3339Nano)
		}
		return val.Time().String()

	case time.Time:
		if opts.NormalizeDatetime {
			return "dt:" + val.UTC().Format(time.RFC3339Nano)
		}
		return val.String()

	// ── Decimal128: normalize by stripping trailing zeros ──
	case primitive.Decimal128:
		if opts.NormalizeDecimal128 {
			return "dec:" + normalizeDecimal128(val)
		}
		return "dec:" + val.String()

	// ── Binary: tag with subtype ──
	case primitive.Binary:
		data := hex.EncodeToString(val.Data)
		if opts.CheckBinarySubtype {
			return fmt.Sprintf("bin:subtype=%d,data=%s", val.Subtype, data)
		}
		return "bin:" + data

	// ── float64: fixed precision ──
	case float64:
		if val == 0 {
			val = 0 // collapse -0.0 to 0.0; they're equal but format differently
		}
		if opts.NormalizeFloatPrecision > 0 {
			return fmt.Sprintf("f:%.*f", opts.NormalizeFloatPrecision, val)
		}
		return val

	case float32:
		f := float64(val)
		if f == 0 {
			f = 0 // collapse -0.0 to 0.0; they're equal but format differently
		}
		if opts.NormalizeFloatPrecision > 0 {
			return fmt.Sprintf("f:%.*f", opts.NormalizeFloatPrecision, f)
		}
		return val

	// ── int types (BSON has both int32 and int64) ──
	case int32:
		return fmt.Sprintf("i32:%d", val)
	case int64:
		return fmt.Sprintf("i64:%d", val)

	// ── Boolean ──
	case bool:
		return val

	// ── Nested document ──
	case bson.M:
		out := make(map[string]interface{})
		for k, vv := range val {
			out[k] = normalizeValue(vv, opts)
		}
		return stableSortedMap(out)

	// ── Array ──
	case primitive.A:
		return normalizeArray(val, opts)
	case []interface{}:
		return normalizeArray(val, opts)

	// ── Null / Undefined ──
	case primitive.Null:
		return nil
	case primitive.Undefined:
		return "undefined"

	// ── Timestamp (used by the oplog) ──
	case primitive.Timestamp:
		return fmt.Sprintf("ts:%d,%d", val.T, val.I)

	// ── Regex ──
	case primitive.Regex:
		return fmt.Sprintf("regex:/%s/%s", val.Pattern, val.Options)

	// ── MinKey / MaxKey ──
	case primitive.MinKey:
		return "minkey"
	case primitive.MaxKey:
		return "maxkey"

	default:
		return fmt.Sprintf("%v", val)
	}
}

func normalizeArray(arr interface{}, opts HashOptions) []interface{} {
	var items []interface{}
	switch a := arr.(type) {
	case primitive.A:
		for _, v := range a {
			items = append(items, normalizeValue(v, opts))
		}
	case []interface{}:
		for _, v := range a {
			items = append(items, normalizeValue(v, opts))
		}
	}
	if opts.SortArrays && len(items) > 0 {
		// Only sort arrays of plain values (string/number). Arrays of nested
		// objects are left alone to avoid scrambling structured data.
		if canSort(items) {
			sort.Slice(items, func(i, j int) bool {
				return fmt.Sprintf("%v", items[i]) < fmt.Sprintf("%v", items[j])
			})
		}
	}
	return items
}

func canSort(items []interface{}) bool {
	for _, item := range items {
		switch item.(type) {
		case map[string]interface{}, bson.M:
			return false // don't sort arrays containing nested objects
		}
	}
	return true
}

// normalizeDecimal128 strips trailing zeros so "123.4500" and "123.45" hash the same.
func normalizeDecimal128(val primitive.Decimal128) string {
	s := val.String()
	// Re-parse and reformat via big.Float
	f, _, err := big.ParseFloat(s, 10, 256, big.ToNearestEven)
	if err != nil {
		return s // fall back to the raw string if parsing fails
	}
	// Format and trim trailing zeros
	result := f.Text('f', 20)
	result = strings.TrimRight(result, "0")
	result = strings.TrimRight(result, ".")
	return result
}

// stableSortedMap sorts keys so json.Marshal produces a stable byte sequence.
func stableSortedMap(m map[string]interface{}) map[string]interface{} {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make(map[string]interface{}, len(m))
	for _, k := range keys {
		out[k] = m[k]
	}
	return out
}

// ──────────────────────────────────────────
// Deep compare: find the real cause when hashes differ
// ──────────────────────────────────────────

// DiffDetail records one field-level difference.
type DiffDetail struct {
	Path      string // field path, e.g. "address.city"
	SrcType   string // source BSON type
	TgtType   string // target BSON type
	SrcValue  string // source value (truncated)
	TgtValue  string // target value (truncated)
	IssueType string // TYPE_MISMATCH / VALUE_DIFF / MISSING_FIELD / EXTRA_FIELD / OBJECTID_DEGRADED
}

// DeepCompare runs a two-stage comparison: a fast hash check first, and only
// when hashes differ does it fall back to a full diff to explain why.
// Returns (passed, []DiffDetail).
func DeepCompare(src, tgt bson.M, opts HashOptions) (bool, []DiffDetail) {
	// Fast path
	if DocHash(src, opts) == DocHash(tgt, opts) {
		return true, nil
	}
	// Slow path: find the specific differences
	diffs := deepDiff(src, tgt, "", opts)
	return false, diffs
}

func deepDiff(src, tgt bson.M, path string, opts HashOptions) []DiffDetail {
	var diffs []DiffDetail

	for k, sv := range src {
		if k == "_id" {
			continue
		}
		fullPath := k
		if path != "" {
			fullPath = path + "." + k
		}
		tv, exists := tgt[k]
		if !exists {
			diffs = append(diffs, DiffDetail{
				Path:      fullPath,
				SrcType:   typeName(sv),
				IssueType: "MISSING_FIELD",
				SrcValue:  truncate(fmt.Sprintf("%v", sv)),
			})
			continue
		}

		// ── Special case: ObjectId degraded to string ──
		if opts.StrictObjectID {
			_, srcIsOID := sv.(primitive.ObjectID)
			_, tgtIsStr := tv.(string)
			if srcIsOID && tgtIsStr {
				diffs = append(diffs, DiffDetail{
					Path:      fullPath,
					SrcType:   "ObjectID",
					TgtType:   "string",
					SrcValue:  fmt.Sprintf("%v", sv),
					TgtValue:  fmt.Sprintf("%v", tv),
					IssueType: "OBJECTID_DEGRADED", // common with CDC through JSON
				})
				continue
			}
		}

		// ── Type mismatch ──
		srcType := typeName(sv)
		tgtType := typeName(tv)
		if srcType != tgtType {
			diffs = append(diffs, DiffDetail{
				Path:      fullPath,
				SrcType:   srcType,
				TgtType:   tgtType,
				SrcValue:  truncate(fmt.Sprintf("%v", sv)),
				TgtValue:  truncate(fmt.Sprintf("%v", tv)),
				IssueType: "TYPE_MISMATCH",
			})
			continue
		}

		// ── Recurse into nested documents ──
		if svMap, ok := sv.(bson.M); ok {
			if tvMap, ok2 := tv.(bson.M); ok2 {
				diffs = append(diffs, deepDiff(svMap, tvMap, fullPath, opts)...)
				continue
			}
		}

		// ── Recurse into arrays of documents, index by index ──
		// Plain-value arrays (tags, scores, embeddings, ...) are left to the
		// normalized whole-array comparison below; an array containing at
		// least one document gets per-element paths like "items[2].name"
		// instead of one opaque "the whole array differs".
		if svArr, tvArr, ok := bothArraysOfDocs(sv, tv); ok {
			if len(svArr) != len(tvArr) {
				diffs = append(diffs, DiffDetail{
					Path:      fullPath,
					SrcType:   "array",
					TgtType:   "array",
					SrcValue:  fmt.Sprintf("len=%d", len(svArr)),
					TgtValue:  fmt.Sprintf("len=%d", len(tvArr)),
					IssueType: "ARRAY_LENGTH_MISMATCH",
				})
				continue
			}
			for i := range svArr {
				elemPath := fmt.Sprintf("%s[%d]", fullPath, i)
				svElem, svIsDoc := svArr[i].(bson.M)
				tvElem, tvIsDoc := tvArr[i].(bson.M)
				if svIsDoc && tvIsDoc {
					diffs = append(diffs, deepDiff(svElem, tvElem, elemPath, opts)...)
					continue
				}
				elemSrcNorm := fmt.Sprintf("%v", normalizeValue(svArr[i], opts))
				elemTgtNorm := fmt.Sprintf("%v", normalizeValue(tvArr[i], opts))
				if elemSrcNorm != elemTgtNorm {
					diffs = append(diffs, DiffDetail{
						Path:      elemPath,
						SrcType:   typeName(svArr[i]),
						TgtType:   typeName(tvArr[i]),
						SrcValue:  truncate(elemSrcNorm),
						TgtValue:  truncate(elemTgtNorm),
						IssueType: "VALUE_DIFF",
					})
				}
			}
			continue
		}

		// ── Value comparison using normalized values ──
		srcNorm := fmt.Sprintf("%v", normalizeValue(sv, opts))
		tgtNorm := fmt.Sprintf("%v", normalizeValue(tv, opts))
		if srcNorm != tgtNorm {
			diffs = append(diffs, DiffDetail{
				Path:      fullPath,
				SrcType:   srcType,
				TgtType:   tgtType,
				SrcValue:  truncate(srcNorm),
				TgtValue:  truncate(tgtNorm),
				IssueType: "VALUE_DIFF",
			})
		}
	}

	// Fields that exist only in target
	for k := range tgt {
		if k == "_id" {
			continue
		}
		fullPath := k
		if path != "" {
			fullPath = path + "." + k
		}
		if _, exists := src[k]; !exists {
			diffs = append(diffs, DiffDetail{
				Path:      fullPath,
				TgtType:   typeName(tgt[k]),
				TgtValue:  truncate(fmt.Sprintf("%v", tgt[k])),
				IssueType: "EXTRA_FIELD",
			})
		}
	}
	return diffs
}

// bothArraysOfDocs returns sv/tv as []interface{} when both are arrays and
// at least one holds a document element - the signal that per-element
// recursion will produce a more precise diff than comparing the whole array
// as one opaque normalized value.
func bothArraysOfDocs(sv, tv interface{}) ([]interface{}, []interface{}, bool) {
	svArr, svOK := toInterfaceSlice(sv)
	tvArr, tvOK := toInterfaceSlice(tv)
	if !svOK || !tvOK {
		return nil, nil, false
	}
	if !containsDoc(svArr) && !containsDoc(tvArr) {
		return nil, nil, false
	}
	return svArr, tvArr, true
}

func toInterfaceSlice(v interface{}) ([]interface{}, bool) {
	switch a := v.(type) {
	case primitive.A:
		return []interface{}(a), true
	case []interface{}:
		return a, true
	default:
		return nil, false
	}
}

func containsDoc(arr []interface{}) bool {
	for _, e := range arr {
		if _, ok := e.(bson.M); ok {
			return true
		}
	}
	return false
}

// typeName returns a human-readable type name.
func typeName(v interface{}) string {
	switch v.(type) {
	case primitive.ObjectID:
		return "ObjectID"
	case primitive.DateTime:
		return "DateTime"
	case time.Time:
		return "Time"
	case primitive.Decimal128:
		return "Decimal128"
	case primitive.Binary:
		return "Binary"
	case float64:
		return "float64"
	case float32:
		return "float32"
	case int32:
		return "int32"
	case int64:
		return "int64"
	case bool:
		return "bool"
	case string:
		return "string"
	case bson.M:
		return "document"
	case primitive.A, []interface{}:
		return "array"
	case nil:
		return "null"
	case primitive.Timestamp:
		return "Timestamp"
	case primitive.Regex:
		return "Regex"
	default:
		return fmt.Sprintf("%T", v)
	}
}

// truncate shortens overly long values so logs don't explode. Counts and
// slices by rune, not byte: a byte-offset slice can land in the middle of a
// multi-byte UTF-8 character (Chinese, emoji, etc.) and corrupt the output.
func truncate(s string) string {
	if utf8.RuneCountInString(s) > 120 {
		return string([]rune(s)[:120]) + "...(truncated)"
	}
	return s
}
