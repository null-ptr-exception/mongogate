package report

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type Result struct {
	Passed bool     `json:"passed"`
	Errors []string `json:"errors,omitempty"`
}

// FieldError records a field-level diff detail.
type FieldError struct {
	DocID     string `json:"doc_id"`
	IssueType string `json:"issue_type"`
	Path      string `json:"path"`
	SrcType   string `json:"src_type"`
	TgtType   string `json:"tgt_type"`
	SrcValue  string `json:"src_value"`
	TgtValue  string `json:"tgt_value"`
}

type DataResult struct {
	NS                  string       `json:"ns"`
	Passed              bool         `json:"passed"`
	SrcCount            int64        `json:"src_count"`
	TgtCount            int64        `json:"tgt_count"`
	CountMatch          bool         `json:"count_match"`
	CountIsExact        bool         `json:"count_is_exact"`
	HashIsExact         bool         `json:"hash_is_exact"`
	MissingCount        int          `json:"missing_count"`
	DifferentCount      int          `json:"different_count"`
	MissingSample       []string     `json:"missing_sample,omitempty"`
	DiffSample          []string     `json:"different_sample,omitempty"`
	FieldErrors         []FieldError `json:"field_errors,omitempty"`
	Errors              []string     `json:"errors,omitempty"`
	ProgressPct         float64      `json:"progress_pct"`
	ExtraInTarget       int          `json:"extra_in_target"`
	ExtraInTargetSample []string     `json:"extra_in_target_sample,omitempty"`
}

// VersionInfo is informational only - it never affects Passed/exit code.
// Surfaced so a human reading the report has the context to correctly
// interpret version-driven diffs elsewhere instead of being surprised by
// them (e.g. a built-in role privilege diff that's actually just a MongoDB
// version difference, not a migration bug).
type VersionInfo struct {
	SrcVersion             string `json:"src_version"`
	TgtVersion             string `json:"tgt_version"`
	SrcFCV                 string `json:"src_fcv"`
	TgtFCV                 string `json:"tgt_fcv"`
	SrcDefaultWriteConcern string `json:"src_default_write_concern"`
	TgtDefaultWriteConcern string `json:"tgt_default_write_concern"`
}

type Report struct {
	mu          sync.RWMutex
	StartTime   time.Time              `json:"start_time"`
	EndTime     time.Time              `json:"end_time"`
	Versions    *VersionInfo           `json:"versions,omitempty"`
	Auth        *Result                `json:"auth,omitempty"`
	Cluster     *Result                `json:"cluster,omitempty"`
	Databases   *Result                `json:"databases,omitempty"`
	Collections map[string]*Result     `json:"collections,omitempty"`
	Indexes     map[string]*Result     `json:"indexes,omitempty"`
	GridFS      map[string]*Result     `json:"gridfs,omitempty"`
	Views       map[string]*Result     `json:"views,omitempty"`
	Data        map[string]*DataResult `json:"data,omitempty"`

	// pendingRanges tracks, per namespace, how many range sub-tasks have
	// yet to report in via MergeData. Not serialized - purely in-flight
	// bookkeeping for range-split collections (see BeginRanges/MergeData).
	pendingRanges map[string]int
}

func New() *Report {
	return &Report{
		StartTime:   time.Now(),
		Collections: make(map[string]*Result),
		Indexes:     make(map[string]*Result),
		GridFS:      make(map[string]*Result),
		Views:       make(map[string]*Result),
		Data:        make(map[string]*DataResult),
	}
}

// ── Setters (thread-safe) ──
func (r *Report) SetVersions(v *VersionInfo) { r.mu.Lock(); defer r.mu.Unlock(); r.Versions = v }
func (r *Report) SetAuth(res *Result)        { r.mu.Lock(); defer r.mu.Unlock(); r.Auth = res }
func (r *Report) SetCluster(res *Result)     { r.mu.Lock(); defer r.mu.Unlock(); r.Cluster = res }
func (r *Report) SetDatabases(res *Result)   { r.mu.Lock(); defer r.mu.Unlock(); r.Databases = res }
func (r *Report) SetCollection(ns string, res *Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Collections[ns] = res
}
func (r *Report) SetIndex(ns string, res *Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Indexes[ns] = res
}
func (r *Report) SetGridFS(db string, res *Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.GridFS[db] = res
}
func (r *Report) SetView(ns string, res *Result) { r.mu.Lock(); defer r.mu.Unlock(); r.Views[ns] = res }
func (r *Report) SetData(ns string, res *DataResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Data[ns] = res
}

func (r *Report) UpdateProgress(ns string, pct float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.Data[ns]; ok {
		d.ProgressPct = pct
	} else {
		r.Data[ns] = &DataResult{NS: ns, ProgressPct: pct}
	}
}

// BeginRanges registers that ns has been split into `total` concurrent
// range sub-tasks, so MergeData knows how many partial results to wait for
// before treating ns as complete. Call once, before any of those tasks'
// workers start, from a single goroutine (the pre-dispatch expansion step
// in VerifyAllData) - MergeData itself is safe to call concurrently, but
// the registration itself is not idempotent/mergeable across callers.
// A ns never registered here is treated by MergeData as a single task
// (today's non-split behavior).
func (r *Report) BeginRanges(ns string, total int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pendingRanges == nil {
		r.pendingRanges = make(map[string]int)
	}
	r.pendingRanges[ns] = total
}

// MergeData accumulates one range (or, for an unsplit collection, the
// whole-collection) partial result into ns's combined DataResult, and
// reports whether every range registered for ns via BeginRanges has now
// reported in. Counts are summed; sample/error slices are appended up to
// their existing caps. Passed/CountMatch are (re)computed only once
// isLast is true - callers must not print/act on the returned result
// before then, since it's incomplete while ranges are still outstanding.
//
// SetData is a full overwrite and unsafe for concurrent range workers on
// the same ns (see git history / docs/TESTING.md); MergeData is the
// merge-aware replacement multi-range callers must use instead.
func (r *Report) MergeData(ns string, delta *DataResult) (combined *DataResult, isLast bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	acc, ok := r.Data[ns]
	if !ok {
		acc = &DataResult{NS: ns, CountIsExact: true, HashIsExact: true}
		r.Data[ns] = acc
	}
	acc.SrcCount += delta.SrcCount
	acc.TgtCount += delta.TgtCount
	acc.MissingCount += delta.MissingCount
	acc.DifferentCount += delta.DifferentCount
	acc.ExtraInTarget += delta.ExtraInTarget
	acc.MissingSample = appendCapped(acc.MissingSample, delta.MissingSample, 50)
	acc.DiffSample = appendCapped(acc.DiffSample, delta.DiffSample, 50)
	acc.ExtraInTargetSample = appendCapped(acc.ExtraInTargetSample, delta.ExtraInTargetSample, 20)
	acc.Errors = appendCapped(acc.Errors, delta.Errors, 20)
	acc.FieldErrors = appendFieldErrorsCapped(acc.FieldErrors, delta.FieldErrors, 200)
	// Every range of one run shares the same opts.SampleRate, so these
	// should already agree across deltas - AND defensively rather than
	// assume it.
	acc.CountIsExact = acc.CountIsExact && delta.CountIsExact
	acc.HashIsExact = acc.HashIsExact && delta.HashIsExact

	remaining, began := r.pendingRanges[ns]
	if !began {
		remaining = 1
	}
	remaining--
	if remaining > 0 {
		r.pendingRanges[ns] = remaining
		return acc, false
	}
	delete(r.pendingRanges, ns)

	acc.CountMatch = acc.SrcCount == acc.TgtCount
	acc.Passed = acc.CountMatch &&
		acc.MissingCount == 0 &&
		acc.DifferentCount == 0 &&
		acc.ExtraInTarget == 0 &&
		len(acc.Errors) == 0
	return acc, true
}

func appendCapped(existing, add []string, cap int) []string {
	for _, s := range add {
		if len(existing) >= cap {
			break
		}
		existing = append(existing, s)
	}
	return existing
}

func appendFieldErrorsCapped(existing, add []FieldError, cap int) []FieldError {
	for _, e := range add {
		if len(existing) >= cap {
			break
		}
		existing = append(existing, e)
	}
	return existing
}

// ── Getters ──
func (r *Report) GetData() map[string]*DataResult {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.Data
}

func (r *Report) SectionPassed(section string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	switch section {
	case "auth":
		return r.Auth == nil || r.Auth.Passed
	case "cluster":
		return r.Cluster == nil || r.Cluster.Passed
	case "databases":
		return r.Databases == nil || r.Databases.Passed
	}
	return true
}

func (r *Report) DataSummary() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	total, passed, missing, different := 0, 0, 0, 0
	for _, d := range r.Data {
		total++
		if d.Passed {
			passed++
		}
		missing += d.MissingCount
		different += d.DifferentCount
	}
	return map[string]interface{}{
		"total": total, "passed": passed,
		"total_missing": missing, "total_different": different,
	}
}

func (r *Report) AllPassed() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.allPassedLocked()
}

func (r *Report) Print() {
	r.mu.Lock()
	r.EndTime = time.Now()
	r.mu.Unlock()
	r.mu.RLock()
	defer r.mu.RUnlock()

	fmt.Println("\n" + line("=", 65))
	fmt.Println("📋 MongoDB Migration Verification Report")
	fmt.Printf("   Start    : %s\n", r.StartTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("   End      : %s\n", r.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("   Duration : %s\n", r.EndTime.Sub(r.StartTime).Round(time.Second))
	if r.Versions != nil {
		fmt.Printf("   Source   : version=%s fcv=%s default_write_concern=%s\n",
			r.Versions.SrcVersion, r.Versions.SrcFCV, r.Versions.SrcDefaultWriteConcern)
		fmt.Printf("   Target   : version=%s fcv=%s default_write_concern=%s\n",
			r.Versions.TgtVersion, r.Versions.TgtFCV, r.Versions.TgtDefaultWriteConcern)
	}
	fmt.Println(line("=", 65))

	printResult("🔐 Account permissions", r.Auth)
	printResult("🖥️  Cluster settings", r.Cluster)
	printResult("🗄️  Databases", r.Databases)

	if len(r.Collections) > 0 {
		fmt.Println("\n📁 Collections:")
		for ns, res := range r.Collections {
			printResult("  "+ns, res)
		}
	}
	if len(r.Indexes) > 0 {
		fmt.Println("\n🔍 Indexes:")
		for ns, res := range r.Indexes {
			printResult("  "+ns, res)
		}
	}
	if len(r.Views) > 0 {
		fmt.Println("\n👁️  Views:")
		for ns, res := range r.Views {
			printResult("  "+ns, res)
		}
	}
	if len(r.GridFS) > 0 {
		fmt.Println("\n🗂️  GridFS:")
		for db, res := range r.GridFS {
			printResult("  "+db, res)
		}
	}
	if len(r.Data) > 0 {
		fmt.Println("\n📊 Data verification:")
		totalMissing, totalDiff, totalExtra := 0, 0, 0
		for _, d := range r.Data {
			status := "✅"
			if !d.Passed {
				status = "❌"
			}
			fmt.Printf("  %s %-42s src=%-8d missing=%-6d diff=%-6d extra_in_target=%-6d exact=%v\n",
				status, d.NS, d.SrcCount, d.MissingCount, d.DifferentCount, d.ExtraInTarget, d.HashIsExact)
			totalMissing += d.MissingCount
			totalDiff += d.DifferentCount
			totalExtra += d.ExtraInTarget
		}
		fmt.Printf("\n  Totals: missing=%d different=%d extra_in_target=%d\n", totalMissing, totalDiff, totalExtra)
	}

	fmt.Println("\n" + line("=", 65))
	if r.allPassedLocked() {
		fmt.Println("🎉 Overall result: ✅ everything passed, cutover can proceed!")
	} else {
		fmt.Println("🚨 Overall result: ❌ diffs found, confirm before cutover")
	}
	fmt.Println(line("=", 65))
}

func (r *Report) allPassedLocked() bool {
	if r.Auth != nil && !r.Auth.Passed {
		return false
	}
	if r.Cluster != nil && !r.Cluster.Passed {
		return false
	}
	if r.Databases != nil && !r.Databases.Passed {
		return false
	}
	for _, v := range r.Collections {
		if !v.Passed {
			return false
		}
	}
	for _, v := range r.Indexes {
		if !v.Passed {
			return false
		}
	}
	for _, v := range r.GridFS {
		if !v.Passed {
			return false
		}
	}
	for _, v := range r.Views {
		if !v.Passed {
			return false
		}
	}
	for _, v := range r.Data {
		if !v.Passed {
			return false
		}
	}
	return true
}

func (r *Report) Save() error {
	r.mu.Lock()
	r.EndTime = time.Now()
	r.mu.Unlock()
	filename := fmt.Sprintf("verify_report_%s.json", time.Now().Format("20060102_150405"))
	r.mu.RLock()
	data, err := json.MarshalIndent(r, "", "  ")
	r.mu.RUnlock()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return err
	}
	fmt.Printf("\n📄 JSON report saved: %s\n", filename)
	return nil
}

func printResult(label string, res *Result) {
	if res == nil {
		return
	}
	status := "✅ PASS"
	if !res.Passed {
		status = "❌ FAIL"
	}
	fmt.Printf("\n%s: %s\n", label, status)
	for _, e := range res.Errors {
		fmt.Printf("  %s\n", e)
	}
}

func line(ch string, n int) string {
	s := ""
	for i := 0; i < n; i++ {
		s += ch
	}
	return s
}
