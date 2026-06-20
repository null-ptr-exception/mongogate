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
	NS             string       `json:"ns"`
	Passed         bool         `json:"passed"`
	SrcCount       int64        `json:"src_count"`
	TgtCount       int64        `json:"tgt_count"`
	CountMatch     bool         `json:"count_match"`
	CountIsExact   bool         `json:"count_is_exact"`
	HashIsExact    bool         `json:"hash_is_exact"`
	MissingCount   int          `json:"missing_count"`
	DifferentCount int          `json:"different_count"`
	MissingSample  []string     `json:"missing_sample,omitempty"`
	DiffSample     []string     `json:"different_sample,omitempty"`
	FieldErrors          []FieldError `json:"field_errors,omitempty"`
	Errors               []string     `json:"errors,omitempty"`
	ProgressPct          float64      `json:"progress_pct"`
	ExtraInTarget        int          `json:"extra_in_target"`
	ExtraInTargetSample  []string     `json:"extra_in_target_sample,omitempty"`
}

type Report struct {
	mu          sync.RWMutex
	StartTime   time.Time              `json:"start_time"`
	EndTime     time.Time              `json:"end_time"`
	Auth        *Result                `json:"auth,omitempty"`
	Cluster     *Result                `json:"cluster,omitempty"`
	Databases   *Result                `json:"databases,omitempty"`
	Collections map[string]*Result     `json:"collections,omitempty"`
	Indexes     map[string]*Result     `json:"indexes,omitempty"`
	GridFS      map[string]*Result     `json:"gridfs,omitempty"`
	Views       map[string]*Result     `json:"views,omitempty"`
	Data        map[string]*DataResult `json:"data,omitempty"`
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
func (r *Report) SetAuth(res *Result)                    { r.mu.Lock(); defer r.mu.Unlock(); r.Auth = res }
func (r *Report) SetCluster(res *Result)                 { r.mu.Lock(); defer r.mu.Unlock(); r.Cluster = res }
func (r *Report) SetDatabases(res *Result)               { r.mu.Lock(); defer r.mu.Unlock(); r.Databases = res }
func (r *Report) SetCollection(ns string, res *Result)   { r.mu.Lock(); defer r.mu.Unlock(); r.Collections[ns] = res }
func (r *Report) SetIndex(ns string, res *Result)        { r.mu.Lock(); defer r.mu.Unlock(); r.Indexes[ns] = res }
func (r *Report) SetGridFS(db string, res *Result)       { r.mu.Lock(); defer r.mu.Unlock(); r.GridFS[db] = res }
func (r *Report) SetView(ns string, res *Result)         { r.mu.Lock(); defer r.mu.Unlock(); r.Views[ns] = res }
func (r *Report) SetData(ns string, res *DataResult)     { r.mu.Lock(); defer r.mu.Unlock(); r.Data[ns] = res }

func (r *Report) UpdateProgress(ns string, pct float64) {
	r.mu.Lock(); defer r.mu.Unlock()
	if d, ok := r.Data[ns]; ok {
		d.ProgressPct = pct
	} else {
		r.Data[ns] = &DataResult{NS: ns, ProgressPct: pct}
	}
}

// ── Getters ──
func (r *Report) GetData() map[string]*DataResult {
	r.mu.RLock(); defer r.mu.RUnlock()
	return r.Data
}

func (r *Report) SectionPassed(section string) bool {
	r.mu.RLock(); defer r.mu.RUnlock()
	switch section {
	case "auth":      return r.Auth == nil || r.Auth.Passed
	case "cluster":   return r.Cluster == nil || r.Cluster.Passed
	case "databases": return r.Databases == nil || r.Databases.Passed
	}
	return true
}

func (r *Report) DataSummary() map[string]interface{} {
	r.mu.RLock(); defer r.mu.RUnlock()
	total, passed, missing, different := 0, 0, 0, 0
	for _, d := range r.Data {
		total++
		if d.Passed { passed++ }
		missing += d.MissingCount
		different += d.DifferentCount
	}
	return map[string]interface{}{
		"total": total, "passed": passed,
		"total_missing": missing, "total_different": different,
	}
}

func (r *Report) AllPassed() bool {
	r.mu.RLock(); defer r.mu.RUnlock()
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
	fmt.Println(line("=", 65))

	printResult("🔐 Account permissions", r.Auth)
	printResult("🖥️  Cluster settings",   r.Cluster)
	printResult("🗄️  Databases",          r.Databases)

	if len(r.Collections) > 0 {
		fmt.Println("\n📁 Collections:")
		for ns, res := range r.Collections { printResult("  "+ns, res) }
	}
	if len(r.Indexes) > 0 {
		fmt.Println("\n🔍 Indexes:")
		for ns, res := range r.Indexes { printResult("  "+ns, res) }
	}
	if len(r.Views) > 0 {
		fmt.Println("\n👁️  Views:")
		for ns, res := range r.Views { printResult("  "+ns, res) }
	}
	if len(r.GridFS) > 0 {
		fmt.Println("\n🗂️  GridFS:")
		for db, res := range r.GridFS { printResult("  "+db, res) }
	}
	if len(r.Data) > 0 {
		fmt.Println("\n📊 Data verification:")
		totalMissing, totalDiff := 0, 0
		for _, d := range r.Data {
			status := "✅"
			if !d.Passed { status = "❌" }
			fmt.Printf("  %s %-42s src=%-8d missing=%-6d diff=%-6d exact=%v\n",
				status, d.NS, d.SrcCount, d.MissingCount, d.DifferentCount, d.HashIsExact)
			totalMissing += d.MissingCount
			totalDiff += d.DifferentCount
		}
		fmt.Printf("\n  Totals: missing=%d different=%d\n", totalMissing, totalDiff)
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
	if r.Auth != nil && !r.Auth.Passed           { return false }
	if r.Cluster != nil && !r.Cluster.Passed     { return false }
	if r.Databases != nil && !r.Databases.Passed { return false }
	for _, v := range r.Collections { if !v.Passed { return false } }
	for _, v := range r.Indexes     { if !v.Passed { return false } }
	for _, v := range r.GridFS      { if !v.Passed { return false } }
	for _, v := range r.Views       { if !v.Passed { return false } }
	for _, v := range r.Data        { if !v.Passed { return false } }
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
	if err != nil { return err }
	if err := os.WriteFile(filename, data, 0644); err != nil { return err }
	fmt.Printf("\n📄 JSON report saved: %s\n", filename)
	return nil
}

func printResult(label string, res *Result) {
	if res == nil { return }
	status := "✅ PASS"
	if !res.Passed { status = "❌ FAIL" }
	fmt.Printf("\n%s: %s\n", label, status)
	for _, e := range res.Errors { fmt.Printf("  %s\n", e) }
}

func line(ch string, n int) string {
	s := ""
	for i := 0; i < n; i++ { s += ch }
	return s
}
