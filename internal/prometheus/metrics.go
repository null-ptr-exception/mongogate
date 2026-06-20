package prometheus

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/null-ptr-exception/mongogate/internal/report"
)

// MetricsServer hand-writes Prometheus text format; no client library dependency.
type MetricsServer struct {
	mu     sync.RWMutex
	port   int
	rpt    *report.Report
	gauges map[string]float64
}

func New(port int, rpt *report.Report) *MetricsServer {
	return &MetricsServer{
		port:   port,
		rpt:    rpt,
		gauges: make(map[string]float64),
	}
}

func (m *MetricsServer) Set(key string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[key] = value
}

func (m *MetricsServer) Start() {
	if m.port == 0 {
		return
	}
	http.HandleFunc("/metrics", m.handleMetrics)
	fmt.Printf("📈 Prometheus metrics at http://localhost:%d/metrics\n", m.port)
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", m.port), nil); err != nil {
			fmt.Printf("[prometheus] failed to start: %v\n", err)
		}
	}()
}

func (m *MetricsServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var sb strings.Builder

	// Static gauges
	for k, v := range m.gauges {
		sb.WriteString(fmt.Sprintf("mongogate_%s %g\n", k, v))
	}

	// Generated dynamically from the report
	for ns, data := range m.rpt.GetData() {
		label := sanitizeLabel(ns)
		passed := 0.0
		if data.Passed { passed = 1.0 }
		sb.WriteString(fmt.Sprintf(`mongogate_ns_passed{ns="%s"} %g`+"\n", label, passed))
		sb.WriteString(fmt.Sprintf(`mongogate_ns_missing{ns="%s"} %d`+"\n", label, data.MissingCount))
		sb.WriteString(fmt.Sprintf(`mongogate_ns_different{ns="%s"} %d`+"\n", label, data.DifferentCount))
		sb.WriteString(fmt.Sprintf(`mongogate_ns_extra_in_target{ns="%s"} %d`+"\n", label, data.ExtraInTarget))
		sb.WriteString(fmt.Sprintf(`mongogate_ns_src_count{ns="%s"} %d`+"\n", label, data.SrcCount))
		sb.WriteString(fmt.Sprintf(`mongogate_ns_tgt_count{ns="%s"} %d`+"\n", label, data.TgtCount))
		sb.WriteString(fmt.Sprintf(`mongogate_ns_progress_pct{ns="%s"} %g`+"\n", label, data.ProgressPct))
	}

	// Overall
	allPassed := 0.0
	if m.rpt.AllPassed() { allPassed = 1.0 }
	sb.WriteString(fmt.Sprintf("mongogate_all_passed %g\n", allPassed))

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprint(w, sb.String())
}

func sanitizeLabel(s string) string {
	return strings.ReplaceAll(s, ".", "_")
}
