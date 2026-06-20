package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/null-ptr-exception/mongogate/internal/report"
)

type Server struct {
	port   int
	rpt    *report.Report
	server *http.Server
}

func New(port int, rpt *report.Report) *Server {
	return &Server{port: port, rpt: rpt}
}

// Start launches the HTTP API server in the background.
func (s *Server) Start() {
	if s.port == 0 {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health",    s.handleHealth)
	mux.HandleFunc("/status",    s.handleStatus)
	mux.HandleFunc("/progress",  s.handleProgress)
	mux.HandleFunc("/report",    s.handleReport)
	mux.HandleFunc("/passed",    s.handlePassed)

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}
	fmt.Printf("\n🌐 HTTP API listening on http://localhost:%d\n", s.port)
	fmt.Printf("   GET /health    → service health\n")
	fmt.Printf("   GET /status    → verification summary\n")
	fmt.Printf("   GET /progress  → per-namespace progress\n")
	fmt.Printf("   GET /report    → full JSON report\n")
	fmt.Printf("   GET /passed    → whether everything passed (for CI/CD)\n")

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[httpapi] failed to start: %v\n", err)
		}
	}()
}

func (s *Server) Stop() {
	if s.server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = s.server.Shutdown(ctx)
}

// GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]string{
		"status": "ok",
		"time":   time.Now().Format(time.RFC3339),
	})
}

// GET /status → verification summary
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"passed":          s.rpt.AllPassed(),
		"auth":            s.rpt.SectionPassed("auth"),
		"cluster":         s.rpt.SectionPassed("cluster"),
		"databases":       s.rpt.SectionPassed("databases"),
		"data_namespaces": s.rpt.DataSummary(),
		"generated_at":    time.Now().Format(time.RFC3339),
	})
}

// GET /progress → per-namespace progress (for CI/CD polling)
func (s *Server) handleProgress(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"data":         s.rpt.GetData(),
		"generated_at": time.Now().Format(time.RFC3339),
	})
}

// GET /report → full JSON report
func (s *Server) handleReport(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_ = json.NewEncoder(w).Encode(s.rpt)
}

// GET /passed → 200=passed, 500=failed (for CI/CD exit-code checks)
func (s *Server) handlePassed(w http.ResponseWriter, r *http.Request) {
	if s.rpt.AllPassed() {
		writeJSON(w, 200, map[string]interface{}{"passed": true})
	} else {
		writeJSON(w, 500, map[string]interface{}{"passed": false})
	}
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
