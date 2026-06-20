package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/smtp"
	"strings"
	"sync"
	"time"

	"github.com/null-ptr-exception/mongogate/internal/config"
)

type AlertManager struct {
	cfg         config.AlertConfig
	mu          sync.Mutex
	lastPct     float64
	lastPctTime time.Time
	firedKeys   map[string]time.Time // de-dupe: suppress repeats of the same key
	inFlight    sync.WaitGroup       // tracks notification goroutines so Wait() can block for them
}

type AlertEvent struct {
	Level   string
	Title   string
	Message string
	NS      string
	Values  map[string]interface{}
}

func New(cfg config.AlertConfig) *AlertManager {
	return &AlertManager{
		cfg:         cfg,
		lastPctTime: time.Now(),
		firedKeys:   make(map[string]time.Time),
	}
}

// Check is called after every metrics update.
func (a *AlertManager) Check(ns string, missing, different int, pct float64) {
	if !a.cfg.Enabled {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	if missing > a.cfg.MissingThreshold {
		a.fireOnce(fmt.Sprintf("missing:%s", ns), AlertEvent{
			Level:   "CRITICAL",
			Title:   "🚨 Missing document count exceeded threshold",
			Message: fmt.Sprintf("[%s] missing=%d threshold=%d", ns, missing, a.cfg.MissingThreshold),
			NS:      ns,
			Values:  map[string]interface{}{"missing": missing},
		})
	}
	if different > a.cfg.DifferentThreshold {
		a.fireOnce(fmt.Sprintf("different:%s", ns), AlertEvent{
			Level:   "CRITICAL",
			Title:   "🚨 Different document count exceeded threshold",
			Message: fmt.Sprintf("[%s] different=%d threshold=%d", ns, different, a.cfg.DifferentThreshold),
			NS:      ns,
			Values:  map[string]interface{}{"different": different},
		})
	}

	// Stuck-progress detection
	if pct > a.lastPct {
		a.lastPct = pct
		a.lastPctTime = time.Now()
	} else if time.Since(a.lastPctTime) > time.Duration(a.cfg.StuckMinutes)*time.Minute {
		a.fireOnce(fmt.Sprintf("stuck:%s", ns), AlertEvent{
			Level:   "WARNING",
			Title:   "⚠️  Verification progress stuck",
			Message: fmt.Sprintf("[%s] progress %.1f%% has not moved in %d minutes", ns, pct, a.cfg.StuckMinutes),
			NS:      ns,
		})
		a.lastPctTime = time.Now()
	}
}

// CheckExtraInTarget fires once the bidirectional scan finds documents that
// exist in target but not in source - the closest signal this tool has to
// "target has diverged independently of source" (e.g. a botched migration
// script, something else writing into target, or a missed delete). It reuses
// DifferentThreshold rather than adding a separate config field, since both
// represent target-side content that doesn't match source.
func (a *AlertManager) CheckExtraInTarget(ns string, extra int) {
	if !a.cfg.Enabled || extra <= a.cfg.DifferentThreshold {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.fireOnce(fmt.Sprintf("extra_in_target:%s", ns), AlertEvent{
		Level:   "CRITICAL",
		Title:   "🚨 Target has documents that don't exist in source",
		Message: fmt.Sprintf("[%s] extra_in_target=%d threshold=%d", ns, extra, a.cfg.DifferentThreshold),
		NS:      ns,
		Values:  map[string]interface{}{"extra_in_target": extra},
	})
}

// fireOnce suppresses repeats of the same key within a 10-minute cooldown.
func (a *AlertManager) fireOnce(key string, event AlertEvent) {
	if last, ok := a.firedKeys[key]; ok {
		if time.Since(last) < 10*time.Minute {
			return
		}
	}
	a.firedKeys[key] = time.Now()
	a.Fire(event)
}

func (a *AlertManager) Fire(event AlertEvent) {
	fmt.Printf("\n[ALERT][%s] %s\n  %s\n", event.Level, event.Title, event.Message)
	if a.cfg.SlackWebhook != "" {
		a.inFlight.Add(1)
		go func() { defer a.inFlight.Done(); a.sendSlack(event) }()
	}
	if a.cfg.EmailSMTP != "" && len(a.cfg.EmailTo) > 0 {
		a.inFlight.Add(1)
		go func() { defer a.inFlight.Done(); a.sendEmail(event) }()
	}
}

// Wait blocks until every in-flight notification goroutine has finished.
// Fire() dispatches Slack/Email sends asynchronously so a slow webhook can't
// stall verification; that means whoever calls os.Exit() right after firing
// the final pass/fail alert MUST call Wait() first, or the process can exit
// before the HTTP request is ever sent - silently dropping exactly the
// notification that matters most.
func (a *AlertManager) Wait() {
	a.inFlight.Wait()
}

// ── Slack ──
func (a *AlertManager) sendSlack(event AlertEvent) {
	color := "#ff0000"
	if event.Level == "WARNING" {
		color = "#ff9900"
	}
	if event.Level == "INFO" {
		color = "#36a64f"
	}
	payload := map[string]interface{}{
		"attachments": []map[string]interface{}{{
			"color": color,
			"title": event.Title,
			"text":  event.Message,
			"fields": []map[string]string{
				{"title": "NS", "value": event.NS, "short": "true"},
				{"title": "Level", "value": event.Level, "short": "true"},
				{"title": "Time", "value": time.Now().Format("2006-01-02 15:04:05"), "short": "true"},
			},
			"footer": "mongogate",
		}},
	}
	body, _ := json.Marshal(payload)
	// A bounded timeout matters here: Fire() dispatches this asynchronously
	// and Wait() blocks the process's exit path on it, so a slow or
	// unreachable webhook must not be able to hang the program forever.
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(a.cfg.SlackWebhook, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("[alert] Slack send failed: %v\n", err)
		return
	}
	defer resp.Body.Close()
}

// ── Email ──
func (a *AlertManager) sendEmail(event AlertEvent) {
	subject := fmt.Sprintf("[mongogate][%s] %s", event.Level, event.Title)
	body := fmt.Sprintf("Level:   %s\nNS:      %s\nMessage: %s\nTime:    %s",
		event.Level, event.NS, event.Message,
		time.Now().Format("2006-01-02 15:04:05"))
	msg := []byte("To: " + strings.Join(a.cfg.EmailTo, ",") + "\r\n" +
		"Subject: " + subject + "\r\n\r\n" + body + "\r\n")

	// smtp.SendMail has no built-in timeout/context support; bound it the
	// same way as sendSlack so a stuck SMTP server can't hang Wait() (and
	// therefore the process's exit path) forever. If it times out, the send
	// goroutine is left running but the process is about to exit anyway.
	done := make(chan error, 1)
	go func() { done <- smtp.SendMail(a.cfg.EmailSMTP, nil, a.cfg.EmailFrom, a.cfg.EmailTo, msg) }()

	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		err = fmt.Errorf("timed out after 10s")
	}
	if err != nil {
		fmt.Printf("[alert] Email send failed: %v\n", err)
	}
}
