package progress

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type Bar struct {
	mu        sync.Mutex
	ns        string
	total     int64
	processed int64
	start     time.Time
	width     int
}

func New(ns string, total int64) *Bar {
	return &Bar{ns: ns, total: total, start: time.Now(), width: 30}
}

func (b *Bar) Update(processed int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.processed = processed
	b.render()
}

func (b *Bar) Done() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.processed = b.total
	b.render()
	fmt.Println()
}

func (b *Bar) render() {
	pct := 0.0
	if b.total > 0 {
		pct = float64(b.processed) / float64(b.total) * 100
	}
	filled := int(float64(b.width) * pct / 100)
	bar := strings.Repeat("█", filled) + strings.Repeat("░", b.width-filled)
	elapsed := time.Since(b.start).Round(time.Second)

	// ETA calculation
	eta := "--:--"
	if b.processed > 0 && b.total > b.processed {
		rate := float64(b.processed) / time.Since(b.start).Seconds()
		remaining := float64(b.total-b.processed) / rate
		etaDur := time.Duration(remaining) * time.Second
		eta = etaDur.Round(time.Second).String()
	}

	fmt.Printf("\r  %-35s [%s] %5.1f%% %d/%d elapsed=%s eta=%s    ",
		truncate(b.ns, 35), bar, pct, b.processed, b.total, elapsed, eta)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-(n-3):]
}
