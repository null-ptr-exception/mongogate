package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

const defaultCheckpointFile = "checkpoints.json"

var (
	cpMu     sync.Mutex
	cpFile   = defaultCheckpointFile
	cpData   map[string]string
	cpLoaded bool
)

// SetCheckpointFile overrides the default global checkpoint filename. Call
// once at startup with a job-specific name (e.g. derived from the
// source/target URI pair) so two different migration jobs run from the same
// working directory don't silently clobber each other's --resume state by
// sharing one hardcoded checkpoints.json.
func SetCheckpointFile(path string) {
	cpMu.Lock()
	defer cpMu.Unlock()
	cpFile = path
	cpLoaded = false
}

func ensureLoaded() {
	if cpLoaded {
		return
	}
	cpData = loadCheckpoints()
	cpLoaded = true
}

func SaveCheckpoint(ns string, lastID interface{}) {
	cpMu.Lock()
	defer cpMu.Unlock()
	ensureLoaded()
	cpData[ns] = fmt.Sprintf("%v", lastID)
	data, _ := json.MarshalIndent(cpData, "", "  ")
	_ = os.WriteFile(cpFile, data, 0644)
}

func LoadCheckpoint(ns string) (string, bool) {
	cpMu.Lock()
	defer cpMu.Unlock()
	ensureLoaded()
	v, ok := cpData[ns]
	return v, ok
}

func ClearCheckpoint(ns string) {
	cpMu.Lock()
	defer cpMu.Unlock()
	ensureLoaded()
	delete(cpData, ns)
	data, _ := json.MarshalIndent(cpData, "", "  ")
	_ = os.WriteFile(cpFile, data, 0644)
}

func loadCheckpoints() map[string]string {
	data, err := os.ReadFile(cpFile)
	if err != nil {
		return make(map[string]string)
	}
	var m map[string]string
	_ = json.Unmarshal(data, &m)
	if m == nil {
		m = make(map[string]string)
	}
	return m
}
