package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

const checkpointFile = "checkpoints.json"

var (
	cpMu   sync.Mutex
	cpData map[string]string
)

func init() {
	cpData = loadCheckpoints()
}

func SaveCheckpoint(ns string, lastID interface{}) {
	cpMu.Lock()
	defer cpMu.Unlock()
	cpData[ns] = fmt.Sprintf("%v", lastID)
	data, _ := json.MarshalIndent(cpData, "", "  ")
	_ = os.WriteFile(checkpointFile, data, 0644)
}

func LoadCheckpoint(ns string) (string, bool) {
	cpMu.Lock()
	defer cpMu.Unlock()
	v, ok := cpData[ns]
	return v, ok
}

func ClearCheckpoint(ns string) {
	cpMu.Lock()
	defer cpMu.Unlock()
	delete(cpData, ns)
	data, _ := json.MarshalIndent(cpData, "", "  ")
	_ = os.WriteFile(checkpointFile, data, 0644)
}

func loadCheckpoints() map[string]string {
	data, err := os.ReadFile(checkpointFile)
	if err != nil {
		return make(map[string]string)
	}
	var m map[string]string
	_ = json.Unmarshal(data, &m)
	return m
}
