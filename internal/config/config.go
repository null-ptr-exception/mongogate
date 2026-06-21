package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type HashOptions struct {
	NormalizeDatetime       bool `yaml:"normalize_datetime"`
	NormalizeFloatPrecision int  `yaml:"normalize_float_precision"`
	NormalizeDecimal128     bool `yaml:"normalize_decimal128"`
	SortArrays              bool `yaml:"sort_arrays"`
	CheckBinarySubtype      bool `yaml:"check_binary_subtype"`
	StrictObjectID          bool `yaml:"strict_objectid"`
}

type AlertConfig struct {
	Enabled        bool   `yaml:"enabled"`
	SlackWebhook   string `yaml:"slack_webhook"`
	EmailSMTP      string `yaml:"email_smtp"`
	EmailFrom      string `yaml:"email_from"`
	EmailTo        []string `yaml:"email_to"`
	MissingThreshold   int `yaml:"missing_threshold"`    // fires when missing > N
	DifferentThreshold int `yaml:"different_threshold"`  // fires when different > N
	StuckMinutes       int `yaml:"stuck_minutes"`        // fires when progress hasn't moved in N minutes
}

type Config struct {
	SourceURI  string `yaml:"source_uri"`
	TargetURI  string `yaml:"target_uri"`
	MonitorURI string `yaml:"monitor_uri"`

	BatchSize   int `yaml:"batch_size"`
	MaxWorkers  int `yaml:"max_workers"`
	RateLimitMS int `yaml:"rate_limit_ms"`
	TimeoutSecs int `yaml:"timeout_secs"`
	RetryCount  int `yaml:"retry_count"`
	RetryWaitMS int `yaml:"retry_wait_ms"`

	SkipDBs    []string `yaml:"skip_dbs"`
	IncludeNS  []string `yaml:"include_ns"`  // format: "db.col" or "db.*"
	ExcludeNS  []string `yaml:"exclude_ns"`

	Verify struct {
		Auth       bool `yaml:"auth"`
		Cluster    bool `yaml:"cluster"`
		Schema     bool `yaml:"schema"`
		Index      bool `yaml:"index"`
		Data       bool `yaml:"data"`
		GridFS     bool `yaml:"gridfs"`
		Sharding   bool `yaml:"sharding"`
		Encryption bool `yaml:"encryption"`
		Views      bool `yaml:"views"`
		Bidirectional bool `yaml:"bidirectional"` // also scan target → source
	} `yaml:"verify"`

	Phase2LagThresholdSeconds int     `yaml:"phase2_lag_threshold_seconds"`
	SampleRate                float64 `yaml:"sample_rate"`

	HashOptions HashOptions `yaml:"hash_options"`
	Alert       AlertConfig `yaml:"alert"`

	// Execution mode
	DryRun    bool   `yaml:"dry_run"`
	AutoRepair bool  `yaml:"auto_repair"`  // automatically repair diffs found
	ExportCSV  string `yaml:"export_csv"`  // diff export path

	// HTTP API
	HTTPPort int `yaml:"http_port"` // 0 = disabled

	// Prometheus
	PrometheusPort int `yaml:"prometheus_port"` // 0 = disabled
}

func Load(path string) (*Config, error) {
	cfg := defaults()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, nil
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, validate(cfg)
}

func defaults() *Config {
	cfg := &Config{
		BatchSize:                 500,
		MaxWorkers:                4,
		RateLimitMS:               10,
		TimeoutSecs:               30,
		RetryCount:                3,
		RetryWaitMS:               500,
		Phase2LagThresholdSeconds: 10,
		SampleRate:                0.1,
		// "admin" is skipped by default: its meaningful content (users,
		// roles, replica set config) is already covered by dedicated,
		// purpose-built checks (VerifyAuth, VerifyCluster) via proper admin
		// commands. Generic raw-document comparison of admin.system.users
		// guarantees false positives - SCRAM credentials are freshly salted
		// per createUser call, so the same password produces different
		// stored bytes on each side even on a 100% correct migration.
		SkipDBs:                   []string{"local", "config", "admin"},
		HTTPPort:                  0,
		PrometheusPort:            0,
	}
	cfg.Verify.Auth      = true
	cfg.Verify.Cluster   = true
	cfg.Verify.Schema    = true
	cfg.Verify.Index     = true
	cfg.Verify.Data      = true
	cfg.Verify.GridFS    = true
	cfg.Verify.Views     = true
	cfg.Verify.Bidirectional = true
	cfg.HashOptions = HashOptions{
		NormalizeDatetime:       true,
		NormalizeFloatPrecision: 10,
		NormalizeDecimal128:     true,
		SortArrays:              false,
		CheckBinarySubtype:      true,
		StrictObjectID:          true,
	}
	cfg.Alert = AlertConfig{
		MissingThreshold:   0,
		DifferentThreshold: 0,
		StuckMinutes:       10,
	}
	return cfg
}

func validate(cfg *Config) error {
	if cfg.SourceURI == "" { return fmt.Errorf("source_uri is required") }
	if cfg.TargetURI == "" { return fmt.Errorf("target_uri is required") }
	if cfg.MonitorURI == "" { return fmt.Errorf("monitor_uri is required") }
	if cfg.BatchSize < 1 || cfg.BatchSize > 10000 {
		return fmt.Errorf("batch_size must be between 1 and 10000")
	}
	if cfg.MaxWorkers < 1 || cfg.MaxWorkers > 32 {
		return fmt.Errorf("max_workers must be between 1 and 32")
	}
	if cfg.SampleRate < 0 || cfg.SampleRate > 1 {
		return fmt.Errorf("sample_rate must be between 0 and 1")
	}
	return nil
}

// NSFilter decides whether a given namespace should be verified.
func (cfg *Config) NSFilter(dbName, colName string) bool {
	ns := dbName + "." + colName

	// include_ns: only verify these (only takes effect if set)
	if len(cfg.IncludeNS) > 0 {
		for _, pattern := range cfg.IncludeNS {
			if matchNS(pattern, ns, dbName) {
				goto checkExclude
			}
		}
		return false
	}

checkExclude:
	// exclude_ns: skip these
	for _, pattern := range cfg.ExcludeNS {
		if matchNS(pattern, ns, dbName) {
			return false
		}
	}
	return true
}

func matchNS(pattern, ns, dbName string) bool {
	if pattern == ns { return true }
	// "db.*" matches every collection in that db
	if strings.HasSuffix(pattern, ".*") {
		prefix := strings.TrimSuffix(pattern, ".*")
		return prefix == dbName
	}
	return false
}

// DBInScope decides whether a whole database should be touched at all, based
// on the database portion of include_ns / exclude_ns. This is what lets
// --include-ns/--exclude-ns narrow a run down to one database or collection
// consistently across every Phase 1 structural check (databases, collections,
// views, GridFS), not just indexes and data.
func (cfg *Config) DBInScope(dbName string) bool {
	if len(cfg.IncludeNS) > 0 {
		found := false
		for _, pattern := range cfg.IncludeNS {
			if dbPart(pattern) == dbName {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for _, pattern := range cfg.ExcludeNS {
		// only a whole-database exclude ("db.*") takes the database out of
		// scope; excluding a single collection still leaves the rest of the
		// database in scope.
		if pattern == dbName+".*" {
			return false
		}
	}
	return true
}

func dbPart(pattern string) string {
	if i := strings.IndexByte(pattern, '.'); i >= 0 {
		return pattern[:i]
	}
	return pattern
}
