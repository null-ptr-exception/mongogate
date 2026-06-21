package verifier

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/null-ptr-exception/mongogate/internal/report"
)

func VerifyCluster(ctx context.Context, src, tgt *mongo.Client,
	verifySharding bool, rpt *report.Report) {

	fmt.Println("\n🖥️  [Phase1] Verifying cluster settings")
	var errors []string

	// ── Replica Set ──
	srcRS := getReplicaSetConfig(ctx, src)
	tgtRS := getReplicaSetConfig(ctx, tgt)
	if srcRS != nil && tgtRS != nil {
		if getInt32(srcRS, "protocolVersion") != getInt32(tgtRS, "protocolVersion") {
			errors = append(errors, "⚠️  Replica Set protocolVersion differs")
		}
		if getBool(srcRS, "writeConcernMajorityJournalDefault") !=
			getBool(tgtRS, "writeConcernMajorityJournalDefault") {
			errors = append(errors, "⚠️  writeConcernMajorityJournalDefault differs")
		}
		// Member topology: the full replset config was already fetched above
		// but, until now, every field except the two scalars checked above
		// was fetched and then ignored - member count, arbiters, voting
		// members, hidden/delayed secondaries were never compared. Raw
		// per-host comparison isn't useful here (hostnames always differ
		// between source and target), so this compares aggregate
		// characteristics instead.
		srcTopo := summarizeTopology(srcRS)
		tgtTopo := summarizeTopology(tgtRS)
		if srcTopo != tgtTopo {
			errors = append(errors, fmt.Sprintf(
				"⚠️  Replica set topology differs: src={members=%d arbiters=%d voters=%d hidden=%d delayed=%d} tgt={members=%d arbiters=%d voters=%d hidden=%d delayed=%d}",
				srcTopo.Total, srcTopo.Arbiters, srcTopo.Voters, srcTopo.Hidden, srcTopo.Delayed,
				tgtTopo.Total, tgtTopo.Arbiters, tgtTopo.Voters, tgtTopo.Hidden, tgtTopo.Delayed))
		}
	}

	// ── Server Parameters ──
	// Still a finite list, not every parameter MongoDB has (hundreds exist,
	// many version-gated or internal-only) - these 5 were chosen because
	// they're deliberately-configured operational knobs with stable
	// defaults across 4.4-8.0 (not version-driven), so a mismatch here is a
	// real config difference, not a version artifact.
	params := []string{
		"slowOpThresholdMs", "maxIncomingConnections",
		"notablescan", "journalCommitInterval", "cursorTimeoutMillis",
	}
	for _, param := range params {
		sv := getServerParam(ctx, src, param)
		tv := getServerParam(ctx, tgt, param)
		if sv != tv && sv != "" {
			errors = append(errors,
				fmt.Sprintf("⚠️  %s: src=%s tgt=%s", param, sv, tv))
		}
	}

	// ── Sharding (optional) ──
	if verifySharding {
		srcShards := countShards(ctx, src)
		tgtShards := countShards(ctx, tgt)
		if srcShards != tgtShards {
			errors = append(errors,
				fmt.Sprintf("❌ Shard count differs: src=%d tgt=%d", srcShards, tgtShards))
		}
		shardKeyErrors := verifyShardKeys(ctx, src, tgt)
		errors = append(errors, shardKeyErrors...)
	}

	res := &report.Result{Passed: len(errors) == 0, Errors: errors}
	rpt.SetCluster(res)
	printStatus("Cluster settings", res.Passed, "")
}

// FetchVersionInfo records each side's MongoDB version, FCV, and default
// read/write concern on the report as informational, non-blocking context -
// none of it adds to errors or affects Passed. All three are confirmed to
// differ by default on any cross-version pair (different binaries default
// to different FCV on initiate, and MongoDB's implicit default write
// concern calculation itself changed by version - a fresh 4.4 cluster
// reports no defaultWriteConcern at all, a fresh 8.0 cluster reports
// {w:"majority"}), so treating a mismatch here as a blocking error would
// fail every single cross-version run regardless of whether anything is
// actually wrong - same reasoning as FCV. Deliberately not folded into a
// generic collection comparison: admin.system.version holds the FCV
// document, but admin is skipped by default (see config.go) because
// raw-comparing the rest of that database guarantees false positives on
// admin.system.users (fresh SCRAM salt per side) - this is the dedicated,
// correct way to surface both instead of relying on that side effect.
func FetchVersionInfo(ctx context.Context, src, tgt *mongo.Client, rpt *report.Report) {
	rpt.SetVersions(&report.VersionInfo{
		SrcVersion:             getBuildInfoVersion(ctx, src),
		TgtVersion:             getBuildInfoVersion(ctx, tgt),
		SrcFCV:                 getFCV(ctx, src),
		TgtFCV:                 getFCV(ctx, tgt),
		SrcDefaultWriteConcern: getDefaultWriteConcern(ctx, src),
		TgtDefaultWriteConcern: getDefaultWriteConcern(ctx, tgt),
	})
}

func getBuildInfoVersion(ctx context.Context, client *mongo.Client) string {
	result := client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}})
	var res bson.M
	if err := result.Decode(&res); err != nil {
		return ""
	}
	return fmt.Sprintf("%v", res["version"])
}

func getFCV(ctx context.Context, client *mongo.Client) string {
	result := client.Database("admin").RunCommand(ctx,
		bson.D{{Key: "getParameter", Value: 1}, {Key: "featureCompatibilityVersion", Value: 1}})
	var res bson.M
	if err := result.Decode(&res); err != nil {
		return ""
	}
	fcv, _ := res["featureCompatibilityVersion"].(bson.M)
	return fmt.Sprintf("%v", fcv["version"])
}

func getDefaultWriteConcern(ctx context.Context, client *mongo.Client) string {
	result := client.Database("admin").RunCommand(ctx, bson.D{{Key: "getDefaultRWConcern", Value: 1}})
	var res bson.M
	if err := result.Decode(&res); err != nil {
		return ""
	}
	if res["defaultWriteConcern"] == nil {
		return "(none - implicit per-version default applies)"
	}
	return fmt.Sprintf("%v", res["defaultWriteConcern"])
}

func getReplicaSetConfig(ctx context.Context, client *mongo.Client) bson.M {
	result := client.Database("admin").RunCommand(ctx,
		bson.D{{Key: "replSetGetConfig", Value: 1}})
	var res bson.M
	if err := result.Decode(&res); err != nil {
		return nil
	}
	cfg, _ := res["config"].(bson.M)
	return cfg
}

func getServerParam(ctx context.Context, client *mongo.Client, param string) string {
	result := client.Database("admin").RunCommand(ctx,
		bson.D{{Key: "getParameter", Value: 1}, {Key: param, Value: 1}})
	var res bson.M
	if err := result.Decode(&res); err != nil {
		return ""
	}
	return fmt.Sprintf("%v", res[param])
}

func countShards(ctx context.Context, client *mongo.Client) int {
	cur, err := client.Database("config").Collection("shards").Find(ctx, bson.M{})
	if err != nil {
		return 0
	}
	defer cur.Close(ctx)
	count := 0
	for cur.Next(ctx) {
		count++
	}
	return count
}

func verifyShardKeys(ctx context.Context, src, tgt *mongo.Client) []string {
	var errors []string
	srcKeys := getShardKeys(ctx, src)
	tgtKeys := getShardKeys(ctx, tgt)
	for ns, key := range srcKeys {
		if tgtKeys[ns] != key {
			errors = append(errors, fmt.Sprintf("❌ Shard key differs: %s src=%s tgt=%s",
				ns, key, tgtKeys[ns]))
		}
	}
	return errors
}

func getShardKeys(ctx context.Context, client *mongo.Client) map[string]string {
	cur, err := client.Database("config").Collection("collections").Find(ctx, bson.M{})
	if err != nil {
		return nil
	}
	defer cur.Close(ctx)
	keys := make(map[string]string)
	for cur.Next(ctx) {
		var doc bson.M
		_ = cur.Decode(&doc)
		ns, _ := doc["_id"].(string)
		keys[ns] = fmt.Sprintf("%v", doc["key"])
	}
	return keys
}

// topologySummary captures replica set member topology at an aggregate
// level (counts, not per-host identity), since exact host addresses always
// differ between source and target and would never be useful to compare.
type topologySummary struct {
	Total    int
	Arbiters int
	Voters   int
	Hidden   int
	Delayed  int
}

func summarizeTopology(cfg bson.M) topologySummary {
	var s topologySummary
	members, _ := cfg["members"].(bson.A)
	s.Total = len(members)
	for _, m := range members {
		mm, ok := m.(bson.M)
		if !ok {
			continue
		}
		if v, ok := mm["arbiterOnly"].(bool); ok && v {
			s.Arbiters++
		}
		votes := int32(1)
		if v, ok := mm["votes"].(int32); ok {
			votes = v
		}
		if votes > 0 {
			s.Voters++
		}
		if v, ok := mm["hidden"].(bool); ok && v {
			s.Hidden++
		}
		if getMemberDelaySecs(mm) > 0 {
			s.Delayed++
		}
	}
	return s
}

// getMemberDelaySecs reads either the current field name (secondaryDelaySecs,
// 5.0+) or its deprecated predecessor (slaveDelay), whichever is present.
func getMemberDelaySecs(mm bson.M) int32 {
	if v, ok := mm["secondaryDelaySecs"].(int32); ok {
		return v
	}
	if v, ok := mm["slaveDelay"].(int32); ok {
		return v
	}
	return 0
}

func getInt32(m bson.M, key string) int32 {
	if v, ok := m[key].(int32); ok {
		return v
	}
	return 0
}
func getBool(m bson.M, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}
