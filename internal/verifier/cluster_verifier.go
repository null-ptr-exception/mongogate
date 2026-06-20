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
	}

	// ── Server Parameters ──
	params := []string{"slowOpThresholdMs", "maxIncomingConnections"}
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
	if err := result.Decode(&res); err != nil { return "" }
	return fmt.Sprintf("%v", res[param])
}

func countShards(ctx context.Context, client *mongo.Client) int {
	cur, err := client.Database("config").Collection("shards").Find(ctx, bson.M{})
	if err != nil { return 0 }
	defer cur.Close(ctx)
	count := 0
	for cur.Next(ctx) { count++ }
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
	if err != nil { return nil }
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

func getInt32(m bson.M, key string) int32 {
	if v, ok := m[key].(int32); ok { return v }
	return 0
}
func getBool(m bson.M, key string) bool {
	if v, ok := m[key].(bool); ok { return v }
	return false
}
