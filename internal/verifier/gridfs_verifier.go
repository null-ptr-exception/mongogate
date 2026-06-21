package verifier

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"

	"github.com/null-ptr-exception/mongogate/internal/report"
)

func VerifyGridFS(ctx context.Context, src, tgt *mongo.Client,
	dbName string, fullVerify bool, rpt *report.Report) {

	fmt.Printf("\n🗂️  [Phase] Verifying GridFS [%s]\n", dbName)
	var errors []string

	srcFiles := getGridFSFiles(ctx, src, dbName)
	tgtFiles := getGridFSFiles(ctx, tgt, dbName)

	for id, sf := range srcFiles {
		tf, ok := tgtFiles[id]
		if !ok {
			errors = append(errors,
				fmt.Sprintf("❌ Missing GridFS file: %s (%s)", sf["filename"], id))
			continue
		}
		// metadata comparison
		if fmt.Sprintf("%v", sf["length"]) != fmt.Sprintf("%v", tf["length"]) {
			errors = append(errors,
				fmt.Sprintf("❌ [%s] size differs: src=%v tgt=%v",
					sf["filename"], sf["length"], tf["length"]))
		}
		// Content hash (only checked in Phase 3 to avoid the extra cost, and
		// only when sizes already match - a size mismatch already proves the
		// content differs without needing to read it). This used to compare
		// the fs.files "md5" field, but the driver stopped writing one years
		// ago (FIPS compliance, removed industry-wide) - that field is always
		// absent on both sides, so the comparison never fired. Computed here
		// directly via a streaming SHA256 over the actual file content
		// instead, so a real byte-level mismatch is no longer silent.
		if fullVerify && fmt.Sprintf("%v", sf["length"]) == fmt.Sprintf("%v", tf["length"]) {
			srcHash, srcErr := gridFSContentHash(ctx, src, dbName, sf["_id"])
			tgtHash, tgtErr := gridFSContentHash(ctx, tgt, dbName, tf["_id"])
			if srcErr != nil || tgtErr != nil {
				errors = append(errors,
					fmt.Sprintf("⚠️  [%s] content hash unavailable: src_err=%v tgt_err=%v",
						sf["filename"], srcErr, tgtErr))
			} else if srcHash != tgtHash {
				errors = append(errors,
					fmt.Sprintf("❌ [%s] content differs (hash mismatch)", sf["filename"]))
			}
		}
		if fmt.Sprintf("%v", sf["filename"]) != fmt.Sprintf("%v", tf["filename"]) {
			errors = append(errors,
				fmt.Sprintf("⚠️  filename differs: src=%v tgt=%v",
					sf["filename"], tf["filename"]))
		}
	}
	for id, tf := range tgtFiles {
		if _, ok := srcFiles[id]; !ok {
			errors = append(errors,
				fmt.Sprintf("⚠️  Extra GridFS file in target: %s (%s)", tf["filename"], id))
		}
	}

	res := &report.Result{Passed: len(errors) == 0, Errors: errors}
	rpt.SetGridFS(dbName, res)
	printStatus(fmt.Sprintf("GridFS [%s]", dbName), res.Passed,
		fmt.Sprintf("src=%d tgt=%d files", len(srcFiles), len(tgtFiles)))
}

// gridFSContentHash streams a GridFS file's content through SHA256 without
// buffering it all in memory - io.Copy reads in fixed-size chunks regardless
// of file size, the same streaming property the rest of this tool relies on
// to keep memory flat (see docs/TESTING.md's large-document findings for
// what happens when that property doesn't hold).
func gridFSContentHash(_ context.Context, client *mongo.Client, dbName string, fileID interface{}) (string, error) {
	bucket, err := gridfs.NewBucket(client.Database(dbName))
	if err != nil {
		return "", err
	}
	stream, err := bucket.OpenDownloadStream(fileID)
	if err != nil {
		return "", err
	}
	defer stream.Close()

	h := sha256.New()
	if _, err := io.Copy(h, stream); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func getGridFSFiles(ctx context.Context, client *mongo.Client,
	dbName string) map[string]bson.M {

	cur, err := client.Database(dbName).Collection("fs.files").
		Find(ctx, bson.M{})
	if err != nil { return nil }
	defer cur.Close(ctx)

	files := make(map[string]bson.M)
	for cur.Next(ctx) {
		var doc bson.M
		_ = cur.Decode(&doc)
		id := fmt.Sprintf("%v", doc["_id"])
		files[id] = doc
	}
	return files
}
