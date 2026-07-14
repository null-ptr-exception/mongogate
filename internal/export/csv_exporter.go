package export

import (
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/null-ptr-exception/mongogate/internal/report"
)

// ExportDiffCSV writes every diff found to a CSV file for DBA review.
func ExportDiffCSV(rpt *report.Report, path string) error {
	if path == "" {
		return nil
	}

	// "auto" appends a timestamp so files never collide.
	if path == "auto" {
		path = fmt.Sprintf("diff_%s.csv", time.Now().Format("20060102_150405"))
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create CSV: %w", err)
	}
	defer f.Close()

	// UTF-8 BOM so Excel renders non-ASCII content correctly.
	_, _ = f.Write([]byte{0xEF, 0xBB, 0xBF})

	w := csv.NewWriter(f)
	defer w.Flush()

	// Header
	_ = w.Write([]string{
		"ns", "issue_type", "doc_id", "path",
		"src_type", "tgt_type", "src_value", "tgt_value", "detected_at",
	})

	now := time.Now().Format("2006-01-02 15:04:05")
	data := rpt.GetData()

	for ns, result := range data {
		// Missing docs
		for _, id := range result.MissingSample {
			_ = w.Write([]string{
				ns, "MISSING_DOC", id, "", "", "", "", "", now,
			})
		}
		// Different docs (with field-level detail)
		for _, sample := range result.DiffSample {
			_ = w.Write([]string{
				ns, "DIFFERENT_DOC", sample, "", "", "", "", "", now,
			})
		}
		// Docs that only exist in target (bidirectional scan)
		for _, id := range result.ExtraInTargetSample {
			_ = w.Write([]string{
				ns, "EXTRA_IN_TARGET", id, "", "", "", "", "", now,
			})
		}
		// Field-level errors
		for _, e := range result.FieldErrors {
			_ = w.Write([]string{
				ns, e.IssueType, e.DocID, e.Path,
				e.SrcType, e.TgtType, e.SrcValue, e.TgtValue, now,
			})
		}
	}

	fmt.Printf("\n📄 Diff report exported: %s\n", path)
	return nil
}
