//go:build integration_agent

package agent_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
	outputpkg "github.com/jimyag/logpilot/internal/log-pilot-agent/output"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/runner"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/transform"
)

// TestPipelineEndToEnd verifies the complete Input→Transform→Output pipeline:
// write JSON log lines → FileInput reads them → json+drop+label transforms →
// FileOutput writes filtered/enriched records.
func TestPipelineEndToEnd(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	outFile := filepath.Join(dir, "out.json")
	metaDir := t.TempDir()

	// Seed log file with 3 lines: INFO, DEBUG (to be dropped), WARN.
	content := `{"level":"INFO","msg":"hello"}` + "\n" +
		`{"level":"DEBUG","msg":"debug line"}` + "\n" +
		`{"level":"WARN","msg":"warn line"}` + "\n"
	if err := os.WriteFile(logFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	// Build pipeline.
	in, err := input.NewFileInput(input.FileConfig{
		Path:              logFile,
		ReadFrom:          "oldest",
		OffsetCommitEvery: 1,
	}, metaDir)
	if err != nil {
		t.Fatal(err)
	}

	transforms := []transform.Transform{
		transform.NewJSONTransform(),
		transform.NewDropTransform("level", "DEBUG"),
		transform.NewLabelTransform(map[string]string{"env": "test"}),
	}

	outSpec := logpilotv1alpha1.OutputSpec{
		Type: "file",
		Config: map[string]apiextensionsv1.JSON{
			"path": {Raw: []byte(`"` + outFile + `"`)},
		},
	}
	out, err := outputpkg.NewFromSpec(outSpec)
	if err != nil {
		t.Fatal(err)
	}

	r := runner.New(runner.Config{
		Name:       "integration-test",
		Input:      in,
		Transforms: transforms,
		Output:     out,
		BatchLen:   10,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.Run(ctx)

	// Parse output.
	raw, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	lines := splitJSONLines(raw)
	if len(lines) != 2 {
		t.Fatalf("expected 2 records (DEBUG dropped), got %d\ncontent:\n%s", len(lines), raw)
	}

	for _, line := range lines {
		var m map[string]interface{}
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("invalid JSON: %v — line: %s", err, line)
		}
		if m["env"] != "test" {
			t.Errorf("expected env=test label, got %v", m["env"])
		}
		if m["level"] == "DEBUG" {
			t.Error("DEBUG record should have been dropped")
		}
	}

	// First record should be INFO hello.
	var first map[string]interface{}
	json.Unmarshal(lines[0], &first)
	if first["msg"] != "hello" {
		t.Errorf("expected first msg=hello, got %v", first["msg"])
	}
}

// TestPipelineOffsetRecovery verifies the pipeline resumes from saved offset on restart.
func TestPipelineOffsetRecovery(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	outFile := filepath.Join(dir, "out.json")
	metaDir := t.TempDir()

	// Write initial content.
	if err := os.WriteFile(logFile, []byte("line1\nline2\n"), 0644); err != nil {
		t.Fatal(err)
	}

	runPipeline := func(outPath string) {
		in, _ := input.NewFileInput(input.FileConfig{
			Path:              logFile,
			ReadFrom:          "oldest",
			OffsetCommitEvery: 1,
		}, metaDir)
		out := outputpkg.NewFileOutput(outPath)
		r := runner.New(runner.Config{
			Input:    in,
			Output:   out,
			BatchLen: 10,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		r.Run(ctx)
	}

	// First run: reads line1 + line2, saves offset.
	runPipeline(outFile)

	// Append line3.
	f, _ := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0644)
	f.WriteString("line3\n")
	f.Close()

	// Second run: should only read line3.
	outFile2 := filepath.Join(dir, "out2.json")
	runPipeline(outFile2)

	raw2, _ := os.ReadFile(outFile2)
	lines2 := splitJSONLines(raw2)
	if len(lines2) != 1 {
		t.Fatalf("expected 1 record in second run, got %d\ncontent:\n%s", len(lines2), raw2)
	}
}

func splitJSONLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' && i > start {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	return lines
}
