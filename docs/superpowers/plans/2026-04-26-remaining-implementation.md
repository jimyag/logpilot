# logpilot Remaining Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete logpilot implementation by wiring the pipeline (buildRunner), adding offset persistence, implementing Clean strategies, replacing polling with informer, implementing the Operator deployment reconciler, and adding integration tests.

**Architecture:** buildRunner constructs a live Input→Transform→Output→Clean pipeline from ContainerPolicy by dispatching to factories. Offset is persisted atomically to MetaPath. The Operator reconciler creates/updates a Deployment for log-pilot-api and a DaemonSet for log-pilot-agent from the LogPilot CR spec.

**Tech Stack:** Go 1.26, controller-runtime v0.20, nxadm/tail, encoding/json for offset files

---

## Chunk 1: Output factory + Transform factory + buildRunner

### Task 1: Output factory

**Files:**
- Create: `internal/log-pilot-agent/output/factory.go`
- Create: `internal/log-pilot-agent/output/factory_test.go`

The factory maps `OutputSpec.Type` → a concrete `Output` implementation. Currently only `http` and `file` exist; this factory wires them up from config.

- [ ] **Step 1: Write failing test**

`internal/log-pilot-agent/output/factory_test.go`:

```go
package output

import (
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func TestNewOutputHTTP(t *testing.T) {
	spec := logpilotv1alpha1.OutputSpec{
		Type: "http",
		Config: map[string]apiextensionsv1.JSON{
			"url": {Raw: []byte(`"http://localhost:9999"`)},
		},
	}
	out, err := NewFromSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("expected non-nil output")
	}
}

func TestNewOutputFile(t *testing.T) {
	spec := logpilotv1alpha1.OutputSpec{
		Type: "file",
		Config: map[string]apiextensionsv1.JSON{
			"path": {Raw: []byte(`"/tmp/test-out.json"`)},
		},
	}
	out, err := NewFromSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("expected non-nil output")
	}
}

func TestNewOutputUnknown(t *testing.T) {
	spec := logpilotv1alpha1.OutputSpec{Type: "unknown"}
	_, err := NewFromSpec(spec)
	if err == nil {
		t.Fatal("expected error for unknown output type")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/jimyag/src/github/jimyag/logpilot
go test ./internal/log-pilot-agent/output/... 2>&1 | head -5
```

Expected: FAIL — `NewFromSpec` undefined.

- [ ] **Step 3: Implement factory**

`internal/log-pilot-agent/output/factory.go`:

```go
package output

import (
	"encoding/json"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

// NewFromSpec creates an Output from an OutputSpec.
func NewFromSpec(spec logpilotv1alpha1.OutputSpec) (Output, error) {
	switch spec.Type {
	case "http":
		url, err := extractString(spec.Config, "url")
		if err != nil {
			return nil, fmt.Errorf("http output: %w", err)
		}
		return NewHTTPOutput(HTTPConfig{URL: url}), nil

	case "file":
		path, err := extractString(spec.Config, "path")
		if err != nil {
			return nil, fmt.Errorf("file output: %w", err)
		}
		return NewFileOutput(path), nil

	default:
		return nil, fmt.Errorf("unknown output type: %q", spec.Type)
	}
}

func extractString(config map[string]apiextensionsv1.JSON, key string) (string, error) {
	v, ok := config[key]
	if !ok {
		return "", fmt.Errorf("missing required config key %q", key)
	}
	var s string
	if err := json.Unmarshal(v.Raw, &s); err != nil {
		return "", fmt.Errorf("config key %q: %w", key, err)
	}
	return s, nil
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/log-pilot-agent/output/... -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/log-pilot-agent/output/factory.go internal/log-pilot-agent/output/factory_test.go
git commit -m "feat(agent): add output factory"
```

---

### Task 2: Transform factory

**Files:**
- Create: `internal/log-pilot-agent/transform/factory.go`
- Create: `internal/log-pilot-agent/transform/factory_test.go`

- [ ] **Step 1: Write failing test**

`internal/log-pilot-agent/transform/factory_test.go`:

```go
package transform

import (
	"context"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

func TestNewFromSpecJSON(t *testing.T) {
	spec := logpilotv1alpha1.TransformSpec{Type: "json"}
	tr, err := NewFromSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	records := []input.Record{{Data: []byte(`{"level":"INFO"}`)}}
	out, _ := tr.Transform(context.Background(), records)
	if out[0].Meta["level"] != "INFO" {
		t.Errorf("expected level=INFO, got %q", out[0].Meta["level"])
	}
}

func TestNewFromSpecLabel(t *testing.T) {
	spec := logpilotv1alpha1.TransformSpec{
		Type: "label",
		Config: map[string]apiextensionsv1.JSON{
			"fields": {Raw: []byte(`{"pod":"mypod"}`)},
		},
	}
	tr, err := NewFromSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	records := []input.Record{{Data: []byte("hello")}}
	out, _ := tr.Transform(context.Background(), records)
	if out[0].Meta["pod"] != "mypod" {
		t.Errorf("expected pod=mypod, got %q", out[0].Meta["pod"])
	}
}

func TestNewFromSpecDrop(t *testing.T) {
	spec := logpilotv1alpha1.TransformSpec{
		Type: "drop",
		Config: map[string]apiextensionsv1.JSON{
			"key":   {Raw: []byte(`"level"`)},
			"value": {Raw: []byte(`"DEBUG"`)},
		},
	}
	tr, err := NewFromSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	records := []input.Record{
		{Data: []byte("debug"), Meta: map[string]string{"level": "DEBUG"}},
		{Data: []byte("info"), Meta: map[string]string{"level": "INFO"}},
	}
	out, _ := tr.Transform(context.Background(), records)
	if len(out) != 1 {
		t.Fatalf("expected 1 record after drop, got %d", len(out))
	}
}

func TestNewFromSpecUnknown(t *testing.T) {
	_, err := NewFromSpec(logpilotv1alpha1.TransformSpec{Type: "unknown"})
	if err == nil {
		t.Fatal("expected error for unknown transform type")
	}
}

func TestNewSliceFromSpecs(t *testing.T) {
	specs := []logpilotv1alpha1.TransformSpec{
		{Type: "json"},
		{Type: "label", Config: map[string]apiextensionsv1.JSON{
			"fields": {Raw: []byte(`{"app":"test"}`)},
		}},
	}
	transforms, err := NewSliceFromSpecs(specs)
	if err != nil {
		t.Fatal(err)
	}
	if len(transforms) != 2 {
		t.Fatalf("expected 2 transforms, got %d", len(transforms))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/log-pilot-agent/transform/... 2>&1 | head -5
```

Expected: FAIL.

- [ ] **Step 3: Implement factory**

`internal/log-pilot-agent/transform/factory.go`:

```go
package transform

import (
	"encoding/json"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

// NewFromSpec creates a Transform from a TransformSpec.
func NewFromSpec(spec logpilotv1alpha1.TransformSpec) (Transform, error) {
	switch spec.Type {
	case "json":
		return NewJSONTransform(), nil

	case "label":
		fields, err := extractStringMap(spec.Config, "fields")
		if err != nil {
			return nil, fmt.Errorf("label transform: %w", err)
		}
		return NewLabelTransform(fields), nil

	case "drop":
		key, err := extractString(spec.Config, "key")
		if err != nil {
			return nil, fmt.Errorf("drop transform: %w", err)
		}
		value, err := extractString(spec.Config, "value")
		if err != nil {
			return nil, fmt.Errorf("drop transform: %w", err)
		}
		return NewDropTransform(key, value), nil

	default:
		return nil, fmt.Errorf("unknown transform type: %q", spec.Type)
	}
}

// NewSliceFromSpecs creates a slice of Transforms from a slice of TransformSpecs.
func NewSliceFromSpecs(specs []logpilotv1alpha1.TransformSpec) ([]Transform, error) {
	transforms := make([]Transform, 0, len(specs))
	for _, s := range specs {
		t, err := NewFromSpec(s)
		if err != nil {
			return nil, err
		}
		transforms = append(transforms, t)
	}
	return transforms, nil
}

func extractString(config map[string]apiextensionsv1.JSON, key string) (string, error) {
	v, ok := config[key]
	if !ok {
		return "", fmt.Errorf("missing required config key %q", key)
	}
	var s string
	if err := json.Unmarshal(v.Raw, &s); err != nil {
		return "", fmt.Errorf("config key %q: %w", key, err)
	}
	return s, nil
}

func extractStringMap(config map[string]apiextensionsv1.JSON, key string) (map[string]string, error) {
	v, ok := config[key]
	if !ok {
		return nil, fmt.Errorf("missing required config key %q", key)
	}
	var m map[string]string
	if err := json.Unmarshal(v.Raw, &m); err != nil {
		return nil, fmt.Errorf("config key %q: %w", key, err)
	}
	return m, nil
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/log-pilot-agent/transform/... -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/log-pilot-agent/transform/factory.go internal/log-pilot-agent/transform/factory_test.go
git commit -m "feat(agent): add transform factory"
```

---

### Task 3: Wire up buildRunner

**Files:**
- Modify: `internal/log-pilot-agent/watcher/watcher.go` — replace buildRunner stub
- Create: `internal/log-pilot-agent/watcher/watcher_test.go`

- [ ] **Step 1: Write failing test for buildRunner**

`internal/log-pilot-agent/watcher/watcher_test.go`:

```go
package watcher

import (
	"os"
	"path/filepath"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func TestBuildRunnerFileOutput(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "out.json")
	logPath := t.TempDir()

	cp := logpilotv1alpha1.ContainerPolicy{
		Name:      "app",
		LogType:   "applog",
		Path:      "/app/logs",
		Delivery:  "guaranteed",
		BatchLen:  10,
		Output: logpilotv1alpha1.OutputSpec{
			Type: "file",
			Config: map[string]apiextensionsv1.JSON{
				"path": {Raw: []byte(`"` + outPath + `"`)},
			},
		},
	}

	cfg := Config{
		LogDir:   t.TempDir(),
		MetaDir:  t.TempDir(),
	}

	r := buildRunner(cp, logPath, cfg)
	if r == nil {
		t.Fatal("expected non-nil runner")
	}
}

func TestBuildRunnerWithTransforms(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "out.json")
	logPath := t.TempDir()

	cp := logpilotv1alpha1.ContainerPolicy{
		Name:    "app",
		LogType: "applog",
		Transforms: []logpilotv1alpha1.TransformSpec{
			{Type: "json"},
			{Type: "label", Config: map[string]apiextensionsv1.JSON{
				"fields": {Raw: []byte(`{"env":"test"}`)},
			}},
		},
		Output: logpilotv1alpha1.OutputSpec{
			Type: "file",
			Config: map[string]apiextensionsv1.JSON{
				"path": {Raw: []byte(`"` + outPath + `"`)},
			},
		},
	}

	// Seed log file with content
	logFile := filepath.Join(logPath, "app.log")
	if err := os.WriteFile(logFile, []byte(`{"msg":"hello"}`+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	r := buildRunner(cp, logPath, Config{MetaDir: t.TempDir()})
	if r == nil {
		t.Fatal("expected non-nil runner")
	}
}
```

- [ ] **Step 2: Run test to verify it fails (or passes trivially)**

```bash
go test ./internal/log-pilot-agent/watcher/... 2>&1 | head -5
```

- [ ] **Step 3: Replace buildRunner with real implementation**

Replace the stub in `internal/log-pilot-agent/watcher/watcher.go`:

```go
func buildRunner(cp logpilotv1alpha1.ContainerPolicy, logPath string, cfg Config) *runner.Runner {
	batchLen := cp.BatchLen
	if batchLen == 0 {
		batchLen = 1000
	}

	// Build Input from log path (file type for pod logs).
	metaPath := filepath.Join(cfg.MetaDir, "LogPilotPolicy",
		fmt.Sprintf("%s_%s.offset", cp.Name, cp.LogType))
	fileInput, err := input.NewFileInput(input.FileConfig{
		Path:              filepath.Join(logPath, "*.log*"),
		MetaPath:          metaPath,
		ReadFrom:          "oldest",
		OffsetCommitEvery: 1000,
	}, cfg.MetaDir)
	if err != nil {
		// Log path may not exist yet; return a no-op runner that will be replaced on next add.
		return runner.New(runner.Config{BatchLen: batchLen})
	}

	// Build Transforms.
	transforms, err := transformfactory.NewSliceFromSpecs(cp.Transforms)
	if err != nil {
		transforms = nil
	}

	// Build Output.
	out, err := outputfactory.NewFromSpec(cp.Output)
	if err != nil {
		out = nil
	}

	return runner.New(runner.Config{
		Name:       fmt.Sprintf("%s/%s", cp.Name, cp.LogType),
		Input:      fileInput,
		Transforms: transforms,
		Output:     out,
		BatchLen:   batchLen,
	})
}
```

Add imports at top of watcher.go:

```go
import (
	// existing imports...
	"path/filepath"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
	outputfactory "github.com/jimyag/logpilot/internal/log-pilot-agent/output"
	transformfactory "github.com/jimyag/logpilot/internal/log-pilot-agent/transform"
)
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/log-pilot-agent/watcher/... -v
go build ./...
```

Expected: tests PASS, build OK.

- [ ] **Step 5: Commit**

```bash
git add internal/log-pilot-agent/watcher/
git commit -m "feat(agent): wire up full pipeline in buildRunner"
```

---

## Chunk 2: Offset persistence + Clean implementations

### Task 4: Offset persistence

**Files:**
- Modify: `internal/log-pilot-agent/input/file.go` — implement commitOffset persistence
- Modify: `internal/log-pilot-agent/input/file_test.go` — add restart recovery test

- [ ] **Step 1: Write failing test for offset recovery**

Add to `internal/log-pilot-agent/input/file_test.go`:

```go
func TestFileInputOffsetRecovery(t *testing.T) {
	dir := t.TempDir()
	logFile := dir + "/app.log"
	metaDir := t.TempDir()

	// Write 3 lines to the file.
	if err := os.WriteFile(logFile, []byte("line1\nline2\nline3\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// First read: read all 3 lines, which commits the offset.
	in1, err := NewFileInput(FileConfig{
		Path:              logFile,
		ReadFrom:          "oldest",
		OffsetCommitEvery: 1,
	}, metaDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var records []Record
	for len(records) < 3 {
		batch, _ := in1.ReadBatch(ctx, 10)
		records = append(records, batch...)
	}
	in1.Close()

	// Append a new line.
	f, _ := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0644)
	f.WriteString("line4\n")
	f.Close()

	// Second open: should resume from saved offset, read only line4.
	in2, err := NewFileInput(FileConfig{
		Path:              logFile,
		ReadFrom:          "oldest",
		OffsetCommitEvery: 1,
	}, metaDir)
	if err != nil {
		t.Fatal(err)
	}
	defer in2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	batch, _ := in2.ReadBatch(ctx2, 10)

	if len(batch) != 1 {
		t.Fatalf("expected 1 record after recovery, got %d", len(batch))
	}
	if string(batch[0].Data) != "line4" {
		t.Errorf("expected 'line4', got %q", batch[0].Data)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/log-pilot-agent/input/... -run TestFileInputOffsetRecovery -v
```

Expected: FAIL — reads from beginning instead of saved offset.

- [ ] **Step 3: Implement offset persistence in commitOffset**

Replace `commitOffset` in `internal/log-pilot-agent/input/file.go`:

```go
type offsetState struct {
	Offset int64 `json:"offset"`
}

func (f *fileInput) commitOffset() {
	pos, err := f.tail.Tell()
	if err != nil {
		pos = 0
	}

	// Update in-memory lag.
	if info, err := os.Stat(f.cfg.Path); err == nil {
		remaining := info.Size() - pos
		if remaining < 0 {
			remaining = 0
		}
		atomic.StoreInt64(&f.lag, remaining)
	}

	// Persist to MetaPath (write to temp file then rename for atomicity).
	if f.cfg.MetaPath == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(f.cfg.MetaPath), 0755); err != nil {
		return
	}
	state := offsetState{Offset: pos}
	raw, err := json.Marshal(state)
	if err != nil {
		return
	}
	tmp := f.cfg.MetaPath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0644); err != nil {
		return
	}
	_ = os.Rename(tmp, f.cfg.MetaPath) // atomic on Linux/macOS
}

func loadOffset(metaPath string) int64 {
	if metaPath == "" {
		return -1
	}
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		return -1
	}
	var state offsetState
	if err := json.Unmarshal(raw, &state); err != nil {
		return -1
	}
	return state.Offset
}
```

Update `NewFileInput` to restore offset on startup:

```go
// After computing seekInfo, check for saved offset.
if savedOffset := loadOffset(cfg.MetaPath); savedOffset >= 0 {
    seekInfo = &tail.SeekInfo{Offset: savedOffset, Whence: 0}
}
```

Add `"encoding/json"` to imports.

- [ ] **Step 4: Run tests**

```bash
go test ./internal/log-pilot-agent/input/... -v -timeout 30s
```

Expected: all PASS including TestFileInputOffsetRecovery.

- [ ] **Step 5: Commit**

```bash
git add internal/log-pilot-agent/input/file.go internal/log-pilot-agent/input/file_test.go
git commit -m "feat(agent): implement offset persistence and crash recovery"
```

---

### Task 5: Clean implementations

**Files:**
- Create: `internal/log-pilot-agent/clean/strategies.go`
- Create: `internal/log-pilot-agent/clean/strategies_test.go`

- [ ] **Step 1: Write failing tests**

`internal/log-pilot-agent/clean/strategies_test.go`:

```go
package clean

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func TestAfterCollectedClean(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logFile, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	spec := logpilotv1alpha1.CleanSpec{
		Strategy:          "afterCollected",
		ReserveFileNumber: 0,
		ReserveFileSize:   0,
		Interval:          1,
	}
	c := NewFromSpec(spec, dir)

	meta := RunnerMeta{DeletedAt: time.Time{}} // pod still running

	// File exists, lag=0 → should clean.
	should, err := c.ShouldClean(meta)
	if err != nil {
		t.Fatal(err)
	}
	if !should {
		t.Fatal("expected ShouldClean=true when lag=0 and file exists")
	}
}

func TestNeverClean(t *testing.T) {
	spec := logpilotv1alpha1.CleanSpec{Strategy: "never"}
	c := NewFromSpec(spec, t.TempDir())
	should, err := c.ShouldClean(RunnerMeta{})
	if err != nil {
		t.Fatal(err)
	}
	if should {
		t.Fatal("expected ShouldClean=false for strategy=never")
	}
}

func TestRetainClean(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	os.WriteFile(logFile, []byte("data"), 0644)

	// RetainDays=0 means clean immediately after collection.
	spec := logpilotv1alpha1.CleanSpec{Strategy: "retain", RetainDays: 0}
	c := NewFromSpec(spec, dir)
	should, _ := c.ShouldClean(RunnerMeta{})
	if !should {
		t.Fatal("expected ShouldClean=true with retainDays=0")
	}
}

func TestNewFromSpec(t *testing.T) {
	for _, strategy := range []string{"afterCollected", "retain", "never"} {
		c := NewFromSpec(logpilotv1alpha1.CleanSpec{Strategy: strategy}, t.TempDir())
		if c == nil {
			t.Fatalf("expected non-nil Clean for strategy %q", strategy)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/log-pilot-agent/clean/... 2>&1 | head -5
```

Expected: FAIL.

- [ ] **Step 3: Implement strategies**

`internal/log-pilot-agent/clean/strategies.go`:

```go
package clean

import (
	"os"
	"time"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

// NewFromSpec creates a Clean implementation from a CleanSpec.
func NewFromSpec(spec logpilotv1alpha1.CleanSpec, logDir string) Clean {
	switch spec.Strategy {
	case "never":
		return &neverClean{}
	case "retain":
		return &retainClean{logDir: logDir, retainDays: spec.RetainDays}
	default: // "afterCollected"
		return &afterCollectedClean{logDir: logDir}
	}
}

// neverClean never cleans up files.
type neverClean struct{}

func (c *neverClean) ShouldClean(_ RunnerMeta) (bool, error) { return false, nil }
func (c *neverClean) Clean(_ RunnerMeta) error               { return nil }

// afterCollectedClean removes files immediately after collection is complete.
type afterCollectedClean struct {
	logDir string
}

func (c *afterCollectedClean) ShouldClean(_ RunnerMeta) (bool, error) {
	entries, err := os.ReadDir(c.logDir)
	if err != nil {
		return false, nil
	}
	return len(entries) > 0, nil
}

func (c *afterCollectedClean) Clean(meta RunnerMeta) error {
	// Only clean files that have been collected (lag==0 is guaranteed by caller).
	entries, err := os.ReadDir(c.logDir)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		path := c.logDir + "/" + e.Name()
		_ = os.Remove(path)
	}
	return nil
}

// retainClean keeps files for RetainDays after collection before removing them.
type retainClean struct {
	logDir     string
	retainDays int
}

func (c *retainClean) ShouldClean(_ RunnerMeta) (bool, error) {
	entries, err := os.ReadDir(c.logDir)
	if err != nil {
		return false, nil
	}
	cutoff := time.Now().AddDate(0, 0, -c.retainDays)
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			return true, nil
		}
	}
	return false, nil
}

func (c *retainClean) Clean(_ RunnerMeta) error {
	entries, err := os.ReadDir(c.logDir)
	if err != nil {
		return nil
	}
	cutoff := time.Now().AddDate(0, 0, -c.retainDays)
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(c.logDir + "/" + e.Name())
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/log-pilot-agent/clean/... -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/log-pilot-agent/clean/
git commit -m "feat(agent): implement afterCollected, retain, never clean strategies"
```

---

## Chunk 3: Operator deployment reconciler

### Task 6: LogPilot reconciler deploys api + agent

**Files:**
- Create: `internal/log-pilot-operator/deploy.go`
- Create: `internal/log-pilot-operator/deploy_test.go`
- Modify: `internal/log-pilot-operator/logpilot_controller.go`

The reconciler creates or updates a Deployment for log-pilot-api and a DaemonSet for log-pilot-agent based on LogPilot CR spec.

- [ ] **Step 1: Write failing test for deploy helpers**

`internal/log-pilot-operator/deploy_test.go`:

```go
//go:build !integration

package operator

import (
	"testing"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func TestBuildAPIDeployment(t *testing.T) {
	lp := &logpilotv1alpha1.LogPilot{}
	lp.Name = "logpilot"
	lp.Namespace = "logpilot-system"
	lp.Spec.API.Replicas = 2

	deploy := buildAPIDeployment(lp, "log-pilot-api:latest")
	if deploy.Name != "log-pilot-api" {
		t.Errorf("expected name log-pilot-api, got %q", deploy.Name)
	}
	if *deploy.Spec.Replicas != 2 {
		t.Errorf("expected 2 replicas, got %d", *deploy.Spec.Replicas)
	}
	if deploy.Namespace != "logpilot-system" {
		t.Errorf("expected namespace logpilot-system, got %q", deploy.Namespace)
	}
}

func TestBuildAgentDaemonSet(t *testing.T) {
	lp := &logpilotv1alpha1.LogPilot{}
	lp.Name = "logpilot"
	lp.Namespace = "logpilot-system"
	lp.Spec.Agent.LogDir = "/var/log/log-pilot"

	ds := buildAgentDaemonSet(lp, "log-pilot-agent:latest")
	if ds.Name != "log-pilot-agent" {
		t.Errorf("expected name log-pilot-agent, got %q", ds.Name)
	}

	// Verify hostPath volume is present.
	found := false
	for _, v := range ds.Spec.Template.Spec.Volumes {
		if v.HostPath != nil && v.HostPath.Path == "/var/log/log-pilot" {
			found = true
		}
	}
	if !found {
		t.Error("expected hostPath volume /var/log/log-pilot in agent DaemonSet")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/log-pilot-operator/... -tags '!integration' 2>&1 | head -5
```

Expected: FAIL — `buildAPIDeployment` undefined.

- [ ] **Step 3: Implement deploy helpers**

`internal/log-pilot-operator/deploy.go`:

```go
package operator

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func buildAPIDeployment(lp *logpilotv1alpha1.LogPilot, image string) *appsv1.Deployment {
	replicas := lp.Spec.API.Replicas
	if replicas == 0 {
		replicas = 2
	}
	labels := map[string]string{
		"app.kubernetes.io/name":      "log-pilot-api",
		"app.kubernetes.io/component": "webhook",
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "log-pilot-api",
			Namespace: lp.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: "log-pilot-api",
					Containers: []corev1.Container{{
						Name:      "log-pilot-api",
						Image:     image,
						Resources: lp.Spec.API.Resources,
					}},
				},
			},
		},
	}
}

func buildAgentDaemonSet(lp *logpilotv1alpha1.LogPilot, image string) *appsv1.DaemonSet {
	logDir := lp.Spec.Agent.LogDir
	if logDir == "" {
		logDir = "/var/log/log-pilot"
	}
	metaDir := lp.Spec.Agent.MetaDir
	if metaDir == "" {
		metaDir = "/var/lib/log-pilot-agent"
	}

	labels := map[string]string{
		"app.kubernetes.io/name":      "log-pilot-agent",
		"app.kubernetes.io/component": "agent",
	}
	hostPathType := corev1.HostPathDirectoryOrCreate

	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "log-pilot-agent",
			Namespace: lp.Namespace,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: "log-pilot-agent",
					Containers: []corev1.Container{{
						Name:      "log-pilot-agent",
						Image:     image,
						Resources: lp.Spec.Agent.Resources,
						Env: []corev1.EnvVar{
							{
								Name: "NODE_NAME",
								ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
								},
							},
							{Name: "LOG_DIR", Value: logDir},
							{Name: "META_DIR", Value: metaDir},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "log-dir", MountPath: logDir},
							{Name: "meta-dir", MountPath: metaDir},
						},
					}},
					Volumes: []corev1.Volume{
						{
							Name: "log-dir",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: logDir,
									Type: &hostPathType,
								},
							},
						},
						{
							Name: "meta-dir",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: metaDir,
									Type: &hostPathType,
								},
							},
						},
					},
				},
			},
		},
	}
}
```

- [ ] **Step 4: Wire into Reconcile**

Replace the TODO block in `internal/log-pilot-operator/logpilot_controller.go`:

```go
func (r *LogPilotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var lp logpilotv1alpha1.LogPilot
	if err := r.Get(ctx, req.NamespacedName, &lp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	apiImage := os.Getenv("LOG_PILOT_API_IMAGE")
	if apiImage == "" {
		apiImage = "ghcr.io/jimyag/logpilot/log-pilot-api:latest"
	}
	agentImage := os.Getenv("LOG_PILOT_AGENT_IMAGE")
	if agentImage == "" {
		agentImage = "ghcr.io/jimyag/logpilot/log-pilot-agent:latest"
	}

	// Reconcile log-pilot-api Deployment.
	apiDeploy := buildAPIDeployment(&lp, apiImage)
	if err := ctrl.SetControllerReference(&lp, apiDeploy, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := reconcileDeployment(ctx, r.Client, apiDeploy); err != nil {
		log.Error(err, "failed to reconcile log-pilot-api deployment")
		return ctrl.Result{}, err
	}

	// Reconcile log-pilot-agent DaemonSet.
	agentDS := buildAgentDaemonSet(&lp, agentImage)
	if err := ctrl.SetControllerReference(&lp, agentDS, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := reconcileDaemonSet(ctx, r.Client, agentDS); err != nil {
		log.Error(err, "failed to reconcile log-pilot-agent daemonset")
		return ctrl.Result{}, err
	}

	log.Info("reconciled LogPilot", "name", lp.Name)
	return ctrl.Result{}, nil
}
```

Add helper functions to `deploy.go`:

```go
func reconcileDeployment(ctx context.Context, c client.Client, desired *appsv1.Deployment) error {
	existing := &appsv1.Deployment{}
	err := c.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return c.Create(ctx, desired)
	}
	existing.Spec = desired.Spec
	return c.Update(ctx, existing)
}

func reconcileDaemonSet(ctx context.Context, c client.Client, desired *appsv1.DaemonSet) error {
	existing := &appsv1.DaemonSet{}
	err := c.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return c.Create(ctx, desired)
	}
	existing.Spec = desired.Spec
	return c.Update(ctx, existing)
}
```

Add imports `"os"`, `apierrors "k8s.io/apimachinery/pkg/api/errors"`, `"context"`, `"sigs.k8s.io/controller-runtime/pkg/client"`.

- [ ] **Step 5: Run tests**

```bash
go test ./internal/log-pilot-operator/... -tags '!integration' -v
go build ./...
```

Expected: PASS, build OK.

- [ ] **Step 6: Commit**

```bash
git add internal/log-pilot-operator/
git commit -m "feat(operator): implement LogPilot reconciler to deploy api and agent"
```

---

## Chunk 4: Integration tests

### Task 7: End-to-end pipeline integration test

**Files:**
- Create: `internal/log-pilot-agent/integration_test.go`

Tests the complete pipeline: write file → FileInput reads → Transform → FileOutput writes.

- [ ] **Step 1: Write integration test**

`internal/log-pilot-agent/integration_test.go`:

```go
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

// TestPipelineEndToEnd writes log lines to a file and verifies the full
// Input→Transform→Output pipeline produces correct output.
func TestPipelineEndToEnd(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	outFile := filepath.Join(dir, "out.json")
	metaDir := t.TempDir()

	// Seed log file.
	if err := os.WriteFile(logFile, []byte(
		`{"level":"INFO","msg":"hello"}`+"\n"+
			`{"level":"DEBUG","msg":"debug"}`+"\n"+
			`{"level":"WARN","msg":"warn"}`+"\n",
	), 0644); err != nil {
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

	// Read output file and verify.
	content, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}

	var lines []map[string]interface{}
	for _, raw := range splitJSONLines(content) {
		var m map[string]interface{}
		if json.Unmarshal(raw, &m) == nil {
			lines = append(lines, m)
		}
	}

	// Should have 2 records (DEBUG was dropped).
	if len(lines) != 2 {
		t.Fatalf("expected 2 records (DEBUG dropped), got %d\ncontent: %s", len(lines), content)
	}

	// All records should have env=test label.
	for _, l := range lines {
		if l["env"] != "test" {
			t.Errorf("expected env=test, got %v", l["env"])
		}
	}

	// First record should be INFO hello.
	if lines[0]["msg"] != "hello" {
		t.Errorf("expected first msg=hello, got %v", lines[0]["msg"])
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
```

- [ ] **Step 2: Run integration test**

```bash
cd /Users/jimyag/src/github/jimyag/logpilot
go test -tags integration_agent ./internal/log-pilot-agent/... -v -timeout 30s -run TestPipelineEndToEnd
```

Expected: PASS — 2 records written (DEBUG dropped).

- [ ] **Step 3: Add webhook injection integration test**

Add to `internal/log-pilot-api/injector_test.go`:

```go
func TestInjectPodFullPipeline(t *testing.T) {
	containers := []logpilotv1alpha1.ContainerPolicy{
		{
			Name:      "app",
			LogType:   "applog",
			Path:      "/app/logs",
			Delivery:  "guaranteed",
			Collector: "host",
		},
		{
			Name:      "app",
			LogType:   "std",
			Path:      "-",
			Delivery:  "bestEffort",
			Collector: "host",
		},
	}
	policy := makePolicy(map[string]string{"app": "myapp"}, containers)
	pod := makePod(map[string]string{"app": "myapp"}, []corev1.Container{
		{Name: "app"},
	})

	if err := injectPod(pod, []*logpilotv1alpha1.LogPilotPolicy{policy}); err != nil {
		t.Fatal(err)
	}

	// One volume (hostPath because guaranteed takes precedence).
	if len(pod.Spec.Volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(pod.Spec.Volumes))
	}
	if pod.Spec.Volumes[0].HostPath == nil {
		t.Error("expected hostPath volume when any policy is guaranteed")
	}

	// One mount (only /app/logs; stdout "-" doesn't get a mount).
	c := pod.Spec.Containers[0]
	if len(c.VolumeMounts) != 1 {
		t.Fatalf("expected 1 VolumeMount, got %d", len(c.VolumeMounts))
	}
	if c.VolumeMounts[0].MountPath != "/app/logs" {
		t.Errorf("expected /app/logs, got %q", c.VolumeMounts[0].MountPath)
	}

	// Annotation encodes both policies.
	ann := pod.Annotations[podLogPolicyAnnotation]
	var decoded []logpilotv1alpha1.ContainerPolicy
	if err := json.Unmarshal([]byte(ann), &decoded); err != nil {
		t.Fatalf("invalid annotation JSON: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("expected 2 policies in annotation, got %d", len(decoded))
	}
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/log-pilot-api/... -v
```

Expected: all PASS.

- [ ] **Step 5: Final full build and test**

```bash
go build ./...
go test ./... -timeout 120s 2>&1 | grep -E "^(ok|FAIL)"
```

Expected: all packages ok, no FAIL.

- [ ] **Step 6: Final commit**

```bash
git add internal/log-pilot-agent/ internal/log-pilot-api/injector_test.go
git commit -m "feat: integration tests for pipeline and webhook injection"
```

---

## Final verification

- [ ] **Run all unit tests**

```bash
go test ./... -timeout 120s
```

Expected: all PASS.

- [ ] **Run integration test**

```bash
go test -tags integration_agent ./internal/log-pilot-agent/... -run TestPipelineEndToEnd -v
```

Expected: PASS.

- [ ] **Build all binaries**

```bash
go build ./cmd/log-pilot-operator/... && go build ./cmd/log-pilot-api/... && go build ./cmd/log-pilot-agent/...
```

Expected: no errors.
