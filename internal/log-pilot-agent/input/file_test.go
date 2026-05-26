package input

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nxadm/tail"
)

func TestFileInputReadsLines(t *testing.T) {
	f, err := os.CreateTemp("", "logpilot-test-*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(f.Name()) }()

	_, _ = f.WriteString("line1\nline2\n")
	_ = f.Close()

	in, err := NewFileInput(FileConfig{
		Path:              f.Name(),
		ReadFrom:          "oldest",
		OffsetCommitEvery: 1,
	}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var records []Record
	for len(records) < 2 {
		batch, err := in.ReadBatch(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, batch...)
	}

	if string(records[0].Data) != "line1" {
		t.Errorf("expected 'line1', got %q", records[0].Data)
	}
	if string(records[1].Data) != "line2" {
		t.Errorf("expected 'line2', got %q", records[1].Data)
	}
}

func TestFileInputIncludeFilter(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/app.log"
	if err := os.WriteFile(path, []byte("hello\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	in, err := NewFileInput(FileConfig{
		Path:     path,
		ReadFrom: "oldest",
		Include:  []string{`.*\.log$`},
	}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	batch, _ := in.ReadBatch(ctx, 10)
	if len(batch) == 0 {
		t.Fatal("expected at least one record with include filter matching .log")
	}
}

func TestFileInputExcludeFilter(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/app.pid"
	if err := os.WriteFile(path, []byte("12345\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	in, err := NewFileInput(FileConfig{
		Path:     path,
		ReadFrom: "oldest",
		Exclude:  []string{`.*\.pid$`},
	}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	batch, _ := in.ReadBatch(ctx, 10)
	if len(batch) != 0 {
		t.Fatalf("expected no records with exclude filter on .pid, got %d", len(batch))
	}
}

func TestFileInputOffsetRecovery(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	metaDir := t.TempDir()

	// Write 3 lines.
	if err := os.WriteFile(logFile, []byte("line1\nline2\nline3\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// First session: read all 3 lines, committing offset each time.
	in1, err := NewFileInput(FileConfig{
		Path:              logFile,
		ReadFrom:          "oldest",
		OffsetCommitEvery: 1,
	}, metaDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx1, cancel1 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel1()
	var records []Record
	for len(records) < 3 {
		batch, _ := in1.ReadBatch(ctx1, 10)
		records = append(records, batch...)
	}
	_ = in1.Close() // forces final commitOffset

	// Append a new line after the first session ends.
	f, _ := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0o644)
	_, _ = f.WriteString("line4\n")
	_ = f.Close()

	// Second session: should resume from saved offset and only read line4.
	in2, err := NewFileInput(FileConfig{
		Path:              logFile,
		ReadFrom:          "oldest", // would read from start if offset not restored
		OffsetCommitEvery: 1,
	}, metaDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in2.Close() }()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	var recovered []Record
	for len(recovered) < 1 {
		batch, _ := in2.ReadBatch(ctx2, 10)
		recovered = append(recovered, batch...)
	}

	if len(recovered) != 1 {
		t.Fatalf("expected 1 record after recovery, got %d", len(recovered))
	}
	if string(recovered[0].Data) != "line4" {
		t.Errorf("expected 'line4', got %q", string(recovered[0].Data))
	}
}

func TestLoadOffsetEmptyPath(t *testing.T) {
	if got := loadOffset(""); got != -1 {
		t.Fatalf("expected -1 for empty meta path, got %d", got)
	}
}

func TestLoadOffsetFileNotExist(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.offset")
	if got := loadOffset(missing); got != -1 {
		t.Fatalf("expected -1 for missing file, got %d", got)
	}
}

func TestLoadOffsetInvalidJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.offset")
	if err := os.WriteFile(path, []byte("not-json"), 0o644); err != nil {
		t.Fatal(err)
	}

	if got := loadOffset(path); got != -1 {
		t.Fatalf("expected -1 for invalid JSON, got %d", got)
	}
}

func TestLoadOffsetSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.offset")
	if err := os.WriteFile(path, []byte(`{"offset":42}`), 0o644); err != nil {
		t.Fatal(err)
	}

	if got := loadOffset(path); got != 42 {
		t.Fatalf("expected offset 42, got %d", got)
	}
}

func TestFileInputLag(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	content := []byte("line1\nline2\n")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}

	metaDir := t.TempDir()
	in, err := NewFileInput(FileConfig{Path: path, ReadFrom: "oldest"}, metaDir)
	if err != nil {
		t.Fatal(err)
	}
	fi := in.(*fileInput)
	defer func() { _ = fi.Close() }()

	initialLag := fi.Lag()
	if initialLag != int64(len(content)) {
		t.Fatalf("expected initial lag %d, got %d", len(content), initialLag)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var records []Record
	for len(records) < 2 {
		batch, err := fi.ReadBatch(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, batch...)
	}

	if err := fi.Commit(); err != nil {
		t.Fatal(err)
	}

	if got := fi.Lag(); got != 0 {
		t.Fatalf("expected lag to drop to 0 after commit, got %d", got)
	}
}

func TestFileInputCommitWritesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("line1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	metaDir := t.TempDir()
	metaPath := filepath.Join(metaDir, "app.log.offset")
	in, err := NewFileInput(FileConfig{Path: path, ReadFrom: "oldest"}, metaDir)
	if err != nil {
		t.Fatal(err)
	}
	fi := in.(*fileInput)
	defer func() { _ = fi.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	batch, err := fi.ReadBatch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 record, got %d", len(batch))
	}

	if err := fi.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(metaPath); err != nil {
		t.Fatalf("expected meta file to exist: %v", err)
	}
	if got := loadOffset(metaPath); got != int64(len("line1\n")) {
		t.Fatalf("expected committed offset %d, got %d", len("line1\n"), got)
	}
}

func TestFileInputPassesFilterIncludeOnly(t *testing.T) {
	fi := &fileInput{includeRe: compilePatterns([]string{"hello"})}
	if !fi.passesFilter("say-hello.log") {
		t.Fatal("expected include filter to allow matching filename")
	}
	if fi.passesFilter("goodbye.log") {
		t.Fatal("expected include filter to reject non-matching filename")
	}
}

func TestFileInputPassesFilterExcludeOnly(t *testing.T) {
	fi := &fileInput{excludeRe: compilePatterns([]string{"error"})}
	if !fi.passesFilter("app.log") {
		t.Fatal("expected exclude filter to allow non-matching filename")
	}
	if fi.passesFilter("error.log") {
		t.Fatal("expected exclude filter to reject matching filename")
	}
}

func TestFileInputPassesFilterBothIncludeAndExclude(t *testing.T) {
	fi := &fileInput{
		includeRe: compilePatterns([]string{"hello"}),
		excludeRe: compilePatterns([]string{"world"}),
	}

	if !fi.passesFilter("hello.log") {
		t.Fatal("expected include match without exclude match to pass")
	}
	if fi.passesFilter("hello-world.log") {
		t.Fatal("expected exclude match to win when include also matches")
	}
	if fi.passesFilter("goodbye.log") {
		t.Fatal("expected missing include match to fail")
	}
}

func TestFileInputReadBatchAfterCloseReturnsNil(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("line1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	in, err := NewFileInput(FileConfig{Path: path, ReadFrom: "oldest"}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fi := in.(*fileInput)

	// Drain existing content so the tail goroutine reaches a stable inotify-wait
	// state (not mid-seek), avoiding a race between commitOffset/Tell and the
	// tail goroutine's concurrent file seeks during initial read.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, _ = fi.ReadBatch(drainCtx, 10)
	drainCancel()

	if err := fi.Close(); err != nil {
		t.Fatal(err)
	}

	batch, err := fi.ReadBatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if batch != nil {
		t.Fatalf("expected nil batch after Close, got %v", batch)
	}
}

func TestFileInputReadBatchSkipsErroredLines(t *testing.T) {
	lines := make(chan *tail.Line, 2)
	lines <- &tail.Line{Err: errors.New("bad line")}
	lines <- &tail.Line{Text: "good"}
	close(lines)

	fi := &fileInput{
		cfg:  FileConfig{Path: "/var/log/app.log"},
		tail: &tail.Tail{Lines: lines},
	}

	batch, err := fi.ReadBatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 || string(batch[0].Data) != "good" {
		t.Fatalf("expected only the good line, got %+v", batch)
	}
}

func TestFileInputReadBatchSkipsFilteredLines(t *testing.T) {
	lines := make(chan *tail.Line, 1)
	lines <- &tail.Line{Text: "skip"}
	close(lines)

	fi := &fileInput{
		cfg:       FileConfig{Path: "/var/log/app.log"},
		tail:      &tail.Tail{Lines: lines},
		includeRe: compilePatterns([]string{`.*\.txt$`}),
	}

	batch, err := fi.ReadBatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 0 {
		t.Fatalf("expected filtered line to be skipped, got %+v", batch)
	}
}

// drainFileInput reads available content with a short timeout to ensure the
// nxadm/tail goroutine has completed its initial file seek and reached a
// stable inotify-wait state. This prevents a race between commitOffset/Tell
// and the tail goroutine's concurrent file seeks during initial read.
func drainFileInput(t *testing.T, fi *fileInput) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, _ = fi.ReadBatch(ctx, 1024)
}

func TestFileInputCommitOffsetNoMetaPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("line1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	in, err := NewFileInput(FileConfig{Path: path, ReadFrom: "oldest"}, "")
	if err != nil {
		t.Fatal(err)
	}
	fi := in.(*fileInput)
	defer func() { _ = fi.Close() }()

	drainFileInput(t, fi)
	fi.commitOffset()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "app.log" {
		t.Fatalf("expected only the log file to remain, got %v", entries)
	}
}

func TestFileInputCommitOffsetMkdirAllError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("line1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	in, err := NewFileInput(FileConfig{Path: path, ReadFrom: "oldest", MetaPath: filepath.Join(blocker, "app.offset")}, "")
	if err != nil {
		t.Fatal(err)
	}
	fi := in.(*fileInput)
	defer func() { _ = fi.Close() }()

	drainFileInput(t, fi)
	fi.commitOffset()

	if _, err := os.Stat(filepath.Join(blocker, "app.offset.tmp")); err == nil {
		t.Fatal("expected no temp offset file to be written when mkdir fails")
	}
}

func TestFileInputCommitOffsetWriteFileError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("line1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	metaPath := filepath.Join(dir, "app.offset")
	if err := os.Mkdir(metaPath+".tmp", 0o755); err != nil {
		t.Fatal(err)
	}

	in, err := NewFileInput(FileConfig{Path: path, ReadFrom: "oldest", MetaPath: metaPath}, "")
	if err != nil {
		t.Fatal(err)
	}
	fi := in.(*fileInput)
	defer func() { _ = fi.Close() }()

	drainFileInput(t, fi)
	fi.commitOffset()

	if _, err := os.Stat(metaPath); err == nil {
		t.Fatal("expected final offset file not to be written when temp file path is a directory")
	}
}

func TestFileInputReadBatchCancelledContext(t *testing.T) {
	fi := &fileInput{tail: &tail.Tail{Lines: make(chan *tail.Line)}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for range 256 {
		batch, err := fi.ReadBatch(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) != 0 {
			t.Fatalf("expected empty batch, got %v", batch)
		}
	}
}

func TestFileInputCommitOffsetAfterClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("line1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	in, err := NewFileInput(FileConfig{Path: path, ReadFrom: "oldest"}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fi := in.(*fileInput)
	drainFileInput(t, fi)
	if err := fi.Close(); err != nil {
		t.Fatal(err)
	}

	fi.commitOffset()
}
