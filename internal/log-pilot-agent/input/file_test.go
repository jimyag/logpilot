package input

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileInputReadsLines(t *testing.T) {
	f, err := os.CreateTemp("", "logpilot-test-*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	_, _ = f.WriteString("line1\nline2\n")
	f.Close()

	in, err := NewFileInput(FileConfig{
		Path:              f.Name(),
		ReadFrom:          "oldest",
		OffsetCommitEvery: 1,
	}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

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
	if err := os.WriteFile(path, []byte("hello\n"), 0644); err != nil {
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
	defer in.Close()

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
	if err := os.WriteFile(path, []byte("12345\n"), 0644); err != nil {
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
	defer in.Close()

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
	if err := os.WriteFile(logFile, []byte("line1\nline2\nline3\n"), 0644); err != nil {
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
	in1.Close() // forces final commitOffset

	// Append a new line after the first session ends.
	f, _ := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0644)
	f.WriteString("line4\n")
	f.Close()

	// Second session: should resume from saved offset and only read line4.
	in2, err := NewFileInput(FileConfig{
		Path:              logFile,
		ReadFrom:          "oldest", // would read from start if offset not restored
		OffsetCommitEvery: 1,
	}, metaDir)
	if err != nil {
		t.Fatal(err)
	}
	defer in2.Close()

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
