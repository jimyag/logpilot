package input

import (
	"context"
	"os"
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
