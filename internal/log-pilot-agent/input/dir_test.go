package input

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDirInputReadsFiles(t *testing.T) {
	dir := t.TempDir()
	// Write two files.
	os.WriteFile(filepath.Join(dir, "a.log"), []byte("line1\nline2\n"), 0644)
	time.Sleep(5 * time.Millisecond) // ensure different mod times
	os.WriteFile(filepath.Join(dir, "b.log"), []byte("line3\n"), 0644)

	in, err := NewDirInput(DirConfig{
		Dir:               dir,
		ReadFrom:          "oldest",
		OffsetCommitEvery: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var records []Record
	for len(records) < 3 {
		batch, err := in.ReadBatch(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, batch...)
		if ctx.Err() != nil {
			break
		}
	}

	if len(records) < 3 {
		t.Fatalf("expected at least 3 records, got %d", len(records))
	}
	if string(records[0].Data) != "line1" {
		t.Errorf("expected line1, got %q", records[0].Data)
	}
}

func TestDirInputFilterExclude(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "app.log"), []byte("hello\n"), 0644)
	os.WriteFile(filepath.Join(dir, "app.pid"), []byte("12345\n"), 0644)

	in, err := NewDirInput(DirConfig{
		Dir:      dir,
		ReadFrom: "oldest",
		Exclude:  []string{`.*\.pid$`},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	batch, _ := in.ReadBatch(ctx, 10)

	for _, r := range batch {
		if string(r.Data) == "12345" {
			t.Error("pid file content should have been excluded")
		}
	}
}

func TestDirInputLogRotation(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")

	// Write initial content.
	os.WriteFile(logFile, []byte("line1\nline2\n"), 0644)

	in, err := NewDirInput(DirConfig{Dir: dir, ReadFrom: "oldest"})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Read first two lines.
	var records []Record
	for len(records) < 2 {
		b, _ := in.ReadBatch(ctx, 10)
		records = append(records, b...)
	}
	if string(records[0].Data) != "line1" {
		t.Errorf("expected line1, got %q", records[0].Data)
	}

	// Simulate log rotation: rename app.log → app.log.1, create new app.log.
	os.Rename(logFile, filepath.Join(dir, "app.log.1"))
	os.WriteFile(logFile, []byte("line3\n"), 0644)

	// Should eventually read line3 from the new app.log.
	for len(records) < 3 {
		b, _ := in.ReadBatch(ctx, 10)
		records = append(records, b...)
		if ctx.Err() != nil {
			t.Fatal("timeout waiting for rotated file")
		}
	}
	if string(records[2].Data) != "line3" {
		t.Errorf("expected line3 after rotation, got %q", records[2].Data)
	}
}

func TestDirInputOffsetRecovery(t *testing.T) {
	dir := t.TempDir()
	metaDir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	metaPath := filepath.Join(metaDir, "state.json")

	os.WriteFile(logFile, []byte("line1\nline2\n"), 0644)

	// First session: read all lines.
	in1, _ := NewDirInput(DirConfig{
		Dir:               dir,
		ReadFrom:          "oldest",
		MetaPath:          metaPath,
		OffsetCommitEvery: 1,
	})
	ctx1, cancel1 := context.WithTimeout(context.Background(), 3*time.Second)
	var r1 []Record
	for len(r1) < 2 {
		b, _ := in1.ReadBatch(ctx1, 10)
		r1 = append(r1, b...)
	}
	in1.Close()
	cancel1()

	// Append new line.
	f, _ := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0644)
	f.WriteString("line3\n")
	f.Close()

	// Second session: should resume from saved offset.
	in2, _ := NewDirInput(DirConfig{
		Dir:               dir,
		ReadFrom:          "oldest",
		MetaPath:          metaPath,
		OffsetCommitEvery: 1,
	})
	defer in2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	var r2 []Record
	for len(r2) < 1 {
		b, _ := in2.ReadBatch(ctx2, 10)
		r2 = append(r2, b...)
		if ctx2.Err() != nil {
			break
		}
	}

	if len(r2) != 1 {
		t.Fatalf("expected 1 record after recovery, got %d", len(r2))
	}
	if string(r2[0].Data) != "line3" {
		t.Errorf("expected line3, got %q", r2[0].Data)
	}
}
