package input

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
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

func TestDirInputLagInitial(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "app.log"), []byte("line1\n"), 0644); err != nil {
		t.Fatal(err)
	}

	in, err := NewDirInput(DirConfig{Dir: dir, ReadFrom: "oldest"})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	if got := in.(*dirInput).Lag(); got < 0 {
		t.Fatalf("expected non-negative lag, got %d", got)
	}
}

func TestDirInputCommit(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(filepath.Join(dir, "app.log"), []byte("line1\n"), 0644); err != nil {
		t.Fatal(err)
	}

	in, err := NewDirInput(DirConfig{Dir: dir, ReadFrom: "oldest", MetaPath: metaPath})
	if err != nil {
		t.Fatal(err)
	}
	di := in.(*dirInput)
	defer di.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	batch, err := di.ReadBatch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 record, got %d", len(batch))
	}

	if err := di.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(metaPath); err != nil {
		t.Fatalf("expected committed state file to exist: %v", err)
	}
}

func TestDirPassesFilterIncludeOnly(t *testing.T) {
	di := &dirInput{includeRe: compilePatternInterfaces([]string{"hello"})}
	if !di.passesFilter("hello.log") {
		t.Fatal("expected include filter to allow matching filename")
	}
	if di.passesFilter("goodbye.log") {
		t.Fatal("expected include filter to reject non-matching filename")
	}
}

func TestDirPassesFilterExcludeOnly(t *testing.T) {
	di := &dirInput{excludeRe: compilePatternInterfaces([]string{"error"})}
	if !di.passesFilter("app.log") {
		t.Fatal("expected exclude filter to allow non-matching filename")
	}
	if di.passesFilter("error.log") {
		t.Fatal("expected exclude filter to reject matching filename")
	}
}

func TestDirPassesFilterBothIncludeExclude(t *testing.T) {
	di := &dirInput{
		includeRe: compilePatternInterfaces([]string{"hello"}),
		excludeRe: compilePatternInterfaces([]string{"world"}),
	}

	if !di.passesFilter("hello.log") {
		t.Fatal("expected include match without exclude match to pass")
	}
	if di.passesFilter("hello-world.log") {
		t.Fatal("expected exclude match to win when include also matches")
	}
	if di.passesFilter("goodbye.log") {
		t.Fatal("expected missing include match to fail")
	}
}

func TestDirInputUpdateLagNilCurrentF(t *testing.T) {
	di := &dirInput{}
	atomic.StoreInt64(&di.lag, 17)

	di.updateLag()

	if got := di.Lag(); got != 17 {
		t.Fatalf("expected lag to remain unchanged, got %d", got)
	}
}

func TestGetInodeFromStat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	if err := os.WriteFile(path, []byte("line1\n"), 0644); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := getInodeFromStat(info); got == 0 {
		t.Fatal("expected real file stat to provide a non-zero inode")
	}

	if got := getInodeFromStat(fakeFileInfo{}); got != 0 {
		t.Fatalf("expected fake file info to return inode 0, got %d", got)
	}
}

func TestDirInputPruneStaleInodesRemovesMissing(t *testing.T) {
	dir := t.TempDir()
	currentPath := filepath.Join(dir, "current.log")
	stalePath := filepath.Join(dir, "stale.log")
	if err := os.WriteFile(currentPath, []byte("current\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stalePath, []byte("stale\n"), 0644); err != nil {
		t.Fatal(err)
	}

	currentInode, err := inodeFromPath(currentPath)
	if err != nil {
		t.Fatal(err)
	}
	staleInode, err := inodeFromPath(stalePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(stalePath); err != nil {
		t.Fatal(err)
	}

	currentKey := strconv.FormatUint(currentInode, 10)
	staleKey := strconv.FormatUint(staleInode, 10)
	di := &dirInput{
		cfg: DirConfig{Dir: dir},
		doneInodes: map[string]int64{
			currentKey: 12,
			staleKey:   6,
		},
	}

	di.pruneStaleInodes()

	if _, ok := di.doneInodes[currentKey]; !ok {
		t.Fatal("expected current inode to remain tracked")
	}
	if _, ok := di.doneInodes[staleKey]; ok {
		t.Fatal("expected stale inode to be removed")
	}
}

type fakeFileInfo struct{}

func (fakeFileInfo) Name() string       { return "fake" }
func (fakeFileInfo) Size() int64        { return 0 }
func (fakeFileInfo) Mode() os.FileMode  { return 0 }
func (fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (fakeFileInfo) IsDir() bool        { return false }
func (fakeFileInfo) Sys() interface{}   { return nil }
