package input

import (
	"context"
	"encoding/json"
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
	os.WriteFile(filepath.Join(dir, "a.log"), []byte("line1\nline2\n"), 0o644)
	time.Sleep(5 * time.Millisecond) // ensure different mod times
	os.WriteFile(filepath.Join(dir, "b.log"), []byte("line3\n"), 0o644)

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
	os.WriteFile(filepath.Join(dir, "app.log"), []byte("hello\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "app.pid"), []byte("12345\n"), 0o644)

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
	os.WriteFile(logFile, []byte("line1\nline2\n"), 0o644)

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
	os.WriteFile(logFile, []byte("line3\n"), 0o644)

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

	os.WriteFile(logFile, []byte("line1\nline2\n"), 0o644)

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
	f, _ := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0o644)
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
	if err := os.WriteFile(filepath.Join(dir, "app.log"), []byte("line1\n"), 0o644); err != nil {
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
	if err := os.WriteFile(filepath.Join(dir, "app.log"), []byte("line1\n"), 0o644); err != nil {
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
	if err := os.WriteFile(path, []byte("line1\n"), 0o644); err != nil {
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
	if err := os.WriteFile(currentPath, []byte("current\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stalePath, []byte("stale\n"), 0o644); err != nil {
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

func TestDirInputReadBatchStopped(t *testing.T) {
	di := &dirInput{}
	atomic.StoreInt32(&di.stopped, 1)

	batch, err := di.ReadBatch(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	if batch != nil {
		t.Fatalf("expected nil batch when stopped, got %v", batch)
	}
}

func TestDirInputOpenFileNoFiles(t *testing.T) {
	di := &dirInput{cfg: DirConfig{Dir: t.TempDir(), ReadFrom: "oldest"}}

	if err := di.openFile(); err == nil {
		t.Fatal("expected openFile to fail when directory has no files")
	}
}

func TestDirInputOpenFileNewestStartsAtEnd(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	content := "line1\nline2\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	di := &dirInput{cfg: DirConfig{Dir: dir, ReadFrom: "newest"}}
	if err := di.openFile(); err != nil {
		t.Fatal(err)
	}
	defer di.currentF.Close()

	if di.currentFile != path {
		t.Fatalf("expected current file %q, got %q", path, di.currentFile)
	}
	if di.offset != int64(len(content)) {
		t.Fatalf("expected offset %d, got %d", len(content), di.offset)
	}
	if got := di.Lag(); got != 0 {
		t.Fatalf("expected lag 0 at end of file, got %d", got)
	}
}

func TestDirInputOpenFileClearsMissingCurrentFile(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.log")
	di := &dirInput{cfg: DirConfig{Dir: dir}, currentFile: missing, offset: 12}

	if err := di.openFile(); err == nil {
		t.Fatal("expected openFile to fail for missing current file")
	}
	if di.currentFile != "" {
		t.Fatalf("expected current file to be cleared, got %q", di.currentFile)
	}
}

func TestDirInputOpenFileSeeksSavedOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	content := "line1\nline2\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	di := &dirInput{cfg: DirConfig{Dir: dir}, currentFile: path, offset: int64(len("line1\n"))}
	if err := di.openFile(); err != nil {
		t.Fatal(err)
	}
	defer di.currentF.Close()

	line, n, err := di.readLine()
	if err != nil {
		t.Fatal(err)
	}
	if line != "line2" {
		t.Fatalf("expected to resume at line2, got %q", line)
	}
	if n != len("line2\n") {
		t.Fatalf("expected to consume %d bytes, got %d", len("line2\n"), n)
	}
}

func TestDirInputDetectRotation(t *testing.T) {
	t.Run("no current file", func(t *testing.T) {
		if (&dirInput{}).detectRotation() {
			t.Fatal("expected detectRotation to be false without an open file")
		}
	})

	t.Run("unchanged file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "app.log")
		if err := os.WriteFile(path, []byte("line1\nline2\n"), 0o644); err != nil {
			t.Fatal(err)
		}

		di := openTestDirFile(t, path)
		if di.detectRotation() {
			t.Fatal("expected detectRotation to be false for unchanged file")
		}
	})

	t.Run("file removed", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "app.log")
		if err := os.WriteFile(path, []byte("line1\n"), 0o644); err != nil {
			t.Fatal(err)
		}

		di := openTestDirFile(t, path)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if !di.detectRotation() {
			t.Fatal("expected detectRotation to report removed file")
		}
	})

	t.Run("inode changed", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "app.log")
		rotated := filepath.Join(dir, "app.log.1")
		if err := os.WriteFile(path, []byte("line1\n"), 0o644); err != nil {
			t.Fatal(err)
		}

		di := openTestDirFile(t, path)
		if err := os.Rename(path, rotated); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("line2\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if !di.detectRotation() {
			t.Fatal("expected detectRotation to report inode replacement")
		}
	})

	t.Run("file truncated", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "app.log")
		if err := os.WriteFile(path, []byte("line1\nline2\n"), 0o644); err != nil {
			t.Fatal(err)
		}

		di := openTestDirFile(t, path)
		di.offset = int64(len("line1\nline2\n"))
		if err := os.Truncate(path, int64(len("line1\n"))); err != nil {
			t.Fatal(err)
		}
		if !di.detectRotation() {
			t.Fatal("expected detectRotation to report truncation")
		}
	})
}

func TestDirInputUpdateLagTracksRemainingBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	content := "line1\nline2\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	di := &dirInput{currentF: f, offset: int64(len("line1\n"))}
	di.updateLag()
	if got := di.Lag(); got != int64(len("line2\n")) {
		t.Fatalf("expected lag %d, got %d", len("line2\n"), got)
	}

	di.offset = int64(len(content) + 5)
	di.updateLag()
	if got := di.Lag(); got != 0 {
		t.Fatalf("expected lag to clamp at 0, got %d", got)
	}
}

func TestDirInputUpdateLagStatError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("line1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	di := &dirInput{currentF: f}
	atomic.StoreInt64(&di.lag, 9)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	di.updateLag()
	if got := di.Lag(); got != 9 {
		t.Fatalf("expected lag to remain unchanged after Stat error, got %d", got)
	}
}

func TestDirInputCommitStateWritesPrunedState(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta", "state.json")
	currentPath := filepath.Join(dir, "current.log")
	stalePath := filepath.Join(dir, "stale.log")
	if err := os.WriteFile(currentPath, []byte("current\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stalePath, []byte("stale\n"), 0o644); err != nil {
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

	di := &dirInput{
		cfg:         DirConfig{Dir: dir, MetaPath: metaPath},
		currentFile: currentPath,
		offset:      4,
		doneInodes: map[string]int64{
			strconv.FormatUint(currentInode, 10): 4,
			strconv.FormatUint(staleInode, 10):   2,
		},
	}

	di.commitState()

	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	var state dirOffsetState
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	if state.CurrentFile != currentPath {
		t.Fatalf("expected current file %q, got %q", currentPath, state.CurrentFile)
	}
	if state.Offset != 4 {
		t.Fatalf("expected offset 4, got %d", state.Offset)
	}
	if _, ok := state.DoneInodes[strconv.FormatUint(staleInode, 10)]; ok {
		t.Fatal("expected stale inode to be pruned before commit")
	}
	if _, ok := state.DoneInodes[strconv.FormatUint(currentInode, 10)]; !ok {
		t.Fatal("expected current inode to remain in committed state")
	}
}

func TestDirInputCommitStateMkdirAllError(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	di := &dirInput{cfg: DirConfig{Dir: dir, MetaPath: filepath.Join(blocker, "state.json")}}
	di.commitState()

	if _, err := os.Stat(filepath.Join(blocker, "state.json.tmp")); err == nil {
		t.Fatal("expected no temp state file to be written")
	}
}

func TestDirInputListFilesMissingDir(t *testing.T) {
	di := &dirInput{cfg: DirConfig{Dir: filepath.Join(t.TempDir(), "missing")}}
	if got := di.listFiles(); got != nil {
		t.Fatalf("expected nil files for missing directory, got %v", got)
	}
}

func TestDirInputListFilesSkipsDirectories(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logPath, []byte("line1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	di := &dirInput{cfg: DirConfig{Dir: dir}}
	files := di.listFiles()
	if len(files) != 1 || files[0] != logPath {
		t.Fatalf("expected only %q, got %v", logPath, files)
	}
}

func TestInodeFromPathMissingFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.log")
	if got, err := inodeFromPath(missing); err == nil || got != 0 {
		t.Fatalf("expected missing path to return error and inode 0, got inode=%d err=%v", got, err)
	}
}

func TestGetInodeFromFileClosedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	if err := os.WriteFile(path, []byte("line1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got := getInodeFromFile(f); got != 0 {
		t.Fatalf("expected closed file to return inode 0, got %d", got)
	}
}

func TestDirInputPruneStaleInodesReadDirError(t *testing.T) {
	di := &dirInput{
		cfg: DirConfig{Dir: filepath.Join(t.TempDir(), "missing")},
		doneInodes: map[string]int64{
			"1": 1,
		},
	}

	di.pruneStaleInodes()

	if got := di.doneInodes["1"]; got != 1 {
		t.Fatalf("expected doneInodes to remain unchanged on read error, got %v", di.doneInodes)
	}
}

func openTestDirFile(t *testing.T, path string) *dirInput {
	t.Helper()

	di := &dirInput{cfg: DirConfig{Dir: filepath.Dir(path)}, currentFile: path}
	if err := di.openFile(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if di.currentF != nil {
			_ = di.currentF.Close()
		}
	})
	return di
}

type fakeFileInfo struct{}

func (fakeFileInfo) Name() string       { return "fake" }
func (fakeFileInfo) Size() int64        { return 0 }
func (fakeFileInfo) Mode() os.FileMode  { return 0 }
func (fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (fakeFileInfo) IsDir() bool        { return false }
func (fakeFileInfo) Sys() any           { return nil }
