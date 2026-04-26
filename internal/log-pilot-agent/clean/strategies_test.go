package clean

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func TestAfterCollectedShouldClean(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logFile, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	spec := logpilotv1alpha1.CleanSpec{Strategy: "afterCollected"}
	c := NewFromSpec(spec, dir)

	should, err := c.ShouldClean(RunnerMeta{})
	if err != nil {
		t.Fatal(err)
	}
	if !should {
		t.Fatal("expected ShouldClean=true when files exist")
	}
}

func TestAfterCollectedCleanRemovesFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.log", "b.log"} {
		os.WriteFile(filepath.Join(dir, name), []byte("x"), 0644)
	}

	c := NewFromSpec(logpilotv1alpha1.CleanSpec{Strategy: "afterCollected"}, dir)
	if err := c.Clean(RunnerMeta{}); err != nil {
		t.Fatal(err)
	}

	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("expected 0 files after clean, got %d", len(entries))
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
	if err := c.Clean(RunnerMeta{}); err != nil {
		t.Fatal(err)
	}
}

func TestRetainCleanRetainDays0(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	os.WriteFile(logFile, []byte("data"), 0644)

	// retainDays=0 means cutoff is now, so any file modified before now should clean.
	spec := logpilotv1alpha1.CleanSpec{Strategy: "retain", RetainDays: 0}
	c := NewFromSpec(spec, dir)

	// Backdate file modification time by 1 second to ensure it's before cutoff.
	past := time.Now().Add(-time.Second)
	os.Chtimes(logFile, past, past)

	should, err := c.ShouldClean(RunnerMeta{})
	if err != nil {
		t.Fatal(err)
	}
	if !should {
		t.Fatal("expected ShouldClean=true with retainDays=0 and old file")
	}
}

func TestRetainCleanFutureRetain(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	os.WriteFile(logFile, []byte("data"), 0644)

	// retainDays=30 means keep files modified within last 30 days.
	spec := logpilotv1alpha1.CleanSpec{Strategy: "retain", RetainDays: 30}
	c := NewFromSpec(spec, dir)

	should, _ := c.ShouldClean(RunnerMeta{})
	if should {
		t.Fatal("expected ShouldClean=false for new file with retainDays=30")
	}
}

func TestNewFromSpecDefaultsToAfterCollected(t *testing.T) {
	// Empty strategy defaults to afterCollected.
	c := NewFromSpec(logpilotv1alpha1.CleanSpec{}, t.TempDir())
	if c == nil {
		t.Fatal("expected non-nil Clean for empty strategy")
	}
	// Verify it behaves like afterCollected by checking it's not neverClean.
	should, _ := c.ShouldClean(RunnerMeta{})
	// Empty dir → no files → ShouldClean=false
	if should {
		t.Fatal("expected false for empty dir")
	}
}
