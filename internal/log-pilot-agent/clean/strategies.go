package clean

import (
	"os"
	"time"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

// NewFromSpec creates a Clean implementation from a CleanSpec.
// logDir is the directory whose files this cleaner manages.
func NewFromSpec(spec logpilotv1alpha1.CleanSpec, logDir string) Clean {
	switch spec.Strategy {
	case "never":
		return &neverClean{}
	case "retain":
		days := spec.RetainDays
		if days <= 0 {
			days = 7 // safe default: retain at least 7 days
		}
		return &retainClean{logDir: logDir, retainDays: days}
	default: // "afterCollected" and empty string
		return &afterCollectedClean{logDir: logDir}
	}
}

// neverClean never removes any files.
type neverClean struct{}

func (c *neverClean) ShouldClean(_ RunnerMeta) (bool, error) { return false, nil }
func (c *neverClean) Clean(_ RunnerMeta) error               { return nil }

// afterCollectedClean removes files as soon as they have been collected (lag==0).
type afterCollectedClean struct {
	logDir string
}

func (c *afterCollectedClean) ShouldClean(meta RunnerMeta) (bool, error) {
	// Don't clean while there are still records waiting to be sent.
	if meta.Lag > 0 {
		return false, nil
	}
	entries, err := os.ReadDir(c.logDir)
	if err != nil {
		return false, nil
	}
	return len(entries) > 0, nil
}

func (c *afterCollectedClean) Clean(_ RunnerMeta) error {
	entries, err := os.ReadDir(c.logDir)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		_ = os.Remove(c.logDir + "/" + e.Name())
	}
	return nil
}

// retainClean keeps files for RetainDays after their last modification before removing them.
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
