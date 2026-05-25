package clean

import "time"

// RunnerMeta holds metadata used to determine cleanup eligibility.
type RunnerMeta struct {
	PolicyName    string
	PodName       string
	PodUID        string
	Namespace     string
	ContainerName string
	LogType       string
	DeletedAt     time.Time // zero if pod is still running
	// Lag is the number of unread bytes/records remaining in the input.
	// A non-zero value means collection is not yet complete.
	Lag int64
}

// Clean manages log file cleanup while a pod is running.
// After pod deletion, cleanup is handled automatically once lag == 0.
type Clean interface {
	ShouldClean(meta RunnerMeta) (bool, error)
	Clean(meta RunnerMeta) error
}
