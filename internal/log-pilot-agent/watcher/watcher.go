package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"sigs.k8s.io/controller-runtime/pkg/client"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
	cleanfactory "github.com/jimyag/logpilot/internal/log-pilot-agent/clean"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
	outputfactory "github.com/jimyag/logpilot/internal/log-pilot-agent/output"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/runner"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/status"
	transformfactory "github.com/jimyag/logpilot/internal/log-pilot-agent/transform"
)

const podLogPolicyAnnotation = "beta.logpilot.io/log-policy"

// Config holds watcher configuration derived from the LogPilot CR.
type Config struct {
	NodeName  string
	LogDir    string
	ConfigDir string
	MetaDir   string
}

// runnerEntry tracks a runner and its log path.
type runnerEntry struct {
	r       *runner.Runner
	logPath string
}

// Watcher watches pods on this node and manages runners per pod/container/logType.
type Watcher struct {
	cfg     Config
	client  client.Client
	status  *status.Server
	runners map[string]*runnerEntry // key: podUID/container/logType
	mu      sync.Mutex
}

// New creates a Watcher.
func New(cfg Config, c client.Client, statusSrv *status.Server) *Watcher {
	return &Watcher{
		cfg:     cfg,
		client:  c,
		status:  statusSrv,
		runners: make(map[string]*runnerEntry),
	}
}

// Start begins watching pods on this node. Blocks until ctx is cancelled.
func (w *Watcher) Start(ctx context.Context) error {
	// Poll pods on this node periodically.
	// A production implementation would use an informer for efficiency.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	seen := make(map[string]bool)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := w.reconcile(ctx, seen); err != nil {
				continue
			}
		}
	}
}

// reconcile lists pods on this node, starts runners for new pods, and cleans up deleted ones.
func (w *Watcher) reconcile(ctx context.Context, seen map[string]bool) error {
	podList := &corev1.PodList{}
	if err := w.client.List(ctx, podList,
		client.MatchingFieldsSelector{
			Selector: fields.OneTermEqualSelector("spec.nodeName", w.cfg.NodeName),
		}); err != nil {
		return err
	}

	current := make(map[string]bool)
	for _, pod := range podList.Items {
		uid := string(pod.UID)
		current[uid] = true
		if !seen[uid] {
			seen[uid] = true
			w.OnPodAdd(pod.DeepCopy())
		}
	}

	// Detect deleted pods.
	for uid := range seen {
		if !current[uid] {
			delete(seen, uid)
			w.onPodDeleted(uid)
		}
	}
	return nil
}

// OnPodAdd starts runners for a pod that has the log policy annotation.
func (w *Watcher) OnPodAdd(pod *corev1.Pod) {
	policies, err := parsePoliciesFromPod(pod)
	if err != nil || len(policies) == 0 {
		return
	}
	for _, cp := range policies {
		key := runnerKey(string(pod.UID), cp.Name, cp.LogType)
		logPath := w.logPath(pod, cp)

		if err := ensureLogPath(pod, cp, logPath); err != nil {
			continue
		}

		r := buildRunner(cp, logPath, w.cfg)
		entry := &runnerEntry{r: r, logPath: logPath}

		w.mu.Lock()
		w.runners[key] = entry
		w.mu.Unlock()

		// Start a goroutine to run the pipeline and keep status updated.
		go func(r *runner.Runner, key string) {
			// Update lag every second while running.
			ticker := time.NewTicker(time.Second)
			done := make(chan struct{})
			go func() {
				for {
					select {
					case <-ticker.C:
						w.status.UpdateRunner(key, r.Lag(), 0)
					case <-done:
						ticker.Stop()
						return
					}
				}
			}()
			r.Run(context.Background())
			close(done)
			w.status.RemoveRunner(key)
		}(r, key)

		w.status.UpdateRunner(key, r.Lag(), 0)
	}
}

// onPodDeleted stops runners for a deleted pod and schedules log dir cleanup.
func (w *Watcher) onPodDeleted(podUID string) {
	w.mu.Lock()
	var toDelete []string
	var entries []*runnerEntry
	for key, entry := range w.runners {
		if len(key) >= len(podUID) && key[:len(podUID)] == podUID {
			toDelete = append(toDelete, key)
			entries = append(entries, entry)
		}
	}
	for _, key := range toDelete {
		delete(w.runners, key)
	}
	w.mu.Unlock()

	for i, entry := range entries {
		go func(entry *runnerEntry, key string) {
			entry.r.Stop()
			// Wait for lag to reach zero (all data collected).
			for entry.r.Lag() > 0 {
				time.Sleep(500 * time.Millisecond)
			}
			w.status.RemoveRunner(key)
			_ = removeLogPath(entry.logPath)
		}(entry, toDelete[i])
	}
}

func (w *Watcher) logPath(pod *corev1.Pod, cp logpilotv1alpha1.ContainerPolicy) string {
	return fmt.Sprintf("%s/LogPilotPolicy/%s/%s/%s/%s/%s",
		w.cfg.LogDir, pod.Namespace, pod.Name, string(pod.UID), cp.Name, cp.LogType)
}

func parsePoliciesFromPod(pod *corev1.Pod) ([]logpilotv1alpha1.ContainerPolicy, error) {
	ann := pod.Annotations[podLogPolicyAnnotation]
	if ann == "" {
		return nil, nil
	}
	var policies []logpilotv1alpha1.ContainerPolicy
	if err := json.Unmarshal([]byte(ann), &policies); err != nil {
		return nil, err
	}
	return policies, nil
}

func runnerKey(podUID, container, logType string) string {
	return fmt.Sprintf("%s/%s/%s", podUID, container, logType)
}

func buildRunner(cp logpilotv1alpha1.ContainerPolicy, logPath string, cfg Config) *runner.Runner {
	batchLen := cp.BatchLen
	if batchLen == 0 {
		batchLen = 1000
	}

	// Use dir Input: scans the logPath directory for all log files,
	// tracks per-file inode and offset, handles rotation and new files.
	metaPath := filepath.Join(cfg.MetaDir, "LogPilotPolicy",
		fmt.Sprintf("%s_%s_dir.json", cp.Name, cp.LogType))
	dirInput, err := input.NewDirInput(input.DirConfig{
		Dir:               logPath,
		MetaPath:          metaPath,
		ReadFrom:          "oldest",
		OffsetCommitEvery: 1000,
	})
	if err != nil {
		return runner.New(runner.Config{BatchLen: batchLen})
	}

	transforms, _ := transformfactory.NewSliceFromSpecs(cp.Transforms)
	out, _ := outputfactory.NewFromSpec(cp.Output)
	cln := cleanfactory.NewFromSpec(cp.Clean, logPath)

	return runner.New(runner.Config{
		Name:       fmt.Sprintf("%s/%s", cp.Name, cp.LogType),
		Input:      dirInput,
		Transforms: transforms,
		Output:     out,
		Clean:      cln,
		BatchLen:   batchLen,
	})
}

// PodLogDir returns the top-level log directory for a pod.
func (w *Watcher) PodLogDir(pod *corev1.Pod) string {
	return fmt.Sprintf("%s/LogPilotPolicy/%s/%s/%s",
		w.cfg.LogDir, pod.Namespace, pod.Name, string(pod.UID))
}

// Cleanup removes the log directory for a pod after all runners have finished.
func (w *Watcher) Cleanup(pod *corev1.Pod) error {
	return os.RemoveAll(w.PodLogDir(pod))
}

// StopAll signals all running runners to stop and waits for them to drain.
// Called during graceful shutdown.
func (w *Watcher) StopAll() {
	w.mu.Lock()
	entries := make([]*runnerEntry, 0, len(w.runners))
	for _, e := range w.runners {
		entries = append(entries, e)
	}
	w.mu.Unlock()

	for _, e := range entries {
		e.r.Stop()
	}

	// Wait for all runners to drain (lag == 0) with a 30s timeout.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		allDone := true
		for _, e := range entries {
			if e.r.Lag() > 0 {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
}
