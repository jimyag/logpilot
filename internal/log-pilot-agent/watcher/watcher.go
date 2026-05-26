package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
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
	Namespace string
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
	kube    kubernetes.Interface
	status  *status.Server
	runners map[string]*runnerEntry // key: podUID/container/logType
	mu      sync.Mutex
}

// New creates a Watcher.
func New(cfg Config, c client.Client, kube kubernetes.Interface, statusSrv *status.Server) *Watcher {
	return &Watcher{
		cfg:     cfg,
		client:  c,
		kube:    kube,
		status:  statusSrv,
		runners: make(map[string]*runnerEntry),
	}
}

// Start begins watching pods on this node. Blocks until ctx is cancelled.
func (w *Watcher) Start(ctx context.Context) error {
	seen := make(map[string]bool)
	clusterSeen := make(map[string]bool)

	// Initial sync: list all existing pods before starting the watch.
	if err := w.syncPods(ctx, seen); err != nil {
		return err
	}
	w.reconcileClusterPolicies(ctx, clusterSeen)

	// clusterPolicy ticker: still needs periodic refresh because ClusterPolicies
	// are not watched here; 30 s is sufficient (lease duration is 30 s).
	clusterTicker := time.NewTicker(30 * time.Second)
	defer clusterTicker.Stop()

	for {
		watcher, err := w.kube.CoreV1().Pods(metav1.NamespaceAll).Watch(ctx,
			metav1.ListOptions{
				FieldSelector: "spec.nodeName=" + w.cfg.NodeName,
			})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Transient error: back off and retry.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}
			continue
		}

		if err := w.consumePodWatch(ctx, watcher, seen, clusterSeen, clusterTicker.C); err != nil {
			watcher.Stop()
			if ctx.Err() != nil {
				return nil
			}
			// Watch expired or connection dropped; relist and rewatch.
			if syncErr := w.syncPods(ctx, seen); syncErr != nil && ctx.Err() != nil {
				return nil
			}
			continue
		}
		watcher.Stop()
		return nil
	}
}

// syncPods lists all pods on this node and starts runners for any new ones.
func (w *Watcher) syncPods(ctx context.Context, seen map[string]bool) error {
	podList := &corev1.PodList{}
	if err := w.client.List(ctx, podList,
		client.MatchingFieldsSelector{
			Selector: fields.OneTermEqualSelector("spec.nodeName", w.cfg.NodeName),
		}); err != nil {
		return err
	}

	current := make(map[string]bool, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		uid := string(pod.UID)
		current[uid] = true
		if !seen[uid] {
			if w.OnPodAdd(pod.DeepCopy()) {
				seen[uid] = true
			}
		}
	}
	// Clean up pods that disappeared since last sync.
	for uid := range seen {
		if !current[uid] {
			delete(seen, uid)
			w.onPodDeleted(uid)
		}
	}
	return nil
}

// consumePodWatch reads events from a pod watch stream, updating seen map and
// starting/stopping runners. Returns nil when ctx is cancelled, or an error
// when the watch channel closes (triggering a relist+rewatch in the caller).
func (w *Watcher) consumePodWatch(
	ctx context.Context,
	watcher watch.Interface,
	seen map[string]bool,
	clusterSeen map[string]bool,
	clusterTick <-chan time.Time,
) error {
	for {
		select {
		case <-ctx.Done():
			return nil

		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			uid := string(pod.UID)
			switch event.Type {
			case watch.Added, watch.Modified:
				if !seen[uid] {
					if w.OnPodAdd(pod.DeepCopy()) {
						seen[uid] = true
					}
				}
			case watch.Deleted:
				delete(seen, uid)
				w.onPodDeleted(uid)
			case watch.Error:
				return fmt.Errorf("watch error event received")
			}

		case <-clusterTick:
			w.reconcileClusterPolicies(ctx, clusterSeen)
		}
	}
}

// OnPodAdd starts runners for a pod that has the log policy annotation.
func (w *Watcher) OnPodAdd(pod *corev1.Pod) bool {
	policies, err := parsePoliciesFromPod(pod)
	if err != nil || len(policies) == 0 {
		return true
	}
	allReady := true
	for _, cp := range policies {
		key := runnerKey(string(pod.UID), cp.Name, cp.LogType)
		logPath := w.logPath(pod, cp)

		w.mu.Lock()
		_, exists := w.runners[key]
		w.mu.Unlock()
		if exists {
			continue
		}

		if err := ensureLogPath(pod, cp, logPath); err != nil {
			allReady = false
			continue
		}

		r, err := buildRunner(cp, logPath, string(pod.UID), w.cfg)
		if err != nil {
			allReady = false
			continue
		}
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
						w.status.UpdateRunner(key, r.Lag(), r.Sent())
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
	return allReady
}

// onPodDeleted stops runners for a deleted pod and schedules log dir cleanup.
func (w *Watcher) onPodDeleted(podUID string) {
	w.mu.Lock()
	var toDelete []string
	var entries []*runnerEntry
	for key, entry := range w.runners {
		if strings.HasPrefix(key, podUID+"/") {
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
			// Wait for runner to fully complete (drain + flush) with a 30s timeout.
			// This avoids goroutine leaks when output is unreachable.
			select {
			case <-entry.r.Done():
			case <-time.After(30 * time.Second):
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

func buildRunner(cp logpilotv1alpha1.ContainerPolicy, logPath, podUID string, cfg Config) (*runner.Runner, error) {
	batchLen := cp.BatchLen
	if batchLen == 0 {
		batchLen = 1000
	}

	// Include podUID in metaPath so multiple pods matching the same policy
	// don't share the same offset file and corrupt each other's state.
	metaPath := filepath.Join(cfg.MetaDir, "LogPilotPolicy",
		podUID, fmt.Sprintf("%s_%s_dir.json", cp.Name, cp.LogType))
	dirInput, err := input.NewDirInput(input.DirConfig{
		Dir:               logPath,
		MetaPath:          metaPath,
		ReadFrom:          "oldest",
		OffsetCommitEvery: 1000,
	})
	if err != nil {
		return nil, err
	}

	transforms, err := transformfactory.NewSliceFromSpecs(cp.Transforms)
	if err != nil {
		return nil, err
	}
	out, err := outputfactory.NewFromSpec(cp.Output)
	if err != nil {
		return nil, err
	}
	cln := cleanfactory.NewFromSpec(cp.Clean, logPath)

	return runner.New(runner.Config{
		Name:       fmt.Sprintf("%s/%s", cp.Name, cp.LogType),
		Input:      dirInput,
		Transforms: transforms,
		Output:     out,
		Clean:      cln,
		BatchLen:   batchLen,
	}), nil
}

func (w *Watcher) reconcileClusterPolicies(ctx context.Context, seen map[string]bool) {
	var policies logpilotv1alpha1.ClusterLogPilotPolicyList
	if err := w.client.List(ctx, &policies); err != nil {
		return
	}
	current := make(map[string]bool, len(policies.Items))
	for i := range policies.Items {
		policy := policies.Items[i]
		key := "ClusterLogPilotPolicy/" + policy.Name
		current[key] = true
		if !w.acquireClusterPolicyLease(ctx, policy.Name) {
			if seen[key] {
				w.stopRunner(key)
				delete(seen, key)
			}
			continue
		}
		if seen[key] {
			continue
		}
		if w.startClusterPolicyRunner(policy, key) {
			seen[key] = true
		}
	}
	for key := range seen {
		if current[key] {
			continue
		}
		w.stopRunner(key)
		delete(seen, key)
	}
}

func (w *Watcher) startClusterPolicyRunner(policy logpilotv1alpha1.ClusterLogPilotPolicy, key string) bool {
	if policy.Spec.Input.Type != "k8sEvent" && policy.Spec.Input.Type != "k8sObjectState" {
		return true
	}
	w.mu.Lock()
	if _, exists := w.runners[key]; exists {
		w.mu.Unlock()
		return true
	}
	w.mu.Unlock()

	namespaces := extractStringSlice(policy.Spec.Input.Config, "namespaces")
	transforms, err := transformfactory.NewSliceFromSpecs(policy.Spec.Transforms)
	if err != nil {
		return false
	}
	out, err := outputfactory.NewFromSpec(policy.Spec.Output)
	if err != nil {
		return false
	}
	batchLen := policy.Spec.Input.BatchLen
	if batchLen == 0 {
		batchLen = 1000
	}
	runnerInput := input.NewK8sEventInput(input.K8sEventConfig{
		Namespaces:          namespaces,
		ResourceVersionPath: filepath.Join(w.cfg.MetaDir, key, "resource-version"),
	}, w.kube)
	if policy.Spec.Input.Type == "k8sObjectState" {
		runnerInput = input.NewK8sObjectStateInput(input.K8sObjectStateConfig{
			Namespaces: namespaces,
			Resources:  extractStringSlice(policy.Spec.Input.Config, "resources"),
		}, w.kube)
	}
	r := runner.New(runner.Config{
		Name:       key,
		Input:      runnerInput,
		Transforms: transforms,
		Output:     out,
		BatchLen:   batchLen,
	})
	w.mu.Lock()
	w.runners[key] = &runnerEntry{r: r}
	w.mu.Unlock()
	go func() {
		ticker := time.NewTicker(time.Second)
		done := make(chan struct{})
		go func() {
			for {
				select {
				case <-ticker.C:
					w.status.UpdateRunner(key, r.Lag(), r.Sent())
				case <-done:
					ticker.Stop()
					return
				}
			}
		}()
		r.Run(context.Background())
		close(done)
		w.status.RemoveRunner(key)
	}()
	w.status.UpdateRunner(key, r.Lag(), 0)
	return true
}

func (w *Watcher) acquireClusterPolicyLease(ctx context.Context, policyName string) bool {
	if w.cfg.Namespace == "" || w.cfg.NodeName == "" {
		return false
	}
	name := dns1123Name("logpilot-cluster-policy-" + policyName)
	duration := int32(30)
	holder := w.cfg.NodeName

	// Retry up to 3 times to handle optimistic-concurrency conflicts (409).
	for range 3 {
		now := metav1.MicroTime{Time: time.Now()}

		lease := &coordinationv1.Lease{}
		err := w.client.Get(ctx, client.ObjectKey{Namespace: w.cfg.Namespace, Name: name}, lease)
		if apierrors.IsNotFound(err) {
			lease = &coordinationv1.Lease{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: w.cfg.Namespace},
				Spec: coordinationv1.LeaseSpec{
					HolderIdentity:       &holder,
					LeaseDurationSeconds: &duration,
					AcquireTime:          &now,
					RenewTime:            &now,
				},
			}
			createErr := w.client.Create(ctx, lease)
			if createErr == nil {
				return true
			}
			if !apierrors.IsAlreadyExists(createErr) {
				return false
			}
			// Another agent created it concurrently; re-fetch and retry.
			continue
		}
		if err != nil {
			return false
		}

		// Someone else holds a non-expired lease.
		if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != holder && lease.Spec.RenewTime != nil {
			ttl := time.Duration(duration) * time.Second
			if time.Since(lease.Spec.RenewTime.Time) <= ttl {
				return false
			}
		}

		lease.Spec.HolderIdentity = &holder
		lease.Spec.LeaseDurationSeconds = &duration
		lease.Spec.RenewTime = &now
		if lease.Spec.AcquireTime == nil {
			lease.Spec.AcquireTime = &now
		}
		updateErr := w.client.Update(ctx, lease)
		if updateErr == nil {
			return true
		}
		if !apierrors.IsConflict(updateErr) {
			return false
		}
		// 409 Conflict: resourceVersion changed between Get and Update.
		// Re-fetch and retry with the latest version.
	}
	return false
}

func dns1123Name(value string) string {
	value = strings.ToLower(value)
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > validation.DNS1123LabelMaxLength {
		out = strings.TrimRight(out[:validation.DNS1123LabelMaxLength], "-")
	}
	if out == "" {
		return "logpilot-cluster-policy"
	}
	return out
}

func (w *Watcher) stopRunner(key string) {
	w.mu.Lock()
	entry, ok := w.runners[key]
	if ok {
		delete(w.runners, key)
	}
	w.mu.Unlock()
	if !ok {
		return
	}
	entry.r.Stop()
	select {
	case <-entry.r.Done():
	case <-time.After(30 * time.Second):
	}
	w.status.RemoveRunner(key)
}

func extractStringSlice(config map[string]apiextensionsv1.JSON, key string) []string {
	v, ok := config[key]
	if !ok {
		return nil
	}
	var out []string
	if err := json.Unmarshal(v.Raw, &out); err != nil {
		return nil
	}
	return out
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

// StopAll signals all running runners to stop and waits for them to fully
// complete (drain + flush + offset commit) before returning.
// Called during graceful shutdown to ensure no data loss.
func (w *Watcher) StopAll() {
	w.mu.Lock()
	entries := make([]*runnerEntry, 0, len(w.runners))
	for _, e := range w.runners {
		entries = append(entries, e)
	}
	w.mu.Unlock()

	// Signal all runners to stop.
	for _, e := range entries {
		e.r.Stop()
	}

	// Wait for each runner goroutine to fully complete (not just lag==0).
	// Done() is closed at the end of runner.Run(), after shutdown() finishes.
	timeout := time.After(30 * time.Second)
	for _, e := range entries {
		select {
		case <-e.r.Done():
		case <-timeout:
			return
		}
	}
}
