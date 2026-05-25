package watcher

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8swatch "k8s.io/apimachinery/pkg/watch"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/output"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/runner"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/status"
)

func newTestWatcher(t *testing.T) *Watcher {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := logpilotv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	return New(
		Config{NodeName: "node1", LogDir: "/logs"},
		clientfake.NewClientBuilder().WithScheme(scheme).Build(),
		kubefake.NewSimpleClientset(),
		status.New(),
	)
}

func TestBuildRunnerFileOutput(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "out.json")
	logPath := t.TempDir()

	cp := logpilotv1alpha1.ContainerPolicy{
		Name:     "app",
		LogType:  "applog",
		Path:     "/app/logs",
		Delivery: "guaranteed",
		BatchLen: 10,
		Output: logpilotv1alpha1.OutputSpec{
			Type: "file",
			Config: map[string]apiextensionsv1.JSON{
				"path": {Raw: []byte(`"` + outPath + `"`)},
			},
		},
	}

	cfg := Config{
		LogDir:  t.TempDir(),
		MetaDir: t.TempDir(),
	}

	r, err := buildRunner(cp, logPath, "test-uid", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("expected non-nil runner")
	}
}

func TestBuildRunnerWithTransforms(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "out.json")
	logPath := t.TempDir()

	// Seed log file so FileInput can open it.
	logFile := filepath.Join(logPath, "app.log")
	if err := os.WriteFile(logFile, []byte(`{"msg":"hello"}`+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	cp := logpilotv1alpha1.ContainerPolicy{
		Name:    "app",
		LogType: "applog",
		Transforms: []logpilotv1alpha1.TransformSpec{
			{Type: "json"},
			{Type: "label", Config: map[string]apiextensionsv1.JSON{
				"fields": {Raw: []byte(`{"env":"test"}`)},
			}},
		},
		Output: logpilotv1alpha1.OutputSpec{
			Type: "file",
			Config: map[string]apiextensionsv1.JSON{
				"path": {Raw: []byte(`"` + outPath + `"`)},
			},
		},
	}

	r, err := buildRunner(cp, logPath, "test-uid", Config{MetaDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("expected non-nil runner")
	}
}

func TestBuildRunnerNoOutput(t *testing.T) {
	// Missing output config should be surfaced instead of starting a runner
	// that reads logs without sending them anywhere.
	cp := logpilotv1alpha1.ContainerPolicy{
		Name:    "app",
		LogType: "applog",
		Output:  logpilotv1alpha1.OutputSpec{Type: "unknown"},
	}
	r, err := buildRunner(cp, t.TempDir(), "test-uid", Config{MetaDir: t.TempDir()})
	if err == nil {
		t.Fatal("expected bad output config to fail")
	}
	if r != nil {
		t.Fatal("expected nil runner for bad output config")
	}
}

func TestParsePoliciesFromPodNoAnnotation(t *testing.T) {
	policies, err := parsePoliciesFromPod(&corev1.Pod{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if policies != nil {
		t.Fatalf("expected nil policies, got %#v", policies)
	}
}

func TestParsePoliciesFromPodValid(t *testing.T) {
	expected := []logpilotv1alpha1.ContainerPolicy{{Name: "app", LogType: "applog"}}
	raw, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{podLogPolicyAnnotation: string(raw)}}}
	policies, err := parsePoliciesFromPod(pod)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	if policies[0].Name != "app" || policies[0].LogType != "applog" {
		t.Fatalf("unexpected policy parsed: %#v", policies[0])
	}
}

func TestParsePoliciesFromPodInvalid(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{podLogPolicyAnnotation: "{"}}}

	if _, err := parsePoliciesFromPod(pod); err == nil {
		t.Fatal("expected invalid JSON annotation to return an error")
	}
}

func TestRunnerKey(t *testing.T) {
	if got := runnerKey("uid", "container", "logtype"); got != "uid/container/logtype" {
		t.Fatalf("unexpected runner key: %q", got)
	}
}

func TestDns1123NameLowercase(t *testing.T) {
	if got := dns1123Name("MyPolicy"); got != "mypolicy" {
		t.Fatalf("expected lowercase dns1123 name, got %q", got)
	}
}

func TestDns1123NameSpecialChars(t *testing.T) {
	if got := dns1123Name("my.policy_name"); got != "my-policy-name" {
		t.Fatalf("expected sanitized dns1123 name, got %q", got)
	}
}

func TestDns1123NameLeadingTrailingDash(t *testing.T) {
	if got := dns1123Name("-my-policy-"); got != "my-policy" {
		t.Fatalf("expected trimmed dns1123 name, got %q", got)
	}
}

func TestDns1123NameLong(t *testing.T) {
	got := dns1123Name(strings.Repeat("a", 70))
	if len(got) != 63 {
		t.Fatalf("expected dns1123 name length 63, got %d", len(got))
	}
}

func TestDns1123NameEmpty(t *testing.T) {
	if got := dns1123Name(""); got != "logpilot-cluster-policy" {
		t.Fatalf("expected fallback dns1123 name, got %q", got)
	}
}

func TestExtractStringSliceMissingKey(t *testing.T) {
	if got := extractStringSlice(map[string]apiextensionsv1.JSON{}, "namespaces"); got != nil {
		t.Fatalf("expected nil slice for missing key, got %#v", got)
	}
}

func TestExtractStringSliceValid(t *testing.T) {
	config := map[string]apiextensionsv1.JSON{
		"namespaces": {Raw: []byte(`["default","kube-system"]`)},
	}
	got := extractStringSlice(config, "namespaces")
	if len(got) != 2 || got[0] != "default" || got[1] != "kube-system" {
		t.Fatalf("unexpected string slice: %#v", got)
	}
}

func TestExtractStringSliceInvalidJSON(t *testing.T) {
	config := map[string]apiextensionsv1.JSON{
		"namespaces": {Raw: []byte(`{"bad":true}`)},
	}
	if got := extractStringSlice(config, "namespaces"); got != nil {
		t.Fatalf("expected nil slice for invalid JSON, got %#v", got)
	}
}

func TestLogPath(t *testing.T) {
	w := newTestWatcher(t)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid"}}
	cp := logpilotv1alpha1.ContainerPolicy{Name: "app", LogType: "applog"}

	if got := w.logPath(pod, cp); got != "/logs/LogPilotPolicy/default/test/uid/app/applog" {
		t.Fatalf("unexpected log path: %q", got)
	}
}

func TestPodLogDir(t *testing.T) {
	w := newTestWatcher(t)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid"}}

	if got := w.PodLogDir(pod); got != "/logs/LogPilotPolicy/default/test/uid" {
		t.Fatalf("unexpected pod log dir: %q", got)
	}
}

func TestStopAllEmpty(t *testing.T) {
	newTestWatcher(t).StopAll()
}

func TestWatcherCleanup(t *testing.T) {
	w := newTestWatcherWithConfig(t, Config{LogDir: t.TempDir()})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mypod", Namespace: "default", UID: "uid1"}}
	podLogDir := w.PodLogDir(pod)
	if err := os.MkdirAll(podLogDir, 0755); err != nil {
		t.Fatal(err)
	}

	if err := w.Cleanup(pod); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(podLogDir); !os.IsNotExist(err) {
		t.Fatalf("expected %s to be removed, got err=%v", podLogDir, err)
	}
}

func TestWatcherCleanupNonExistent(t *testing.T) {
	w := newTestWatcherWithConfig(t, Config{LogDir: t.TempDir()})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mypod", Namespace: "default", UID: "uid1"}}

	if err := w.Cleanup(pod); err != nil {
		t.Fatalf("expected no error cleaning up missing dir, got %v", err)
	}
}

func TestStopRunnerNoEntry(t *testing.T) {
	newTestWatcher(t).stopRunner("missing")
}

func TestStopRunnerWithEntry(t *testing.T) {
	w := newTestWatcher(t)
	key := "uid1/container/log"
	r := newRunningRunner(t)

	w.mu.Lock()
	w.runners[key] = &runnerEntry{r: r}
	w.mu.Unlock()

	w.stopRunner(key)

	w.mu.Lock()
	_, exists := w.runners[key]
	w.mu.Unlock()
	if exists {
		t.Fatalf("expected runner %q to be removed", key)
	}
	waitRunnerDone(t, r.Done())
}

func TestOnPodDeletedNoPodRunners(t *testing.T) {
	newTestWatcher(t).onPodDeleted("missing")
}

func TestOnPodDeletedWithRunners(t *testing.T) {
	w := newTestWatcher(t)
	r1 := newRunningRunner(t)
	r2 := newRunningRunner(t)
	r3 := newRunningRunner(t)

	w.mu.Lock()
	w.runners["uid1/app/log"] = &runnerEntry{r: r1}
	w.runners["uid1/sidecar/log"] = &runnerEntry{r: r2}
	w.runners["uid2/app/log"] = &runnerEntry{r: r3}
	w.mu.Unlock()
	defer w.stopRunner("uid2/app/log")

	w.onPodDeleted("uid1")

	w.mu.Lock()
	_, hasFirst := w.runners["uid1/app/log"]
	_, hasSecond := w.runners["uid1/sidecar/log"]
	_, hasOther := w.runners["uid2/app/log"]
	w.mu.Unlock()
	if hasFirst || hasSecond {
		t.Fatal("expected deleted pod runners to be removed")
	}
	if !hasOther {
		t.Fatal("expected unrelated runner to remain")
	}
	waitRunnerDone(t, r1.Done())
	waitRunnerDone(t, r2.Done())
}

func TestOnPodAddNoPolicies(t *testing.T) {
	w := newTestWatcherWithConfig(t, Config{LogDir: t.TempDir(), MetaDir: t.TempDir()})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mypod", Namespace: "default", UID: "uid1"}}

	if ok := w.OnPodAdd(pod); !ok {
		t.Fatal("expected pod without policies to be treated as ready")
	}
	if len(w.runners) != 0 {
		t.Fatalf("expected no runners, got %d", len(w.runners))
	}
}

func TestOnPodAddInvalidAnnotation(t *testing.T) {
	w := newTestWatcherWithConfig(t, Config{LogDir: t.TempDir(), MetaDir: t.TempDir()})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "mypod",
		Namespace: "default",
		UID:       "uid1",
		Annotations: map[string]string{
			podLogPolicyAnnotation: "{",
		},
	}}

	if ok := w.OnPodAdd(pod); !ok {
		t.Fatal("expected pod with invalid annotation to return ready")
	}
	if len(w.runners) != 0 {
		t.Fatalf("expected no runners, got %d", len(w.runners))
	}
}

func TestSyncPodsEmptyList(t *testing.T) {
	scheme := newTestScheme(t)
	c := clientfake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(obj ctrlclient.Object) []string {
			return []string{obj.(*corev1.Pod).Spec.NodeName}
		}).
		Build()
	w := New(Config{NodeName: "node1", LogDir: t.TempDir(), MetaDir: t.TempDir()}, c, kubefake.NewSimpleClientset(), status.New())
	seen := make(map[string]bool)

	if err := w.syncPods(context.Background(), seen); err != nil {
		t.Fatalf("expected empty sync to succeed, got %v", err)
	}
	if len(seen) != 0 {
		t.Fatalf("expected no seen pods, got %#v", seen)
	}
}

func TestConsumePodWatchCtxCancel(t *testing.T) {
	w := newTestWatcher(t)
	fw := k8swatch.NewFake()
	ctx, cancel := context.WithCancel(context.Background())
	seen := make(map[string]bool)
	clusterSeen := make(map[string]bool)
	tick := make(chan time.Time)
	done := make(chan error, 1)

	go func() {
		done <- w.consumePodWatch(ctx, fw, seen, clusterSeen, tick)
	}()
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("expected nil on ctx cancel, got %v", err)
	}
}

func TestConsumePodWatchChannelClosed(t *testing.T) {
	w := newTestWatcher(t)
	fw := k8swatch.NewFake()
	done := make(chan error, 1)

	go func() {
		done <- w.consumePodWatch(context.Background(), fw, make(map[string]bool), make(map[string]bool), make(chan time.Time))
	}()
	fw.Stop()

	if err := <-done; err == nil || err.Error() != "watch channel closed" {
		t.Fatalf("expected watch channel closed error, got %v", err)
	}
}

func TestConsumePodWatchAddedEvent(t *testing.T) {
	w := newTestWatcher(t)
	fw := k8swatch.NewFake()
	seen := make(map[string]bool)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mypod", Namespace: "default", UID: "uid1"}}
	done := make(chan error, 1)

	go func() {
		done <- w.consumePodWatch(context.Background(), fw, seen, make(map[string]bool), make(chan time.Time))
	}()
	fw.Add(pod)
	fw.Stop()

	if err := <-done; err == nil || err.Error() != "watch channel closed" {
		t.Fatalf("expected watch channel closed error, got %v", err)
	}
	if !seen[string(pod.UID)] {
		t.Fatalf("expected pod %q to be marked seen", pod.UID)
	}
}

func TestConsumePodWatchDeletedEvent(t *testing.T) {
	w := newTestWatcher(t)
	fw := k8swatch.NewFake()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mypod", Namespace: "default", UID: "uid1"}}
	seen := map[string]bool{string(pod.UID): true}
	done := make(chan error, 1)

	go func() {
		done <- w.consumePodWatch(context.Background(), fw, seen, make(map[string]bool), make(chan time.Time))
	}()
	fw.Delete(pod)
	fw.Stop()

	if err := <-done; err == nil || err.Error() != "watch channel closed" {
		t.Fatalf("expected watch channel closed error, got %v", err)
	}
	if seen[string(pod.UID)] {
		t.Fatalf("expected pod %q to be removed from seen", pod.UID)
	}
}

func TestConsumePodWatchErrorEvent(t *testing.T) {
	w := newTestWatcher(t)
	fw := k8swatch.NewFake()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mypod", Namespace: "default", UID: "uid1"}}
	done := make(chan error, 1)

	go func() {
		done <- w.consumePodWatch(context.Background(), fw, make(map[string]bool), make(map[string]bool), make(chan time.Time))
	}()
	fw.Error(pod)

	if err := <-done; err == nil || err.Error() != "watch error event received" {
		t.Fatalf("expected watch error event received, got %v", err)
	}
}

func TestAcquireClusterPolicyLeaseNoNamespace(t *testing.T) {
	w := newTestWatcherWithConfig(t, Config{NodeName: "node1"})

	if got := w.acquireClusterPolicyLease(context.Background(), "my-policy"); got {
		t.Fatal("expected lease acquisition without namespace to fail")
	}
}

func TestAcquireClusterPolicyLeaseCreateNew(t *testing.T) {
	w := newTestWatcherWithConfig(t, Config{Namespace: "default", NodeName: "node1"})

	if got := w.acquireClusterPolicyLease(context.Background(), "my-policy"); !got {
		t.Fatal("expected lease to be created")
	}

	lease := &coordinationv1.Lease{}
	if err := w.client.Get(context.Background(), ctrlclient.ObjectKey{Namespace: "default", Name: dns1123Name("logpilot-cluster-policy-my-policy")}, lease); err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "node1" {
		t.Fatalf("expected holder identity node1, got %#v", lease.Spec.HolderIdentity)
	}
}

func TestReconcileClusterPoliciesEmpty(t *testing.T) {
	w := newTestWatcherWithConfig(t, Config{Namespace: "default", NodeName: "node1", MetaDir: t.TempDir()})
	seen := make(map[string]bool)

	w.reconcileClusterPolicies(context.Background(), seen)

	if len(seen) != 0 {
		t.Fatalf("expected no seen policies, got %#v", seen)
	}
	if len(w.runners) != 0 {
		t.Fatalf("expected no runners, got %d", len(w.runners))
	}
}

func TestReconcileClusterPoliciesNoNamespace(t *testing.T) {
	policy := newClusterPolicy("policy-a", "k8sEvent", filepath.Join(t.TempDir(), "out.json"))
	w := newTestWatcherWithConfig(t, Config{NodeName: "node1", MetaDir: t.TempDir()}, &policy)
	seen := make(map[string]bool)

	w.reconcileClusterPolicies(context.Background(), seen)

	if len(seen) != 0 {
		t.Fatalf("expected no seen policies, got %#v", seen)
	}
	if len(w.runners) != 0 {
		t.Fatalf("expected no runners, got %d", len(w.runners))
	}
}

func TestStartClusterPolicyRunner(t *testing.T) {
	w := newTestWatcherWithConfig(t, Config{Namespace: "default", NodeName: "node1", MetaDir: t.TempDir()})
	key := "ClusterLogPilotPolicy/policy-a"
	policy := newClusterPolicy("policy-a", "k8sEvent", filepath.Join(t.TempDir(), "out.json"))
	defer w.stopRunner(key)

	if ok := w.startClusterPolicyRunner(policy, key); !ok {
		t.Fatal("expected cluster policy runner to start")
	}

	w.mu.Lock()
	_, exists := w.runners[key]
	w.mu.Unlock()
	if !exists {
		t.Fatalf("expected runner %q to exist", key)
	}
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := logpilotv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func newTestWatcherWithConfig(t *testing.T, cfg Config, objs ...runtime.Object) *Watcher {
	t.Helper()

	if cfg.NodeName == "" {
		cfg.NodeName = "node1"
	}
	if cfg.LogDir == "" {
		cfg.LogDir = t.TempDir()
	}
	if cfg.MetaDir == "" {
		cfg.MetaDir = t.TempDir()
	}

	return New(
		cfg,
		clientfake.NewClientBuilder().WithScheme(newTestScheme(t)).WithRuntimeObjects(objs...).Build(),
		kubefake.NewSimpleClientset(),
		status.New(),
	)
}

func newClusterPolicy(name, inputType, outputPath string) logpilotv1alpha1.ClusterLogPilotPolicy {
	return logpilotv1alpha1.ClusterLogPilotPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: logpilotv1alpha1.ClusterLogPilotPolicySpec{
			Input: logpilotv1alpha1.InputSpec{Type: inputType},
			Output: logpilotv1alpha1.OutputSpec{
				Type: "file",
				Config: map[string]apiextensionsv1.JSON{
					"path": {Raw: []byte(`"` + outputPath + `"`)},
				},
			},
		},
	}
}

func newRunningRunner(t *testing.T) *runner.Runner {
	t.Helper()

	in := &blockingInput{started: make(chan struct{})}
	r := runner.New(runner.Config{Input: in, Output: noopOutput{}, BatchLen: 1})
	go r.Run(context.Background())

	select {
	case <-in.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runner to start")
	}
	return r
}

func waitRunnerDone(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runner to stop")
	}
}

type blockingInput struct {
	started   chan struct{}
	cancelled bool
}

func (in *blockingInput) ReadBatch(ctx context.Context, _ int) ([]input.Record, error) {
	if in.started != nil {
		close(in.started)
		in.started = nil
	}
	if in.cancelled {
		return nil, nil
	}
	<-ctx.Done()
	in.cancelled = true
	return nil, nil
}

func (*blockingInput) Commit() error { return nil }
func (*blockingInput) Lag() int64    { return 0 }
func (*blockingInput) Close() error  { return nil }

type noopOutput struct{}

func (noopOutput) WriteBatch(context.Context, []input.Record) error { return nil }
func (noopOutput) Close() error                                     { return nil }

var _ output.Output = noopOutput{}
