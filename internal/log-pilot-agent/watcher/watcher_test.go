package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	k8swatch "k8s.io/apimachinery/pkg/watch"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
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
		kubefake.NewSimpleClientset(), //nolint:staticcheck
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
	if err := os.WriteFile(logFile, []byte(`{"msg":"hello"}`+"\n"), 0o644); err != nil {
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

func TestBuildRunnerInvalidTransform(t *testing.T) {
	cp := logpilotv1alpha1.ContainerPolicy{
		Name:       "app",
		LogType:    "applog",
		Transforms: []logpilotv1alpha1.TransformSpec{{Type: "unknown"}},
		Output: logpilotv1alpha1.OutputSpec{
			Type: "file",
			Config: map[string]apiextensionsv1.JSON{
				"path": {Raw: []byte(`"` + filepath.Join(t.TempDir(), "out.json") + `"`)},
			},
		},
	}
	r, err := buildRunner(cp, t.TempDir(), "test-uid", Config{MetaDir: t.TempDir()})
	if err == nil {
		t.Fatal("expected invalid transform to fail")
	}
	if r != nil {
		t.Fatal("expected nil runner for invalid transform")
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
	if err := os.MkdirAll(podLogDir, 0o755); err != nil {
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

func TestOnPodAddStartsRunnerForValidPolicy(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "out.json")
	policy := logpilotv1alpha1.ContainerPolicy{
		Name:    "app",
		LogType: "applog",
		Path:    "/var/log/app",
		Output: logpilotv1alpha1.OutputSpec{
			Type: "file",
			Config: map[string]apiextensionsv1.JSON{
				"path": {Raw: []byte(`"` + outPath + `"`)},
			},
		},
	}
	pod := podWithPolicies(t, "uid-valid", policy)
	w := newTestWatcherWithConfig(t, Config{LogDir: t.TempDir(), MetaDir: t.TempDir()})

	if ok := w.OnPodAdd(pod); !ok {
		t.Fatal("expected pod with valid policy to be ready")
	}

	key := runnerKey(string(pod.UID), policy.Name, policy.LogType)
	w.mu.Lock()
	entry := w.runners[key]
	w.mu.Unlock()
	if entry == nil {
		t.Fatalf("expected runner %q to be created", key)
	}
	if info, err := os.Stat(entry.logPath); err != nil {
		t.Fatalf("expected log path to exist: %v", err)
	} else if !info.IsDir() {
		t.Fatalf("expected %s to be a directory", entry.logPath)
	}

	w.StopAll()
	waitRunnerDone(t, entry.r.Done())
}

func TestOnPodAddBuildRunnerFailureReturnsNotReady(t *testing.T) {
	policy := logpilotv1alpha1.ContainerPolicy{
		Name:    "app",
		LogType: "applog",
		Path:    "/var/log/app",
		Output:  logpilotv1alpha1.OutputSpec{Type: "unknown"},
	}
	w := newTestWatcherWithConfig(t, Config{LogDir: t.TempDir(), MetaDir: t.TempDir()})

	if ok := w.OnPodAdd(podWithPolicies(t, "uid-bad-output", policy)); ok {
		t.Fatal("expected invalid output policy to report not ready")
	}
	if len(w.runners) != 0 {
		t.Fatalf("expected no runners, got %d", len(w.runners))
	}
}

func TestOnPodAddEnsureLogPathFailureReturnsNotReady(t *testing.T) {
	logDir := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(logDir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	policy := logpilotv1alpha1.ContainerPolicy{
		Name:    "app",
		LogType: "applog",
		Path:    "/var/log/app",
		Output: logpilotv1alpha1.OutputSpec{
			Type: "file",
			Config: map[string]apiextensionsv1.JSON{
				"path": {Raw: []byte(`"` + filepath.Join(t.TempDir(), "out.json") + `"`)},
			},
		},
	}
	w := newTestWatcherWithConfig(t, Config{LogDir: logDir, MetaDir: t.TempDir()})

	if ok := w.OnPodAdd(podWithPolicies(t, "uid-bad-logdir", policy)); ok {
		t.Fatal("expected log path failure to report not ready")
	}
	if len(w.runners) != 0 {
		t.Fatalf("expected no runners, got %d", len(w.runners))
	}
}

func TestSyncPodsSeenPodIsSkipped(t *testing.T) {
	policy := logpilotv1alpha1.ContainerPolicy{
		Name:    "app",
		LogType: "applog",
		Path:    "/var/log/app",
		Output: logpilotv1alpha1.OutputSpec{
			Type: "file",
			Config: map[string]apiextensionsv1.JSON{
				"path": {Raw: []byte(`"` + filepath.Join(t.TempDir(), "out.json") + `"`)},
			},
		},
	}
	pod := podWithPolicies(t, "uid-seen", policy)
	pod.Spec.NodeName = "node1"
	w := New(
		Config{NodeName: "node1", LogDir: t.TempDir(), MetaDir: t.TempDir()},
		newIndexedPodClient(t, pod),
		kubefake.NewSimpleClientset(), //nolint:staticcheck
		status.New(),
	)
	seen := map[string]bool{string(pod.UID): true}

	if err := w.syncPods(context.Background(), seen); err != nil {
		t.Fatalf("expected sync to succeed, got %v", err)
	}
	if !seen[string(pod.UID)] {
		t.Fatalf("expected pod %q to remain seen", pod.UID)
	}
	if len(w.runners) != 0 {
		t.Fatalf("expected seen pod to be skipped, got %d runners", len(w.runners))
	}
}

func TestSyncPodsRemovesMissingSeenPod(t *testing.T) {
	w := New(
		Config{NodeName: "node1", LogDir: t.TempDir(), MetaDir: t.TempDir()},
		newIndexedPodClient(t),
		kubefake.NewSimpleClientset(), //nolint:staticcheck
		status.New(),
	)
	r := newRunningRunner(t)
	key := runnerKey("uid-missing", "app", "applog")
	w.mu.Lock()
	w.runners[key] = &runnerEntry{r: r}
	w.mu.Unlock()
	seen := map[string]bool{"uid-missing": true}

	if err := w.syncPods(context.Background(), seen); err != nil {
		t.Fatalf("expected sync to succeed, got %v", err)
	}
	if seen["uid-missing"] {
		t.Fatal("expected missing pod to be removed from seen")
	}
	waitRunnerDone(t, r.Done())
}

func TestSyncPodsLeavesPodUnseenWhenRunnerNotReady(t *testing.T) {
	pod := podWithPolicies(t, "uid-unready", logpilotv1alpha1.ContainerPolicy{
		Name:    "app",
		LogType: "applog",
		Path:    "/var/log/app",
		Output:  logpilotv1alpha1.OutputSpec{Type: "unknown"},
	})
	pod.Spec.NodeName = "node1"
	w := New(
		Config{NodeName: "node1", LogDir: t.TempDir(), MetaDir: t.TempDir()},
		newIndexedPodClient(t, pod),
		kubefake.NewSimpleClientset(), //nolint:staticcheck
		status.New(),
	)
	seen := make(map[string]bool)

	if err := w.syncPods(context.Background(), seen); err != nil {
		t.Fatalf("expected sync to succeed, got %v", err)
	}
	if seen[string(pod.UID)] {
		t.Fatalf("expected pod %q to remain unseen", pod.UID)
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
	w := New(Config{NodeName: "node1", LogDir: t.TempDir(), MetaDir: t.TempDir()}, c, kubefake.NewSimpleClientset(), status.New()) //nolint:staticcheck
	seen := make(map[string]bool)

	if err := w.syncPods(context.Background(), seen); err != nil {
		t.Fatalf("expected empty sync to succeed, got %v", err)
	}
	if len(seen) != 0 {
		t.Fatalf("expected no seen pods, got %#v", seen)
	}
}

func TestSyncPodsAddsReadyPod(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "mypod", Namespace: "default", UID: "uid-ready"},
		Spec:       corev1.PodSpec{NodeName: "node1"},
	}
	w := New(
		Config{NodeName: "node1", LogDir: t.TempDir(), MetaDir: t.TempDir()},
		newIndexedPodClient(t, pod),
		kubefake.NewSimpleClientset(), //nolint:staticcheck
		status.New(),
	)
	seen := make(map[string]bool)

	if err := w.syncPods(context.Background(), seen); err != nil {
		t.Fatalf("expected sync to succeed, got %v", err)
	}
	if !seen[string(pod.UID)] {
		t.Fatalf("expected pod %q to be marked seen", pod.UID)
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

func TestConsumePodWatchClusterTickReconcilesPolicies(t *testing.T) {
	policy := newClusterPolicy("policy-a", "k8sEvent", filepath.Join(t.TempDir(), "out.json"))
	w := newTestWatcherWithConfig(t, Config{Namespace: "default", NodeName: "node1", MetaDir: t.TempDir()}, &policy)
	fw := k8swatch.NewFake()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clusterSeen := make(map[string]bool)
	tick := make(chan time.Time, 1)
	done := make(chan error, 1)
	key := "ClusterLogPilotPolicy/policy-a"

	go func() {
		done <- w.consumePodWatch(ctx, fw, make(map[string]bool), clusterSeen, tick)
	}()
	tick <- time.Now()
	time.Sleep(50 * time.Millisecond)
	cancel()
	fw.Stop()

	if err := <-done; err != nil {
		t.Fatalf("expected nil on ctx cancel, got %v", err)
	}
	if !clusterSeen[key] {
		t.Fatalf("expected cluster policy %q to be seen", key)
	}
	w.stopRunner(key)
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

func TestAcquireClusterPolicyLeaseHeldByOtherNode(t *testing.T) {
	other := "node2"
	duration := int32(30)
	renewed := metav1.NewMicroTime(time.Now())
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: dns1123Name("logpilot-cluster-policy-my-policy"), Namespace: "default"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &other,
			LeaseDurationSeconds: &duration,
			RenewTime:            &renewed,
		},
	}
	w := newTestWatcherWithConfig(t, Config{Namespace: "default", NodeName: "node1"}, lease)

	if got := w.acquireClusterPolicyLease(context.Background(), "my-policy"); got {
		t.Fatal("expected active lease held by another node to fail")
	}
}

func TestAcquireClusterPolicyLeaseRenewsSameHolder(t *testing.T) {
	holder := "node1"
	duration := int32(30)
	oldRenew := metav1.NewMicroTime(time.Now().Add(-time.Minute))
	leaseName := dns1123Name("logpilot-cluster-policy-my-policy")
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: leaseName, Namespace: "default"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &duration,
			RenewTime:            &oldRenew,
		},
	}
	w := newTestWatcherWithConfig(t, Config{Namespace: "default", NodeName: "node1"}, lease)

	if got := w.acquireClusterPolicyLease(context.Background(), "my-policy"); !got {
		t.Fatal("expected same holder to renew lease")
	}

	updated := &coordinationv1.Lease{}
	if err := w.client.Get(context.Background(), ctrlclient.ObjectKey{Namespace: "default", Name: leaseName}, updated); err != nil {
		t.Fatal(err)
	}
	if updated.Spec.AcquireTime == nil {
		t.Fatal("expected missing acquire time to be set on renewal")
	}
	if updated.Spec.RenewTime == nil || !updated.Spec.RenewTime.After(oldRenew.Time) {
		t.Fatalf("expected renew time to advance, got %#v", updated.Spec.RenewTime)
	}
}

func TestAcquireClusterPolicyLeaseRetriesAlreadyExists(t *testing.T) {
	leaseName := dns1123Name("logpilot-cluster-policy-my-policy")
	baseClient := clientfake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()
	createCalls := 0
	client := &interceptClient{
		Client: baseClient,
		createHook: func(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.CreateOption) error {
			createCalls++
			if createCalls != 1 {
				return baseClient.Create(ctx, obj, opts...)
			}
			lease := obj.(*coordinationv1.Lease).DeepCopy()
			if err := baseClient.Create(ctx, lease); err != nil {
				t.Fatalf("seed lease on already-exists path: %v", err)
			}
			return apierrors.NewAlreadyExists(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, lease.Name)
		},
	}
	w := New(Config{Namespace: "default", NodeName: "node1"}, client, kubefake.NewSimpleClientset(), status.New()) //nolint:staticcheck

	if got := w.acquireClusterPolicyLease(context.Background(), "my-policy"); !got {
		t.Fatal("expected already-exists retry to succeed")
	}
	lease := &coordinationv1.Lease{}
	if err := baseClient.Get(context.Background(), ctrlclient.ObjectKey{Namespace: "default", Name: leaseName}, lease); err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "node1" {
		t.Fatalf("expected holder identity node1, got %#v", lease.Spec.HolderIdentity)
	}
}

func TestAcquireClusterPolicyLeaseRetriesConflict(t *testing.T) {
	holder := "node1"
	duration := int32(30)
	leaseName := dns1123Name("logpilot-cluster-policy-my-policy")
	baseClient := clientfake.NewClientBuilder().WithScheme(newTestScheme(t)).WithRuntimeObjects(&coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: leaseName, Namespace: "default"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &duration,
		},
	}).Build()
	updateCalls := 0
	client := &interceptClient{
		Client: baseClient,
		updateHook: func(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.UpdateOption) error {
			updateCalls++
			if updateCalls == 1 {
				return apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, obj.GetName(), fmt.Errorf("conflict"))
			}
			return baseClient.Update(ctx, obj, opts...)
		},
	}
	w := New(Config{Namespace: "default", NodeName: "node1"}, client, kubefake.NewSimpleClientset(), status.New()) //nolint:staticcheck

	if got := w.acquireClusterPolicyLease(context.Background(), "my-policy"); !got {
		t.Fatal("expected conflict retry to succeed")
	}
	if updateCalls < 2 {
		t.Fatalf("expected at least two update attempts, got %d", updateCalls)
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

func TestReconcileClusterPoliciesLifecycle(t *testing.T) {
	policy := newClusterPolicy("policy-a", "k8sEvent", filepath.Join(t.TempDir(), "out.json"))
	w := newTestWatcherWithConfig(t, Config{Namespace: "default", NodeName: "node1", MetaDir: t.TempDir()}, &policy)
	seen := make(map[string]bool)
	key := "ClusterLogPilotPolicy/policy-a"

	w.reconcileClusterPolicies(context.Background(), seen)
	if !seen[key] {
		t.Fatalf("expected policy %q to be seen", key)
	}
	w.mu.Lock()
	entry := w.runners[key]
	w.mu.Unlock()
	if entry == nil {
		t.Fatalf("expected runner %q to exist", key)
	}

	w.reconcileClusterPolicies(context.Background(), seen)
	if err := w.client.Delete(context.Background(), &policy); err != nil {
		t.Fatal(err)
	}
	w.reconcileClusterPolicies(context.Background(), seen)
	if seen[key] {
		t.Fatalf("expected policy %q to be removed after deletion", key)
	}
	waitRunnerDone(t, entry.r.Done())
}

func TestReconcileClusterPoliciesStopsRunnerWhenLeaseLost(t *testing.T) {
	other := "node2"
	duration := int32(30)
	renewed := metav1.NewMicroTime(time.Now())
	policy := newClusterPolicy("policy-a", "k8sEvent", filepath.Join(t.TempDir(), "out.json"))
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: dns1123Name("logpilot-cluster-policy-policy-a"), Namespace: "default"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &other,
			LeaseDurationSeconds: &duration,
			RenewTime:            &renewed,
		},
	}
	w := newTestWatcherWithConfig(t, Config{Namespace: "default", NodeName: "node1", MetaDir: t.TempDir()}, &policy, lease)
	key := "ClusterLogPilotPolicy/policy-a"
	r := newRunningRunner(t)
	w.mu.Lock()
	w.runners[key] = &runnerEntry{r: r}
	w.mu.Unlock()
	seen := map[string]bool{key: true}

	w.reconcileClusterPolicies(context.Background(), seen)

	if seen[key] {
		t.Fatalf("expected policy %q to be removed after lease loss", key)
	}
	waitRunnerDone(t, r.Done())
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

func TestStartClusterPolicyRunnerIgnoresUnsupportedInput(t *testing.T) {
	w := newTestWatcherWithConfig(t, Config{Namespace: "default", NodeName: "node1", MetaDir: t.TempDir()})
	key := "ClusterLogPilotPolicy/policy-a"
	policy := newClusterPolicy("policy-a", "dir", filepath.Join(t.TempDir(), "out.json"))

	if ok := w.startClusterPolicyRunner(policy, key); !ok {
		t.Fatal("expected unsupported input type to be ignored successfully")
	}
	if len(w.runners) != 0 {
		t.Fatalf("expected no runners for unsupported input, got %d", len(w.runners))
	}
}

func TestStartClusterPolicyRunnerBadTransform(t *testing.T) {
	w := newTestWatcherWithConfig(t, Config{Namespace: "default", NodeName: "node1", MetaDir: t.TempDir()})
	policy := newClusterPolicy("policy-a", "k8sEvent", filepath.Join(t.TempDir(), "out.json"))
	policy.Spec.Transforms = []logpilotv1alpha1.TransformSpec{{Type: "unknown"}}

	if ok := w.startClusterPolicyRunner(policy, "ClusterLogPilotPolicy/policy-a"); ok {
		t.Fatal("expected invalid transform to prevent runner start")
	}
}

func TestStartClusterPolicyRunnerObjectState(t *testing.T) {
	w := newTestWatcherWithConfig(t, Config{Namespace: "default", NodeName: "node1", MetaDir: t.TempDir()})
	key := "ClusterLogPilotPolicy/policy-object-state"
	policy := newClusterPolicy("policy-object-state", "k8sObjectState", filepath.Join(t.TempDir(), "out.json"))
	policy.Spec.Input.Config = map[string]apiextensionsv1.JSON{
		"namespaces": {Raw: []byte(`["default"]`)},
		"resources":  {Raw: []byte(`["node"]`)},
	}
	defer w.stopRunner(key)

	if ok := w.startClusterPolicyRunner(policy, key); !ok {
		t.Fatal("expected object state runner to start")
	}

	w.mu.Lock()
	_, exists := w.runners[key]
	w.mu.Unlock()
	if !exists {
		t.Fatalf("expected runner %q to exist", key)
	}
}

func TestStopAllStopsRunningRunners(t *testing.T) {
	w := newTestWatcher(t)
	r1 := newRunningRunner(t)
	r2 := newRunningRunner(t)
	w.mu.Lock()
	w.runners["uid1/app/log"] = &runnerEntry{r: r1}
	w.runners["uid2/app/log"] = &runnerEntry{r: r2}
	w.mu.Unlock()

	w.StopAll()

	waitRunnerDone(t, r1.Done())
	waitRunnerDone(t, r2.Done())
}

func TestWatcherStartContextCancel(t *testing.T) {
	scheme := newTestScheme(t)
	c := clientfake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(obj ctrlclient.Object) []string {
			pod := obj.(*corev1.Pod)
			return []string{pod.Spec.NodeName}
		}).
		Build()

	kube := kubefake.NewSimpleClientset() //nolint:staticcheck
	fw := k8swatch.NewFake()
	watchStarted := make(chan struct{})
	once := sync.Once{}
	kube.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, k8swatch.Interface, error) {
		once.Do(func() { close(watchStarted) })
		return true, fw, nil
	})

	w := New(
		Config{NodeName: "node1", LogDir: t.TempDir(), MetaDir: t.TempDir()},
		c,
		kube,
		status.New(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	select {
	case <-watchStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("watch did not start in time")
	}
	cancel()
	fw.Stop()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return in time")
	}
}

func TestWatcherStartWatchChannelClosedThenCancel(t *testing.T) {
	scheme := newTestScheme(t)
	c := clientfake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(obj ctrlclient.Object) []string {
			pod := obj.(*corev1.Pod)
			return []string{pod.Spec.NodeName}
		}).
		Build()

	kube := kubefake.NewSimpleClientset() //nolint:staticcheck
	callCount := 0
	var mu sync.Mutex
	fw1 := k8swatch.NewFake()
	fw2 := k8swatch.NewFake()
	firstWatchStarted := make(chan struct{})
	secondWatchStarted := make(chan struct{})
	kube.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, k8swatch.Interface, error) {
		mu.Lock()
		defer mu.Unlock()
		callCount++
		if callCount == 1 {
			close(firstWatchStarted)
			return true, fw1, nil
		}
		if callCount == 2 {
			close(secondWatchStarted)
		}
		return true, fw2, nil
	})

	w := New(
		Config{NodeName: "node1", LogDir: t.TempDir(), MetaDir: t.TempDir()},
		c,
		kube,
		status.New(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	select {
	case <-firstWatchStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first watch did not start in time")
	}
	fw1.Stop()

	select {
	case <-secondWatchStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("second watch did not start in time")
	}
	cancel()
	fw2.Stop()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return in time")
	}
}

func TestWatcherStartWatchErrorThenCancel(t *testing.T) {
	scheme := newTestScheme(t)
	c := clientfake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(obj ctrlclient.Object) []string {
			pod := obj.(*corev1.Pod)
			return []string{pod.Spec.NodeName}
		}).
		Build()

	kube := kubefake.NewSimpleClientset() //nolint:staticcheck
	callCount := 0
	var mu sync.Mutex
	fw := k8swatch.NewFake()
	firstWatchAttempted := make(chan struct{})
	kube.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, k8swatch.Interface, error) {
		mu.Lock()
		defer mu.Unlock()
		callCount++
		if callCount == 1 {
			close(firstWatchAttempted)
			return true, nil, fmt.Errorf("transient watch error")
		}
		return true, fw, nil
	})

	w := New(
		Config{NodeName: "node1", LogDir: t.TempDir(), MetaDir: t.TempDir()},
		c,
		kube,
		status.New(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	select {
	case <-firstWatchAttempted:
	case <-time.After(2 * time.Second):
		t.Fatal("watch error path was not exercised in time")
	}
	cancel()
	fw.Stop()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return in time")
	}
}

func podWithPolicies(t *testing.T, uid string, policies ...logpilotv1alpha1.ContainerPolicy) *corev1.Pod {
	t.Helper()

	raw, err := json.Marshal(policies)
	if err != nil {
		t.Fatal(err)
	}
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "mypod",
		Namespace: "default",
		UID:       types.UID(uid),
		Annotations: map[string]string{
			podLogPolicyAnnotation: string(raw),
		},
	}}
}

func newIndexedPodClient(t *testing.T, objs ...runtime.Object) ctrlclient.Client {
	t.Helper()

	return clientfake.NewClientBuilder().
		WithScheme(newTestScheme(t)).
		WithRuntimeObjects(objs...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(obj ctrlclient.Object) []string {
			pod := obj.(*corev1.Pod)
			return []string{pod.Spec.NodeName}
		}).
		Build()
}

type interceptClient struct {
	ctrlclient.Client
	createHook func(context.Context, ctrlclient.Object, ...ctrlclient.CreateOption) error
	updateHook func(context.Context, ctrlclient.Object, ...ctrlclient.UpdateOption) error
}

func (c *interceptClient) Create(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.CreateOption) error {
	if c.createHook != nil {
		return c.createHook(ctx, obj, opts...)
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *interceptClient) Update(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.UpdateOption) error {
	if c.updateHook != nil {
		return c.updateHook(ctx, obj, opts...)
	}
	return c.Client.Update(ctx, obj, opts...)
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
		kubefake.NewSimpleClientset(), //nolint:staticcheck
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
