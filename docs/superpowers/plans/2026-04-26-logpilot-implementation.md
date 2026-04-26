# logpilot Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement logpilot — a K8s platform-level log collection system with three components: log-pilot-operator (CRD lifecycle), log-pilot-api (MutatingWebhook), and log-pilot-agent (log collection pipeline).

**Architecture:** log-pilot-operator manages the system using kubebuilder; log-pilot-api uses auto-cert-webhook to inject volumes and annotations into pods matching a LogPilotPolicy; log-pilot-agent runs as a DaemonSet watching pods on each node, creating runners (Input→Transform→Output→Clean pipelines) per pod/logType.

**Tech Stack:** Go 1.26, kubebuilder v4, auto-cert-webhook, controller-runtime, k8s.io/api, k8s.io/client-go, fsnotify, tail (file tailing)

---

## Chunk 1: CRD Types

### Task 1: Fill in LogPilot CRD types

**Files:**
- Modify: `api/v1alpha1/logpilot_types.go`
- Modify: `api/v1alpha1/logpilotpolicy_types.go`
- Modify: `api/v1alpha1/clusterlogpilotpolicy_types.go`

These are the shared data contracts used by all three components. Get them right before anything else.

- [ ] **Step 1: Replace LogPilotSpec in `api/v1alpha1/logpilot_types.go`**

```go
// LogPilotSpec defines the desired state of LogPilot (cluster singleton).
type LogPilotSpec struct {
	Agent AgentSpec `json:"agent,omitempty"`
	API   APISpec   `json:"api,omitempty"`
}

type AgentSpec struct {
	// ConfigDir is the host path where runner YAML files are stored.
	// +kubebuilder:default="/var/lib/log-pilot-agent/conf"
	ConfigDir string `json:"configDir,omitempty"`
	// MetaDir is the host path for offset and ResourceVersion persistence.
	// +kubebuilder:default="/var/lib/log-pilot-agent/meta"
	MetaDir string `json:"metaDir,omitempty"`
	// LogDir is the host path root for NoLost log files.
	// +kubebuilder:default="/var/log/log-pilot"
	LogDir  string           `json:"logDir,omitempty"`
	SelfLog AgentSelfLogSpec `json:"selfLog,omitempty"`
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

type AgentSelfLogSpec struct {
	// +kubebuilder:default="/var/log/log-pilot-agent"
	Dir string `json:"dir,omitempty"`
	// +kubebuilder:default=10
	ReserveCount int `json:"reserveCount,omitempty"`
}

type APISpec struct {
	// +kubebuilder:default=2
	Replicas int32 `json:"replicas,omitempty"`
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}
```

Add `corev1 "k8s.io/api/core/v1"` to imports.

- [ ] **Step 2: Replace LogPilotPolicySpec in `api/v1alpha1/logpilotpolicy_types.go`**

```go
// LogPilotPolicySpec defines log collection policy for pods matching a selector.
// Policy names must be globally unique within the cluster.
// Either (Selector + Containers) or (Input + Transforms + Output) must be set.
type LogPilotPolicySpec struct {
	// Selector matches pods by label. Used with Containers for pod log collection.
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`
	// Containers defines per-container collection pipelines.
	// +optional
	Containers []ContainerPolicy `json:"containers,omitempty"`

	// Input defines a standalone pipeline source (e.g. k8sEvent).
	// +optional
	Input *InputSpec `json:"input,omitempty"`
	// +optional
	Transforms []TransformSpec `json:"transforms,omitempty"`
	// +optional
	Output *OutputSpec `json:"output,omitempty"`
}

// ContainerPolicy defines the log collection pipeline for one container+logType pair.
type ContainerPolicy struct {
	// Name is the container name.
	// +kubebuilder:validation:Required
	Name string `json:"name"`
	// LogType is a user-defined label for this log stream (e.g. "applog", "std").
	// +kubebuilder:validation:Required
	LogType string `json:"logType"`
	// Path is the container-internal log directory. Use "-" for stdout/stderr.
	// +kubebuilder:validation:Required
	Path string `json:"path"`
	// Collector specifies how logs are collected.
	// +kubebuilder:validation:Enum=host;sidecar
	// +kubebuilder:default=host
	Collector string `json:"collector,omitempty"`
	// Delivery specifies the delivery guarantee.
	// +kubebuilder:validation:Enum=guaranteed;bestEffort
	// +kubebuilder:default=guaranteed
	Delivery string `json:"delivery,omitempty"`

	// BatchLen is the max records per batch across all pipeline stages.
	// +kubebuilder:default=1000
	BatchLen int `json:"batchLen,omitempty"`
	// BatchSize is the max bytes per output batch (bytes).
	// +kubebuilder:default=5242880
	BatchSize int `json:"batchSize,omitempty"`
	// BatchInterval is the max seconds to wait before flushing output.
	// +kubebuilder:default=300
	BatchInterval int `json:"batchInterval,omitempty"`

	// +optional
	Input InputSpec `json:"input,omitempty"`
	// +optional
	Transforms []TransformSpec `json:"transforms,omitempty"`
	// +kubebuilder:validation:Required
	Output OutputSpec `json:"output"`
	// +optional
	Clean CleanSpec `json:"clean,omitempty"`
}

type InputSpec struct {
	// Type is the input type: file, dir, k8sEvent, mongo.
	// +kubebuilder:validation:Required
	Type string `json:"type"`
	// BatchLen is the max records to read per batch.
	// +kubebuilder:default=1000
	BatchLen int `json:"batchLen,omitempty"`
	// Config holds type-specific configuration.
	// +optional
	Config map[string]apiextensionsv1.JSON `json:"config,omitempty"`
}

type TransformSpec struct {
	// Type is the transform type: json, label, drop, regex, multiline.
	// +kubebuilder:validation:Required
	Type string `json:"type"`
	// BatchLen is the max records to process per batch.
	// +kubebuilder:default=1000
	BatchLen int `json:"batchLen,omitempty"`
	// +optional
	Config map[string]apiextensionsv1.JSON `json:"config,omitempty"`
}

type OutputSpec struct {
	// Type is the output type: kafka, elasticsearch, http, file.
	// +kubebuilder:validation:Required
	Type string `json:"type"`
	// BatchLen is the max records per send batch.
	// +kubebuilder:default=500
	BatchLen int `json:"batchLen,omitempty"`
	// BatchSize is the max bytes per send batch.
	// +kubebuilder:default=5242880
	BatchSize int `json:"batchSize,omitempty"`
	// BatchInterval is the max seconds to wait before flushing.
	// +kubebuilder:default=300
	BatchInterval int `json:"batchInterval,omitempty"`
	// +optional
	Config map[string]apiextensionsv1.JSON `json:"config,omitempty"`
}

type CleanSpec struct {
	// Strategy controls file cleanup while the pod is running.
	// After pod deletion, all files are always cleaned once lag==0.
	// +kubebuilder:validation:Enum=afterCollected;retain;never
	// +kubebuilder:default=afterCollected
	Strategy string `json:"strategy,omitempty"`
	// RetainDays is the number of days to retain files after collection (strategy=retain).
	RetainDays int `json:"retainDays,omitempty"`
	// Interval is the cleanup check interval in seconds.
	// +kubebuilder:default=10
	Interval int `json:"interval,omitempty"`
	// ReserveFileNumber is the minimum number of files to keep.
	// +kubebuilder:default=10
	ReserveFileNumber int `json:"reserveFileNumber,omitempty"`
	// ReserveFileSize is the minimum total file size to keep in KB.
	// +kubebuilder:default=10240
	ReserveFileSize int `json:"reserveFileSize,omitempty"`
}
```

Add imports:
```go
import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)
```

- [ ] **Step 3: Replace ClusterLogPilotPolicySpec in `api/v1alpha1/clusterlogpilotpolicy_types.go`**

```go
// ClusterLogPilotPolicySpec defines a cluster-scoped standalone pipeline (e.g. K8s events).
type ClusterLogPilotPolicySpec struct {
	// +kubebuilder:validation:Required
	Input InputSpec `json:"input"`
	// +optional
	Transforms []TransformSpec `json:"transforms,omitempty"`
	// +kubebuilder:validation:Required
	Output OutputSpec `json:"output"`
}
```

- [ ] **Step 4: Regenerate deepcopy and CRD manifests**

```bash
cd /Users/jimyag/src/github/jimyag/logpilot
go get k8s.io/apiextensions-apiserver
make generate
make manifests
```

Expected: `api/v1alpha1/zz_generated.deepcopy.go` updated, `config/crd/bases/` populated.

- [ ] **Step 5: Verify build**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add api/ config/crd/
git commit -m "feat(api): define CRD types for LogPilot, LogPilotPolicy, ClusterLogPilotPolicy"
```

---

## Chunk 2: log-pilot-api (Webhook)

### Task 2: Implement pod injection logic

**Files:**
- Modify: `internal/log-pilot-api/api.go`
- Create: `internal/log-pilot-api/injector.go`
- Create: `internal/log-pilot-api/injector_test.go`
- Modify: `cmd/log-pilot-api/main.go`

The webhook reads LogPilotPolicy CRs, matches pod labels against selectors, and injects Volume/VolumeMount/Annotation/Env into matching pods.

- [ ] **Step 1: Write failing test for policy matching**

`internal/log-pilot-api/injector_test.go`:

```go
package logpilotapi

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func TestMatchesPolicy(t *testing.T) {
	policy := &logpilotv1alpha1.LogPilotPolicy{
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "myapp"},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"app": "myapp"},
		},
	}
	if !matchesPolicy(pod, policy) {
		t.Fatal("expected pod to match policy")
	}

	podNoMatch := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"app": "other"},
		},
	}
	if matchesPolicy(podNoMatch, policy) {
		t.Fatal("expected pod not to match policy")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/jimyag/src/github/jimyag/logpilot
go test ./internal/log-pilot-api/... 2>&1 | head -20
```

Expected: FAIL — `matchesPolicy` undefined.

- [ ] **Step 3: Implement `injector.go`**

`internal/log-pilot-api/injector.go`:

```go
package logpilotapi

import (
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

const (
	logVolumeName          = "log-pilot-logs"
	logHostPath            = "/var/log/log-pilot"
	podLogPolicyAnnotation = "beta.logpilot.io/log-policy"
)

// matchesPolicy returns true if the pod labels satisfy the policy selector.
func matchesPolicy(pod *corev1.Pod, policy *logpilotv1alpha1.LogPilotPolicy) bool {
	if policy.Spec.Selector == nil {
		return false
	}
	selector, err := metav1.LabelSelectorAsSelector(policy.Spec.Selector)
	if err != nil {
		return false
	}
	return selector.Matches(labels.Set(pod.Labels))
}

// injectPod modifies pod in-place: adds Volume, VolumeMounts, env vars, and annotation.
func injectPod(pod *corev1.Pod, policies []*logpilotv1alpha1.LogPilotPolicy) error {
	var matched []*logpilotv1alpha1.LogPilotPolicy
	for _, p := range policies {
		if matchesPolicy(pod, p) {
			matched = append(matched, p)
		}
	}
	if len(matched) == 0 {
		return nil
	}

	// Collect all container policies from all matched policies.
	var allContainerPolicies []logpilotv1alpha1.ContainerPolicy
	for _, p := range matched {
		allContainerPolicies = append(allContainerPolicies, p.Spec.Containers...)
	}

	// Inject env vars and volume mounts per container.
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		var containerPolicies []logpilotv1alpha1.ContainerPolicy
		for _, cp := range allContainerPolicies {
			if cp.Name == c.Name {
				containerPolicies = append(containerPolicies, cp)
			}
		}
		if len(containerPolicies) == 0 {
			continue
		}
		injectEnvVars(c)
		injectVolumeMounts(pod, c, containerPolicies)
	}

	// Inject the shared hostPath volume (guaranteed mode) or emptyDir (bestEffort).
	ensureLogVolume(pod, allContainerPolicies)

	// Write policy annotation for log-pilot-agent to read.
	raw, err := json.Marshal(allContainerPolicies)
	if err != nil {
		return fmt.Errorf("marshal container policies: %w", err)
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[podLogPolicyAnnotation] = string(raw)
	return nil
}

func injectEnvVars(c *corev1.Container) {
	existing := make(map[string]bool)
	for _, e := range c.Env {
		existing[e.Name] = true
	}
	for _, e := range downwardAPIEnvVars() {
		if !existing[e.Name] {
			c.Env = append(c.Env, e)
		}
	}
}

func downwardAPIEnvVars() []corev1.EnvVar {
	field := func(name, path string) corev1.EnvVar {
		return corev1.EnvVar{
			Name: name,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  path,
				},
			},
		}
	}
	return []corev1.EnvVar{
		field("POD_NAME", "metadata.name"),
		field("NAMESPACE", "metadata.namespace"),
		field("POD_UID", "metadata.uid"),
	}
}

func injectVolumeMounts(pod *corev1.Pod, c *corev1.Container, policies []logpilotv1alpha1.ContainerPolicy) {
	existing := make(map[string]bool)
	for _, vm := range c.VolumeMounts {
		existing[vm.MountPath] = true
	}
	for _, cp := range policies {
		if cp.Path == "-" || existing[cp.Path] {
			continue
		}
		subPathExpr := volumeSubPath(cp)
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
			Name:        logVolumeName,
			MountPath:   cp.Path,
			SubPathExpr: subPathExpr,
		})
		existing[cp.Path] = true
	}
}

func volumeSubPath(cp logpilotv1alpha1.ContainerPolicy) string {
	if cp.Delivery == "bestEffort" {
		return fmt.Sprintf("%s/%s", cp.Name, cp.LogType)
	}
	// guaranteed: use downward API env vars for path isolation
	return fmt.Sprintf("LogPilotPolicy/$(POD_NAMESPACE)/$(POD_NAME)/$(POD_UID)/%s/%s", cp.Name, cp.LogType)
}

func ensureLogVolume(pod *corev1.Pod, policies []logpilotv1alpha1.ContainerPolicy) {
	for _, v := range pod.Spec.Volumes {
		if v.Name == logVolumeName {
			return
		}
	}
	// If any policy is guaranteed, use hostPath; otherwise emptyDir.
	for _, cp := range policies {
		if cp.Delivery == "guaranteed" || cp.Delivery == "" {
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
				Name: logVolumeName,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{Path: logHostPath},
				},
			})
			return
		}
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: logVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	})
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/log-pilot-api/... -v
```

Expected: PASS.

- [ ] **Step 5: Wire webhook to read policies from K8s**

Update `internal/log-pilot-api/api.go`:

```go
package logpilotapi

import (
	"context"
	"encoding/json"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	webhook "github.com/jimyag/auto-cert-webhook"
	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

type API struct {
	client client.Client
	scheme *runtime.Scheme
}

func New(c client.Client, scheme *runtime.Scheme) *API {
	return &API{client: c, scheme: scheme}
}

func (a *API) Configure() webhook.Config {
	return webhook.Config{Name: "log-pilot-api"}
}

func (a *API) Webhooks() []webhook.Hook {
	return []webhook.Hook{
		{Path: "/mutate-pod", Type: webhook.Mutating, Admit: a.mutatePod},
	}
}

func (a *API) mutatePod(ar admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	pod := &corev1.Pod{}
	if err := json.Unmarshal(ar.Request.Object.Raw, pod); err != nil {
		return webhook.Errored(err)
	}

	// List all LogPilotPolicies in the pod's namespace.
	policyList := &logpilotv1alpha1.LogPilotPolicyList{}
	if err := a.client.List(context.Background(), policyList,
		client.InNamespace(ar.Request.Namespace)); err != nil {
		return webhook.Errored(err)
	}

	policies := make([]*logpilotv1alpha1.LogPilotPolicy, len(policyList.Items))
	for i := range policyList.Items {
		policies[i] = &policyList.Items[i]
	}

	original := pod.DeepCopy()
	if err := injectPod(pod, policies); err != nil {
		return webhook.Errored(err)
	}

	return webhook.PatchResponse(original, pod)
}
```

- [ ] **Step 6: Update `cmd/log-pilot-api/main.go`**

```go
package main

import (
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	webhook "github.com/jimyag/auto-cert-webhook"
	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
	logpilotapi "github.com/jimyag/logpilot/internal/log-pilot-api"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(logpilotv1alpha1.AddToScheme(scheme))
}

func main() {
	cfg, err := config.GetConfig()
	if err != nil {
		panic(err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		panic(err)
	}
	if err := webhook.Run(logpilotapi.New(c, scheme)); err != nil {
		os.Exit(1)
	}
}
```

- [ ] **Step 7: Build**

```bash
go build ./cmd/log-pilot-api/...
```

- [ ] **Step 8: Commit**

```bash
git add cmd/log-pilot-api/ internal/log-pilot-api/
git commit -m "feat(api): implement pod injection webhook"
```

---

## Chunk 3: log-pilot-agent — Pipeline Interfaces & Record

### Task 3: Runner config and pipeline types

**Files:**
- Create: `internal/log-pilot-agent/runner/config.go`
- Create: `internal/log-pilot-agent/runner/runner.go`
- Create: `internal/log-pilot-agent/runner/runner_test.go`

The runner is the core pipeline: reads from Input, passes through Transform chain, writes to Output, and runs Clean periodically.

- [ ] **Step 1: Create `runner/config.go`**

```go
package runner

import (
	"github.com/jimyag/logpilot/internal/log-pilot-agent/clean"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/output"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/transform"
)

// Config holds everything needed to build a Runner.
type Config struct {
	Name   string
	Input  input.Input
	Transforms []transform.Transform
	Output output.Output
	Clean  clean.Clean
	// BatchLen is the max records per read cycle.
	BatchLen int
}
```

- [ ] **Step 2: Write failing test for runner lifecycle**

`runner/runner_test.go`:

```go
package runner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

type mockInput struct {
	records []input.Record
	pos     int
	lag     int64
}

func (m *mockInput) ReadBatch(_ context.Context, size int) ([]input.Record, error) {
	if m.pos >= len(m.records) {
		time.Sleep(10 * time.Millisecond)
		return nil, nil
	}
	end := m.pos + size
	if end > len(m.records) {
		end = len(m.records)
	}
	batch := m.records[m.pos:end]
	m.pos = end
	atomic.StoreInt64(&m.lag, int64(len(m.records)-m.pos))
	return batch, nil
}

func (m *mockInput) Lag() int64    { return atomic.LoadInt64(&m.lag) }
func (m *mockInput) Close() error  { return nil }

func TestRunnerProcessesRecords(t *testing.T) {
	var received []input.Record
	out := &mockOutput{onWrite: func(records []input.Record) { received = append(received, records...) }}

	records := []input.Record{{Data: []byte("line1")}, {Data: []byte("line2")}}
	r := New(Config{
		Name:     "test",
		Input:    &mockInput{records: records, lag: int64(len(records))},
		Output:   out,
		BatchLen: 10,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	r.Run(ctx)

	if len(received) != 2 {
		t.Fatalf("expected 2 records, got %d", len(received))
	}
}

type mockOutput struct {
	onWrite func([]input.Record)
}

func (m *mockOutput) WriteBatch(_ context.Context, records []input.Record) error {
	if m.onWrite != nil {
		m.onWrite(records)
	}
	return nil
}
func (m *mockOutput) Close() error { return nil }
```

- [ ] **Step 3: Run test to verify it fails**

```bash
go test ./internal/log-pilot-agent/runner/... 2>&1 | head -10
```

Expected: FAIL — `New` undefined.

- [ ] **Step 4: Implement `runner/runner.go`**

```go
package runner

import (
	"context"
	"sync/atomic"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/clean"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

// Runner executes an Input→Transform→Output→Clean pipeline.
type Runner struct {
	cfg     Config
	stopped int32 // atomic
}

func New(cfg Config) *Runner {
	if cfg.BatchLen == 0 {
		cfg.BatchLen = 1000
	}
	return &Runner{cfg: cfg}
}

// Run blocks until ctx is cancelled, then drains and shuts down gracefully.
func (r *Runner) Run(ctx context.Context) {
	for {
		if atomic.LoadInt32(&r.stopped) > 0 {
			break
		}
		records, err := r.cfg.Input.ReadBatch(ctx, r.cfg.BatchLen)
		if err != nil || ctx.Err() != nil {
			break
		}
		if len(records) == 0 {
			continue
		}
		records = r.applyTransforms(ctx, records)
		if len(records) == 0 {
			continue
		}
		_ = r.cfg.Output.WriteBatch(ctx, records)
	}
	r.shutdown()
}

// Lag returns the number of unread bytes in the input.
func (r *Runner) Lag() int64 { return r.cfg.Input.Lag() }

// Stop signals the runner to stop after the current batch.
func (r *Runner) Stop() { atomic.StoreInt32(&r.stopped, 1) }

func (r *Runner) applyTransforms(ctx context.Context, records []input.Record) []input.Record {
	for _, t := range r.cfg.Transforms {
		var err error
		records, err = t.Transform(ctx, records)
		if err != nil || len(records) == 0 {
			return nil
		}
	}
	return records
}

func (r *Runner) shutdown() {
	// Drain remaining input after stop signal.
	ctx := context.Background()
	for {
		records, err := r.cfg.Input.ReadBatch(ctx, r.cfg.BatchLen)
		if err != nil || len(records) == 0 {
			break
		}
		records = r.applyTransforms(ctx, records)
		if len(records) > 0 {
			_ = r.cfg.Output.WriteBatch(ctx, records)
		}
	}
	r.cfg.Output.Close()
	r.cfg.Input.Close()

	// Force commit all offsets.
	if r.cfg.Clean != nil {
		_ = r.cfg.Clean.Clean(clean.RunnerMeta{})
	}
}
```

- [ ] **Step 5: Run tests**

```bash
go test ./internal/log-pilot-agent/runner/... -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/log-pilot-agent/runner/
git commit -m "feat(agent): implement pipeline runner with graceful shutdown"
```

---

## Chunk 4: log-pilot-agent — File Input

### Task 4: File Input implementation

**Files:**
- Create: `internal/log-pilot-agent/input/file.go`
- Create: `internal/log-pilot-agent/input/file_test.go`

- [ ] **Step 1: Add fsnotify and tail dependencies**

```bash
cd /Users/jimyag/src/github/jimyag/logpilot
go get github.com/nxadm/tail
go get github.com/fsnotify/fsnotify
```

- [ ] **Step 2: Write failing test**

`input/file_test.go`:

```go
package input

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestFileInputReadsLines(t *testing.T) {
	f, err := os.CreateTemp("", "logpilot-test-*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	_, _ = f.WriteString("line1\nline2\n")
	f.Close()

	cfg := FileConfig{
		Path:             f.Name(),
		ReadFrom:         "oldest",
		OffsetCommitEvery: 1,
	}
	in, err := NewFileInput(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var records []Record
	for len(records) < 2 {
		batch, err := in.ReadBatch(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, batch...)
	}

	if string(records[0].Data) != "line1" {
		t.Errorf("expected 'line1', got %q", records[0].Data)
	}
	if string(records[1].Data) != "line2" {
		t.Errorf("expected 'line2', got %q", records[1].Data)
	}
}
```

- [ ] **Step 3: Run to verify it fails**

```bash
go test ./internal/log-pilot-agent/input/... 2>&1 | head -10
```

Expected: FAIL — `NewFileInput` undefined.

- [ ] **Step 4: Implement `input/file.go`**

```go
package input

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"

	"github.com/nxadm/tail"
)

// FileConfig configures a file-based Input.
type FileConfig struct {
	Path              string   `yaml:"path"`
	MetaPath          string   `yaml:"metaPath"`
	ReadFrom          string   `yaml:"readFrom"`    // newest | oldest
	Include           []string `yaml:"include"`
	Exclude           []string `yaml:"exclude"`
	OffsetCommitEvery int      `yaml:"offsetCommitEvery"`
}

type fileInput struct {
	cfg       FileConfig
	tail      *tail.Tail
	stopped   int32
	lag       int64
	readCount int
	includeRe []*regexp.Regexp
	excludeRe []*regexp.Regexp
}

// NewFileInput creates a new file-tailing Input.
func NewFileInput(cfg FileConfig, _ string) (Input, error) {
	if cfg.OffsetCommitEvery == 0 {
		cfg.OffsetCommitEvery = 20000
	}
	seekInfo := &tail.SeekInfo{Offset: 0, Whence: 0} // oldest
	if cfg.ReadFrom == "newest" {
		seekInfo = &tail.SeekInfo{Offset: 0, Whence: 2}
	}

	t, err := tail.TailFile(cfg.Path, tail.Config{
		Follow:    true,
		ReOpen:    true, // handles log rotation
		MustExist: false,
		Location:  seekInfo,
	})
	if err != nil {
		return nil, err
	}

	fi := &fileInput{cfg: cfg, tail: t}
	fi.includeRe = compilePatterns(cfg.Include)
	fi.excludeRe = compilePatterns(cfg.Exclude)

	// Initialize lag from file size.
	if info, err := os.Stat(cfg.Path); err == nil {
		atomic.StoreInt64(&fi.lag, info.Size())
	}

	return fi, nil
}

func (f *fileInput) ReadBatch(ctx context.Context, size int) ([]Record, error) {
	if atomic.LoadInt32(&f.stopped) > 0 {
		return nil, nil
	}
	var records []Record
	for len(records) < size {
		select {
		case line, ok := <-f.tail.Lines:
			if !ok {
				return records, nil
			}
			if line.Err != nil {
				continue
			}
			text := line.Text
			if !f.passesFilter(filepath.Base(f.cfg.Path)) {
				continue
			}
			records = append(records, Record{Data: []byte(text)})
			f.readCount++
			if f.readCount%f.cfg.OffsetCommitEvery == 0 {
				f.commitOffset()
			}
		case <-ctx.Done():
			return records, nil
		default:
			if len(records) > 0 {
				return records, nil
			}
			// Block until a line arrives or context expires.
			select {
			case line, ok := <-f.tail.Lines:
				if !ok {
					return records, nil
				}
				if line.Err != nil {
					continue
				}
				records = append(records, Record{Data: []byte(line.Text)})
				f.readCount++
			case <-ctx.Done():
				return records, nil
			}
		}
	}
	return records, nil
}

func (f *fileInput) Lag() int64 { return atomic.LoadInt64(&f.lag) }

func (f *fileInput) Close() error {
	atomic.StoreInt32(&f.stopped, 1)
	f.commitOffset()
	return f.tail.Stop()
}

func (f *fileInput) commitOffset() {
	if info, err := os.Stat(f.cfg.Path); err == nil {
		// Approximate lag as remaining file size.
		atomic.StoreInt64(&f.lag, info.Size()-f.tail.Tell())
	}
	// TODO: persist offset to MetaPath
}

func (f *fileInput) passesFilter(filename string) bool {
	if len(f.includeRe) > 0 {
		matched := false
		for _, re := range f.includeRe {
			if re.MatchString(filename) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, re := range f.excludeRe {
		if re.MatchString(filename) {
			return false
		}
	}
	return true
}

func compilePatterns(patterns []string) []*regexp.Regexp {
	var res []*regexp.Regexp
	for _, p := range patterns {
		if re, err := regexp.Compile(p); err == nil {
			res = append(res, re)
		}
	}
	return res
}
```

- [ ] **Step 5: Run tests**

```bash
go test ./internal/log-pilot-agent/input/... -v -timeout 30s
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/log-pilot-agent/input/
git commit -m "feat(agent): implement file input with rotation support"
```

---

## Chunk 5: log-pilot-agent — Transforms & Outputs

### Task 5: Core transforms

**Files:**
- Create: `internal/log-pilot-agent/transform/json.go`
- Create: `internal/log-pilot-agent/transform/label.go`
- Create: `internal/log-pilot-agent/transform/drop.go`
- Create: `internal/log-pilot-agent/transform/transform_test.go`

- [ ] **Step 1: Write tests**

`transform/transform_test.go`:

```go
package transform

import (
	"context"
	"testing"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

func TestJSONTransform(t *testing.T) {
	tr := NewJSONTransform()
	records := []input.Record{{Data: []byte(`{"level":"INFO","msg":"hello"}`)}}
	out, err := tr.Transform(context.Background(), records)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 record, got %d", len(out))
	}
	if out[0].Meta["level"] != "INFO" {
		t.Errorf("expected level=INFO, got %q", out[0].Meta["level"])
	}
}

func TestLabelTransform(t *testing.T) {
	tr := NewLabelTransform(map[string]string{"pod": "mypod"})
	records := []input.Record{{Data: []byte("hello")}}
	out, _ := tr.Transform(context.Background(), records)
	if out[0].Meta["pod"] != "mypod" {
		t.Errorf("expected pod=mypod, got %q", out[0].Meta["pod"])
	}
}

func TestDropTransform(t *testing.T) {
	tr := NewDropTransform("level", "DEBUG")
	records := []input.Record{
		{Data: []byte("debug line"), Meta: map[string]string{"level": "DEBUG"}},
		{Data: []byte("info line"), Meta: map[string]string{"level": "INFO"}},
	}
	out, _ := tr.Transform(context.Background(), records)
	if len(out) != 1 {
		t.Fatalf("expected 1 record after drop, got %d", len(out))
	}
	if string(out[0].Data) != "info line" {
		t.Errorf("unexpected record: %q", out[0].Data)
	}
}
```

- [ ] **Step 2: Run to verify fails**

```bash
go test ./internal/log-pilot-agent/transform/... 2>&1 | head -10
```

Expected: FAIL.

- [ ] **Step 3: Implement transforms**

`transform/json.go`:
```go
package transform

import (
	"context"
	"encoding/json"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

type jsonTransform struct{}

func NewJSONTransform() Transform { return &jsonTransform{} }

func (t *jsonTransform) Transform(_ context.Context, records []input.Record) ([]input.Record, error) {
	for i := range records {
		var m map[string]string
		if err := json.Unmarshal(records[i].Data, &m); err == nil {
			if records[i].Meta == nil {
				records[i].Meta = make(map[string]string)
			}
			for k, v := range m {
				records[i].Meta[k] = v
			}
		}
	}
	return records, nil
}
```

`transform/label.go`:
```go
package transform

import (
	"context"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

type labelTransform struct{ fields map[string]string }

func NewLabelTransform(fields map[string]string) Transform { return &labelTransform{fields: fields} }

func (t *labelTransform) Transform(_ context.Context, records []input.Record) ([]input.Record, error) {
	for i := range records {
		if records[i].Meta == nil {
			records[i].Meta = make(map[string]string)
		}
		for k, v := range t.fields {
			records[i].Meta[k] = v
		}
	}
	return records, nil
}
```

`transform/drop.go`:
```go
package transform

import (
	"context"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

type dropTransform struct{ key, value string }

func NewDropTransform(key, value string) Transform { return &dropTransform{key: key, value: value} }

func (t *dropTransform) Transform(_ context.Context, records []input.Record) ([]input.Record, error) {
	out := records[:0]
	for _, r := range records {
		if r.Meta[t.key] != t.value {
			out = append(out, r)
		}
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/log-pilot-agent/transform/... -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/log-pilot-agent/transform/
git commit -m "feat(agent): implement json, label, drop transforms"
```

### Task 6: HTTP Output implementation

**Files:**
- Create: `internal/log-pilot-agent/output/http.go`
- Create: `internal/log-pilot-agent/output/file.go`
- Create: `internal/log-pilot-agent/output/output_test.go`

- [ ] **Step 1: Write test for file output (simplest to test)**

`output/output_test.go`:
```go
package output

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

func TestFileOutput(t *testing.T) {
	f, _ := os.CreateTemp("", "logpilot-out-*.log")
	defer os.Remove(f.Name())
	f.Close()

	out := NewFileOutput(f.Name())
	defer out.Close()

	records := []input.Record{
		{Data: []byte("hello"), Meta: map[string]string{"pod": "mypod"}},
	}
	if err := out.WriteBatch(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	out.Close()

	content, _ := os.ReadFile(f.Name())
	if !strings.Contains(string(content), "hello") {
		t.Errorf("expected 'hello' in output file, got: %s", content)
	}
}
```

- [ ] **Step 2: Run to verify fails**

```bash
go test ./internal/log-pilot-agent/output/... 2>&1 | head -10
```

Expected: FAIL.

- [ ] **Step 3: Implement file and HTTP outputs**

`output/file.go`:
```go
package output

import (
	"context"
	"encoding/json"
	"os"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

type fileOutput struct {
	path string
	f    *os.File
}

func NewFileOutput(path string) Output {
	return &fileOutput{path: path}
}

func (o *fileOutput) WriteBatch(_ context.Context, records []input.Record) error {
	if o.f == nil {
		f, err := os.OpenFile(o.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		o.f = f
	}
	enc := json.NewEncoder(o.f)
	for _, r := range records {
		entry := map[string]interface{}{"data": string(r.Data)}
		for k, v := range r.Meta {
			entry[k] = v
		}
		_ = enc.Encode(entry)
	}
	return nil
}

func (o *fileOutput) Close() error {
	if o.f != nil {
		return o.f.Close()
	}
	return nil
}
```

`output/http.go`:
```go
package output

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/jimyag/logpilot/internal/log-pilot-agent/input"
)

type httpOutput struct {
	url    string
	client *http.Client
}

// HTTPConfig configures the HTTP output.
type HTTPConfig struct {
	URL string `yaml:"url"`
}

func NewHTTPOutput(cfg HTTPConfig) Output {
	return &httpOutput{url: cfg.URL, client: &http.Client{}}
}

func (o *httpOutput) WriteBatch(ctx context.Context, records []input.Record) error {
	entries := make([]map[string]interface{}, len(records))
	for i, r := range records {
		entry := map[string]interface{}{"data": string(r.Data)}
		for k, v := range r.Meta {
			entry[k] = v
		}
		entries[i] = entry
	}
	body, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("http output: status %d", resp.StatusCode)
	}
	return nil
}

func (o *httpOutput) Close() error { return nil }
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/log-pilot-agent/output/... -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/log-pilot-agent/output/
git commit -m "feat(agent): implement file and http outputs"
```

---

## Chunk 6: log-pilot-agent — Pod Watcher & Main

### Task 7: K8s Event Input

**Files:**
- Create: `internal/log-pilot-agent/input/k8sevent.go`

- [ ] **Step 1: Implement K8s event input**

```go
package input

import (
	"context"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// K8sEventConfig configures the k8sEvent input.
type K8sEventConfig struct {
	Namespaces          []string `yaml:"namespaces"`
	ResourceVersionPath string   `yaml:"resourceVersionPath"`
}

type k8sEventInput struct {
	cfg    K8sEventConfig
	client client.Client
	queue  chan Record
	lag    int64
	cancel context.CancelFunc
}

// NewK8sEventInput creates an input that watches K8s Event objects.
func NewK8sEventInput(cfg K8sEventConfig, c client.Client) Input {
	in := &k8sEventInput{
		cfg:    cfg,
		client: c,
		queue:  make(chan Record, 1000),
	}
	ctx, cancel := context.WithCancel(context.Background())
	in.cancel = cancel
	go in.watch(ctx)
	return in
}

func (k *k8sEventInput) watch(ctx context.Context) {
	// TODO: use controller-runtime watch with ResourceVersion persistence
	eventList := &corev1.EventList{}
	for _, ns := range k.cfg.Namespaces {
		_ = k.client.List(ctx, eventList, client.InNamespace(ns))
		for _, ev := range eventList.Items {
			raw, _ := ev.Marshal()
			select {
			case k.queue <- Record{Data: raw}:
				atomic.AddInt64(&k.lag, 1)
			case <-ctx.Done():
				return
			}
		}
	}
}

func (k *k8sEventInput) ReadBatch(ctx context.Context, size int) ([]Record, error) {
	var records []Record
	for len(records) < size {
		select {
		case r := <-k.queue:
			records = append(records, r)
			atomic.AddInt64(&k.lag, -1)
		case <-ctx.Done():
			return records, nil
		default:
			return records, nil
		}
	}
	return records, nil
}

func (k *k8sEventInput) Lag() int64   { return atomic.LoadInt64(&k.lag) }
func (k *k8sEventInput) Close() error { k.cancel(); return nil }
```

- [ ] **Step 2: Build**

```bash
go build ./internal/log-pilot-agent/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/log-pilot-agent/input/k8sevent.go
git commit -m "feat(agent): implement k8sEvent input"
```

### Task 8: Pod Watcher

**Files:**
- Create: `internal/log-pilot-agent/watcher/watcher.go`
- Create: `internal/log-pilot-agent/watcher/symlink.go`

The watcher watches pods on the current node, creates runners when pods with policies appear, and handles graceful cleanup on pod deletion.

- [ ] **Step 1: Implement watcher**

`watcher/watcher.go`:

```go
package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/runner"
)

const podLogPolicyAnnotation = "beta.logpilot.io/log-policy"

// Config holds watcher configuration derived from LogPilot CR.
type Config struct {
	NodeName  string
	LogDir    string
	ConfigDir string
	MetaDir   string
}

// Watcher watches pods on this node and manages runners.
type Watcher struct {
	cfg     Config
	client  client.Client
	runners map[string]*runner.Runner // key: podUID/container/logType
	mu      sync.Mutex
}

func New(cfg Config, c client.Client) *Watcher {
	return &Watcher{cfg: cfg, client: c, runners: make(map[string]*runner.Runner)}
}

// Start blocks watching pods on this node until ctx is done.
func (w *Watcher) Start(ctx context.Context) error {
	// TODO: use controller-runtime informer filtered by spec.nodeName
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
		if err := ensureSymlink(pod, cp, logPath, w.cfg.LogDir); err != nil {
			continue
		}
		r := buildRunner(cp, logPath, w.cfg)
		w.mu.Lock()
		w.runners[key] = r
		w.mu.Unlock()
		go r.Run(context.Background())
	}
}

// OnPodDelete waits for runners to drain then cleans up.
func (w *Watcher) OnPodDelete(pod *corev1.Pod) {
	policies, err := parsePoliciesFromPod(pod)
	if err != nil {
		return
	}
	for _, cp := range policies {
		key := runnerKey(string(pod.UID), cp.Name, cp.LogType)
		w.mu.Lock()
		r, ok := w.runners[key]
		w.mu.Unlock()
		if !ok {
			continue
		}
		// Stop input, drain, then clean up directory.
		go func(r *runner.Runner, key string, pod *corev1.Pod, cp logpilotv1alpha1.ContainerPolicy) {
			r.Stop()
			// Wait for lag == 0 before removing files.
			for r.Lag() > 0 {
				// poll
			}
			w.mu.Lock()
			delete(w.runners, key)
			w.mu.Unlock()
			_ = os.RemoveAll(w.logPath(pod, cp))
		}(r, key, pod, cp)
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

func buildRunner(_ logpilotv1alpha1.ContainerPolicy, _ string, _ Config) *runner.Runner {
	// TODO: build runner from ContainerPolicy (input, transforms, output, clean)
	return runner.New(runner.Config{BatchLen: 1000})
}
```

`watcher/symlink.go`:

```go
package watcher

import (
	"fmt"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

// ensureSymlink creates the unified log path, pointing to the actual storage location.
func ensureSymlink(pod *corev1.Pod, cp logpilotv1alpha1.ContainerPolicy, logPath, logDir string) error {
	if err := os.MkdirAll(filepath.Dir(logPath), 0755); err != nil {
		return err
	}
	switch {
	case cp.Path == "-":
		// stdout: symlink to K8s native stdout path
		target := fmt.Sprintf("/var/log/pods/%s_%s_%s/%s",
			pod.Namespace, pod.Name, string(pod.UID), cp.Name)
		return forceSymlink(target, logPath)

	case cp.Delivery == "bestEffort":
		// emptyDir: symlink to kubelet emptyDir path
		target := fmt.Sprintf("/var/lib/kubelet/pods/%s/volumes/kubernetes.io~empty-dir/pods-log/%s/%s",
			string(pod.UID), cp.Name, cp.LogType)
		return forceSymlink(target, logPath)

	default:
		// guaranteed/hostPath: directory already exists via VolumeMount, no symlink needed
		return os.MkdirAll(logPath, 0755)
	}
}

func forceSymlink(target, link string) error {
	_ = os.Remove(link)
	return os.Symlink(target, link)
}
```

- [ ] **Step 2: Build**

```bash
go build ./internal/log-pilot-agent/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/log-pilot-agent/watcher/
git commit -m "feat(agent): implement pod watcher and symlink management"
```

### Task 9: Agent main entry point

**Files:**
- Create: `cmd/log-pilot-agent/main.go`

- [ ] **Step 1: Implement agent main**

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/client"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
	"github.com/jimyag/logpilot/internal/log-pilot-agent/watcher"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(logpilotv1alpha1.AddToScheme(scheme))
}

func main() {
	ctrl.SetLogger(zap.New())
	log := ctrl.Log.WithName("log-pilot-agent")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.GetConfig()
	if err != nil {
		log.Error(err, "failed to get k8s config")
		os.Exit(1)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		log.Error(err, "failed to create k8s client")
		os.Exit(1)
	}

	nodeName := os.Getenv("NODE_NAME")
	w := watcher.New(watcher.Config{
		NodeName:  nodeName,
		LogDir:    envOrDefault("LOG_DIR", "/var/log/log-pilot"),
		ConfigDir: envOrDefault("CONFIG_DIR", "/var/lib/log-pilot-agent/conf"),
		MetaDir:   envOrDefault("META_DIR", "/var/lib/log-pilot-agent/meta"),
	}, c)

	log.Info("log-pilot-agent started", "node", nodeName)

	if err := w.Start(ctx); err != nil {
		log.Error(err, "watcher error")
		os.Exit(1)
	}

	<-ctx.Done()
	log.Info("log-pilot-agent shutting down gracefully")
	// Runners drain themselves via Stop() + shutdown() before process exits.
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
```

- [ ] **Step 2: Build all**

```bash
go build ./...
```

Expected: all three binaries compile successfully.

- [ ] **Step 3: Commit**

```bash
git add cmd/log-pilot-agent/
git commit -m "feat(agent): implement agent entry point with graceful shutdown"
```

---

## Chunk 7: log-pilot-operator Controllers

### Task 10: Implement operator reconcilers

**Files:**
- Modify: `internal/log-pilot-operator/logpilot_controller.go`
- Modify: `internal/log-pilot-operator/logpilotpolicy_controller.go`
- Modify: `internal/log-pilot-operator/clusterlogpilotpolicy_controller.go`

The operator reconciles LogPilot (deploys/updates api+agent), and validates LogPilotPolicy / ClusterLogPilotPolicy.

- [ ] **Step 1: Implement LogPilot reconciler**

`internal/log-pilot-operator/logpilot_controller.go` — replace Reconcile body:

```go
func (r *LogPilotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var lp logpilotv1alpha1.LogPilot
	if err := r.Get(ctx, req.NamespacedName, &lp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// TODO: deploy/update log-pilot-api Deployment
	// TODO: deploy/update log-pilot-agent DaemonSet
	// TODO: update status conditions

	log.Info("reconciled LogPilot", "name", lp.Name)
	return ctrl.Result{}, nil
}
```

- [ ] **Step 2: Implement LogPilotPolicy reconciler (validation only)**

```go
func (r *LogPilotPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var policy logpilotv1alpha1.LogPilotPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Validate: must have either (Selector+Containers) or (Input+Output).
	if policy.Spec.Selector == nil && policy.Spec.Input == nil {
		log.Error(nil, "LogPilotPolicy must define selector+containers or input+output",
			"name", policy.Name)
	}

	return ctrl.Result{}, nil
}
```

- [ ] **Step 3: Build**

```bash
go build ./...
```

- [ ] **Step 4: Run tests**

```bash
go test ./... -v -timeout 60s 2>&1 | tail -20
```

- [ ] **Step 5: Commit**

```bash
git add internal/log-pilot-operator/
git commit -m "feat(operator): implement LogPilot reconciler scaffold and policy validation"
```

---

## Chunk 8: /status endpoint & IsDoneCollected

### Task 11: HTTP status server in agent

**Files:**
- Create: `internal/log-pilot-agent/status/server.go`
- Create: `internal/log-pilot-agent/status/server_test.go`

- [ ] **Step 1: Write test**

```go
package status

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestStatusServer(t *testing.T) {
	srv := New()
	srv.UpdateRunner("test-runner", 42, 100)

	req := httptest.NewRequest("GET", "/status", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	var resp Response
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Runners) != 1 {
		t.Fatalf("expected 1 runner, got %d", len(resp.Runners))
	}
	if resp.Runners[0].Lag != 42 {
		t.Errorf("expected lag=42, got %d", resp.Runners[0].Lag)
	}
}
```

- [ ] **Step 2: Run to verify fails**

```bash
go test ./internal/log-pilot-agent/status/... 2>&1 | head -5
```

- [ ] **Step 3: Implement status server**

```go
package status

import (
	"encoding/json"
	"net/http"
	"sync"
)

type RunnerStatus struct {
	Name string `json:"name"`
	Lag  int64  `json:"lag"`
	Sent int64  `json:"sent"`
}

type Response struct {
	Runners []RunnerStatus `json:"runners"`
}

type Server struct {
	mu      sync.RWMutex
	runners map[string]RunnerStatus
}

func New() *Server {
	return &Server{runners: make(map[string]RunnerStatus)}
}

func (s *Server) UpdateRunner(name string, lag, sent int64) {
	s.mu.Lock()
	s.runners[name] = RunnerStatus{Name: name, Lag: lag, Sent: sent}
	s.mu.Unlock()
}

func (s *Server) RemoveRunner(name string) {
	s.mu.Lock()
	delete(s.runners, name)
	s.mu.Unlock()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	runners := make([]RunnerStatus, 0, len(s.runners))
	for _, rs := range s.runners {
		runners = append(runners, rs)
	}
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(Response{Runners: runners})
}

func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, s)
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/log-pilot-agent/status/... -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/log-pilot-agent/status/
git commit -m "feat(agent): implement /status HTTP endpoint for IsDoneCollected"
```

---

## Final verification

- [ ] **Run full test suite**

```bash
go test ./... -v -timeout 120s 2>&1 | grep -E "^(ok|FAIL|---)"
```

Expected: all packages pass.

- [ ] **Build all binaries**

```bash
go build ./cmd/log-pilot-operator/...
go build ./cmd/log-pilot-api/...
go build ./cmd/log-pilot-agent/...
```

Expected: three binaries, no errors.

- [ ] **Final commit**

```bash
git add -A
git commit -m "feat: complete logpilot initial implementation"
```
