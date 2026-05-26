// Package input provides tests for k8s object state input.
//
//lint:file-ignore SA1019 NewSimpleClientset is required until WithApplyConfig codegen is available
package input

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
)

func TestK8sObjectStateInputSnapshotsSelectedResources(t *testing.T) {
	client := fake.NewSimpleClientset( //nolint:staticcheck
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
			Spec:       corev1.PodSpec{NodeName: "node-1"},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:         "app",
					RestartCount: 2,
					LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Reason:   "OOMKilled",
							ExitCode: 137,
						},
					},
				}},
			},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			},
		},
	)
	in := NewK8sObjectStateInput(K8sObjectStateConfig{
		Namespaces: []string{"default"},
		Resources:  []string{"pod", "node"},
	}, client)
	defer func() { _ = in.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	records := readUntil(ctx, t, in, 2)

	kinds := map[string]bool{}
	for _, record := range records {
		var payload map[string]any
		if err := json.Unmarshal(record.Data, &payload); err != nil {
			t.Fatal(err)
		}
		kinds[payload["kind"].(string)] = true
	}
	if !kinds["Pod"] || !kinds["Node"] {
		t.Fatalf("expected Pod and Node snapshots, got %v", kinds)
	}
}

func TestK8sObjectStateInputSnapshotsWorkloads(t *testing.T) {
	client := fake.NewSimpleClientset( //nolint:staticcheck
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "deploy", Namespace: "default"},
			Status:     appsv1.DeploymentStatus{Replicas: 2, ReadyReplicas: 1, UnavailableReplicas: 1},
		},
		&appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "sts", Namespace: "default"},
			Status:     appsv1.StatefulSetStatus{Replicas: 2, ReadyReplicas: 2},
		},
		&appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Name: "ds", Namespace: "default"},
			Status:     appsv1.DaemonSetStatus{DesiredNumberScheduled: 3, NumberReady: 2},
		},
		&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default"},
			Status:     batchv1.JobStatus{Succeeded: 1},
		},
	)
	in := NewK8sObjectStateInput(K8sObjectStateConfig{
		Namespaces: []string{"default"},
		Resources:  []string{"deployment", "statefulset", "daemonset", "job"},
	}, client)
	defer func() { _ = in.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	records := readUntil(ctx, t, in, 4)

	kinds := map[string]bool{}
	for _, record := range records {
		var payload map[string]any
		if err := json.Unmarshal(record.Data, &payload); err != nil {
			t.Fatal(err)
		}
		kinds[payload["kind"].(string)] = true
	}
	for _, kind := range []string{"Deployment", "StatefulSet", "DaemonSet", "Job"} {
		if !kinds[kind] {
			t.Fatalf("expected %s snapshot, got %v", kind, kinds)
		}
	}
}

func TestK8sObjectStateInputLag(t *testing.T) {
	in := &k8sObjectStateInput{}
	if got := in.Lag(); got != 0 {
		t.Fatalf("expected zero lag, got %d", got)
	}
}

func TestK8sObjectStateInputCommit(t *testing.T) {
	in := &k8sObjectStateInput{}
	if err := in.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

func TestHandleObjectPod(t *testing.T) {
	assertHandleObjectEnqueues(t, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "default"}}, "Pod")
}

func TestHandleObjectNode(t *testing.T) {
	assertHandleObjectEnqueues(t, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}, "Node")
}

func TestHandleObjectDeployment(t *testing.T) {
	assertHandleObjectEnqueues(t, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "deploy", Namespace: "default"}}, "Deployment")
}

func TestHandleObjectStatefulSet(t *testing.T) {
	assertHandleObjectEnqueues(t, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "sts", Namespace: "default"}}, "StatefulSet")
}

func TestHandleObjectDaemonSet(t *testing.T) {
	assertHandleObjectEnqueues(t, &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "ds", Namespace: "default"}}, "DaemonSet")
}

func TestHandleObjectJob(t *testing.T) {
	assertHandleObjectEnqueues(t, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default"}}, "Job")
}

func TestHandleObjectUnknown(t *testing.T) {
	in := &k8sObjectStateInput{client: fake.NewSimpleClientset(), queue: make(chan Record, 1)} //nolint:staticcheck
	in.handleObject(context.Background(), watch.Event{Type: watch.Added, Object: &metav1.Status{Status: metav1.StatusSuccess}})

	select {
	case record := <-in.queue:
		t.Fatalf("unexpected record for unknown object: %+v", record)
	default:
	}
}

func TestContainerStateFinished(t *testing.T) {
	startedAt := metav1.NewTime(time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC))
	finishedAt := metav1.NewTime(time.Date(2024, time.January, 2, 3, 5, 6, 0, time.UTC))

	state := containerState(corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{
			Reason:     "Completed",
			Message:    "finished cleanly",
			ExitCode:   42,
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
		},
	})

	if got := state["state"]; got != "terminated" {
		t.Fatalf("expected terminated state, got %v", got)
	}
	if got := state["exitCode"]; got != int32(42) {
		t.Fatalf("expected exit code 42, got %v", got)
	}
	if got := state["startedAt"]; got != startedAt.Time.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("unexpected startedAt: %v", got)
	}
	if got := state["finishedAt"]; got != finishedAt.Time.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("unexpected finishedAt: %v", got)
	}
}

func TestK8sObjectStateEnqueueContextCancelled(t *testing.T) {
	in := &k8sObjectStateInput{queue: make(chan Record)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := in.enqueue(ctx, "Pod", "Added", map[string]any{"name": "pod"}); err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestK8sObjectStateResources(t *testing.T) {
	defaults := (&k8sObjectStateInput{}).resources()
	if len(defaults) != 6 {
		t.Fatalf("expected default resources, got %v", defaults)
	}

	custom := (&k8sObjectStateInput{cfg: K8sObjectStateConfig{Resources: []string{"pod"}}}).resources()
	if len(custom) != 1 || custom[0] != "pod" {
		t.Fatalf("expected custom resources, got %v", custom)
	}
}

func TestK8sObjectStateNamespaces(t *testing.T) {
	defaults := (&k8sObjectStateInput{}).namespaces()
	if len(defaults) != 1 || defaults[0] != metav1.NamespaceAll {
		t.Fatalf("expected default namespaces, got %v", defaults)
	}

	custom := (&k8sObjectStateInput{cfg: K8sObjectStateConfig{Namespaces: []string{"default"}}}).namespaces()
	if len(custom) != 1 || custom[0] != "default" {
		t.Fatalf("expected custom namespaces, got %v", custom)
	}
}

func TestOptionalTime(t *testing.T) {
	if got := optionalTime(nil); got != "" {
		t.Fatalf("expected empty string for nil time, got %q", got)
	}

	zero := metav1.Time{}
	if got := optionalTime(&zero); got != zero.Time.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("expected zero time to format consistently, got %q", got)
	}

	now := metav1.NewTime(time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC))
	if got := optionalTime(&now); got != now.Time.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("expected non-zero time to be formatted, got %q", got)
	}
}

func TestK8sObjectStateWatch(t *testing.T) {
	t.Run("watch error", func(t *testing.T) {
		in := &k8sObjectStateInput{}
		called := false
		in.watch(context.Background(), func() (watch.Interface, error) {
			return nil, errors.New("boom")
		}, func(context.Context, watch.Event) {
			called = true
		})
		if called {
			t.Fatal("expected handler not to be called when watch creation fails")
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		in := &k8sObjectStateInput{}
		ctx, cancel := context.WithCancel(context.Background())
		fw := watch.NewFake()
		done := make(chan struct{})
		go func() {
			in.watch(ctx, func() (watch.Interface, error) { return fw, nil }, func(context.Context, watch.Event) {
				t.Error("handler should not run after cancellation")
			})
			close(done)
		}()
		cancel()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("watch did not stop after context cancellation")
		}
	})

	t.Run("event and close", func(t *testing.T) {
		in := &k8sObjectStateInput{}
		fw := watch.NewFake()
		handled := make(chan watch.Event, 1)
		done := make(chan struct{})
		go func() {
			in.watch(context.Background(), func() (watch.Interface, error) { return fw, nil }, func(_ context.Context, ev watch.Event) {
				handled <- ev
			})
			close(done)
		}()

		fw.Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "default"}})
		select {
		case ev := <-handled:
			if ev.Type != watch.Added {
				t.Fatalf("expected Added event, got %s", ev.Type)
			}
		case <-time.After(time.Second):
			t.Fatal("expected watch event to be handled")
		}

		fw.Stop()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("watch did not stop after channel close")
		}
	})
}

func TestK8sObjectStateReadBatchReturnsPartialOnClosedQueue(t *testing.T) {
	in := &k8sObjectStateInput{queue: make(chan Record, 1)}
	in.queue <- Record{Data: []byte("one")}
	close(in.queue)

	batch, err := in.ReadBatch(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected partial batch of 1 record, got %d", len(batch))
	}
}

func TestK8sObjectStateReadBatchCancelledContext(t *testing.T) {
	in := &k8sObjectStateInput{queue: make(chan Record)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for range 256 {
		batch, err := in.ReadBatch(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) != 0 {
			t.Fatalf("expected empty batch, got %v", batch)
		}
	}
}

func TestContainerState(t *testing.T) {
	t.Run("waiting", func(t *testing.T) {
		state := containerState(corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "Pending", Message: "waiting"}})
		if got := state["state"]; got != "waiting" {
			t.Fatalf("expected waiting state, got %v", got)
		}
	})

	t.Run("running", func(t *testing.T) {
		startedAt := metav1.NewTime(time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC))
		state := containerState(corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: startedAt}})
		if got := state["state"]; got != "running" {
			t.Fatalf("expected running state, got %v", got)
		}
	})

	t.Run("empty", func(t *testing.T) {
		if got := containerState(corev1.ContainerState{}); got != nil {
			t.Fatalf("expected nil state, got %v", got)
		}
	})
}

func assertHandleObjectEnqueues(t *testing.T, obj runtime.Object, wantKind string) {
	t.Helper()

	in := &k8sObjectStateInput{client: fake.NewSimpleClientset(), queue: make(chan Record, 1)} //nolint:staticcheck
	in.handleObject(context.Background(), watch.Event{Type: watch.Added, Object: obj})

	select {
	case record := <-in.queue:
		if got := record.Meta["kind"]; got != wantKind {
			t.Fatalf("expected record kind %q, got %q", wantKind, got)
		}
		if got := record.Meta["action"]; got != string(watch.Added) {
			t.Fatalf("expected action %q, got %q", watch.Added, got)
		}

		var payload map[string]any
		if err := json.Unmarshal(record.Data, &payload); err != nil {
			t.Fatal(err)
		}
		if got := payload["kind"]; got != wantKind {
			t.Fatalf("expected payload kind %q, got %v", wantKind, got)
		}
		if got := payload["action"]; got != string(watch.Added) {
			t.Fatalf("expected payload action %q", watch.Added)
		}
	default:
		t.Fatalf("expected %s record to be enqueued", wantKind)
	}
}
