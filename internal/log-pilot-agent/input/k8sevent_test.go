package input

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"
)

func TestK8sEventInputListsAndCommitsResourceVersion(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "started",
			Namespace:       "default",
			ResourceVersion: "10",
		},
		Reason: "Started",
	})
	metaPath := filepath.Join(t.TempDir(), "events.rv")
	in := NewK8sEventInput(K8sEventConfig{
		Namespaces:          []string{"default"},
		ResourceVersionPath: metaPath,
	}, client)
	defer in.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	records := readUntil(ctx, t, in, 1)
	if len(records) != 1 {
		t.Fatalf("expected 1 event, got %d", len(records))
	}
	if records[0].Meta["resourceVersion"] == "" {
		t.Fatal("expected resourceVersion metadata")
	}
	if err := in.Commit(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) == "" {
		t.Fatal("expected committed resourceVersion")
	}
}

func TestK8sEventInputWatchesNewEvents(t *testing.T) {
	client := fake.NewSimpleClientset()
	in := NewK8sEventInput(K8sEventConfig{Namespaces: []string{"default"}}, client)
	defer in.Close()

	_, err := client.CoreV1().Events("default").Create(context.Background(), &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "created",
			Namespace: "default",
		},
		Reason: "Created",
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	records := readUntil(ctx, t, in, 1)
	if len(records) != 1 {
		t.Fatalf("expected watched event, got %d", len(records))
	}
}

func readUntil(ctx context.Context, t *testing.T, in Input, count int) []Record {
	t.Helper()
	var records []Record
	for len(records) < count && ctx.Err() == nil {
		batch, err := in.ReadBatch(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, batch...)
		if len(records) < count {
			time.Sleep(20 * time.Millisecond)
		}
	}
	return records
}

func TestK8sEventInputLag(t *testing.T) {
	in := &k8sEventInput{}
	if got := in.Lag(); got != 0 {
		t.Fatalf("expected zero lag, got %d", got)
	}
}

func TestK8sEventInputCommitNoRV(t *testing.T) {
	in := &k8sEventInput{}
	if err := in.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

func TestConsumeWatchChannelClosed(t *testing.T) {
	in := &k8sEventInput{queue: make(chan Record, 1)}
	fw := watch.NewFake()

	done := make(chan struct{})
	go func() {
		in.consumeWatch(context.Background(), fw)
		close(done)
	}()

	fw.Stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumeWatch did not return after watch channel closed")
	}

	select {
	case record := <-in.queue:
		t.Fatalf("unexpected record after closed watch: %+v", record)
	default:
	}
}

func TestConsumeWatchErrorEvent(t *testing.T) {
	in := &k8sEventInput{queue: make(chan Record, 1)}
	fw := watch.NewFake()

	done := make(chan struct{})
	go func() {
		in.consumeWatch(context.Background(), fw)
		close(done)
	}()

	fw.Error(&metav1.Status{Status: metav1.StatusFailure, Message: "boom"})
	fw.Delete(&corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "deleted", ResourceVersion: "99"}})
	fw.Stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumeWatch did not return after error watch event")
	}

	select {
	case record := <-in.queue:
		t.Fatalf("unexpected record after ignored watch events: %+v", record)
	default:
	}

	if got := in.getLastResourceVersion(); got != "" {
		t.Fatalf("expected resource version to remain empty, got %q", got)
	}
}

func TestK8sEventRunNamespaceStopsOnCancel(t *testing.T) {
	client := fake.NewSimpleClientset()
	in := &k8sEventInput{client: client, queue: make(chan Record, 1), cancel: func() {}}

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	client.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		close(started)
		<-ctx.Done()
		return true, nil, ctx.Err()
	})

	done := make(chan struct{})
	go func() {
		in.runNamespace(ctx, "default")
		close(done)
	}()

	<-started
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runNamespace did not stop after context cancellation")
	}
}

func TestK8sEventRunNamespaceRetriesAfterListError(t *testing.T) {
	client := fake.NewSimpleClientset()
	in := &k8sEventInput{client: client, queue: make(chan Record, 1), cancel: func() {}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listCalls := 0
	client.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		listCalls++
		if listCalls == 1 {
			return true, nil, errors.New("boom")
		}
		return true, &corev1.EventList{ListMeta: metav1.ListMeta{ResourceVersion: "5"}}, nil
	})
	client.PrependWatchReactor("events", func(action clientgotesting.Action) (bool, watch.Interface, error) {
		fw := watch.NewFake()
		fw.Stop()
		cancel()
		return true, fw, nil
	})

	done := make(chan struct{})
	go func() {
		in.runNamespace(ctx, "default")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("runNamespace did not exit after retry")
	}

	if listCalls < 2 {
		t.Fatalf("expected listNamespace to be retried, got %d calls", listCalls)
	}
}

func TestK8sEventListNamespaceResumeSkipsEnqueue(t *testing.T) {
	client := fake.NewSimpleClientset()
	in := &k8sEventInput{client: client, queue: make(chan Record, 1), cancel: func() {}}
	in.setLastResourceVersion("123")

	client.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{
			ListMeta: metav1.ListMeta{ResourceVersion: "456"},
			Items: []corev1.Event{{
				ObjectMeta: metav1.ObjectMeta{Name: "resumed", Namespace: "default", ResourceVersion: "789"},
			}},
		}, nil
	})

	if err := in.listNamespace(context.Background(), "default"); err != nil {
		t.Fatalf("listNamespace() error = %v", err)
	}
	if got := in.getLastResourceVersion(); got != "456" {
		t.Fatalf("expected last resource version to update, got %q", got)
	}
	if len(in.queue) != 0 {
		t.Fatalf("expected no records enqueued when resuming, got %d", len(in.queue))
	}
}

func TestK8sEventListNamespaceResourceExpiredFallsBack(t *testing.T) {
	client := fake.NewSimpleClientset()
	in := &k8sEventInput{client: client, queue: make(chan Record, 2), cancel: func() {}}
	in.setLastResourceVersion("123")

	listCalls := 0
	client.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		listCalls++
		if listCalls == 1 {
			return true, nil, apierrors.NewResourceExpired("expired")
		}
		return true, &corev1.EventList{
			ListMeta: metav1.ListMeta{ResourceVersion: "456"},
			Items: []corev1.Event{{
				ObjectMeta: metav1.ObjectMeta{Name: "fresh", Namespace: "default", ResourceVersion: "457"},
				Reason:     "Started",
			}},
		}, nil
	})

	if err := in.listNamespace(context.Background(), "default"); err != nil {
		t.Fatalf("listNamespace() error = %v", err)
	}
	if listCalls != 2 {
		t.Fatalf("expected fallback list call, got %d calls", listCalls)
	}
	if got := in.getLastResourceVersion(); got != "456" {
		t.Fatalf("expected last resource version 456, got %q", got)
	}
	if len(in.queue) != 1 {
		t.Fatalf("expected 1 enqueued record after relist, got %d", len(in.queue))
	}
}

func TestK8sEventConsumeWatchSkipsDeletedAndNonEvent(t *testing.T) {
	in := &k8sEventInput{queue: make(chan Record, 1)}
	fw := watch.NewFake()

	done := make(chan struct{})
	go func() {
		in.consumeWatch(context.Background(), fw)
		close(done)
	}()

	fw.Delete(&corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "deleted", ResourceVersion: "9"}})
	fw.Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "default"}})
	fw.Stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumeWatch did not return after ignored events")
	}
	if len(in.queue) != 0 {
		t.Fatalf("expected ignored watch events to skip enqueue, got %d", len(in.queue))
	}
	if got := in.getLastResourceVersion(); got != "" {
		t.Fatalf("expected resource version to remain empty, got %q", got)
	}
}

func TestK8sEventEnqueueContextCancelled(t *testing.T) {
	in := &k8sEventInput{queue: make(chan Record), cancel: func() {}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := in.enqueue(ctx, corev1.Event{}); err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestK8sEventReadBatchReturnsPartialOnClosedQueue(t *testing.T) {
	in := &k8sEventInput{queue: make(chan Record, 1)}
	in.queue <- Record{Data: []byte("one"), Meta: map[string]string{"resourceVersion": "9"}}
	close(in.queue)

	batch, err := in.ReadBatch(context.Background(), 2)
	if err != nil {
		t.Fatalf("ReadBatch() error = %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected partial batch of 1 record, got %d", len(batch))
	}
	if got := in.getReadResourceVersion(); got != "9" {
		t.Fatalf("expected read resource version 9, got %q", got)
	}
}

func TestK8sEventReadBatchWithoutResourceVersionMeta(t *testing.T) {
	in := &k8sEventInput{queue: make(chan Record, 1)}
	in.queue <- Record{Data: []byte("one")}
	close(in.queue)

	batch, err := in.ReadBatch(context.Background(), 2)
	if err != nil {
		t.Fatalf("ReadBatch() error = %v", err)
	}
	if len(batch) != 1 {
		t.Fatalf("expected 1 record, got %d", len(batch))
	}
	if got := in.getReadResourceVersion(); got != "" {
		t.Fatalf("expected read resource version to remain empty, got %q", got)
	}
}

func TestK8sEventReadBatchCancelledContext(t *testing.T) {
	in := &k8sEventInput{queue: make(chan Record)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for i := 0; i < 256; i++ {
		batch, err := in.ReadBatch(ctx, 1)
		if err != nil {
			t.Fatalf("ReadBatch() error = %v", err)
		}
		if len(batch) != 0 {
			t.Fatalf("expected empty batch, got %v", batch)
		}
	}
}

func TestK8sEventCommitEmptyPath(t *testing.T) {
	in := &k8sEventInput{cfg: K8sEventConfig{}, cancel: func() {}}
	in.setLastResourceVersion("9")
	if err := in.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

func TestK8sEventCommitMkdirAllError(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	in := &k8sEventInput{cfg: K8sEventConfig{ResourceVersionPath: filepath.Join(blocker, "events.rv")}, cancel: func() {}}
	in.setReadResourceVersion("9")

	if err := in.Commit(); err == nil {
		t.Fatal("expected Commit to fail when parent directory cannot be created")
	}
}

func TestK8sEventCommitWriteFileError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.rv")
	if err := os.Mkdir(path+".tmp", 0o755); err != nil {
		t.Fatal(err)
	}

	in := &k8sEventInput{cfg: K8sEventConfig{ResourceVersionPath: path}, cancel: func() {}}
	in.setReadResourceVersion("9")

	if err := in.Commit(); err == nil {
		t.Fatal("expected Commit to fail when temp file path is a directory")
	}
}

func TestK8sEventCommitRenameError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.rv")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}

	in := &k8sEventInput{cfg: K8sEventConfig{ResourceVersionPath: path}, cancel: func() {}}
	in.setReadResourceVersion("9")

	if err := in.Commit(); err == nil {
		t.Fatal("expected Commit to fail when rename target is a directory")
	}
}

func TestK8sEventListNamespaceReturnsError(t *testing.T) {
	client := fake.NewSimpleClientset()
	in := &k8sEventInput{client: client, queue: make(chan Record, 1), cancel: func() {}}

	client.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("boom")
	})

	if err := in.listNamespace(context.Background(), "default"); err == nil {
		t.Fatal("expected listNamespace to return list errors")
	}
}

func TestK8sEventListNamespaceReturnsEnqueueError(t *testing.T) {
	client := fake.NewSimpleClientset()
	in := &k8sEventInput{client: client, queue: make(chan Record), cancel: func() {}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client.PrependReactor("list", "events", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.EventList{Items: []corev1.Event{{ObjectMeta: metav1.ObjectMeta{Name: "event", ResourceVersion: "7"}}}}, nil
	})

	if err := in.listNamespace(ctx, "default"); err == nil {
		t.Fatal("expected listNamespace to return enqueue error when context is cancelled")
	}
}

func TestK8sEventConsumeWatchEnqueuesEvent(t *testing.T) {
	in := &k8sEventInput{queue: make(chan Record, 1)}
	fw := watch.NewFake()

	done := make(chan struct{})
	go func() {
		in.consumeWatch(context.Background(), fw)
		close(done)
	}()

	fw.Add(&corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "created", ResourceVersion: "11"}})
	fw.Stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumeWatch did not return after enqueueing event")
	}
	if got := in.getLastResourceVersion(); got != "11" {
		t.Fatalf("expected resource version 11, got %q", got)
	}
	if len(in.queue) != 1 {
		t.Fatalf("expected 1 enqueued event, got %d", len(in.queue))
	}
}

func TestK8sEventConsumeWatchStopsOnContextCancel(t *testing.T) {
	in := &k8sEventInput{queue: make(chan Record, 1)}
	fw := watch.NewFake()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		in.consumeWatch(ctx, fw)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumeWatch did not stop after context cancellation")
	}
}

func TestK8sEventCommitUsesLastResourceVersion(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "events.rv")
	in := &k8sEventInput{cfg: K8sEventConfig{ResourceVersionPath: metaPath}, cancel: func() {}}
	in.setLastResourceVersion("15")

	if err := in.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "15" {
		t.Fatalf("expected committed last resource version, got %q", raw)
	}
}

func TestLoadResourceVersionEmptyMissingAndSuccess(t *testing.T) {
	if got := loadResourceVersion(""); got != "" {
		t.Fatalf("expected empty resource version for empty path, got %q", got)
	}

	missing := filepath.Join(t.TempDir(), "missing.rv")
	if got := loadResourceVersion(missing); got != "" {
		t.Fatalf("expected empty resource version for missing file, got %q", got)
	}

	path := filepath.Join(t.TempDir(), "events.rv")
	if err := os.WriteFile(path, []byte("21"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := loadResourceVersion(path); got != "21" {
		t.Fatalf("expected loaded resource version 21, got %q", got)
	}
}
