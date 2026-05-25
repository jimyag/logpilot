package input

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
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
