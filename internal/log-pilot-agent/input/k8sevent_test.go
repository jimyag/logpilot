package input

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
