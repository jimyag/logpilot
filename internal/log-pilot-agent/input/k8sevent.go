package input

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// K8sEventConfig configures the K8s event input.
type K8sEventConfig struct {
	// Namespaces to watch. Empty means all namespaces.
	Namespaces []string `yaml:"namespaces"`
	// ResourceVersionPath is where the last-seen ResourceVersion is persisted.
	ResourceVersionPath string `yaml:"resourceVersionPath"`
}

type k8sEventInput struct {
	cfg    K8sEventConfig
	client kubernetes.Interface
	queue  chan Record
	lag    int64
	cancel context.CancelFunc
	mu     sync.Mutex

	lastResourceVersion string
	readResourceVersion string
}

// NewK8sEventInput creates an input that lists K8s Event objects and then
// continuously watches for changes.
func NewK8sEventInput(cfg K8sEventConfig, c kubernetes.Interface) Input {
	in := &k8sEventInput{
		cfg:                 cfg,
		client:              c,
		queue:               make(chan Record, 1000),
		lastResourceVersion: loadResourceVersion(cfg.ResourceVersionPath),
	}
	ctx, cancel := context.WithCancel(context.Background())
	in.cancel = cancel
	go in.run(ctx)
	return in
}

func (k *k8sEventInput) run(ctx context.Context) {
	for {
		if err := k.listAndWatch(ctx); err != nil && ctx.Err() == nil {
			time.Sleep(time.Second)
			continue
		}
		return
	}
}

func (k *k8sEventInput) listAndWatch(ctx context.Context) error {
	namespaces := k.cfg.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{""}
	}
	for _, namespace := range namespaces {
		if err := k.listNamespace(ctx, namespace); err != nil {
			return err
		}
		go k.watchNamespace(ctx, namespace)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (k *k8sEventInput) listNamespace(ctx context.Context, namespace string) error {
	resourceVersion := k.getLastResourceVersion()
	resume := resourceVersion != ""
	list, err := k.client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{
		ResourceVersion: resourceVersion,
	})
	if apierrors.IsResourceExpired(err) {
		k.setLastResourceVersion("")
		resume = false
		list, err = k.client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	}
	if err != nil {
		return err
	}
	k.setLastResourceVersion(list.ResourceVersion)
	if resume {
		return nil
	}
	for i := range list.Items {
		if err := k.enqueue(ctx, list.Items[i]); err != nil {
			return err
		}
	}
	return nil
}

func (k *k8sEventInput) watchNamespace(ctx context.Context, namespace string) {
	for ctx.Err() == nil {
		w, err := k.client.CoreV1().Events(namespace).Watch(ctx, metav1.ListOptions{
			ResourceVersion: k.getLastResourceVersion(),
		})
		if apierrors.IsResourceExpired(err) {
			k.setLastResourceVersion("")
			_ = k.listNamespace(ctx, namespace)
			continue
		}
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		k.consumeWatch(ctx, w)
	}
}

func (k *k8sEventInput) consumeWatch(ctx context.Context, w watch.Interface) {
	defer w.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-w.ResultChan():
			if !ok {
				return
			}
			if ev.Type == watch.Deleted {
				continue
			}
			eventObj, ok := ev.Object.(*corev1.Event)
			if !ok {
				continue
			}
			k.setLastResourceVersion(eventObj.ResourceVersion)
			_ = k.enqueue(ctx, *eventObj)
		}
	}
}

func (k *k8sEventInput) enqueue(ctx context.Context, ev corev1.Event) error {
	raw, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	select {
	case k.queue <- Record{
		Data: raw,
		Meta: map[string]string{"resourceVersion": ev.ResourceVersion},
	}:
		atomic.AddInt64(&k.lag, 1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (k *k8sEventInput) ReadBatch(ctx context.Context, size int) ([]Record, error) {
	var records []Record
	for len(records) < size {
		select {
		case r, ok := <-k.queue:
			if !ok {
				return records, nil
			}
			records = append(records, r)
			if r.Meta != nil && r.Meta["resourceVersion"] != "" {
				k.setReadResourceVersion(r.Meta["resourceVersion"])
			}
			atomic.AddInt64(&k.lag, -1)
		case <-ctx.Done():
			return records, nil
		default:
			return records, nil
		}
	}
	return records, nil
}

func (k *k8sEventInput) Commit() error {
	resourceVersion := k.getReadResourceVersion()
	if resourceVersion == "" {
		resourceVersion = k.getLastResourceVersion()
	}
	if k.cfg.ResourceVersionPath == "" || resourceVersion == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(k.cfg.ResourceVersionPath), 0755); err != nil {
		return err
	}
	tmp := k.cfg.ResourceVersionPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(resourceVersion), 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, k.cfg.ResourceVersionPath); err != nil {
		return fmt.Errorf("commit k8s event resourceVersion: %w", err)
	}
	return nil
}

func (k *k8sEventInput) Lag() int64 { return atomic.LoadInt64(&k.lag) }

func (k *k8sEventInput) Close() error {
	k.cancel()
	return k.Commit()
}

func loadResourceVersion(path string) string {
	if path == "" {
		return ""
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(raw)
}

func (k *k8sEventInput) getLastResourceVersion() string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.lastResourceVersion
}

func (k *k8sEventInput) setLastResourceVersion(resourceVersion string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.lastResourceVersion = resourceVersion
}

func (k *k8sEventInput) getReadResourceVersion() string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.readResourceVersion
}

func (k *k8sEventInput) setReadResourceVersion(resourceVersion string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.readResourceVersion = resourceVersion
}
