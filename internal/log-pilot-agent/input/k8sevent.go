package input

import (
	"context"
	"encoding/json"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	client client.Client
	queue  chan Record
	lag    int64
	cancel context.CancelFunc
}

// NewK8sEventInput creates an input that lists K8s Event objects once and queues them.
// Full watch-with-ResourceVersion persistence is a future enhancement.
func NewK8sEventInput(cfg K8sEventConfig, c client.Client) Input {
	in := &k8sEventInput{
		cfg:    cfg,
		client: c,
		queue:  make(chan Record, 1000),
	}
	ctx, cancel := context.WithCancel(context.Background())
	in.cancel = cancel
	go in.list(ctx)
	return in
}

func (k *k8sEventInput) list(ctx context.Context) {
	namespaces := k.cfg.Namespaces
	if len(namespaces) == 0 {
		namespaces = []string{""}
	}
	for _, ns := range namespaces {
		var list corev1.EventList
		if err := k.client.List(ctx, &list, client.InNamespace(ns)); err != nil {
			continue
		}
		for _, ev := range list.Items {
			raw, err := json.Marshal(ev)
			if err != nil {
				continue
			}
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
		case r, ok := <-k.queue:
			if !ok {
				return records, nil
			}
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
