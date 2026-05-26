package input

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// K8sObjectStateConfig configures Kubernetes object state collection.
type K8sObjectStateConfig struct {
	// Namespaces to watch for namespaced resources. Empty means all namespaces.
	Namespaces []string `yaml:"namespaces"`
	// Resources to collect. Empty means pod,node,deployment,statefulset,daemonset,job.
	Resources []string `yaml:"resources"`
}

type k8sObjectStateInput struct {
	cfg    K8sObjectStateConfig
	client kubernetes.Interface
	queue  chan Record
	lag    int64
	cancel context.CancelFunc
}

// NewK8sObjectStateInput creates an input that snapshots Kubernetes object
// state and then watches for changes.
func NewK8sObjectStateInput(cfg K8sObjectStateConfig, c kubernetes.Interface) Input {
	in := &k8sObjectStateInput{
		cfg:    cfg,
		client: c,
		queue:  make(chan Record, 1000),
	}
	ctx, cancel := context.WithCancel(context.Background())
	in.cancel = cancel
	go in.run(ctx)
	return in
}

func (k *k8sObjectStateInput) run(ctx context.Context) {
	for _, resource := range k.resources() {
		switch resource {
		case "pod", "pods":
			k.startPodCollectors(ctx)
		case "node", "nodes":
			go k.collectNodes(ctx)
		case "deployment", "deployments":
			k.startDeploymentCollectors(ctx)
		case "statefulset", "statefulsets":
			k.startStatefulSetCollectors(ctx)
		case "daemonset", "daemonsets":
			k.startDaemonSetCollectors(ctx)
		case "job", "jobs":
			k.startJobCollectors(ctx)
		}
	}
	<-ctx.Done()
}

func (k *k8sObjectStateInput) resources() []string {
	if len(k.cfg.Resources) > 0 {
		return k.cfg.Resources
	}
	return []string{"pod", "node", "deployment", "statefulset", "daemonset", "job"}
}

func (k *k8sObjectStateInput) namespaces() []string {
	if len(k.cfg.Namespaces) > 0 {
		return k.cfg.Namespaces
	}
	return []string{metav1.NamespaceAll}
}

func (k *k8sObjectStateInput) startPodCollectors(ctx context.Context) {
	for _, namespace := range k.namespaces() {
		ns := namespace
		go func() {
			for ctx.Err() == nil {
				list, err := k.client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
				if err == nil {
					for i := range list.Items {
						_ = k.enqueue(ctx, "Pod", "snapshot", podState(&list.Items[i]))
					}
					k.watch(ctx, func() (watch.Interface, error) {
						return k.client.CoreV1().Pods(ns).Watch(ctx, metav1.ListOptions{ResourceVersion: list.ResourceVersion})
					}, k.handleObject)
				}
				time.Sleep(time.Second)
			}
		}()
	}
}

func (k *k8sObjectStateInput) collectNodes(ctx context.Context) {
	for ctx.Err() == nil {
		list, err := k.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err == nil {
			for i := range list.Items {
				_ = k.enqueue(ctx, "Node", "snapshot", nodeState(&list.Items[i]))
			}
			k.watch(ctx, func() (watch.Interface, error) {
				return k.client.CoreV1().Nodes().Watch(ctx, metav1.ListOptions{ResourceVersion: list.ResourceVersion})
			}, k.handleObject)
		}
		time.Sleep(time.Second)
	}
}

func (k *k8sObjectStateInput) startDeploymentCollectors(ctx context.Context) {
	for _, namespace := range k.namespaces() {
		ns := namespace
		go func() {
			for ctx.Err() == nil {
				list, err := k.client.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
				if err == nil {
					for i := range list.Items {
						_ = k.enqueue(ctx, "Deployment", "snapshot", deploymentState(&list.Items[i]))
					}
					k.watch(ctx, func() (watch.Interface, error) {
						return k.client.AppsV1().Deployments(ns).Watch(ctx, metav1.ListOptions{ResourceVersion: list.ResourceVersion})
					}, k.handleObject)
				}
				time.Sleep(time.Second)
			}
		}()
	}
}

func (k *k8sObjectStateInput) startStatefulSetCollectors(ctx context.Context) {
	for _, namespace := range k.namespaces() {
		ns := namespace
		go func() {
			for ctx.Err() == nil {
				list, err := k.client.AppsV1().StatefulSets(ns).List(ctx, metav1.ListOptions{})
				if err == nil {
					for i := range list.Items {
						_ = k.enqueue(ctx, "StatefulSet", "snapshot", statefulSetState(&list.Items[i]))
					}
					k.watch(ctx, func() (watch.Interface, error) {
						return k.client.AppsV1().StatefulSets(ns).Watch(ctx, metav1.ListOptions{ResourceVersion: list.ResourceVersion})
					}, k.handleObject)
				}
				time.Sleep(time.Second)
			}
		}()
	}
}

func (k *k8sObjectStateInput) startDaemonSetCollectors(ctx context.Context) {
	for _, namespace := range k.namespaces() {
		ns := namespace
		go func() {
			for ctx.Err() == nil {
				list, err := k.client.AppsV1().DaemonSets(ns).List(ctx, metav1.ListOptions{})
				if err == nil {
					for i := range list.Items {
						_ = k.enqueue(ctx, "DaemonSet", "snapshot", daemonSetState(&list.Items[i]))
					}
					k.watch(ctx, func() (watch.Interface, error) {
						return k.client.AppsV1().DaemonSets(ns).Watch(ctx, metav1.ListOptions{ResourceVersion: list.ResourceVersion})
					}, k.handleObject)
				}
				time.Sleep(time.Second)
			}
		}()
	}
}

func (k *k8sObjectStateInput) startJobCollectors(ctx context.Context) {
	for _, namespace := range k.namespaces() {
		ns := namespace
		go func() {
			for ctx.Err() == nil {
				list, err := k.client.BatchV1().Jobs(ns).List(ctx, metav1.ListOptions{})
				if err == nil {
					for i := range list.Items {
						_ = k.enqueue(ctx, "Job", "snapshot", jobState(&list.Items[i]))
					}
					k.watch(ctx, func() (watch.Interface, error) {
						return k.client.BatchV1().Jobs(ns).Watch(ctx, metav1.ListOptions{ResourceVersion: list.ResourceVersion})
					}, k.handleObject)
				}
				time.Sleep(time.Second)
			}
		}()
	}
}

func (k *k8sObjectStateInput) watch(ctx context.Context, watchFn func() (watch.Interface, error), handle func(context.Context, watch.Event)) {
	w, err := watchFn()
	if err != nil {
		return
	}
	defer w.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-w.ResultChan():
			if !ok {
				return
			}
			handle(ctx, ev)
		}
	}
}

func (k *k8sObjectStateInput) handleObject(ctx context.Context, ev watch.Event) {
	action := string(ev.Type)
	switch obj := ev.Object.(type) {
	case *corev1.Pod:
		_ = k.enqueue(ctx, "Pod", action, podState(obj))
	case *corev1.Node:
		_ = k.enqueue(ctx, "Node", action, nodeState(obj))
	case *appsv1.Deployment:
		_ = k.enqueue(ctx, "Deployment", action, deploymentState(obj))
	case *appsv1.StatefulSet:
		_ = k.enqueue(ctx, "StatefulSet", action, statefulSetState(obj))
	case *appsv1.DaemonSet:
		_ = k.enqueue(ctx, "DaemonSet", action, daemonSetState(obj))
	case *batchv1.Job:
		_ = k.enqueue(ctx, "Job", action, jobState(obj))
	}
}

func (k *k8sObjectStateInput) enqueue(ctx context.Context, kind, action string, state map[string]any) error {
	record := map[string]any{
		"kind":      kind,
		"action":    action,
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"object":    state,
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	select {
	case k.queue <- Record{Data: raw, Meta: map[string]string{"kind": kind, "action": action}}:
		atomic.AddInt64(&k.lag, 1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (k *k8sObjectStateInput) ReadBatch(ctx context.Context, size int) ([]Record, error) {
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

func (k *k8sObjectStateInput) Commit() error { return nil }
func (k *k8sObjectStateInput) Lag() int64    { return atomic.LoadInt64(&k.lag) }
func (k *k8sObjectStateInput) Close() error {
	k.cancel()
	return nil
}

func metadataState(obj metav1.Object) map[string]any {
	return map[string]any{
		"name":            obj.GetName(),
		"namespace":       obj.GetNamespace(),
		"uid":             string(obj.GetUID()),
		"resourceVersion": obj.GetResourceVersion(),
		"labels":          obj.GetLabels(),
		"annotations":     obj.GetAnnotations(),
		"creationTime":    obj.GetCreationTimestamp().Time.UTC().Format(time.RFC3339Nano),
	}
}

func podState(pod *corev1.Pod) map[string]any {
	containers := make([]map[string]any, 0, len(pod.Status.ContainerStatuses))
	for _, status := range pod.Status.ContainerStatuses {
		containers = append(containers, map[string]any{
			"name":         status.Name,
			"ready":        status.Ready,
			"restartCount": status.RestartCount,
			"state":        containerState(status.State),
			"lastState":    containerState(status.LastTerminationState),
		})
	}
	return map[string]any{
		"metadata":          metadataState(pod),
		"nodeName":          pod.Spec.NodeName,
		"phase":             string(pod.Status.Phase),
		"reason":            pod.Status.Reason,
		"message":           pod.Status.Message,
		"conditions":        pod.Status.Conditions,
		"containerStatuses": containers,
	}
}

func containerState(state corev1.ContainerState) map[string]any {
	switch {
	case state.Waiting != nil:
		return map[string]any{"state": "waiting", "reason": state.Waiting.Reason, "message": state.Waiting.Message}
	case state.Running != nil:
		return map[string]any{"state": "running", "startedAt": state.Running.StartedAt.Time.UTC().Format(time.RFC3339Nano)}
	case state.Terminated != nil:
		return map[string]any{
			"state":      "terminated",
			"reason":     state.Terminated.Reason,
			"message":    state.Terminated.Message,
			"exitCode":   state.Terminated.ExitCode,
			"startedAt":  state.Terminated.StartedAt.Time.UTC().Format(time.RFC3339Nano),
			"finishedAt": state.Terminated.FinishedAt.Time.UTC().Format(time.RFC3339Nano),
		}
	default:
		return nil
	}
}

func nodeState(node *corev1.Node) map[string]any {
	return map[string]any{
		"metadata":    metadataState(node),
		"conditions":  node.Status.Conditions,
		"capacity":    node.Status.Capacity,
		"allocatable": node.Status.Allocatable,
	}
}

func deploymentState(deployment *appsv1.Deployment) map[string]any {
	return map[string]any{
		"metadata":            metadataState(deployment),
		"replicas":            deployment.Status.Replicas,
		"readyReplicas":       deployment.Status.ReadyReplicas,
		"availableReplicas":   deployment.Status.AvailableReplicas,
		"updatedReplicas":     deployment.Status.UpdatedReplicas,
		"unavailableReplicas": deployment.Status.UnavailableReplicas,
		"observedGeneration":  deployment.Status.ObservedGeneration,
		"conditions":          deployment.Status.Conditions,
	}
}

func statefulSetState(statefulSet *appsv1.StatefulSet) map[string]any {
	return map[string]any{
		"metadata":           metadataState(statefulSet),
		"replicas":           statefulSet.Status.Replicas,
		"readyReplicas":      statefulSet.Status.ReadyReplicas,
		"availableReplicas":  statefulSet.Status.AvailableReplicas,
		"updatedReplicas":    statefulSet.Status.UpdatedReplicas,
		"currentReplicas":    statefulSet.Status.CurrentReplicas,
		"observedGeneration": statefulSet.Status.ObservedGeneration,
		"conditions":         statefulSet.Status.Conditions,
	}
}

func daemonSetState(daemonSet *appsv1.DaemonSet) map[string]any {
	return map[string]any{
		"metadata":               metadataState(daemonSet),
		"desiredNumberScheduled": daemonSet.Status.DesiredNumberScheduled,
		"currentNumberScheduled": daemonSet.Status.CurrentNumberScheduled,
		"numberReady":            daemonSet.Status.NumberReady,
		"updatedNumberScheduled": daemonSet.Status.UpdatedNumberScheduled,
		"numberAvailable":        daemonSet.Status.NumberAvailable,
		"numberUnavailable":      daemonSet.Status.NumberUnavailable,
		"observedGeneration":     daemonSet.Status.ObservedGeneration,
		"conditions":             daemonSet.Status.Conditions,
	}
}

func jobState(job *batchv1.Job) map[string]any {
	return map[string]any{
		"metadata":         metadataState(job),
		"active":           job.Status.Active,
		"succeeded":        job.Status.Succeeded,
		"failed":           job.Status.Failed,
		"ready":            job.Status.Ready,
		"completedIndexes": job.Status.CompletedIndexes,
		"failedIndexes":    job.Status.FailedIndexes,
		"conditions":       job.Status.Conditions,
		"startTime":        optionalTime(job.Status.StartTime),
		"completionTime":   optionalTime(job.Status.CompletionTime),
	}
}

func optionalTime(t *metav1.Time) string {
	if t == nil {
		return ""
	}
	return t.Time.UTC().Format(time.RFC3339Nano)
}
