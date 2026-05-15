package input

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestK8sObjectStateInputSnapshotsSelectedResources(t *testing.T) {
	client := fake.NewSimpleClientset(
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
	defer in.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	records := readUntil(ctx, t, in, 2)

	kinds := map[string]bool{}
	for _, record := range records {
		var payload map[string]interface{}
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
	client := fake.NewSimpleClientset(
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
	defer in.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	records := readUntil(ctx, t, in, 4)

	kinds := map[string]bool{}
	for _, record := range records {
		var payload map[string]interface{}
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
