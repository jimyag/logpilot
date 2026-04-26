package logpilotapi

import (
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func makePolicy(labels map[string]string, containers []logpilotv1alpha1.ContainerPolicy) *logpilotv1alpha1.LogPilotPolicy {
	return &logpilotv1alpha1.LogPilotPolicy{
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Containers: containers,
		},
	}
}

func makePod(labels map[string]string, containers []corev1.Container) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Labels:    labels,
		},
		Spec: corev1.PodSpec{Containers: containers},
	}
}

func TestMatchesPolicy(t *testing.T) {
	policy := makePolicy(map[string]string{"app": "myapp"}, nil)

	pod := makePod(map[string]string{"app": "myapp"}, nil)
	if !matchesPolicy(pod, policy) {
		t.Fatal("expected pod to match policy")
	}

	podNoMatch := makePod(map[string]string{"app": "other"}, nil)
	if matchesPolicy(podNoMatch, policy) {
		t.Fatal("expected pod not to match policy")
	}
}

func TestMatchesPolicyNilSelector(t *testing.T) {
	policy := &logpilotv1alpha1.LogPilotPolicy{}
	pod := makePod(map[string]string{"app": "myapp"}, nil)
	if matchesPolicy(pod, policy) {
		t.Fatal("nil selector should not match any pod")
	}
}

func TestInjectPodNoMatch(t *testing.T) {
	pod := makePod(map[string]string{"app": "other"}, []corev1.Container{{Name: "app"}})
	policy := makePolicy(map[string]string{"app": "myapp"}, nil)

	original := pod.DeepCopy()
	if err := injectPod(pod, []*logpilotv1alpha1.LogPilotPolicy{policy}); err != nil {
		t.Fatal(err)
	}
	if pod.Annotations[podLogPolicyAnnotation] != "" {
		t.Error("expected no annotation on non-matching pod")
	}
	if len(pod.Spec.Volumes) != len(original.Spec.Volumes) {
		t.Error("expected no volumes added for non-matching pod")
	}
}

func TestInjectPodMatch(t *testing.T) {
	containers := []logpilotv1alpha1.ContainerPolicy{
		{
			Name:      "app",
			LogType:   "applog",
			Path:      "/app/logs",
			Delivery:  "guaranteed",
			Collector: "host",
		},
	}
	policy := makePolicy(map[string]string{"app": "myapp"}, containers)
	pod := makePod(map[string]string{"app": "myapp"}, []corev1.Container{{Name: "app"}})

	if err := injectPod(pod, []*logpilotv1alpha1.LogPilotPolicy{policy}); err != nil {
		t.Fatal(err)
	}

	// Annotation should be set.
	if pod.Annotations[podLogPolicyAnnotation] == "" {
		t.Error("expected log policy annotation to be set")
	}

	// Volume should be added.
	if len(pod.Spec.Volumes) == 0 {
		t.Fatal("expected log volume to be added")
	}
	if pod.Spec.Volumes[0].Name != logVolumeName {
		t.Errorf("expected volume name %q, got %q", logVolumeName, pod.Spec.Volumes[0].Name)
	}
	if pod.Spec.Volumes[0].HostPath == nil {
		t.Error("expected hostPath volume for guaranteed delivery")
	}

	// VolumeMount should be added on the container.
	c := pod.Spec.Containers[0]
	if len(c.VolumeMounts) == 0 {
		t.Fatal("expected volume mount to be added to container")
	}
	if c.VolumeMounts[0].MountPath != "/app/logs" {
		t.Errorf("expected mountPath /app/logs, got %q", c.VolumeMounts[0].MountPath)
	}

	// Env vars should be injected.
	envNames := make(map[string]bool)
	for _, e := range c.Env {
		envNames[e.Name] = true
	}
	for _, expected := range []string{"POD_NAME", "NAMESPACE", "POD_UID"} {
		if !envNames[expected] {
			t.Errorf("expected env var %q to be injected", expected)
		}
	}
}

func TestInjectPodBestEffortUsesEmptyDir(t *testing.T) {
	containers := []logpilotv1alpha1.ContainerPolicy{
		{Name: "app", LogType: "std", Path: "-", Delivery: "bestEffort"},
	}
	policy := makePolicy(map[string]string{"app": "myapp"}, containers)
	pod := makePod(map[string]string{"app": "myapp"}, []corev1.Container{{Name: "app"}})

	if err := injectPod(pod, []*logpilotv1alpha1.LogPilotPolicy{policy}); err != nil {
		t.Fatal(err)
	}

	// stdout path "-" should not add a VolumeMount.
	if len(pod.Spec.Containers[0].VolumeMounts) != 0 {
		t.Error("expected no VolumeMount for stdout path")
	}

	// Volume should be emptyDir.
	if len(pod.Spec.Volumes) == 0 {
		t.Fatal("expected volume to be added")
	}
	if pod.Spec.Volumes[0].EmptyDir == nil {
		t.Error("expected emptyDir volume for bestEffort delivery")
	}
}

func TestInjectPodFullPipeline(t *testing.T) {
	containers := []logpilotv1alpha1.ContainerPolicy{
		{
			Name:      "app",
			LogType:   "applog",
			Path:      "/app/logs",
			Delivery:  "guaranteed",
			Collector: "host",
		},
		{
			Name:      "app",
			LogType:   "std",
			Path:      "-",
			Delivery:  "bestEffort",
			Collector: "host",
		},
	}
	policy := makePolicy(map[string]string{"app": "myapp"}, containers)
	pod := makePod(map[string]string{"app": "myapp"}, []corev1.Container{
		{Name: "app"},
	})

	if err := injectPod(pod, []*logpilotv1alpha1.LogPilotPolicy{policy}); err != nil {
		t.Fatal(err)
	}

	// One volume (hostPath because guaranteed takes precedence over bestEffort).
	if len(pod.Spec.Volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(pod.Spec.Volumes))
	}
	if pod.Spec.Volumes[0].HostPath == nil {
		t.Error("expected hostPath volume when any policy has guaranteed delivery")
	}

	// One VolumeMount: only /app/logs; stdout "-" does not get a mount.
	c := pod.Spec.Containers[0]
	if len(c.VolumeMounts) != 1 {
		t.Fatalf("expected 1 VolumeMount, got %d", len(c.VolumeMounts))
	}
	if c.VolumeMounts[0].MountPath != "/app/logs" {
		t.Errorf("expected /app/logs, got %q", c.VolumeMounts[0].MountPath)
	}

	// Annotation should encode both container policies.
	ann := pod.Annotations[podLogPolicyAnnotation]
	var decoded []logpilotv1alpha1.ContainerPolicy
	if err := json.Unmarshal([]byte(ann), &decoded); err != nil {
		t.Fatalf("invalid annotation JSON: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("expected 2 policies in annotation, got %d", len(decoded))
	}
}
