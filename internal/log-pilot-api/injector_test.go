package logpilotapi

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

func TestMatchesPolicyInvalidSelector(t *testing.T) {
	policy := &logpilotv1alpha1.LogPilotPolicy{
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Selector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "app",
					Operator: metav1.LabelSelectorOperator("bogus"),
					Values:   []string{"myapp"},
				}},
			},
		},
	}

	if matchesPolicy(makePod(map[string]string{"app": "myapp"}, nil), policy) {
		t.Fatal("invalid selector should not match")
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

func TestInjectEnvVarsDeduplicatesExistingValues(t *testing.T) {
	container := &corev1.Container{Env: []corev1.EnvVar{{Name: "POD_NAME", Value: "existing"}}}

	injectEnvVars(container)

	count := 0
	for _, env := range container.Env {
		if env.Name == "POD_NAME" {
			count++
			if env.Value != "existing" {
				t.Fatalf("expected existing POD_NAME to be preserved, got %#v", env)
			}
		}
	}
	if count != 1 {
		t.Fatalf("expected 1 POD_NAME env var, got %d", count)
	}
	if len(container.Env) != 3 {
		t.Fatalf("expected all downward API vars to be present once, got %d", len(container.Env))
	}
}

func TestInjectVolumeMountsSkipsDashAndExistingPaths(t *testing.T) {
	container := &corev1.Container{VolumeMounts: []corev1.VolumeMount{{MountPath: "/app/logs"}}}
	policies := []logpilotv1alpha1.ContainerPolicy{
		{Name: "app", Path: "-", LogType: "stdout"},
		{Name: "app", Path: "/app/logs", LogType: "existing"},
		{Name: "app", Path: "/other", LogType: "access"},
	}

	injectVolumeMounts(container, policies)

	if len(container.VolumeMounts) != 2 {
		t.Fatalf("expected 2 volume mounts, got %d", len(container.VolumeMounts))
	}
	if container.VolumeMounts[1].MountPath != "/other" {
		t.Fatalf("expected only /other mount to be added, got %#v", container.VolumeMounts)
	}
}

func TestVolumeSubPathExprBestEffort(t *testing.T) {
	cp := logpilotv1alpha1.ContainerPolicy{Name: "app", LogType: "access", Delivery: "bestEffort"}
	if got := volumeSubPathExpr(cp); got != "app/access" {
		t.Fatalf("expected app/access, got %q", got)
	}
}

func TestEnsureLogVolumeReturnsWhenAlreadyPresent(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: logVolumeName}}}}

	ensureLogVolume(pod, []logpilotv1alpha1.ContainerPolicy{{Delivery: "guaranteed"}})

	if len(pod.Spec.Volumes) != 1 {
		t.Fatalf("expected existing log volume to be reused, got %d volumes", len(pod.Spec.Volumes))
	}
}

func TestInjectPodSkipsContainersWithoutMatchingContainerPolicy(t *testing.T) {
	policy := makePolicy(map[string]string{"app": "myapp"}, []logpilotv1alpha1.ContainerPolicy{{
		Name:     "sidecar",
		LogType:  "applog",
		Path:     "/app/logs",
		Delivery: "guaranteed",
	}})
	pod := makePod(map[string]string{"app": "myapp"}, []corev1.Container{{Name: "app"}})

	if err := injectPod(pod, []*logpilotv1alpha1.LogPilotPolicy{policy}); err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.Containers[0].Env) != 0 {
		t.Fatalf("expected container env to remain unchanged, got %#v", pod.Spec.Containers[0].Env)
	}
	if len(pod.Spec.Containers[0].VolumeMounts) != 0 {
		t.Fatalf("expected container mounts to remain unchanged, got %#v", pod.Spec.Containers[0].VolumeMounts)
	}
	if _, ok := pod.Annotations[podLogPolicyAnnotation]; !ok {
		t.Fatal("expected pod annotation to be added")
	}
	if len(pod.Spec.Volumes) != 1 || pod.Spec.Volumes[0].Name != logVolumeName {
		t.Fatalf("expected log volume to be added, got %#v", pod.Spec.Volumes)
	}
}

func invalidJSONConfig() map[string]apiextensionsv1.JSON {
	return map[string]apiextensionsv1.JSON{"path": {Raw: []byte("not-json")}}
}

func TestInjectPodReturnsMarshalError(t *testing.T) {
	policy := makePolicy(map[string]string{"app": "myapp"}, []logpilotv1alpha1.ContainerPolicy{{
		Name:     "app",
		LogType:  "applog",
		Path:     "/app/logs",
		Delivery: "guaranteed",
		Output: logpilotv1alpha1.OutputSpec{
			Type:   "file",
			Config: invalidJSONConfig(),
		},
	}})
	pod := makePod(map[string]string{"app": "myapp"}, []corev1.Container{{Name: "app"}})

	err := injectPod(pod, []*logpilotv1alpha1.LogPilotPolicy{policy})
	if err == nil || !strings.Contains(err.Error(), "marshal container policies") {
		t.Fatalf("expected marshal container policies error, got %v", err)
	}
}

func TestMutatePodReturnsListError(t *testing.T) {
	scheme := newTestScheme(t)
	api := New(fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		List: func(context.Context, ctrlclient.WithWatch, ctrlclient.ObjectList, ...ctrlclient.ListOption) error {
			return errors.New("list failed")
		},
	}).Build(), scheme)

	resp := api.mutatePod(newAdmissionReview(newTestPod(t, map[string]string{"app": "myapp"})))
	if resp == nil || resp.Allowed {
		t.Fatalf("expected list failure to reject request, got %+v", resp)
	}
	if resp.Result == nil || !strings.Contains(resp.Result.Message, "list failed") {
		t.Fatalf("expected list error message, got %+v", resp.Result)
	}
}

func TestMutatePodReturnsInjectionError(t *testing.T) {
	scheme := newTestScheme(t)
	badPolicy := logpilotv1alpha1.LogPilotPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-policy", Namespace: "default"},
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "myapp"}},
			Containers: []logpilotv1alpha1.ContainerPolicy{{
				Name:     "app",
				LogType:  "applog",
				Path:     "/app/logs",
				Delivery: "guaranteed",
				Output: logpilotv1alpha1.OutputSpec{
					Type:   "file",
					Config: invalidJSONConfig(),
				},
			}},
		},
	}
	api := New(fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		List: func(_ context.Context, _ ctrlclient.WithWatch, list ctrlclient.ObjectList, _ ...ctrlclient.ListOption) error {
			policyList := list.(*logpilotv1alpha1.LogPilotPolicyList)
			policyList.Items = []logpilotv1alpha1.LogPilotPolicy{badPolicy}
			return nil
		},
	}).Build(), scheme)

	resp := api.mutatePod(newAdmissionReview(newTestPod(t, map[string]string{"app": "myapp"})))
	if resp == nil || resp.Allowed {
		t.Fatalf("expected injection failure to reject request, got %+v", resp)
	}
	if resp.Result == nil || !strings.Contains(resp.Result.Message, "marshal container policies") {
		t.Fatalf("expected marshal error message, got %+v", resp.Result)
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
