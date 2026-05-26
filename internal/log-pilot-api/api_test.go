package logpilotapi

import (
	"encoding/json"
	"testing"

	webhook "github.com/jimyag/auto-cert-webhook"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := logpilotv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add logpilot scheme: %v", err)
	}
	return scheme
}

func newTestPod(t *testing.T, labels map[string]string) []byte {
	t.Helper()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app"}},
		},
	}
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("marshal pod: %v", err)
	}
	return raw
}

func newAdmissionReview(raw []byte) admissionv1.AdmissionReview {
	return admissionv1.AdmissionReview{
		Request: &admissionv1.AdmissionRequest{
			Namespace: "default",
			Object:    runtime.RawExtension{Raw: raw},
		},
	}
}

func patchOps(t *testing.T, resp *admissionv1.AdmissionResponse) []map[string]any {
	t.Helper()

	if resp == nil {
		t.Fatal("expected admission response")
	}
	if len(resp.Patch) == 0 {
		return nil
	}

	var ops []map[string]any
	if err := json.Unmarshal(resp.Patch, &ops); err != nil {
		t.Fatalf("unmarshal patch: %v", err)
	}
	return ops
}

func TestAPINew(t *testing.T) {
	scheme := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	api := New(c, scheme)
	if api == nil {
		t.Fatal("expected API instance")
	}
}

func TestAPIConfigure(t *testing.T) {
	api := New(fake.NewClientBuilder().Build(), runtime.NewScheme())

	if got := api.Configure().Name; got != "log-pilot-api" {
		t.Fatalf("expected name log-pilot-api, got %q", got)
	}
}

func TestAPIWebhooks(t *testing.T) {
	api := New(fake.NewClientBuilder().Build(), runtime.NewScheme())

	hooks := api.Webhooks()
	if len(hooks) != 1 {
		t.Fatalf("expected 1 webhook, got %d", len(hooks))
	}
	if hooks[0].Path != "/mutate-pod" {
		t.Fatalf("expected path /mutate-pod, got %q", hooks[0].Path)
	}
	if hooks[0].Type != webhook.Mutating {
		t.Fatalf("expected mutating webhook type, got %v", hooks[0].Type)
	}
	if hooks[0].Admit == nil {
		t.Fatal("expected admit handler")
	}
}

func TestMutatePodInvalidJSON(t *testing.T) {
	scheme := newTestScheme(t)
	api := New(fake.NewClientBuilder().WithScheme(scheme).Build(), scheme)

	resp := api.mutatePod(newAdmissionReview([]byte(`{invalid}`)))
	if resp == nil {
		t.Fatal("expected admission response")
	}
	if resp.Allowed {
		t.Fatal("expected invalid JSON to be rejected")
	}
}

func TestMutatePodNoPolicyMatch(t *testing.T) {
	scheme := newTestScheme(t)
	policy := &logpilotv1alpha1.LogPilotPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default"},
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "other"}},
			Containers: []logpilotv1alpha1.ContainerPolicy{{
				Name:    "app",
				LogType: "applog",
				Path:    "/app/logs",
			}},
		},
	}
	api := New(fake.NewClientBuilder().WithScheme(scheme).WithObjects(policy).Build(), scheme)

	resp := api.mutatePod(newAdmissionReview(newTestPod(t, map[string]string{"app": "myapp"})))
	if resp == nil {
		t.Fatal("expected admission response")
	}
	if !resp.Allowed {
		t.Fatalf("expected request to be allowed, got result: %+v", resp.Result)
	}
	if ops := patchOps(t, resp); len(ops) != 0 {
		t.Fatalf("expected no patch operations, got %d", len(ops))
	}
}

func TestMutatePodWithMatchingPolicy(t *testing.T) {
	scheme := newTestScheme(t)
	policy := &logpilotv1alpha1.LogPilotPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "match", Namespace: "default"},
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "myapp"}},
			Containers: []logpilotv1alpha1.ContainerPolicy{{
				Name:      "app",
				LogType:   "applog",
				Path:      "/app/logs",
				Delivery:  "guaranteed",
				Collector: "host",
			}},
		},
	}
	api := New(fake.NewClientBuilder().WithScheme(scheme).WithObjects(policy).Build(), scheme)

	resp := api.mutatePod(newAdmissionReview(newTestPod(t, map[string]string{"app": "myapp"})))
	if resp == nil {
		t.Fatal("expected admission response")
	}
	if !resp.Allowed {
		t.Fatalf("expected request to be allowed, got result: %+v", resp.Result)
	}
	if ops := patchOps(t, resp); len(ops) == 0 {
		t.Fatal("expected patch operations for matching policy")
	}
}
