package logpilotapi

import (
	"context"
	"encoding/json"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	webhook "github.com/jimyag/auto-cert-webhook"
	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

// API implements webhook.Admission for log-pilot-api.
type API struct {
	client client.Client
	scheme *runtime.Scheme
}

// New creates an API with the given K8s client and scheme.
func New(c client.Client, scheme *runtime.Scheme) *API {
	return &API{client: c, scheme: scheme}
}

func (a *API) Configure() webhook.Config {
	return webhook.Config{
		Name: "log-pilot-api",
	}
}

func (a *API) Webhooks() []webhook.Hook {
	return []webhook.Hook{
		{
			Path:  "/mutate-pod",
			Type:  webhook.Mutating,
			Admit: a.mutatePod,
		},
	}
}

func (a *API) mutatePod(ar admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	pod := &corev1.Pod{}
	if err := json.Unmarshal(ar.Request.Object.Raw, pod); err != nil {
		return webhook.Errored(err)
	}

	policyList := &logpilotv1alpha1.LogPilotPolicyList{}
	if err := a.client.List(context.Background(), policyList,
		client.InNamespace(ar.Request.Namespace)); err != nil {
		return webhook.Errored(err)
	}

	policies := make([]*logpilotv1alpha1.LogPilotPolicy, len(policyList.Items))
	for i := range policyList.Items {
		policies[i] = &policyList.Items[i]
	}

	original := pod.DeepCopy()
	if err := injectPod(pod, policies); err != nil {
		return webhook.Errored(err)
	}

	return webhook.PatchResponse(original, pod)
}
