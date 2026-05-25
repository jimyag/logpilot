//go:build !integration

package operator

import (
	"context"
	"testing"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func makeLogPilot(name, ns string) *logpilotv1alpha1.LogPilot {
	lp := &logpilotv1alpha1.LogPilot{}
	lp.Name = name
	lp.Namespace = ns
	return lp
}

func newOperatorScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := logpilotv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add logpilot scheme: %v", err)
	}
	return s
}

func TestBuildAPIDeploymentDefaults(t *testing.T) {
	lp := makeLogPilot("logpilot", "logpilot-system")

	deploy := buildAPIDeployment(lp, "log-pilot-api:latest")

	if deploy.Name != "log-pilot-api" {
		t.Errorf("expected name log-pilot-api, got %q", deploy.Name)
	}
	if deploy.Namespace != "logpilot-system" {
		t.Errorf("expected namespace logpilot-system, got %q", deploy.Namespace)
	}
	if *deploy.Spec.Replicas != 2 {
		t.Errorf("expected 2 default replicas, got %d", *deploy.Spec.Replicas)
	}
	if deploy.Spec.Template.Spec.Containers[0].Image != "log-pilot-api:latest" {
		t.Errorf("unexpected image: %q", deploy.Spec.Template.Spec.Containers[0].Image)
	}
}

func TestBuildAPIDeploymentCustomReplicas(t *testing.T) {
	lp := makeLogPilot("logpilot", "default")
	lp.Spec.API.Replicas = 3

	deploy := buildAPIDeployment(lp, "log-pilot-api:v1")
	if *deploy.Spec.Replicas != 3 {
		t.Errorf("expected 3 replicas, got %d", *deploy.Spec.Replicas)
	}
}

func TestBuildAgentDaemonSetDefaults(t *testing.T) {
	lp := makeLogPilot("logpilot", "logpilot-system")

	ds := buildAgentDaemonSet(lp, "log-pilot-agent:latest")

	if ds.Name != "log-pilot-agent" {
		t.Errorf("expected name log-pilot-agent, got %q", ds.Name)
	}
	if ds.Namespace != "logpilot-system" {
		t.Errorf("expected namespace logpilot-system, got %q", ds.Namespace)
	}

	// Verify hostPath volumes are present with default paths.
	foundLog := false
	for _, v := range ds.Spec.Template.Spec.Volumes {
		if v.HostPath != nil && v.HostPath.Path == "/var/log/log-pilot" {
			foundLog = true
		}
	}
	if !foundLog {
		t.Error("expected hostPath volume /var/log/log-pilot in agent DaemonSet")
	}
}

func TestBuildAgentDaemonSetCustomLogDir(t *testing.T) {
	lp := makeLogPilot("logpilot", "default")
	lp.Spec.Agent.LogDir = "/custom/log/dir"

	ds := buildAgentDaemonSet(lp, "log-pilot-agent:v1")

	found := false
	for _, v := range ds.Spec.Template.Spec.Volumes {
		if v.HostPath != nil && v.HostPath.Path == "/custom/log/dir" {
			found = true
		}
	}
	if !found {
		t.Error("expected custom hostPath volume /custom/log/dir")
	}

	// Verify LOG_DIR env var is set.
	c := ds.Spec.Template.Spec.Containers[0]
	for _, e := range c.Env {
		if e.Name == "LOG_DIR" && e.Value == "/custom/log/dir" {
			return
		}
	}
	t.Error("expected LOG_DIR=/custom/log/dir in agent container env")
}

func TestBuildSupportObjectsCount(t *testing.T) {
	lp := makeLogPilot("logpilot", "default")

	objects := buildSupportObjects(lp)
	if len(objects) != 8 {
		t.Fatalf("expected 8 support objects, got %d", len(objects))
	}
}

func TestBuildManagedObjectsCount(t *testing.T) {
	lp := makeLogPilot("logpilot", "default")

	objects := buildManagedObjects(lp, "api:latest", "agent:latest")
	if len(objects) != 10 {
		t.Fatalf("expected 10 managed objects, got %d", len(objects))
	}
}

func TestBuildServiceAccountName(t *testing.T) {
	lp := makeLogPilot("logpilot", "default")

	sa := buildServiceAccount(lp, "test-sa")
	if sa.Name != "test-sa" {
		t.Fatalf("expected service account name test-sa, got %q", sa.Name)
	}
}

func TestBuildAPIService(t *testing.T) {
	lp := makeLogPilot("logpilot", "default")

	svc := buildAPIService(lp)
	if len(svc.Spec.Ports) != 1 {
		t.Fatalf("expected 1 service port, got %d", len(svc.Spec.Ports))
	}
	if svc.Spec.Ports[0].Port != 8443 {
		t.Fatalf("expected service port 8443, got %d", svc.Spec.Ports[0].Port)
	}
	if svc.Spec.Selector["app.kubernetes.io/name"] != "log-pilot-api" {
		t.Fatalf("expected service selector app.kubernetes.io/name=log-pilot-api, got %q", svc.Spec.Selector["app.kubernetes.io/name"])
	}
}

func TestBuildAPIClusterRole(t *testing.T) {
	lp := makeLogPilot("logpilot", "default")

	role := buildAPIClusterRole(lp)
	if len(role.Rules) != 4 {
		t.Fatalf("expected 4 policy rules, got %d", len(role.Rules))
	}
}

func TestBuildAPIClusterRoleBinding(t *testing.T) {
	lp := makeLogPilot("logpilot", "default")

	binding := buildAPIClusterRoleBinding(lp)
	if len(binding.Subjects) == 0 {
		t.Fatal("expected cluster role binding subjects")
	}
	if binding.Subjects[0].Name != "log-pilot-api" {
		t.Fatalf("expected first subject log-pilot-api, got %q", binding.Subjects[0].Name)
	}
}

func TestBuildAgentClusterRole(t *testing.T) {
	lp := makeLogPilot("logpilot", "default")

	role := buildAgentClusterRole(lp)
	if len(role.Rules) != 5 {
		t.Fatalf("expected 5 policy rules, got %d", len(role.Rules))
	}
}

func TestBuildAgentClusterRoleBinding(t *testing.T) {
	lp := makeLogPilot("logpilot", "default")

	binding := buildAgentClusterRoleBinding(lp)
	if len(binding.Subjects) == 0 {
		t.Fatal("expected cluster role binding subjects")
	}
	if binding.Subjects[0].Name != "log-pilot-agent" {
		t.Fatalf("expected first subject log-pilot-agent, got %q", binding.Subjects[0].Name)
	}
}

func TestBuildMutatingWebhook(t *testing.T) {
	lp := makeLogPilot("logpilot", "default")

	webhook := buildMutatingWebhook(lp)
	if webhook.Name != "log-pilot-api" {
		t.Fatalf("expected webhook config name log-pilot-api, got %q", webhook.Name)
	}
	if len(webhook.Webhooks) != 1 {
		t.Fatalf("expected 1 mutating webhook, got %d", len(webhook.Webhooks))
	}
	if webhook.Webhooks[0].Name != "pods.logpilot.jimyag.com" {
		t.Fatalf("expected webhook name pods.logpilot.jimyag.com, got %q", webhook.Webhooks[0].Name)
	}
	service := webhook.Webhooks[0].ClientConfig.Service
	if service == nil || service.Path == nil || *service.Path != "/mutate-pod" {
		t.Fatalf("expected webhook path /mutate-pod, got %+v", service)
	}
	if service.Port == nil || *service.Port != 8443 {
		t.Fatalf("expected webhook port 8443, got %+v", service.Port)
	}
}

func TestPreserveImmutableServiceClusterIP(t *testing.T) {
	lp := makeLogPilot("logpilot", "default")
	existing := buildAPIService(lp)
	desired := buildAPIService(lp)
	existing.Spec.ClusterIP = "10.0.0.1"
	desired.Spec.ClusterIP = ""

	preserveImmutableAndRuntimeFields(existing, desired)

	if desired.Spec.ClusterIP != "10.0.0.1" {
		t.Fatalf("expected clusterIP to be preserved, got %q", desired.Spec.ClusterIP)
	}
}

func TestPreserveImmutableWebhookCABundle(t *testing.T) {
	lp := makeLogPilot("logpilot", "default")
	existing := buildMutatingWebhook(lp)
	desired := buildMutatingWebhook(lp)
	existing.Webhooks[0].ClientConfig.CABundle = []byte("cert")
	desired.Webhooks[0].ClientConfig.CABundle = nil

	preserveImmutableAndRuntimeFields(existing, desired)

	if got := string(desired.Webhooks[0].ClientConfig.CABundle); got != "cert" {
		t.Fatalf("expected CA bundle cert, got %q", got)
	}
}

func TestIntstrFromInt(t *testing.T) {
	if got := intstrFromInt(8443); got.IntVal != 8443 {
		t.Fatalf("expected int value 8443, got %d", got.IntVal)
	}
}

func TestReconcileDeploymentCreate(t *testing.T) {
	s := newOperatorScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()

	lp := makeLogPilot("logpilot", "default")
	desired := buildAPIDeployment(lp, "log-pilot-api:latest")

	if err := reconcileDeployment(context.Background(), c, desired); err != nil {
		t.Fatalf("expected create to succeed: %v", err)
	}

	got := &appsv1.Deployment{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(desired), got); err != nil {
		t.Fatalf("expected deployment to exist: %v", err)
	}
}

func TestReconcileDeploymentUpdate(t *testing.T) {
	s := newOperatorScheme(t)
	lp := makeLogPilot("logpilot", "default")
	existing := buildAPIDeployment(lp, "log-pilot-api:v1")
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(existing).Build()

	desired := buildAPIDeployment(lp, "log-pilot-api:v2")
	if err := reconcileDeployment(context.Background(), c, desired); err != nil {
		t.Fatalf("expected update to succeed: %v", err)
	}

	got := &appsv1.Deployment{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(desired), got); err != nil {
		t.Fatalf("expected deployment to exist: %v", err)
	}
	if got.Spec.Template.Spec.Containers[0].Image != "log-pilot-api:v2" {
		t.Errorf("expected updated image, got %q", got.Spec.Template.Spec.Containers[0].Image)
	}
}

func TestReconcileDaemonSetCreate(t *testing.T) {
	s := newOperatorScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()

	lp := makeLogPilot("logpilot", "default")
	desired := buildAgentDaemonSet(lp, "log-pilot-agent:latest")

	if err := reconcileDaemonSet(context.Background(), c, desired); err != nil {
		t.Fatalf("expected create to succeed: %v", err)
	}

	got := &appsv1.DaemonSet{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(desired), got); err != nil {
		t.Fatalf("expected daemonset to exist: %v", err)
	}
}

func TestReconcileDaemonSetUpdate(t *testing.T) {
	s := newOperatorScheme(t)
	lp := makeLogPilot("logpilot", "default")
	existing := buildAgentDaemonSet(lp, "log-pilot-agent:v1")
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(existing).Build()

	desired := buildAgentDaemonSet(lp, "log-pilot-agent:v2")
	if err := reconcileDaemonSet(context.Background(), c, desired); err != nil {
		t.Fatalf("expected update to succeed: %v", err)
	}

	got := &appsv1.DaemonSet{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(desired), got); err != nil {
		t.Fatalf("expected daemonset to exist: %v", err)
	}
	if got.Spec.Template.Spec.Containers[0].Image != "log-pilot-agent:v2" {
		t.Errorf("expected updated image, got %q", got.Spec.Template.Spec.Containers[0].Image)
	}
}

func TestReconcileObjectCreate(t *testing.T) {
	s := newOperatorScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()

	lp := makeLogPilot("logpilot", "default")
	sa := buildServiceAccount(lp, "test-sa")

	if err := reconcileObject(context.Background(), c, sa); err != nil {
		t.Fatalf("expected create: %v", err)
	}

	got := &corev1.ServiceAccount{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(sa), got); err != nil {
		t.Fatalf("expected service account to exist: %v", err)
	}
}

func TestReconcileObjectUpdate(t *testing.T) {
	s := newOperatorScheme(t)
	lp := makeLogPilot("logpilot", "default")
	existing := buildServiceAccount(lp, "test-sa")
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(existing).Build()

	desired := buildServiceAccount(lp, "test-sa")
	if err := reconcileObject(context.Background(), c, desired); err != nil {
		t.Fatalf("expected update: %v", err)
	}

	got := &corev1.ServiceAccount{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(desired), got); err != nil {
		t.Fatalf("expected service account to exist: %v", err)
	}
}

func TestReconcileObjectServicePreservesClusterIP(t *testing.T) {
	s := newOperatorScheme(t)
	lp := makeLogPilot("logpilot", "default")

	existingSvc := buildAPIService(lp)
	existingSvc.Spec.ClusterIP = "10.0.0.1"
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(existingSvc).Build()

	desired := buildAPIService(lp)
	if err := reconcileObject(context.Background(), c, desired); err != nil {
		t.Fatalf("expected update: %v", err)
	}

	got := &corev1.Service{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(desired), got); err != nil {
		t.Fatalf("expected service: %v", err)
	}
	if got.Spec.ClusterIP != "10.0.0.1" {
		t.Errorf("expected ClusterIP preserved as 10.0.0.1, got %q", got.Spec.ClusterIP)
	}
}

func TestCleanupLogPilotAllNotFound(t *testing.T) {
	s := newOperatorScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	lp := makeLogPilot("logpilot", "default")

	if err := cleanupLogPilot(context.Background(), c, lp); err != nil {
		t.Fatalf("expected nil for all-not-found: %v", err)
	}
}

func TestCleanupLogPilotDeletesManagedObjects(t *testing.T) {
	s := newOperatorScheme(t)
	lp := makeLogPilot("logpilot", "default")
	objects := buildManagedObjects(lp, "api:latest", "agent:latest")
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objects...).Build()

	if err := cleanupLogPilot(context.Background(), c, lp); err != nil {
		t.Fatalf("expected cleanup to succeed: %v", err)
	}

	for _, obj := range objects {
		fetched := obj.DeepCopyObject().(client.Object)
		err := c.Get(context.Background(), client.ObjectKeyFromObject(obj), fetched)
		if err == nil {
			t.Fatalf("expected %T %s to be deleted", obj, client.ObjectKeyFromObject(obj))
		}
	}
}
