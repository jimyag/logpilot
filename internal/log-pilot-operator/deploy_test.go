//go:build !integration

package operator

import (
	"testing"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func makeLogPilot(name, ns string) *logpilotv1alpha1.LogPilot {
	lp := &logpilotv1alpha1.LogPilot{}
	lp.Name = name
	lp.Namespace = ns
	return lp
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
