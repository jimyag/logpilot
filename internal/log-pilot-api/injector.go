package logpilotapi

import (
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

const (
	logVolumeName          = "log-pilot-logs"
	logHostPath            = "/var/log/log-pilot"
	podLogPolicyAnnotation = "beta.logpilot.io/log-policy"
)

// matchesPolicy returns true if the pod labels satisfy the policy selector.
func matchesPolicy(pod *corev1.Pod, policy *logpilotv1alpha1.LogPilotPolicy) bool {
	if policy.Spec.Selector == nil {
		return false
	}
	selector, err := metav1.LabelSelectorAsSelector(policy.Spec.Selector)
	if err != nil {
		return false
	}
	return selector.Matches(labels.Set(pod.Labels))
}

// injectPod modifies pod in-place based on all matching policies.
// Returns nil if no policies matched (pod is unchanged).
func injectPod(pod *corev1.Pod, policies []*logpilotv1alpha1.LogPilotPolicy) error {
	var containerPolicies []logpilotv1alpha1.ContainerPolicy
	for _, p := range policies {
		if matchesPolicy(pod, p) {
			containerPolicies = append(containerPolicies, p.Spec.Containers...)
		}
	}
	if len(containerPolicies) == 0 {
		return nil
	}

	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		var matched []logpilotv1alpha1.ContainerPolicy
		for _, cp := range containerPolicies {
			if cp.Name == c.Name {
				matched = append(matched, cp)
			}
		}
		if len(matched) == 0 {
			continue
		}
		injectEnvVars(c)
		injectVolumeMounts(c, matched)
	}

	ensureLogVolume(pod, containerPolicies)

	raw, err := json.Marshal(containerPolicies)
	if err != nil {
		return fmt.Errorf("marshal container policies: %w", err)
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[podLogPolicyAnnotation] = string(raw)
	return nil
}

func injectEnvVars(c *corev1.Container) {
	existing := make(map[string]bool, len(c.Env))
	for _, e := range c.Env {
		existing[e.Name] = true
	}
	for _, e := range downwardAPIEnvVars() {
		if !existing[e.Name] {
			c.Env = append(c.Env, e)
		}
	}
}

func downwardAPIEnvVars() []corev1.EnvVar {
	fieldRef := func(name, path string) corev1.EnvVar {
		return corev1.EnvVar{
			Name: name,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  path,
				},
			},
		}
	}
	return []corev1.EnvVar{
		fieldRef("POD_NAME", "metadata.name"),
		fieldRef("NAMESPACE", "metadata.namespace"),
		fieldRef("POD_UID", "metadata.uid"),
	}
}

func injectVolumeMounts(c *corev1.Container, policies []logpilotv1alpha1.ContainerPolicy) {
	existing := make(map[string]bool, len(c.VolumeMounts))
	for _, vm := range c.VolumeMounts {
		existing[vm.MountPath] = true
	}
	for _, cp := range policies {
		if cp.Path == "-" || existing[cp.Path] {
			continue
		}
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
			Name:        logVolumeName,
			MountPath:   cp.Path,
			SubPathExpr: volumeSubPathExpr(cp),
		})
		existing[cp.Path] = true
	}
}

// volumeSubPathExpr returns the subPathExpr for the VolumeMount.
// For guaranteed delivery: uses downward API env vars for pod-level isolation.
// For bestEffort: uses a simpler path.
func volumeSubPathExpr(cp logpilotv1alpha1.ContainerPolicy) string {
	if cp.Delivery == "bestEffort" {
		return fmt.Sprintf("%s/%s", cp.Name, cp.LogType)
	}
	return fmt.Sprintf("LogPilotPolicy/$(NAMESPACE)/$(POD_NAME)/$(POD_UID)/%s/%s", cp.Name, cp.LogType)
}

// ensureLogVolume adds the shared log volume to the pod if not already present.
// Uses hostPath for guaranteed delivery, emptyDir for bestEffort.
func ensureLogVolume(pod *corev1.Pod, policies []logpilotv1alpha1.ContainerPolicy) {
	for _, v := range pod.Spec.Volumes {
		if v.Name == logVolumeName {
			return
		}
	}
	// If any policy requires guaranteed delivery, use hostPath.
	for _, cp := range policies {
		if cp.Delivery != "bestEffort" {
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
				Name: logVolumeName,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{Path: logHostPath},
				},
			})
			return
		}
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: logVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	})
}
