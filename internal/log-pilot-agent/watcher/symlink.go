package watcher

import (
	"fmt"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

// ensureLogPath creates the unified log path for a container policy.
// For guaranteed/hostPath: creates the directory (already mounted via VolumeMount).
// For bestEffort: creates a symlink to the kubelet emptyDir path.
// For stdout ("-"): creates a symlink to the K8s native stdout path.
func ensureLogPath(pod *corev1.Pod, cp logpilotv1alpha1.ContainerPolicy, logPath string) error {
	if err := os.MkdirAll(filepath.Dir(logPath), 0755); err != nil {
		return fmt.Errorf("create parent dirs for %s: %w", logPath, err)
	}

	switch {
	case cp.Path == "-":
		// stdout: symlink to K8s native stdout path.
		target := fmt.Sprintf("/var/log/pods/%s_%s_%s/%s",
			pod.Namespace, pod.Name, string(pod.UID), cp.Name)
		return forceSymlink(target, logPath)

	case cp.Delivery == "bestEffort":
		// emptyDir: symlink to kubelet emptyDir path.
		target := fmt.Sprintf(
			"/var/lib/kubelet/pods/%s/volumes/kubernetes.io~empty-dir/pods-log/%s/%s",
			string(pod.UID), cp.Name, cp.LogType)
		return forceSymlink(target, logPath)

	default:
		// guaranteed/hostPath: VolumeMount already directs writes here.
		return os.MkdirAll(logPath, 0755)
	}
}

// removeLogPath removes the log path (symlink or directory) for a container policy.
func removeLogPath(logPath string) error {
	return os.RemoveAll(logPath)
}

func forceSymlink(target, link string) error {
	_ = os.Remove(link)
	return os.Symlink(target, link)
}
