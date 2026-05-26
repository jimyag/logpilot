package watcher

import (
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func TestForceSymlinkCreates(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(dir, "link")
	target := filepath.Join(dir, "target-a")

	if err := forceSymlink(target, link); err != nil {
		t.Fatal(err)
	}

	info, err := os.Lstat(link)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("expected %s to be a symlink", link)
	}
	got, err := os.Readlink(link)
	if err != nil {
		t.Fatal(err)
	}
	if got != target {
		t.Fatalf("expected symlink target %q, got %q", target, got)
	}
}

func TestForceSymlinkOverwrites(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(dir, "link")
	target1 := filepath.Join(dir, "target-a")
	target2 := filepath.Join(dir, "target-b")

	if err := forceSymlink(target1, link); err != nil {
		t.Fatal(err)
	}
	if err := forceSymlink(target2, link); err != nil {
		t.Fatal(err)
	}

	got, err := os.Readlink(link)
	if err != nil {
		t.Fatal(err)
	}
	if got != target2 {
		t.Fatalf("expected symlink target %q, got %q", target2, got)
	}
}

func TestRemoveLogPathExisting(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "logs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := removeLogPath(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("expected %s to be removed, got err=%v", dir, err)
	}
}

func TestRemoveLogPathNotExist(t *testing.T) {
	if err := removeLogPath("/nonexistent/path"); err != nil {
		t.Fatalf("expected no error removing missing path, got %v", err)
	}
}

func TestEnsureLogPathStdout(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "logs", "app", "applog")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid"}}
	cp := logpilotv1alpha1.ContainerPolicy{Name: "app", LogType: "applog", Path: "-"}

	if err := ensureLogPath(pod, cp, logPath); err != nil {
		t.Fatal(err)
	}

	info, err := os.Lstat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("expected %s to be a symlink", logPath)
	}
	got, err := os.Readlink(logPath)
	if err != nil {
		t.Fatal(err)
	}
	expected := "/var/log/pods/default_test_uid/app"
	if got != expected {
		t.Fatalf("expected symlink target %q, got %q", expected, got)
	}
}

func TestEnsureLogPathBestEffort(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "logs", "app", "applog")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid"}}
	cp := logpilotv1alpha1.ContainerPolicy{Name: "app", LogType: "applog", Delivery: "bestEffort"}

	if err := ensureLogPath(pod, cp, logPath); err != nil {
		t.Fatal(err)
	}

	info, err := os.Lstat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("expected %s to be a symlink", logPath)
	}
	got, err := os.Readlink(logPath)
	if err != nil {
		t.Fatal(err)
	}
	expected := "/var/lib/kubelet/pods/uid/volumes/kubernetes.io~empty-dir/log-pilot-logs/app/applog"
	if got != expected {
		t.Fatalf("expected symlink target %q, got %q", expected, got)
	}
}

func TestEnsureLogPathGuaranteed(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "logs", "app", "applog")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "uid"}}
	cp := logpilotv1alpha1.ContainerPolicy{Name: "app", LogType: "applog", Delivery: "guaranteed"}

	if err := ensureLogPath(pod, cp, logPath); err != nil {
		t.Fatal(err)
	}

	info, err := os.Lstat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("expected %s to be a directory, not a symlink", logPath)
	}
	if !info.IsDir() {
		t.Fatalf("expected %s to be a directory", logPath)
	}
}
