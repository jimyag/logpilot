package watcher

import (
	"os"
	"path/filepath"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func TestBuildRunnerFileOutput(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "out.json")
	logPath := t.TempDir()

	cp := logpilotv1alpha1.ContainerPolicy{
		Name:     "app",
		LogType:  "applog",
		Path:     "/app/logs",
		Delivery: "guaranteed",
		BatchLen: 10,
		Output: logpilotv1alpha1.OutputSpec{
			Type: "file",
			Config: map[string]apiextensionsv1.JSON{
				"path": {Raw: []byte(`"` + outPath + `"`)},
			},
		},
	}

	cfg := Config{
		LogDir:  t.TempDir(),
		MetaDir: t.TempDir(),
	}

	r, err := buildRunner(cp, logPath, "test-uid", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("expected non-nil runner")
	}
}

func TestBuildRunnerWithTransforms(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "out.json")
	logPath := t.TempDir()

	// Seed log file so FileInput can open it.
	logFile := filepath.Join(logPath, "app.log")
	if err := os.WriteFile(logFile, []byte(`{"msg":"hello"}`+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	cp := logpilotv1alpha1.ContainerPolicy{
		Name:    "app",
		LogType: "applog",
		Transforms: []logpilotv1alpha1.TransformSpec{
			{Type: "json"},
			{Type: "label", Config: map[string]apiextensionsv1.JSON{
				"fields": {Raw: []byte(`{"env":"test"}`)},
			}},
		},
		Output: logpilotv1alpha1.OutputSpec{
			Type: "file",
			Config: map[string]apiextensionsv1.JSON{
				"path": {Raw: []byte(`"` + outPath + `"`)},
			},
		},
	}

	r, err := buildRunner(cp, logPath, "test-uid", Config{MetaDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("expected non-nil runner")
	}
}

func TestBuildRunnerNoOutput(t *testing.T) {
	// Missing output config should be surfaced instead of starting a runner
	// that reads logs without sending them anywhere.
	cp := logpilotv1alpha1.ContainerPolicy{
		Name:    "app",
		LogType: "applog",
		Output:  logpilotv1alpha1.OutputSpec{Type: "unknown"},
	}
	r, err := buildRunner(cp, t.TempDir(), "test-uid", Config{MetaDir: t.TempDir()})
	if err == nil {
		t.Fatal("expected bad output config to fail")
	}
	if r != nil {
		t.Fatal("expected nil runner for bad output config")
	}
}
