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

	r := buildRunner(cp, logPath, cfg)
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

	r := buildRunner(cp, logPath, Config{MetaDir: t.TempDir()})
	if r == nil {
		t.Fatal("expected non-nil runner")
	}
}

func TestBuildRunnerNoOutput(t *testing.T) {
	// Missing output config should return a minimal runner, not panic.
	cp := logpilotv1alpha1.ContainerPolicy{
		Name:    "app",
		LogType: "applog",
		Output:  logpilotv1alpha1.OutputSpec{Type: "unknown"},
	}
	r := buildRunner(cp, t.TempDir(), Config{MetaDir: t.TempDir()})
	if r == nil {
		t.Fatal("expected non-nil runner even with bad output config")
	}
}
