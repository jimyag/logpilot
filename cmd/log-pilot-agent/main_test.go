package main

import (
	"os"
	"testing"
)

func TestEnvOrDefault(t *testing.T) {
	if err := os.Setenv("TEST_KEY", "myval"); err != nil {
		t.Fatalf("Setenv failed: %v", err)
	}
	defer os.Unsetenv("TEST_KEY")

	if got := envOrDefault("TEST_KEY", "default"); got != "myval" {
		t.Errorf("expected myval, got %q", got)
	}
	if got := envOrDefault("NONEXISTENT_KEY_XYZ", "fallback"); got != "fallback" {
		t.Errorf("expected fallback, got %q", got)
	}
}
