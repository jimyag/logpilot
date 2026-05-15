//go:build e2e
// +build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jimyag/logpilot/test/utils"
)

var (
	// managerImage is the manager image to be built and loaded for testing.
	managerImage = "example.com/logpilot:v0.0.1"
	// collectorImage receives logpilot HTTP output during e2e tests.
	collectorImage = "example.com/logpilot-e2e-collector:v0.0.1"
	// shouldCleanupCertManager tracks whether CertManager was installed by this suite.
	shouldCleanupCertManager = false
)

// TestE2E runs the e2e test suite to validate the solution in an isolated environment.
// The default setup requires Kind and CertManager.
//
// To skip CertManager installation, set: CERT_MANAGER_INSTALL_SKIP=true
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting logpilot e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	By("building the manager image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager image")

	By("building the e2e collector image")
	ExpectWithOffset(1, buildCollectorImage()).To(Succeed(), "Failed to build the collector image")

	// TODO(user): If you want to change the e2e test vendor from Kind,
	// ensure the image is built and available, then remove the following block.
	By("loading the manager image on Kind")
	err = utils.LoadImageToKindClusterWithName(managerImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager image into Kind")

	By("loading the collector image on Kind")
	err = utils.LoadImageToKindClusterWithName(collectorImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the collector image into Kind")

	setupCertManager()
})

var _ = AfterSuite(func() {
	teardownCertManager()
})

// setupCertManager installs CertManager if needed for webhook tests.
// Skips installation if CERT_MANAGER_INSTALL_SKIP=true or if already present.
func setupCertManager() {
	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true" {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager installation (CERT_MANAGER_INSTALL_SKIP=true)\n")
		return
	}

	By("checking if CertManager is already installed")
	if utils.IsCertManagerCRDsInstalled() {
		_, _ = fmt.Fprintf(GinkgoWriter, "CertManager is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupCertManager = true

	By("installing CertManager")
	Expect(utils.InstallCertManager()).To(Succeed(), "Failed to install CertManager")
}

// teardownCertManager uninstalls CertManager if it was installed by setupCertManager.
// This ensures we only remove what we installed.
func teardownCertManager() {
	if !shouldCleanupCertManager {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping CertManager cleanup (not installed by this suite)\n")
		return
	}

	By("uninstalling CertManager")
	utils.UninstallCertManager()
}

func buildCollectorImage() error {
	projectDir, err := utils.GetProjectDir()
	if err != nil {
		return err
	}
	tmpDir, err := os.MkdirTemp("", "logpilot-e2e-collector-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	source := `package main

import (
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
)

var records int64

func main() {
	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var entries []map[string]interface{}
		if err := json.Unmarshal(body, &entries); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		atomic.AddInt64(&records, int64(len(entries)))
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int64{"records": atomic.LoadInt64(&records)})
	})
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
`
	if err := os.WriteFile(filepath.Join(tmpDir, "collector.go"), []byte(source), 0o644); err != nil {
		return err
	}
	binPath := filepath.Join(tmpDir, "collector")
	cmd := exec.Command("go", "build", "-trimpath", "-ldflags", "-s -w", "-o", binPath, filepath.Join(tmpDir, "collector.go"))
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("build collector binary: %s: %w", output, err)
	}

	dockerfile := `FROM scratch
COPY collector /collector
USER 65532:65532
ENTRYPOINT ["/collector"]
`
	cmd = exec.Command("docker", "build", "-t", collectorImage, "-f", "-", tmpDir)
	cmd.Dir = projectDir
	cmd.Stdin = strings.NewReader(dockerfile)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("build collector image: %s: %w", output, err)
	}
	return nil
}
