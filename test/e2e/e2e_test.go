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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/jimyag/logpilot/test/utils"
)

// namespace where the project is deployed in
const namespace = "logpilot-system"

// serviceAccountName created for the project
const serviceAccountName = "logpilot-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "logpilot-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "logpilot-metrics-binding"

const stateTestNamespace = "logpilot-e2e-state"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("removing state test namespace")
		cmd = exec.Command("kubectl", "delete", "ns", stateTestNamespace, "--ignore-not-found")
		_, _ = utils.Run(cmd)

		By("removing the LogPilot runtime")
		cmd = exec.Command("kubectl", "delete", "logpilot", "logpilot", "-n", namespace,
			"--ignore-not-found", "--timeout=2m")
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				// Get the name of the controller-manager pod
				cmd := exec.Command(
					"kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				// Validate the pod's status
				cmd = exec.Command(
					"kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command(
				"kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=logpilot-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("ensuring the controller pod is ready")
			verifyControllerPodReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Controller pod not ready")
			}
			Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

			// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": [
								"for i in $(seq 1 30); do curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics && exit 0 || sleep 2; done; exit 1"
							],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		It("should collect Kubernetes object state through ClusterLogPilotPolicy", func() {
			By("allowing the LogPilot agent hostPath daemonset in the manager namespace")
			cmd := exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
				"pod-security.kubernetes.io/enforce=privileged")
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to relax pod security for LogPilot agent")

			By("creating the state test namespace")
			cmd = exec.Command("kubectl", "create", "ns", stateTestNamespace)
			_, _ = utils.Run(cmd)

			By("deploying the e2e HTTP collector")
			applyYAML(fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: collector
  namespace: %[1]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: collector
  template:
    metadata:
      labels:
        app: collector
    spec:
      containers:
      - name: collector
        image: %[2]s
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: collector
  namespace: %[1]s
spec:
  selector:
    app: collector
  ports:
  - port: 8080
    targetPort: 8080
`, stateTestNamespace, collectorImage))
			rolloutStatus("deployment/collector", stateTestNamespace, 2*time.Minute)

			By("creating the LogPilot runtime")
			applyYAML(fmt.Sprintf(`
apiVersion: logpilot.logpilot.jimyag.com/v1alpha1
kind: LogPilot
metadata:
  name: logpilot
  namespace: %[1]s
spec:
  api:
    replicas: 1
  agent: {}
			`, namespace))
			waitForResource("deployment/log-pilot-api", namespace, 2*time.Minute)
			waitForResource("daemonset/log-pilot-agent", namespace, 2*time.Minute)
			rolloutStatus("deployment/log-pilot-api", namespace, 3*time.Minute)
			rolloutStatus("daemonset/log-pilot-agent", namespace, 3*time.Minute)

			By("creating a k8sObjectState cluster policy")
			applyYAML(fmt.Sprintf(`
apiVersion: logpilot.logpilot.jimyag.com/v1alpha1
kind: ClusterLogPilotPolicy
metadata:
  name: e2e-object-state
spec:
  input:
    type: k8sObjectState
    batchLen: 20
    config:
      namespaces:
      - %[1]s
      resources:
      - pod
      - node
      - deployment
      - daemonset
      - job
  transforms:
  - type: label
    config:
      fields:
        source: k8sObjectState
  output:
    type: http
    config:
      url: http://collector.%[1]s.svc.cluster.local:8080/ingest
`, stateTestNamespace))
			cmd = exec.Command("kubectl", "wait", "--for=condition=Accepted",
				"clusterlogpilotpolicy/e2e-object-state", "--timeout=60s")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "ClusterLogPilotPolicy should be accepted")

			By("creating workload state changes")
			applyYAML(fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: state-verify
  namespace: %[1]s
spec:
  replicas: 2
  selector:
    matchLabels:
      app: state-verify
  template:
    metadata:
      labels:
        app: state-verify
    spec:
      containers:
      - name: app
        image: %[2]s
        imagePullPolicy: IfNotPresent
---
apiVersion: batch/v1
kind: Job
metadata:
  name: state-verify-job
  namespace: %[1]s
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: job
        image: busybox:1.36
        command: ["sh", "-c", "echo state-job"]
`, stateTestNamespace, collectorImage))
			rolloutStatus("deployment/state-verify", stateTestNamespace, 2*time.Minute)

			By("verifying state records reached the collector")
			Eventually(func(g Gomega) {
				records, err := collectorRecordCount()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(records).To(BeNumerically(">=", 8))
			}, 2*time.Minute, 2*time.Second).Should(Succeed())

			By("verifying only one agent owns the cluster state runner")
			Eventually(func(g Gomega) {
				count, sent, err := stateRunnerStatus()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(count).To(Equal(1))
				g.Expect(sent).To(BeNumerically(">=", 8))
			}, 2*time.Minute, 2*time.Second).Should(Succeed())
		})

		// TODO: Customize the e2e test suite with scenarios specific to your project.
		// Consider applying sample/CR(s) and check their status and/or verifying
		// the reconciliation by using the metrics, i.e.:
		// metricsOutput, err := getMetricsOutput()
		// Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
		// Expect(metricsOutput).To(ContainSubstring(
		//    fmt.Sprintf(`controller_runtime_reconcile_total{controller="%s",result="success"} 1`,
		//    strings.ToLower(<Kind>),
		// ))
	})
})

func applyYAML(manifest string) {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(strings.TrimSpace(manifest) + "\n")
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to apply manifest")
}

func rolloutStatus(resource, ns string, timeout time.Duration) {
	cmd := exec.Command("kubectl", "-n", ns, "rollout", "status", resource,
		"--timeout", fmt.Sprintf("%ds", int(timeout.Seconds())))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Rollout did not complete")
}

func waitForResource(resource, ns string, timeout time.Duration) {
	EventuallyWithOffset(1, func(g Gomega) {
		cmd := exec.Command("kubectl", "-n", ns, "get", resource)
		_, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred())
	}, timeout, time.Second).Should(Succeed())
}

func collectorRecordCount() (int, error) {
	pf := exec.Command("kubectl", "-n", stateTestNamespace, "port-forward", "svc/collector", "18084:8080")
	if err := pf.Start(); err != nil {
		return 0, err
	}
	defer func() {
		_ = pf.Process.Kill()
		_, _ = pf.Process.Wait()
	}()
	time.Sleep(time.Second)

	resp, err := http.Get("http://127.0.0.1:18084/stats")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var stats struct {
		Records int `json:"records"`
	}
	if err := json.Unmarshal(body, &stats); err != nil {
		return 0, err
	}
	return stats.Records, nil
}

func stateRunnerStatus() (int, int, error) {
	cmd := exec.Command("kubectl", "-n", namespace, "get", "pods",
		"-l", "app.kubernetes.io/name=log-pilot-agent",
		"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	output, err := utils.Run(cmd)
	if err != nil {
		return 0, 0, err
	}
	runnerCount := 0
	sentTotal := 0
	for idx, pod := range utils.GetNonEmptyLines(output) {
		localPort := 19094 + idx
		pf := exec.Command("kubectl", "-n", namespace, "port-forward", "pod/"+pod,
			fmt.Sprintf("%d:9090", localPort))
		if err := pf.Start(); err != nil {
			return 0, 0, err
		}
		time.Sleep(time.Second)
		resp, err := http.Get("http://127.0.0.1:" + strconv.Itoa(localPort) + "/status")
		_ = pf.Process.Kill()
		_, _ = pf.Process.Wait()
		if err != nil {
			return 0, 0, err
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return 0, 0, readErr
		}
		var status struct {
			Runners []struct {
				Name string `json:"name"`
				Sent int    `json:"sent"`
			} `json:"runners"`
		}
		if err := json.Unmarshal(body, &status); err != nil {
			return 0, 0, err
		}
		for _, runner := range status.Runners {
			if runner.Name == "ClusterLogPilotPolicy/e2e-object-state" {
				runnerCount++
				sentTotal += runner.Sent
			}
		}
	}
	return runnerCount, sentTotal, nil
}

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	// Temporary file to store the token request
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		// Execute kubectl command to create the token
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		// Parse the JSON output to extract the token
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
