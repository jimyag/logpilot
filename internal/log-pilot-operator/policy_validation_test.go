//go:build !integration

package operator

import (
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

func jsonValue(raw string) apiextensionsv1.JSON {
	return apiextensionsv1.JSON{Raw: []byte(raw)}
}

func fileOutputSpec() logpilotv1alpha1.OutputSpec {
	return logpilotv1alpha1.OutputSpec{
		Type: "file",
		Config: map[string]apiextensionsv1.JSON{
			"path": jsonValue(`"/var/log/output.log"`),
		},
	}
}

func httpOutputSpec() logpilotv1alpha1.OutputSpec {
	return logpilotv1alpha1.OutputSpec{
		Type: "http",
		Config: map[string]apiextensionsv1.JSON{
			"url": jsonValue(`"http://localhost:9999"`),
		},
	}
}

func validContainerPolicy() logpilotv1alpha1.ContainerPolicy {
	return logpilotv1alpha1.ContainerPolicy{
		Name:    "app",
		LogType: "applog",
		Path:    "/app/logs",
		Output:  fileOutputSpec(),
	}
}

func TestValidateLogPilotPolicyValid_ContainersMode(t *testing.T) {
	policy := &logpilotv1alpha1.LogPilotPolicy{
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"app": "demo"}},
			Containers: []logpilotv1alpha1.ContainerPolicy{validContainerPolicy()},
		},
	}

	ok, message := validateLogPilotPolicy(policy)
	if !ok {
		t.Fatalf("expected policy to be valid, got message %q", message)
	}
}

func TestValidateLogPilotPolicyMissingBoth(t *testing.T) {
	ok, _ := validateLogPilotPolicy(&logpilotv1alpha1.LogPilotPolicy{})
	if ok {
		t.Fatal("expected policy without selector/input to be invalid")
	}
}

func TestValidateLogPilotPolicyK8sEventNotAllowed(t *testing.T) {
	policy := &logpilotv1alpha1.LogPilotPolicy{
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Input:  &logpilotv1alpha1.InputSpec{Type: "k8sEvent"},
			Output: &[]logpilotv1alpha1.OutputSpec{fileOutputSpec()}[0],
		},
	}

	ok, _ := validateLogPilotPolicy(policy)
	if ok {
		t.Fatal("expected k8sEvent standalone input to be rejected")
	}
}

func TestValidateLogPilotPolicyK8sObjectStateNotAllowed(t *testing.T) {
	policy := &logpilotv1alpha1.LogPilotPolicy{
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Input:  &logpilotv1alpha1.InputSpec{Type: "k8sObjectState"},
			Output: &[]logpilotv1alpha1.OutputSpec{fileOutputSpec()}[0],
		},
	}

	ok, _ := validateLogPilotPolicy(policy)
	if ok {
		t.Fatal("expected k8sObjectState standalone input to be rejected")
	}
}

func TestValidateLogPilotPolicyContainerMissingOutput(t *testing.T) {
	policy := &logpilotv1alpha1.LogPilotPolicy{
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "demo"}},
			Containers: []logpilotv1alpha1.ContainerPolicy{{
				Name:    "app",
				LogType: "applog",
				Path:    "/app/logs",
			}},
		},
	}

	ok, _ := validateLogPilotPolicy(policy)
	if ok {
		t.Fatal("expected container without output to be invalid")
	}
}

func TestValidateLogPilotPolicyInvalidTransform(t *testing.T) {
	container := validContainerPolicy()
	container.Transforms = []logpilotv1alpha1.TransformSpec{{Type: "unknown"}}
	policy := &logpilotv1alpha1.LogPilotPolicy{
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"app": "demo"}},
			Containers: []logpilotv1alpha1.ContainerPolicy{container},
		},
	}

	ok, _ := validateLogPilotPolicy(policy)
	if ok {
		t.Fatal("expected invalid transform to be rejected")
	}
}

func TestValidateLogPilotPolicyValid_StandaloneMode(t *testing.T) {
	output := fileOutputSpec()
	policy := &logpilotv1alpha1.LogPilotPolicy{
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Input:  &logpilotv1alpha1.InputSpec{Type: "file"},
			Output: &output,
		},
	}

	ok, message := validateLogPilotPolicy(policy)
	if !ok {
		t.Fatalf("expected standalone policy to be valid, got message %q", message)
	}
}

func TestValidateClusterLogPilotPolicyK8sEvent(t *testing.T) {
	policy := &logpilotv1alpha1.ClusterLogPilotPolicy{
		Spec: logpilotv1alpha1.ClusterLogPilotPolicySpec{
			Input:  logpilotv1alpha1.InputSpec{Type: "k8sEvent"},
			Output: fileOutputSpec(),
		},
	}

	ok, message := validateClusterLogPilotPolicy(policy)
	if !ok {
		t.Fatalf("expected cluster policy to be valid, got message %q", message)
	}
}

func TestValidateClusterLogPilotPolicyK8sObjectState(t *testing.T) {
	policy := &logpilotv1alpha1.ClusterLogPilotPolicy{
		Spec: logpilotv1alpha1.ClusterLogPilotPolicySpec{
			Input:  logpilotv1alpha1.InputSpec{Type: "k8sObjectState"},
			Output: fileOutputSpec(),
		},
	}

	ok, message := validateClusterLogPilotPolicy(policy)
	if !ok {
		t.Fatalf("expected cluster policy to be valid, got message %q", message)
	}
}

func TestValidateClusterLogPilotPolicyInvalidType(t *testing.T) {
	policy := &logpilotv1alpha1.ClusterLogPilotPolicy{
		Spec: logpilotv1alpha1.ClusterLogPilotPolicySpec{
			Input:  logpilotv1alpha1.InputSpec{Type: "file"},
			Output: fileOutputSpec(),
		},
	}

	ok, _ := validateClusterLogPilotPolicy(policy)
	if ok {
		t.Fatal("expected unsupported cluster input type to be rejected")
	}
}

func TestValidateInputFile(t *testing.T) {
	if err := validateInput(logpilotv1alpha1.InputSpec{Type: "file"}); err != nil {
		t.Fatalf("expected file input to be valid: %v", err)
	}
}

func TestValidateInputDir(t *testing.T) {
	if err := validateInput(logpilotv1alpha1.InputSpec{Type: "dir"}); err != nil {
		t.Fatalf("expected dir input to be valid: %v", err)
	}
}

func TestValidateInputK8sEvent(t *testing.T) {
	if err := validateInput(logpilotv1alpha1.InputSpec{Type: "k8sEvent"}); err != nil {
		t.Fatalf("expected k8sEvent input to be valid: %v", err)
	}
}

func TestValidateInputK8sObjectState(t *testing.T) {
	if err := validateInput(logpilotv1alpha1.InputSpec{Type: "k8sObjectState"}); err != nil {
		t.Fatalf("expected k8sObjectState input to be valid: %v", err)
	}
}

func TestValidateInputUnknown(t *testing.T) {
	if err := validateInput(logpilotv1alpha1.InputSpec{Type: "unknown"}); err == nil {
		t.Fatal("expected unknown input type to fail")
	}
}

func TestValidateOutputHTTPNoURL(t *testing.T) {
	if err := validateOutput(logpilotv1alpha1.OutputSpec{Type: "http"}); err == nil {
		t.Fatal("expected http output without url to fail")
	}
}

func TestValidateOutputHTTPWithURL(t *testing.T) {
	if err := validateOutput(httpOutputSpec()); err != nil {
		t.Fatalf("expected http output to be valid: %v", err)
	}
}

func TestValidateOutputFileNoPath(t *testing.T) {
	if err := validateOutput(logpilotv1alpha1.OutputSpec{Type: "file"}); err == nil {
		t.Fatal("expected file output without path to fail")
	}
}

func TestValidateOutputFileWithPath(t *testing.T) {
	if err := validateOutput(fileOutputSpec()); err != nil {
		t.Fatalf("expected file output to be valid: %v", err)
	}
}

func TestValidateOutputUnknown(t *testing.T) {
	if err := validateOutput(logpilotv1alpha1.OutputSpec{Type: "grpc"}); err == nil {
		t.Fatal("expected unknown output type to fail")
	}
}

func TestValidateTransformsEmpty(t *testing.T) {
	if err := validateTransforms(nil); err != nil {
		t.Fatalf("expected empty transforms to be valid: %v", err)
	}
}

func TestValidateTransformsJSON(t *testing.T) {
	if err := validateTransforms([]logpilotv1alpha1.TransformSpec{{Type: "json"}}); err != nil {
		t.Fatalf("expected json transform to be valid: %v", err)
	}
}

func TestValidateTransformsLabelNoFields(t *testing.T) {
	if err := validateTransforms([]logpilotv1alpha1.TransformSpec{{Type: "label"}}); err == nil {
		t.Fatal("expected label transform without fields to fail")
	}
}

func TestValidateTransformsLabelWithFields(t *testing.T) {
	transforms := []logpilotv1alpha1.TransformSpec{{
		Type: "label",
		Config: map[string]apiextensionsv1.JSON{
			"fields": jsonValue(`["namespace"]`),
		},
	}}
	if err := validateTransforms(transforms); err != nil {
		t.Fatalf("expected label transform to be valid: %v", err)
	}
}

func TestValidateTransformsDropNoKey(t *testing.T) {
	transforms := []logpilotv1alpha1.TransformSpec{{
		Type: "drop",
		Config: map[string]apiextensionsv1.JSON{
			"value": jsonValue(`"debug"`),
		},
	}}
	if err := validateTransforms(transforms); err == nil || !strings.Contains(err.Error(), "key") {
		t.Fatalf("expected missing key error, got %v", err)
	}
}

func TestValidateTransformsDropNoValue(t *testing.T) {
	transforms := []logpilotv1alpha1.TransformSpec{{
		Type: "drop",
		Config: map[string]apiextensionsv1.JSON{
			"key": jsonValue(`"level"`),
		},
	}}
	if err := validateTransforms(transforms); err == nil || !strings.Contains(err.Error(), "value") {
		t.Fatalf("expected missing value error, got %v", err)
	}
}

func TestValidateTransformsDropComplete(t *testing.T) {
	transforms := []logpilotv1alpha1.TransformSpec{{
		Type: "drop",
		Config: map[string]apiextensionsv1.JSON{
			"key":   jsonValue(`"level"`),
			"value": jsonValue(`"debug"`),
		},
	}}
	if err := validateTransforms(transforms); err != nil {
		t.Fatalf("expected drop transform to be valid: %v", err)
	}
}

func TestValidateTransformsUnknown(t *testing.T) {
	if err := validateTransforms([]logpilotv1alpha1.TransformSpec{{Type: "unknown"}}); err == nil {
		t.Fatal("expected unknown transform to fail")
	}
}

func TestAcceptedConditionTrue(t *testing.T) {
	condition := acceptedCondition(true, 7, "policy accepted")
	if condition.Reason != "Accepted" {
		t.Fatalf("expected reason Accepted, got %q", condition.Reason)
	}
	if condition.Status != metav1.ConditionTrue {
		t.Fatalf("expected status true, got %q", condition.Status)
	}
}

func TestAcceptedConditionFalse(t *testing.T) {
	condition := acceptedCondition(false, 7, "invalid policy")
	if condition.Reason != "Invalid" {
		t.Fatalf("expected reason Invalid, got %q", condition.Reason)
	}
	if condition.Status != metav1.ConditionFalse {
		t.Fatalf("expected status false, got %q", condition.Status)
	}
}

func TestSetLogPilotPolicyCondition(t *testing.T) {
	policy := &logpilotv1alpha1.LogPilotPolicy{ObjectMeta: metav1.ObjectMeta{Generation: 3}}

	setLogPilotPolicyCondition(policy, true, "ok")

	condition := meta.FindStatusCondition(policy.Status.Conditions, conditionAccepted)
	if condition == nil {
		t.Fatal("expected accepted condition to be set")
	}
	if condition.Message != "ok" {
		t.Fatalf("expected message ok, got %q", condition.Message)
	}
}

func TestSetClusterLogPilotPolicyCondition(t *testing.T) {
	policy := &logpilotv1alpha1.ClusterLogPilotPolicy{ObjectMeta: metav1.ObjectMeta{Generation: 5}}

	setClusterLogPilotPolicyCondition(policy, false, "invalid")

	condition := meta.FindStatusCondition(policy.Status.Conditions, conditionAccepted)
	if condition == nil {
		t.Fatal("expected accepted condition to be set")
	}
	if condition.Message != "invalid" {
		t.Fatalf("expected message invalid, got %q", condition.Message)
	}
}

func TestValidateLogPilotPolicyInvalidStandaloneInput(t *testing.T) {
	output := fileOutputSpec()
	policy := &logpilotv1alpha1.LogPilotPolicy{
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Input:  &logpilotv1alpha1.InputSpec{Type: "bogus"},
			Output: &output,
		},
	}

	ok, message := validateLogPilotPolicy(policy)
	if ok || !strings.Contains(message, "unknown input type") {
		t.Fatalf("expected standalone input validation error, got ok=%v message=%q", ok, message)
	}
}

func TestValidateLogPilotPolicyInvalidStandaloneOutput(t *testing.T) {
	output := logpilotv1alpha1.OutputSpec{Type: "http"}
	policy := &logpilotv1alpha1.LogPilotPolicy{
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Input:  &logpilotv1alpha1.InputSpec{Type: "file"},
			Output: &output,
		},
	}

	ok, message := validateLogPilotPolicy(policy)
	if ok || !strings.Contains(message, "http output requires config.url") {
		t.Fatalf("expected standalone output validation error, got ok=%v message=%q", ok, message)
	}
}

func TestValidateLogPilotPolicyInvalidContainerOutputConfig(t *testing.T) {
	container := validContainerPolicy()
	container.Output = logpilotv1alpha1.OutputSpec{Type: "file"}
	policy := &logpilotv1alpha1.LogPilotPolicy{
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"app": "demo"}},
			Containers: []logpilotv1alpha1.ContainerPolicy{container},
		},
	}

	ok, message := validateLogPilotPolicy(policy)
	if ok || !strings.Contains(message, "containers[0]: file output requires config.path") {
		t.Fatalf("expected container output validation error, got ok=%v message=%q", ok, message)
	}
}

func TestValidateLogPilotPolicyInvalidTopLevelTransforms(t *testing.T) {
	output := fileOutputSpec()
	policy := &logpilotv1alpha1.LogPilotPolicy{
		Spec: logpilotv1alpha1.LogPilotPolicySpec{
			Input:      &logpilotv1alpha1.InputSpec{Type: "file"},
			Output:     &output,
			Transforms: []logpilotv1alpha1.TransformSpec{{Type: "label"}},
		},
	}

	ok, message := validateLogPilotPolicy(policy)
	if ok || !strings.Contains(message, "label requires config.fields") {
		t.Fatalf("expected transform validation error, got ok=%v message=%q", ok, message)
	}
}

func TestValidateClusterLogPilotPolicyInvalidOutput(t *testing.T) {
	policy := &logpilotv1alpha1.ClusterLogPilotPolicy{
		Spec: logpilotv1alpha1.ClusterLogPilotPolicySpec{
			Input:  logpilotv1alpha1.InputSpec{Type: "k8sEvent"},
			Output: logpilotv1alpha1.OutputSpec{Type: "file"},
		},
	}

	ok, message := validateClusterLogPilotPolicy(policy)
	if ok || !strings.Contains(message, "file output requires config.path") {
		t.Fatalf("expected cluster output validation error, got ok=%v message=%q", ok, message)
	}
}

func TestValidateClusterLogPilotPolicyInvalidTransforms(t *testing.T) {
	policy := &logpilotv1alpha1.ClusterLogPilotPolicy{
		Spec: logpilotv1alpha1.ClusterLogPilotPolicySpec{
			Input:      logpilotv1alpha1.InputSpec{Type: "k8sEvent"},
			Output:     fileOutputSpec(),
			Transforms: []logpilotv1alpha1.TransformSpec{{Type: "drop", Config: map[string]apiextensionsv1.JSON{"key": jsonValue(`"level"`)}}},
		},
	}

	ok, message := validateClusterLogPilotPolicy(policy)
	if ok || !strings.Contains(message, "drop requires config.value") {
		t.Fatalf("expected cluster transform validation error, got ok=%v message=%q", ok, message)
	}
}
