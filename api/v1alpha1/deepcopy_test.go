package v1alpha1

import (
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func assertDeepCopyEqual[T any](t *testing.T, orig *T, copy *T) {
	t.Helper()
	if copy == nil {
		t.Fatal("DeepCopy returned nil")
	}
	if copy == orig {
		t.Fatal("DeepCopy returned the original pointer")
	}
	if !reflect.DeepEqual(*copy, *orig) {
		t.Fatalf("DeepCopy mismatch: got %#v want %#v", *copy, *orig)
	}
}

func assertDeepCopyIntoEqual[T any](t *testing.T, orig *T, into *T) {
	t.Helper()
	if !reflect.DeepEqual(*into, *orig) {
		t.Fatalf("DeepCopyInto mismatch: got %#v want %#v", *into, *orig)
	}
}

func jsonValue(raw string) apiextensionsv1.JSON {
	return apiextensionsv1.JSON{Raw: []byte(raw)}
}

func sampleResourceRequirements() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
}

func sampleCondition(conditionType string) metav1.Condition {
	return metav1.Condition{
		Type:               conditionType,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: 7,
		LastTransitionTime: metav1.NewTime(time.Unix(1700000000, 0)),
		Reason:             "Configured",
		Message:            "resource is configured",
	}
}

func sampleObjectMeta(name, namespace string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:              name,
		Namespace:         namespace,
		UID:               types.UID(name + "-uid"),
		ResourceVersion:   "42",
		Generation:        3,
		CreationTimestamp: metav1.NewTime(time.Unix(1700000100, 0)),
		Labels: map[string]string{
			"app":  "logpilot",
			"name": name,
		},
		Annotations: map[string]string{
			"example.com/annotation": "value",
		},
		Finalizers: []string{"example.com/finalizer"},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "v1",
			Kind:       "Pod",
			Name:       "owner-pod",
			UID:        types.UID("owner-uid"),
		}},
	}
}

func sampleListMeta() metav1.ListMeta {
	remaining := int64(1)
	return metav1.ListMeta{
		ResourceVersion:    "99",
		Continue:           "next-token",
		RemainingItemCount: &remaining,
	}
}

func sampleAgentSelfLogSpec() AgentSelfLogSpec {
	return AgentSelfLogSpec{
		Dir:          "/var/log/log-pilot-agent",
		ReserveCount: 5,
	}
}

func sampleAgentSpec() AgentSpec {
	return AgentSpec{
		ConfigDir: "/var/lib/log-pilot-agent/conf",
		MetaDir:   "/var/lib/log-pilot-agent/meta",
		LogDir:    "/var/log/log-pilot",
		SelfLog:   sampleAgentSelfLogSpec(),
		Resources: sampleResourceRequirements(),
	}
}

func sampleAPISpec() APISpec {
	return APISpec{
		Replicas:  3,
		Resources: sampleResourceRequirements(),
	}
}

func sampleLogPilotSpec() LogPilotSpec {
	return LogPilotSpec{
		Agent: sampleAgentSpec(),
		API:   sampleAPISpec(),
	}
}

func sampleLogPilotStatus() LogPilotStatus {
	return LogPilotStatus{Conditions: []metav1.Condition{sampleCondition("Ready")}}
}

func sampleSelector() *metav1.LabelSelector {
	return &metav1.LabelSelector{
		MatchLabels: map[string]string{"app": "demo"},
		MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key:      "tier",
			Operator: metav1.LabelSelectorOpIn,
			Values:   []string{"backend", "worker"},
		}},
	}
}

func sampleInputSpec() InputSpec {
	return InputSpec{
		Type:     "file",
		BatchLen: 11,
		Config: map[string]apiextensionsv1.JSON{
			"path":     jsonValue(`"/var/log/app.log"`),
			"patterns": jsonValue(`["*.log","*.txt"]`),
		},
	}
}

func sampleTransformSpec(kind string) TransformSpec {
	return TransformSpec{
		Type:     kind,
		BatchLen: 13,
		Config: map[string]apiextensionsv1.JSON{
			"enabled": jsonValue(`true`),
			"rule":    jsonValue(`"keep"`),
		},
	}
}

func sampleOutputSpec() OutputSpec {
	return OutputSpec{
		Type:          "http",
		BatchLen:      17,
		BatchSize:     19,
		BatchInterval: 23,
		Config: map[string]apiextensionsv1.JSON{
			"timeout": jsonValue(`30`),
			"url":     jsonValue(`"https://example.com/ingest"`),
		},
	}
}

func sampleCleanSpec() CleanSpec {
	return CleanSpec{
		Strategy:          "retain",
		RetainDays:        7,
		Interval:          9,
		ReserveFileNumber: 11,
		ReserveFileSize:   13,
	}
}

func sampleContainerPolicy() ContainerPolicy {
	return ContainerPolicy{
		Name:          "app",
		LogType:       "stdout",
		Path:          "/var/log/app",
		Collector:     "sidecar",
		Delivery:      "bestEffort",
		BatchLen:      29,
		BatchSize:     31,
		BatchInterval: 37,
		Input:         sampleInputSpec(),
		Transforms: []TransformSpec{
			sampleTransformSpec("json"),
			sampleTransformSpec("drop"),
		},
		Output: sampleOutputSpec(),
		Clean:  sampleCleanSpec(),
	}
}

func sampleLogPilotPolicySpec() LogPilotPolicySpec {
	input := sampleInputSpec()
	output := sampleOutputSpec()
	return LogPilotPolicySpec{
		Selector: sampleSelector(),
		Containers: []ContainerPolicy{
			sampleContainerPolicy(),
		},
		Input: &input,
		Transforms: []TransformSpec{
			sampleTransformSpec("label"),
			sampleTransformSpec("drop"),
		},
		Output: &output,
	}
}

func sampleLogPilotPolicyStatus() LogPilotPolicyStatus {
	return LogPilotPolicyStatus{Conditions: []metav1.Condition{sampleCondition("Applied")}}
}

func sampleClusterLogPilotPolicySpec() ClusterLogPilotPolicySpec {
	return ClusterLogPilotPolicySpec{
		Input: sampleInputSpec(),
		Transforms: []TransformSpec{
			sampleTransformSpec("json"),
			sampleTransformSpec("label"),
		},
		Output: sampleOutputSpec(),
	}
}

func sampleClusterLogPilotPolicyStatus() ClusterLogPilotPolicyStatus {
	return ClusterLogPilotPolicyStatus{Conditions: []metav1.Condition{sampleCondition("Accepted")}}
}

func sampleLogPilot() *LogPilot {
	return &LogPilot{
		TypeMeta:   metav1.TypeMeta{APIVersion: GroupVersion.String(), Kind: "LogPilot"},
		ObjectMeta: sampleObjectMeta("logpilot", "logpilot-system"),
		Spec:       sampleLogPilotSpec(),
		Status:     sampleLogPilotStatus(),
	}
}

func sampleLogPilotList() *LogPilotList {
	return &LogPilotList{
		TypeMeta: metav1.TypeMeta{APIVersion: GroupVersion.String(), Kind: "LogPilotList"},
		ListMeta: sampleListMeta(),
		Items:    []LogPilot{*sampleLogPilot()},
	}
}

func sampleLogPilotPolicy() *LogPilotPolicy {
	return &LogPilotPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: GroupVersion.String(), Kind: "LogPilotPolicy"},
		ObjectMeta: sampleObjectMeta("policy", "default"),
		Spec:       sampleLogPilotPolicySpec(),
		Status:     sampleLogPilotPolicyStatus(),
	}
}

func sampleLogPilotPolicyList() *LogPilotPolicyList {
	return &LogPilotPolicyList{
		TypeMeta: metav1.TypeMeta{APIVersion: GroupVersion.String(), Kind: "LogPilotPolicyList"},
		ListMeta: sampleListMeta(),
		Items:    []LogPilotPolicy{*sampleLogPilotPolicy()},
	}
}

func sampleClusterLogPilotPolicy() *ClusterLogPilotPolicy {
	return &ClusterLogPilotPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: GroupVersion.String(), Kind: "ClusterLogPilotPolicy"},
		ObjectMeta: sampleObjectMeta("cluster-policy", ""),
		Spec:       sampleClusterLogPilotPolicySpec(),
		Status:     sampleClusterLogPilotPolicyStatus(),
	}
}

func sampleClusterLogPilotPolicyList() *ClusterLogPilotPolicyList {
	return &ClusterLogPilotPolicyList{
		TypeMeta: metav1.TypeMeta{APIVersion: GroupVersion.String(), Kind: "ClusterLogPilotPolicyList"},
		ListMeta: sampleListMeta(),
		Items:    []ClusterLogPilotPolicy{*sampleClusterLogPilotPolicy()},
	}
}

func TestAPISpecDeepCopy(t *testing.T) {
	orig := &APISpec{Replicas: 2, Resources: sampleResourceRequirements()}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into APISpec
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *APISpec
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}

func TestAgentSelfLogSpecDeepCopy(t *testing.T) {
	orig := &AgentSelfLogSpec{Dir: "/var/log/self", ReserveCount: 4}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into AgentSelfLogSpec
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *AgentSelfLogSpec
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}

func TestAgentSpecDeepCopy(t *testing.T) {
	orig := &AgentSpec{
		ConfigDir: "/conf",
		MetaDir:   "/meta",
		LogDir:    "/log",
		SelfLog:   sampleAgentSelfLogSpec(),
		Resources: sampleResourceRequirements(),
	}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into AgentSpec
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *AgentSpec
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}

func TestCleanSpecDeepCopy(t *testing.T) {
	orig := &CleanSpec{Strategy: "retain", RetainDays: 3, Interval: 5, ReserveFileNumber: 7, ReserveFileSize: 9}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into CleanSpec
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *CleanSpec
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}

func TestClusterLogPilotPolicyDeepCopy(t *testing.T) {
	orig := sampleClusterLogPilotPolicy()
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	obj := orig.DeepCopyObject()
	if obj == nil {
		t.Fatal("DeepCopyObject returned nil")
	}
	objTyped, ok := obj.(*ClusterLogPilotPolicy)
	if !ok {
		t.Fatalf("DeepCopyObject returned %T", obj)
	}
	assertDeepCopyEqual(t, orig, objTyped)

	var into ClusterLogPilotPolicy
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *ClusterLogPilotPolicy
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
	if nilOrig.DeepCopyObject() != nil {
		t.Fatal("nil DeepCopyObject should return nil")
	}
}

func TestClusterLogPilotPolicyListDeepCopy(t *testing.T) {
	orig := sampleClusterLogPilotPolicyList()
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	obj := orig.DeepCopyObject()
	if obj == nil {
		t.Fatal("DeepCopyObject returned nil")
	}
	objTyped, ok := obj.(*ClusterLogPilotPolicyList)
	if !ok {
		t.Fatalf("DeepCopyObject returned %T", obj)
	}
	assertDeepCopyEqual(t, orig, objTyped)

	var into ClusterLogPilotPolicyList
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *ClusterLogPilotPolicyList
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
	if nilOrig.DeepCopyObject() != nil {
		t.Fatal("nil DeepCopyObject should return nil")
	}
}

func TestClusterLogPilotPolicySpecDeepCopy(t *testing.T) {
	orig := &ClusterLogPilotPolicySpec{
		Input:      sampleInputSpec(),
		Transforms: []TransformSpec{sampleTransformSpec("json"), sampleTransformSpec("label")},
		Output:     sampleOutputSpec(),
	}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into ClusterLogPilotPolicySpec
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *ClusterLogPilotPolicySpec
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}

func TestClusterLogPilotPolicyStatusDeepCopy(t *testing.T) {
	orig := &ClusterLogPilotPolicyStatus{Conditions: []metav1.Condition{sampleCondition("Accepted")}}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into ClusterLogPilotPolicyStatus
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *ClusterLogPilotPolicyStatus
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}

func TestContainerPolicyDeepCopy(t *testing.T) {
	orig := &ContainerPolicy{
		Name:          "web",
		LogType:       "stderr",
		Path:          "/var/log/web",
		Collector:     "host",
		Delivery:      "guaranteed",
		BatchLen:      10,
		BatchSize:     20,
		BatchInterval: 30,
		Input:         sampleInputSpec(),
		Transforms:    []TransformSpec{sampleTransformSpec("json"), sampleTransformSpec("drop")},
		Output:        sampleOutputSpec(),
		Clean:         sampleCleanSpec(),
	}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into ContainerPolicy
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *ContainerPolicy
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}

func TestInputSpecDeepCopy(t *testing.T) {
	orig := &InputSpec{Type: "dir", BatchLen: 6, Config: map[string]apiextensionsv1.JSON{"path": jsonValue(`"/var/log"`), "recursive": jsonValue(`true`)}}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into InputSpec
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *InputSpec
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}

func TestLogPilotDeepCopy(t *testing.T) {
	orig := sampleLogPilot()
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	obj := orig.DeepCopyObject()
	if obj == nil {
		t.Fatal("DeepCopyObject returned nil")
	}
	objTyped, ok := obj.(*LogPilot)
	if !ok {
		t.Fatalf("DeepCopyObject returned %T", obj)
	}
	assertDeepCopyEqual(t, orig, objTyped)

	var into LogPilot
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *LogPilot
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
	if nilOrig.DeepCopyObject() != nil {
		t.Fatal("nil DeepCopyObject should return nil")
	}
}

func TestLogPilotListDeepCopy(t *testing.T) {
	orig := sampleLogPilotList()
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	obj := orig.DeepCopyObject()
	if obj == nil {
		t.Fatal("DeepCopyObject returned nil")
	}
	objTyped, ok := obj.(*LogPilotList)
	if !ok {
		t.Fatalf("DeepCopyObject returned %T", obj)
	}
	assertDeepCopyEqual(t, orig, objTyped)

	var into LogPilotList
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *LogPilotList
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
	if nilOrig.DeepCopyObject() != nil {
		t.Fatal("nil DeepCopyObject should return nil")
	}
}

func TestLogPilotPolicyDeepCopy(t *testing.T) {
	orig := sampleLogPilotPolicy()
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	obj := orig.DeepCopyObject()
	if obj == nil {
		t.Fatal("DeepCopyObject returned nil")
	}
	objTyped, ok := obj.(*LogPilotPolicy)
	if !ok {
		t.Fatalf("DeepCopyObject returned %T", obj)
	}
	assertDeepCopyEqual(t, orig, objTyped)

	var into LogPilotPolicy
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *LogPilotPolicy
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
	if nilOrig.DeepCopyObject() != nil {
		t.Fatal("nil DeepCopyObject should return nil")
	}
}

func TestLogPilotPolicyListDeepCopy(t *testing.T) {
	orig := sampleLogPilotPolicyList()
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	obj := orig.DeepCopyObject()
	if obj == nil {
		t.Fatal("DeepCopyObject returned nil")
	}
	objTyped, ok := obj.(*LogPilotPolicyList)
	if !ok {
		t.Fatalf("DeepCopyObject returned %T", obj)
	}
	assertDeepCopyEqual(t, orig, objTyped)

	var into LogPilotPolicyList
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *LogPilotPolicyList
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
	if nilOrig.DeepCopyObject() != nil {
		t.Fatal("nil DeepCopyObject should return nil")
	}
}

func TestLogPilotPolicySpecDeepCopy(t *testing.T) {
	orig := &LogPilotPolicySpec{
		Selector:   sampleSelector(),
		Containers: []ContainerPolicy{sampleContainerPolicy()},
		Input: func() *InputSpec {
			input := sampleInputSpec()
			return &input
		}(),
		Transforms: []TransformSpec{sampleTransformSpec("label"), sampleTransformSpec("drop")},
		Output: func() *OutputSpec {
			output := sampleOutputSpec()
			return &output
		}(),
	}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into LogPilotPolicySpec
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *LogPilotPolicySpec
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}

func TestLogPilotPolicyStatusDeepCopy(t *testing.T) {
	orig := &LogPilotPolicyStatus{Conditions: []metav1.Condition{sampleCondition("Ready")}}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into LogPilotPolicyStatus
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *LogPilotPolicyStatus
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}

func TestLogPilotSpecDeepCopy(t *testing.T) {
	orig := &LogPilotSpec{Agent: sampleAgentSpec(), API: sampleAPISpec()}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into LogPilotSpec
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *LogPilotSpec
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}

func TestLogPilotStatusDeepCopy(t *testing.T) {
	orig := &LogPilotStatus{Conditions: []metav1.Condition{sampleCondition("Ready")}}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into LogPilotStatus
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *LogPilotStatus
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}

func TestOutputSpecDeepCopy(t *testing.T) {
	orig := &OutputSpec{Type: "file", BatchLen: 5, BatchSize: 7, BatchInterval: 9, Config: map[string]apiextensionsv1.JSON{"path": jsonValue(`"/var/log/output.log"`), "compress": jsonValue(`false`)}}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into OutputSpec
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *OutputSpec
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}

func TestTransformSpecDeepCopy(t *testing.T) {
	orig := &TransformSpec{Type: "json", BatchLen: 8, Config: map[string]apiextensionsv1.JSON{"keep": jsonValue(`"message"`), "flatten": jsonValue(`true`)}}
	copy := orig.DeepCopy()
	assertDeepCopyEqual(t, orig, copy)

	var into TransformSpec
	orig.DeepCopyInto(&into)
	assertDeepCopyIntoEqual(t, orig, &into)

	var nilOrig *TransformSpec
	if nilOrig.DeepCopy() != nil {
		t.Fatal("nil DeepCopy should return nil")
	}
}
