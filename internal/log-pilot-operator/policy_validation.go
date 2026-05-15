package operator

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	logpilotv1alpha1 "github.com/jimyag/logpilot/api/v1alpha1"
)

const conditionAccepted = "Accepted"

func validateLogPilotPolicy(policy *logpilotv1alpha1.LogPilotPolicy) (bool, string) {
	hasContainers := policy.Spec.Selector != nil && len(policy.Spec.Containers) > 0
	hasStandalone := policy.Spec.Input != nil && policy.Spec.Output != nil
	if !hasContainers && !hasStandalone {
		return false, "spec must define selector+containers or input+output"
	}
	if hasStandalone {
		if policy.Spec.Input.Type == "k8sEvent" || policy.Spec.Input.Type == "k8sObjectState" {
			return false, "Kubernetes cluster inputs must use ClusterLogPilotPolicy"
		}
		if err := validateInput(*policy.Spec.Input); err != nil {
			return false, err.Error()
		}
		if err := validateOutput(*policy.Spec.Output); err != nil {
			return false, err.Error()
		}
	}
	for i, cp := range policy.Spec.Containers {
		if cp.Output.Type == "" {
			return false, fmt.Sprintf("containers[%d].output.type is required", i)
		}
		if err := validateOutput(cp.Output); err != nil {
			return false, fmt.Sprintf("containers[%d]: %s", i, err)
		}
		if err := validateTransforms(cp.Transforms); err != nil {
			return false, fmt.Sprintf("containers[%d]: %s", i, err)
		}
	}
	if err := validateTransforms(policy.Spec.Transforms); err != nil {
		return false, err.Error()
	}
	return true, "policy accepted"
}

func validateClusterLogPilotPolicy(policy *logpilotv1alpha1.ClusterLogPilotPolicy) (bool, string) {
	if policy.Spec.Input.Type != "k8sEvent" && policy.Spec.Input.Type != "k8sObjectState" {
		return false, "ClusterLogPilotPolicy currently supports k8sEvent and k8sObjectState inputs"
	}
	if err := validateOutput(policy.Spec.Output); err != nil {
		return false, err.Error()
	}
	if err := validateTransforms(policy.Spec.Transforms); err != nil {
		return false, err.Error()
	}
	return true, "policy accepted"
}

func validateInput(input logpilotv1alpha1.InputSpec) error {
	switch input.Type {
	case "file", "dir", "k8sEvent", "k8sObjectState":
		return nil
	default:
		return fmt.Errorf("unknown input type %q", input.Type)
	}
}

func validateOutput(output logpilotv1alpha1.OutputSpec) error {
	switch output.Type {
	case "http":
		if _, ok := output.Config["url"]; !ok {
			return fmt.Errorf("http output requires config.url")
		}
	case "file":
		if _, ok := output.Config["path"]; !ok {
			return fmt.Errorf("file output requires config.path")
		}
	default:
		return fmt.Errorf("unknown output type %q", output.Type)
	}
	return nil
}

func validateTransforms(transforms []logpilotv1alpha1.TransformSpec) error {
	for i, transform := range transforms {
		switch transform.Type {
		case "json":
		case "label":
			if _, ok := transform.Config["fields"]; !ok {
				return fmt.Errorf("transforms[%d] label requires config.fields", i)
			}
		case "drop":
			missing := make([]string, 0, 2)
			if _, ok := transform.Config["key"]; !ok {
				missing = append(missing, "key")
			}
			if _, ok := transform.Config["value"]; !ok {
				missing = append(missing, "value")
			}
			if len(missing) > 0 {
				return fmt.Errorf("transforms[%d] drop requires config.%s", i, strings.Join(missing, " and config."))
			}
		default:
			return fmt.Errorf("unknown transform type %q", transform.Type)
		}
	}
	return nil
}

func acceptedCondition(ok bool, generation int64, message string) metav1.Condition {
	status := metav1.ConditionTrue
	reason := "Accepted"
	if !ok {
		status = metav1.ConditionFalse
		reason = "Invalid"
	}
	return metav1.Condition{
		Type:               conditionAccepted,
		Status:             status,
		ObservedGeneration: generation,
		Reason:             reason,
		Message:            message,
	}
}

func setLogPilotPolicyCondition(policy *logpilotv1alpha1.LogPilotPolicy, ok bool, message string) {
	meta.SetStatusCondition(&policy.Status.Conditions, acceptedCondition(ok, policy.Generation, message))
}

func setClusterLogPilotPolicyCondition(policy *logpilotv1alpha1.ClusterLogPilotPolicy, ok bool, message string) {
	meta.SetStatusCondition(&policy.Status.Conditions, acceptedCondition(ok, policy.Generation, message))
}
