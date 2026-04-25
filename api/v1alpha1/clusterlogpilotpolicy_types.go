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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterLogPilotPolicySpec defines a cluster-scoped standalone pipeline (e.g. K8s events).
type ClusterLogPilotPolicySpec struct {
	// +kubebuilder:validation:Required
	Input InputSpec `json:"input"`
	// +optional
	Transforms []TransformSpec `json:"transforms,omitempty"`
	// +kubebuilder:validation:Required
	Output OutputSpec `json:"output"`
}

// ClusterLogPilotPolicyStatus defines the observed state of ClusterLogPilotPolicy.
type ClusterLogPilotPolicyStatus struct {
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// ClusterLogPilotPolicy is the Schema for the clusterlogpilotpolicies API
type ClusterLogPilotPolicy struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec ClusterLogPilotPolicySpec `json:"spec"`

	// +optional
	Status ClusterLogPilotPolicyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ClusterLogPilotPolicyList contains a list of ClusterLogPilotPolicy
type ClusterLogPilotPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ClusterLogPilotPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterLogPilotPolicy{}, &ClusterLogPilotPolicyList{})
}
