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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LogPilotSpec defines the desired state of LogPilot (cluster singleton).
type LogPilotSpec struct {
	Agent AgentSpec `json:"agent,omitempty"`
	API   APISpec   `json:"api,omitempty"`
}

type AgentSpec struct {
	// +kubebuilder:default="/var/lib/log-pilot-agent/conf"
	ConfigDir string `json:"configDir,omitempty"`
	// +kubebuilder:default="/var/lib/log-pilot-agent/meta"
	MetaDir string `json:"metaDir,omitempty"`
	// +kubebuilder:default="/var/log/log-pilot"
	LogDir  string           `json:"logDir,omitempty"`
	SelfLog AgentSelfLogSpec `json:"selfLog,omitempty"`
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

type AgentSelfLogSpec struct {
	// +kubebuilder:default="/var/log/log-pilot-agent"
	Dir string `json:"dir,omitempty"`
	// +kubebuilder:default=10
	ReserveCount int `json:"reserveCount,omitempty"`
}

type APISpec struct {
	// +kubebuilder:default=2
	Replicas int32 `json:"replicas,omitempty"`
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// LogPilotStatus defines the observed state of LogPilot.
type LogPilotStatus struct {
	// conditions represent the current state of the LogPilot resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// LogPilot is the Schema for the logpilots API
type LogPilot struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec LogPilotSpec `json:"spec"`

	// +optional
	Status LogPilotStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// LogPilotList contains a list of LogPilot
type LogPilotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []LogPilot `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LogPilot{}, &LogPilotList{})
}
