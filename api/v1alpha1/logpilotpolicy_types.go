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
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LogPilotPolicySpec defines the desired state of LogPilotPolicy.
// Policy names must be globally unique within the cluster.
// Either (Selector + Containers) or (Input + Output) must be set.
type LogPilotPolicySpec struct {
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`
	// +optional
	Containers []ContainerPolicy `json:"containers,omitempty"`
	// +optional
	Input *InputSpec `json:"input,omitempty"`
	// +optional
	Transforms []TransformSpec `json:"transforms,omitempty"`
	// +optional
	Output *OutputSpec `json:"output,omitempty"`
}

// ContainerPolicy defines the log collection pipeline for one container+logType pair.
type ContainerPolicy struct {
	// +kubebuilder:validation:Required
	Name string `json:"name"`
	// +kubebuilder:validation:Required
	LogType string `json:"logType"`
	// Path is the container-internal log directory. Use "-" for stdout/stderr.
	// +kubebuilder:validation:Required
	Path string `json:"path"`
	// +kubebuilder:validation:Enum=host;sidecar
	// +kubebuilder:default=host
	Collector string `json:"collector,omitempty"`
	// +kubebuilder:validation:Enum=guaranteed;bestEffort
	// +kubebuilder:default=guaranteed
	Delivery string `json:"delivery,omitempty"`
	// +kubebuilder:default=1000
	BatchLen int `json:"batchLen,omitempty"`
	// +kubebuilder:default=5242880
	BatchSize int `json:"batchSize,omitempty"`
	// +kubebuilder:default=300
	BatchInterval int             `json:"batchInterval,omitempty"`
	Input         InputSpec       `json:"input,omitempty"`
	Transforms    []TransformSpec `json:"transforms,omitempty"`
	// +kubebuilder:validation:Required
	Output OutputSpec `json:"output"`
	Clean  CleanSpec  `json:"clean,omitempty"`
}

// InputSpec defines a data source.
type InputSpec struct {
	// Type is the input type: file, dir, k8sEvent, mongo.
	// +kubebuilder:validation:Required
	Type string `json:"type"`
	// +kubebuilder:default=1000
	BatchLen int                             `json:"batchLen,omitempty"`
	// +optional
	Config map[string]apiextensionsv1.JSON `json:"config,omitempty"`
}

// TransformSpec defines a data transformation step.
type TransformSpec struct {
	// Type is the transform type: json, label, drop, regex, multiline.
	// +kubebuilder:validation:Required
	Type string `json:"type"`
	// +kubebuilder:default=1000
	BatchLen int                             `json:"batchLen,omitempty"`
	// +optional
	Config map[string]apiextensionsv1.JSON `json:"config,omitempty"`
}

// OutputSpec defines a data destination.
type OutputSpec struct {
	// Type is the output type: kafka, elasticsearch, http, file.
	// +kubebuilder:validation:Required
	Type string `json:"type"`
	// +kubebuilder:default=500
	BatchLen int `json:"batchLen,omitempty"`
	// +kubebuilder:default=5242880
	BatchSize int `json:"batchSize,omitempty"`
	// +kubebuilder:default=300
	BatchInterval int                             `json:"batchInterval,omitempty"`
	// +optional
	Config map[string]apiextensionsv1.JSON `json:"config,omitempty"`
}

// CleanSpec controls log file cleanup while a pod is running.
// After pod deletion, all files are always cleaned once lag == 0.
type CleanSpec struct {
	// +kubebuilder:validation:Enum=afterCollected;retain;never
	// +kubebuilder:default=afterCollected
	Strategy   string `json:"strategy,omitempty"`
	RetainDays int    `json:"retainDays,omitempty"`
	// +kubebuilder:default=10
	Interval int `json:"interval,omitempty"`
	// +kubebuilder:default=10
	ReserveFileNumber int `json:"reserveFileNumber,omitempty"`
	// +kubebuilder:default=10240
	ReserveFileSize int `json:"reserveFileSize,omitempty"`
}

// LogPilotPolicyStatus defines the observed state of LogPilotPolicy.
type LogPilotPolicyStatus struct {
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// LogPilotPolicy is the Schema for the logpilotpolicies API
type LogPilotPolicy struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec LogPilotPolicySpec `json:"spec"`

	// +optional
	Status LogPilotPolicyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// LogPilotPolicyList contains a list of LogPilotPolicy
type LogPilotPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []LogPilotPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LogPilotPolicy{}, &LogPilotPolicyList{})
}
