// Copyright (c) 2018-2026 Splunk Inc. All rights reserved.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v4

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// QueuePausedAnnotation is the annotation that pauses the reconciliation (triggers
	// an immediate requeue)
	QueuePausedAnnotation = "queue.enterprise.splunk.com/paused"
)

// +kubebuilder:validation:XValidation:rule="(self.provider != 'sqs' && self.provider != 'sqs_cp') || has(self.sqs)",message="sqs must be provided when provider is sqs or sqs_cp"
// QueueSpec defines the desired state of Queue
type QueueSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=sqs;sqs_cp
	// Provider of queue resources
	Provider string `json:"provider"`

	// +kubebuilder:validation:Required
	// sqs specific inputs
	SQS SQSSpec `json:"sqs"`
}

type SQSSpec struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// Name of the queue
	Name string `json:"name"`

	// +optional
	// +kubebuilder:validation:Pattern=`^(?:us|ap|eu|me|af|sa|ca|cn|il)(?:-[a-z]+){1,3}-\d$`
	// Auth Region of the resources
	AuthRegion string `json:"authRegion"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// Name of the dead letter queue resource
	DLQ string `json:"dlq"`

	// +optional
	// +kubebuilder:validation:Pattern=`^https?://[^\s/$.?#].[^\s]*$`
	// Amazon SQS Service endpoint
	Endpoint string `json:"endpoint"`

	// +optional
	// List of remote storage volumes
	VolList []VolumeSpec `json:"volumes,omitempty"`

	// +optional
	// Maximum number of connections to the SQS service
	MaxConnections *int32 `json:"maxConnections,omitempty"`

	// +optional
	// Message group ID for FIFO queues
	MessageGroupID string `json:"messageGroupID,omitempty"`

	// +optional
	// +kubebuilder:validation:Enum=max_count;none
	// Retry policy for failed messages
	RetryPolicy string `json:"retryPolicy,omitempty"`

	// +optional
	// Maximum retries per part when retry_policy is max_count
	MaxRetriesPerPart *int32 `json:"maxRetriesPerPart,omitempty"`

	// +optional
	// Connection timeout in seconds
	TimeoutConnect *int32 `json:"timeoutConnect,omitempty"`

	// +optional
	// Read timeout in seconds
	TimeoutRead *int32 `json:"timeoutRead,omitempty"`

	// +optional
	// Write timeout in seconds
	TimeoutWrite *int32 `json:"timeoutWrite,omitempty"`

	// +optional
	// Receive message timeout in seconds
	TimeoutReceiveMessage *int32 `json:"timeoutReceiveMessage,omitempty"`

	// +optional
	// Visibility timeout in seconds
	TimeoutVisibility *int32 `json:"timeoutVisibility,omitempty"`

	// +optional
	// Buffer visibility in seconds
	BufferVisibility *int32 `json:"bufferVisibility,omitempty"`

	// +optional
	// Maximum number of executor worker threads
	ExecutorMaxWorkersCount *int32 `json:"executorMaxWorkersCount,omitempty"`

	// +optional
	// Minimum number of pending messages before sending
	MinPendingMessages *int32 `json:"minPendingMessages,omitempty"`

	// +optional
	// Number of retries for renewing message visibility
	RenewRetries *int32 `json:"renewRetries,omitempty"`

	// +optional
	// Encoding format for messages (e.g. "s2s")
	EncodingFormat string `json:"encodingFormat,omitempty"`

	// +optional
	// Interval between send operations (e.g. "5s")
	SendInterval string `json:"sendInterval,omitempty"`

	// +optional
	// Dead letter queue process interval (e.g. "1d")
	DLQProcessInterval string `json:"dlqProcessInterval,omitempty"`

	// +optional
	// Whether to enable shared receipts
	EnableSharedReceipts *bool `json:"enableSharedReceipts,omitempty"`
}

// QueueStatus defines the observed state of Queue
type QueueStatus struct {
	// Phase of the queue
	Phase Phase `json:"phase"`

	// Resource revision tracker
	ResourceRevMap map[string]string `json:"resourceRevMap"`

	// Auxillary message describing CR status
	Message string `json:"message"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Queue is the Schema for a Splunk Enterprise queue
// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=queues,scope=Namespaced,shortName=queue
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Status of queue"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Age of queue resource"
// +kubebuilder:printcolumn:name="Message",type="string",JSONPath=".status.message",description="Auxillary message describing CR status"
// +kubebuilder:storageversion

// Queue is the Schema for the queues API
type Queue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	Spec   QueueSpec   `json:"spec"`
	Status QueueStatus `json:"status,omitempty,omitzero"`
}

// DeepCopyObject implements runtime.Object
func (in *Queue) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// +kubebuilder:object:root=true

// QueueList contains a list of Queue
type QueueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Queue `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Queue{}, &QueueList{})
}
