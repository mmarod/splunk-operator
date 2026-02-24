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

package enterprise

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	enterpriseApi "github.com/splunk/splunk-operator/api/v4"
	spltest "github.com/splunk/splunk-operator/pkg/splunk/test"
	splutil "github.com/splunk/splunk-operator/pkg/splunk/util"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func init() {
	GetReadinessScriptLocation = func() string {
		fileLocation, _ := filepath.Abs("../../../" + readinessScriptLocation)
		return fileLocation
	}
	GetLivenessScriptLocation = func() string {
		fileLocation, _ := filepath.Abs("../../../" + livenessScriptLocation)
		return fileLocation
	}
	GetStartupScriptLocation = func() string {
		fileLocation, _ := filepath.Abs("../../../" + startupScriptLocation)
		return fileLocation
	}
}

func TestApplyIngestorCluster(t *testing.T) {
	os.Setenv("SPLUNK_GENERAL_TERMS", "--accept-sgt-current-at-splunk-com")

	ctx := context.TODO()

	scheme := runtime.NewScheme()
	_ = enterpriseApi.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	queue := &enterpriseApi.Queue{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Queue",
			APIVersion: "enterprise.splunk.com/v4",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "queue",
			Namespace: "test",
		},
		Spec: enterpriseApi.QueueSpec{
			Provider: "sqs",
			SQS: enterpriseApi.SQSSpec{
				Name:       "test-queue",
				AuthRegion: "us-west-2",
				Endpoint:   "https://sqs.us-west-2.amazonaws.com",
				DLQ:        "sqs-dlq-test",
			},
		},
	}
	c.Create(ctx, queue)

	objStorage := &enterpriseApi.ObjectStorage{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ObjectStorage",
			APIVersion: "enterprise.splunk.com/v4",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "os",
			Namespace: "test",
		},
		Spec: enterpriseApi.ObjectStorageSpec{
			Provider: "s3",
			S3: enterpriseApi.S3Spec{
				Endpoint: "https://s3.us-west-2.amazonaws.com",
				Path:     "bucket/key",
			},
		},
	}
	c.Create(ctx, objStorage)

	cr := &enterpriseApi.IngestorCluster{
		TypeMeta: metav1.TypeMeta{
			Kind:       "IngestorCluster",
			APIVersion: "enterprise.splunk.com/v4",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "test",
		},
		Spec: enterpriseApi.IngestorClusterSpec{
			Replicas: 3,
			CommonSplunkSpec: enterpriseApi.CommonSplunkSpec{
				Mock:           true,
				ServiceAccount: "sa",
			},
			QueueRef: corev1.ObjectReference{
				Name:      queue.Name,
				Namespace: queue.Namespace,
			},
			ObjectStorageRef: corev1.ObjectReference{
				Name:      objStorage.Name,
				Namespace: objStorage.Namespace,
			},
		},
	}
	c.Create(ctx, cr)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-secrets",
			Namespace: "test",
		},
		Data: map[string][]byte{"password": []byte("dummy")},
	}
	c.Create(ctx, secret)

	probeConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "splunk-test-probe-configmap",
			Namespace: "test",
		},
	}
	c.Create(ctx, probeConfigMap)

	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "splunk-test-ingestor",
			Namespace: "test",
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/instance": "splunk-test-ingestor",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/instance": "splunk-test-ingestor",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "splunk-test-ingestor",
							Image: "splunk/splunk:latest",
							Ports: []corev1.ContainerPort{
								{
									Name:          "http",
									ContainerPort: 8080,
								},
							},
						},
					},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{
			Replicas:        replicas,
			ReadyReplicas:   replicas,
			UpdatedReplicas: replicas,
			CurrentRevision: "v1",
			UpdateRevision:  "v1",
		},
	}
	c.Create(ctx, sts)

	pod0 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "splunk-test-ingestor-0",
			Namespace: "test",
			Labels: map[string]string{
				"app.kubernetes.io/instance": "splunk-test-ingestor",
				"controller-revision-hash":   "v1",
			},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "dummy-volume",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
				{
					Name: "mnt-splunk-secrets",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: "test-secrets",
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{Ready: true},
			},
		},
	}

	pod1 := pod0.DeepCopy()
	pod1.ObjectMeta.Name = "splunk-test-ingestor-1"

	pod2 := pod0.DeepCopy()
	pod2.ObjectMeta.Name = "splunk-test-ingestor-2"

	c.Create(ctx, pod0)
	c.Create(ctx, pod1)
	c.Create(ctx, pod2)

	// First reconcile
	cr.Spec.Replicas = replicas
	cr.Status.ReadyReplicas = cr.Spec.Replicas

	result, err := ApplyIngestorCluster(ctx, c, cr)
	assert.NoError(t, err)
	assert.True(t, result.Requeue)
	assert.NotEqual(t, enterpriseApi.PhaseError, cr.Status.Phase)

	// Verify ConfigMap was created
	var cm corev1.ConfigMap
	cmName := GetIngestorQueueConfigMapName(cr.GetName())
	err = c.Get(ctx, types.NamespacedName{Namespace: "test", Name: cmName}, &cm)
	assert.NoError(t, err)
	assert.Contains(t, cm.Data, "app.conf")
	assert.Contains(t, cm.Data, "outputs.conf")
	assert.Contains(t, cm.Data, "default-mode.conf")
	assert.Contains(t, cm.Data, "local.meta")
	assert.Contains(t, cm.Data["outputs.conf"], "remote_queue:test-queue")

	// Second reconcile should now yield Ready (no REST API calls needed)
	cr.Status.TelAppInstalled = true
	result, err = ApplyIngestorCluster(ctx, c, cr)
	assert.NoError(t, err)
	assert.Equal(t, enterpriseApi.PhaseReady, cr.Status.Phase)
}

func TestGetIngestorStatefulSet(t *testing.T) {
	// Object definitions
	os.Setenv("SPLUNK_GENERAL_TERMS", "--accept-sgt-current-at-splunk-com")

	queue := enterpriseApi.Queue{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Queue",
			APIVersion: "enterprise.splunk.com/v4",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "queue",
		},
		Spec: enterpriseApi.QueueSpec{
			Provider: "sqs",
			SQS: enterpriseApi.SQSSpec{
				Name:       "test-queue",
				AuthRegion: "us-west-2",
				Endpoint:   "https://sqs.us-west-2.amazonaws.com",
				DLQ:        "sqs-dlq-test",
			},
		},
	}

	cr := enterpriseApi.IngestorCluster{
		TypeMeta: metav1.TypeMeta{
			Kind: "IngestorCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "test",
		},
		Spec: enterpriseApi.IngestorClusterSpec{
			Replicas: 0,
			QueueRef: corev1.ObjectReference{
				Name: queue.Name,
			},
		},
	}

	ctx := context.TODO()

	c := spltest.NewMockClient()
	_, err := splutil.ApplyNamespaceScopedSecretObject(ctx, c, "test")
	if err != nil {
		t.Errorf("Failed to create namespace scoped object")
	}

	test := func(want string) {
		f := func() (interface{}, error) {
			if err := validateIngestorClusterSpec(ctx, c, &cr); err != nil {
				t.Errorf("validateIngestorClusterSpec() returned error: %v", err)
			}
			return getIngestorStatefulSet(ctx, c, &cr)
		}
		configTester(t, "getIngestorStatefulSet()", f, want)
	}

	// Define additional service port in CR and verify the statefulset has the new port
	cr.Spec.ServiceTemplate.Spec.Ports = []corev1.ServicePort{{Name: "user-defined", Port: 32000, Protocol: "UDP"}}
	test(loadFixture(t, "statefulset_ingestor.json"))

	// Create a service account
	current := corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "defaults",
			Namespace: "test",
		},
	}
	_ = splutil.CreateResource(ctx, c, &current)
	cr.Spec.ServiceAccount = "defaults"
	test(loadFixture(t, "statefulset_ingestor_with_serviceaccount.json"))

	// Add extraEnv
	cr.Spec.CommonSplunkSpec.ExtraEnv = []corev1.EnvVar{
		{
			Name:  "TEST_ENV_VAR",
			Value: "test_value",
		},
	}
	test(loadFixture(t, "statefulset_ingestor_with_extraenv.json"))

	// Add additional label to cr metadata to transfer to the statefulset
	cr.ObjectMeta.Labels = make(map[string]string)
	cr.ObjectMeta.Labels["app.kubernetes.io/test-extra-label"] = "test-extra-label-value"
	test(loadFixture(t, "statefulset_ingestor_with_labels.json"))
}

func TestGenerateIngestorOutputsConf(t *testing.T) {
	provider := "sqs_smartbus"

	queue := &enterpriseApi.QueueSpec{
		Provider: "sqs",
		SQS: enterpriseApi.SQSSpec{
			Name:       "test-queue",
			AuthRegion: "us-west-2",
			Endpoint:   "https://sqs.us-west-2.amazonaws.com",
			DLQ:        "sqs-dlq-test",
		},
	}

	objStorage := &enterpriseApi.ObjectStorageSpec{
		Provider: "s3",
		S3: enterpriseApi.S3Spec{
			Endpoint: "https://s3.us-west-2.amazonaws.com",
			Path:     "bucket/key",
		},
	}

	// With credentials — backward compat: defaults match previous hardcoded values
	conf := generateIngestorOutputsConf(queue, objStorage, "mykey", "mysecret")
	assert.Contains(t, conf, "[remote_queue:test-queue]")
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.type = %s", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.auth_region = us-west-2", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.endpoint = https://sqs.us-west-2.amazonaws.com", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.endpoint = https://s3.us-west-2.amazonaws.com", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.path = s3://bucket/key", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.dead_letter_queue.name = sqs-dlq-test", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.encoding_format = s2s", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.max_count.max_retries_per_part = 4", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.retry_policy = max_count", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.send_interval = 5s", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.access_key = mykey", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.secret_key = mysecret", provider))

	// Without credentials (IRSA)
	conf = generateIngestorOutputsConf(queue, objStorage, "", "")
	assert.NotContains(t, conf, "access_key")
	assert.NotContains(t, conf, "secret_key")

	// Optional fields should not appear when not set
	assert.NotContains(t, conf, "max_connections")
	assert.NotContains(t, conf, "message_group_id")
	assert.NotContains(t, conf, "timeout.connect")
	assert.NotContains(t, conf, "sslVerifyServerCert")
}

func TestGenerateIngestorOutputsConfSQSCP(t *testing.T) {
	provider := "sqs_smartbus_cp"

	queue := &enterpriseApi.QueueSpec{
		Provider: "sqs_cp",
		SQS: enterpriseApi.SQSSpec{
			Name:       "test-queue",
			AuthRegion: "us-west-2",
			Endpoint:   "https://sqs.us-west-2.amazonaws.com",
			DLQ:        "sqs-dlq-test",
		},
	}

	objStorage := &enterpriseApi.ObjectStorageSpec{
		Provider: "s3",
		S3: enterpriseApi.S3Spec{
			Endpoint: "https://s3.us-west-2.amazonaws.com",
			Path:     "bucket/key",
		},
	}

	conf := generateIngestorOutputsConf(queue, objStorage, "key", "secret")
	assert.Contains(t, conf, "[remote_queue:test-queue]")
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.type = %s", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.auth_region = us-west-2", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.access_key = key", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.secret_key = secret", provider))
}

func TestGenerateIngestorOutputsConfAllFields(t *testing.T) {
	provider := "sqs_smartbus"
	maxConn := int32(10)
	maxRetries := int32(8)
	timeoutConnect := int32(30)
	timeoutRead := int32(60)
	timeoutWrite := int32(45)
	timeoutRecv := int32(20)
	timeoutVis := int32(300)
	bufferVis := int32(120)
	execWorkers := int32(4)
	minPending := int32(100)
	renewRetries := int32(3)
	sslVerify := true

	queue := &enterpriseApi.QueueSpec{
		Provider: "sqs",
		SQS: enterpriseApi.SQSSpec{
			Name:                    "test-queue",
			AuthRegion:              "us-west-2",
			Endpoint:                "https://sqs.us-west-2.amazonaws.com",
			DLQ:                     "sqs-dlq-test",
			MaxConnections:          &maxConn,
			MessageGroupID:          "my-group",
			RetryPolicy:             "none",
			MaxRetriesPerPart:       &maxRetries,
			TimeoutConnect:          &timeoutConnect,
			TimeoutRead:             &timeoutRead,
			TimeoutWrite:            &timeoutWrite,
			TimeoutReceiveMessage:   &timeoutRecv,
			TimeoutVisibility:       &timeoutVis,
			BufferVisibility:        &bufferVis,
			ExecutorMaxWorkersCount: &execWorkers,
			MinPendingMessages:      &minPending,
			RenewRetries:            &renewRetries,
			EncodingFormat:          "json",
			SendInterval:            "10s",
			DLQProcessInterval:      "2d",
		},
	}

	objStorage := &enterpriseApi.ObjectStorageSpec{
		Provider: "s3",
		S3: enterpriseApi.S3Spec{
			Endpoint:             "https://s3.us-west-2.amazonaws.com",
			Path:                 "bucket/key",
			SSLVerifyServerCert:  &sslVerify,
			SSLVersions:          "tls1.2",
			SSLCommonNameToCheck: "*.example.com",
			SSLAltNameToCheck:    "alt.example.com",
			SSLRootCAPath:        "/opt/splunk/etc/auth/ca.pem",
			CipherSuite:          "ECDHE-RSA-AES256-GCM-SHA384",
			ECDHCurves:           "prime256v1",
			DHFile:               "/opt/splunk/etc/auth/dh.pem",
			EncryptionScheme:     "SSE-KMS",
			KMSEndpoint:          "https://kms.us-west-2.amazonaws.com",
			KeyID:                "my-key-id",
			KeyRefreshInterval:   "1d",
		},
	}

	conf := generateIngestorOutputsConf(queue, objStorage, "mykey", "mysecret")

	// Verify all SQS fields
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.encoding_format = json", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.max_count.max_retries_per_part = 8", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.retry_policy = none", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.send_interval = 10s", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.max_connections = 10", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.message_group_id = my-group", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.timeout.connect = 30", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.timeout.read = 60", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.timeout.write = 45", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.timeout.receive_message = 20", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.timeout.visibility = 300", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.buffer.visibility = 120", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.executor_max_workers_count = 4", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.min_pending_messages = 100", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.renew_retries = 3", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.dead_letter_queue.process_interval = 2d", provider))

	// Verify all S3/large_message_store fields
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.sslVerifyServerCert = true", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.sslVersions = tls1.2", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.sslCommonNameToCheck = *.example.com", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.sslAltNameToCheck = alt.example.com", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.sslRootCAPath = /opt/splunk/etc/auth/ca.pem", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.cipherSuite = ECDHE-RSA-AES256-GCM-SHA384", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.ecdhCurves = prime256v1", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.dhFile = /opt/splunk/etc/auth/dh.pem", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.encryption_scheme = SSE-KMS", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.kms_endpoint = https://kms.us-west-2.amazonaws.com", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.key_id = my-key-id", provider))
	assert.Contains(t, conf, fmt.Sprintf("remote_queue.%s.large_message_store.key_refresh_interval = 1d", provider))
}

func TestGenerateIngestorDefaultModeConf(t *testing.T) {
	conf := generateIngestorDefaultModeConf()

	assert.Contains(t, conf, "[pipeline:remotequeueruleset]")
	assert.Contains(t, conf, "disabled = false")
	assert.Contains(t, conf, "[pipeline:ruleset]")
	assert.Contains(t, conf, "disabled = true")
	assert.Contains(t, conf, "[pipeline:remotequeuetyping]")
	assert.Contains(t, conf, "[pipeline:remotequeueoutput]")
	assert.Contains(t, conf, "[pipeline:typing]")
	assert.Contains(t, conf, "[pipeline:indexerPipe]")

	// Count stanzas
	stanzaCount := strings.Count(conf, "[pipeline:")
	assert.Equal(t, 6, stanzaCount)
}

func TestGenerateIngestorAppConf(t *testing.T) {
	conf := generateIngestorAppConf()
	assert.Contains(t, conf, "[install]")
	assert.Contains(t, conf, "state = enabled")
	assert.Contains(t, conf, "allows_disable = false")
	assert.Contains(t, conf, "[package]")
	assert.Contains(t, conf, "check_for_updates = false")
	assert.Contains(t, conf, "[ui]")
	assert.Contains(t, conf, "is_visible = false")
	assert.Contains(t, conf, "is_manageable = false")
	assert.NotContains(t, conf, "[launcher]")
}

func TestGenerateIngestorLocalMeta(t *testing.T) {
	checksum := "abc123def456"
	meta := generateIngestorLocalMeta(checksum)
	assert.Contains(t, meta, "[]")
	assert.Contains(t, meta, "access = read : [ * ], write : [ admin ]")
	assert.Contains(t, meta, "export = system")
	assert.Contains(t, meta, "[app/install/install_source_checksum]")
	assert.Contains(t, meta, "data = abc123def456")
}

func TestComputeIngestorConfChecksum(t *testing.T) {
	outputs := "some outputs conf"
	defaultMode := "some default mode conf"

	// Same inputs produce same checksum
	c1 := computeIngestorConfChecksum(outputs, defaultMode)
	c2 := computeIngestorConfChecksum(outputs, defaultMode)
	assert.Equal(t, c1, c2)

	// Different inputs produce different checksum
	c3 := computeIngestorConfChecksum(outputs+"changed", defaultMode)
	assert.NotEqual(t, c1, c3)

	// Checksum is a 64-char hex string (SHA-256)
	assert.Len(t, c1, 64)
}

func TestBuildAndApplyIngestorQueueConfigMap(t *testing.T) {
	ctx := context.TODO()

	scheme := runtime.NewScheme()
	_ = enterpriseApi.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	cr := &enterpriseApi.IngestorCluster{
		TypeMeta: metav1.TypeMeta{
			Kind:       "IngestorCluster",
			APIVersion: "enterprise.splunk.com/v4",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "myingestor",
			Namespace: "test",
		},
	}
	c.Create(ctx, cr)

	qosCfg := &QueueOSConfig{
		Queue: enterpriseApi.QueueSpec{
			Provider: "sqs",
			SQS: enterpriseApi.SQSSpec{
				Name:       "test-queue",
				AuthRegion: "us-west-2",
				Endpoint:   "https://sqs.us-west-2.amazonaws.com",
				DLQ:        "sqs-dlq-test",
			},
		},
		OS: enterpriseApi.ObjectStorageSpec{
			Provider: "s3",
			S3: enterpriseApi.S3Spec{
				Endpoint: "https://s3.us-west-2.amazonaws.com",
				Path:     "bucket/key",
			},
		},
		AccessKey: "ak",
		SecretKey: "sk",
	}

	changed, err := buildAndApplyIngestorQueueConfigMap(ctx, c, cr, qosCfg)
	assert.NoError(t, err)
	assert.True(t, changed, "first apply should report changed (create)")

	// Verify ConfigMap was created
	var cm corev1.ConfigMap
	cmName := GetIngestorQueueConfigMapName(cr.GetName())
	err = c.Get(ctx, types.NamespacedName{Namespace: "test", Name: cmName}, &cm)
	assert.NoError(t, err)
	assert.Equal(t, "splunk-myingestor-ingestor-queue-config", cm.Name)

	// Verify all keys present
	assert.Contains(t, cm.Data, "app.conf")
	assert.Contains(t, cm.Data, "local.meta")
	assert.Contains(t, cm.Data, "outputs.conf")
	assert.Contains(t, cm.Data, "default-mode.conf")

	// Verify outputs.conf content
	assert.Contains(t, cm.Data["outputs.conf"], "[remote_queue:test-queue]")
	assert.Contains(t, cm.Data["outputs.conf"], "access_key = ak")

	// Verify local.meta contains checksum stanza
	assert.Contains(t, cm.Data["local.meta"], "[app/install/install_source_checksum]")
	assert.Contains(t, cm.Data["local.meta"], "data = ")

	// Verify owner reference
	assert.Equal(t, 1, len(cm.OwnerReferences))
	assert.Equal(t, "myingestor", cm.OwnerReferences[0].Name)

	// Re-apply with same config: should report no change
	changed, err = buildAndApplyIngestorQueueConfigMap(ctx, c, cr, qosCfg)
	assert.NoError(t, err)
	assert.False(t, changed, "re-apply with same config should report no change")

	// Update and re-apply: change credentials
	qosCfg.AccessKey = "new-ak"
	changed, err = buildAndApplyIngestorQueueConfigMap(ctx, c, cr, qosCfg)
	assert.NoError(t, err)
	assert.True(t, changed, "re-apply with changed config should report changed")

	err = c.Get(ctx, types.NamespacedName{Namespace: "test", Name: cmName}, &cm)
	assert.NoError(t, err)
	assert.Contains(t, cm.Data["outputs.conf"], "access_key = new-ak")
}

func TestGetQueueAndObjectStorageInputsForIngestorConfFiles(t *testing.T) {
	provider := "sqs_smartbus"

	queue := &enterpriseApi.QueueSpec{
		Provider: "sqs",
		SQS: enterpriseApi.SQSSpec{
			Name:       "test-queue",
			AuthRegion: "us-west-2",
			Endpoint:   "https://sqs.us-west-2.amazonaws.com",
			DLQ:        "sqs-dlq-test",
			VolList: []enterpriseApi.VolumeSpec{
				{SecretRef: "secret"},
			},
		},
	}

	objStorage := &enterpriseApi.ObjectStorageSpec{
		Provider: "s3",
		S3: enterpriseApi.S3Spec{
			Endpoint: "https://s3.us-west-2.amazonaws.com",
			Path:     "bucket/key",
		},
	}

	config := getQueueAndObjectStorageInputsForIngestorConfFiles(queue, objStorage, "key", "secret")

	assert.Equal(t, 12, len(config))
	assert.Equal(t, [][]string{
		{"remote_queue.type", provider},
		{fmt.Sprintf("remote_queue.%s.auth_region", provider), queue.SQS.AuthRegion},
		{fmt.Sprintf("remote_queue.%s.endpoint", provider), queue.SQS.Endpoint},
		{fmt.Sprintf("remote_queue.%s.large_message_store.endpoint", provider), objStorage.S3.Endpoint},
		{fmt.Sprintf("remote_queue.%s.large_message_store.path", provider), "s3://" + objStorage.S3.Path},
		{fmt.Sprintf("remote_queue.%s.dead_letter_queue.name", provider), queue.SQS.DLQ},
		{fmt.Sprintf("remote_queue.%s.encoding_format", provider), "s2s"},
		{fmt.Sprintf("remote_queue.%s.max_count.max_retries_per_part", provider), "4"},
		{fmt.Sprintf("remote_queue.%s.retry_policy", provider), "max_count"},
		{fmt.Sprintf("remote_queue.%s.send_interval", provider), "5s"},
		{fmt.Sprintf("remote_queue.%s.access_key", provider), "key"},
		{fmt.Sprintf("remote_queue.%s.secret_key", provider), "secret"},
	}, config)
}

func TestGetPipelineInputsForConfFile(t *testing.T) {
	config := getPipelineInputsForConfFile(false)
	assert.Equal(t, 6, len(config))
	assert.Equal(t, [][]string{
		{"pipeline:remotequeueruleset", "disabled", "false"},
		{"pipeline:ruleset", "disabled", "true"},
		{"pipeline:remotequeuetyping", "disabled", "false"},
		{"pipeline:remotequeueoutput", "disabled", "false"},
		{"pipeline:typing", "disabled", "true"},
		{"pipeline:indexerPipe", "disabled", "true"},
	}, config)

	// For indexer, no indexerPipe stanza
	config = getPipelineInputsForConfFile(true)
	assert.Equal(t, 5, len(config))
}
