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
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"time"

	enterpriseApi "github.com/splunk/splunk-operator/api/v4"
	splcommon "github.com/splunk/splunk-operator/pkg/splunk/common"
	splctrl "github.com/splunk/splunk-operator/pkg/splunk/splkcontroller"
	splutil "github.com/splunk/splunk-operator/pkg/splunk/util"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ApplyIngestorCluster reconciles the state of an IngestorCluster custom resource
func ApplyIngestorCluster(ctx context.Context, client client.Client, cr *enterpriseApi.IngestorCluster) (reconcile.Result, error) {
	var err error

	// Unless modified, reconcile for this object will be requeued after 5 seconds
	result := reconcile.Result{
		Requeue:      true,
		RequeueAfter: time.Second * 5,
	}

	reqLogger := log.FromContext(ctx)
	scopedLog := reqLogger.WithName("ApplyIngestorCluster")

	if cr.Status.ResourceRevMap == nil {
		cr.Status.ResourceRevMap = make(map[string]string)
	}

	eventPublisher := GetEventPublisher(ctx, cr)
	ctx = context.WithValue(ctx, splcommon.EventPublisherKey, eventPublisher)

	cr.Kind = "IngestorCluster"

	// Validate and updates defaults for CR
	err = validateIngestorClusterSpec(ctx, client, cr)
	if err != nil {
		eventPublisher.Warning(ctx, "validateIngestorClusterSpec", fmt.Sprintf("validate ingestor cluster spec failed %s", err.Error()))
		scopedLog.Error(err, "Failed to validate ingestor cluster spec")
		return result, err
	}

	// Initialize phase
	cr.Status.Phase = enterpriseApi.PhaseError

	// Update the CR Status
	defer updateCRStatus(ctx, client, cr, &err)
	cr.Status.Replicas = cr.Spec.Replicas

	// If needed, migrate the app framework status
	err = checkAndMigrateAppDeployStatus(ctx, client, cr, &cr.Status.AppContext, &cr.Spec.AppFrameworkConfig, true)
	if err != nil {
		return result, err
	}

	// If app framework is configured, then do following things
	// Initialize the S3 clients based on providers
	// Check the status of apps on remote storage
	if len(cr.Spec.AppFrameworkConfig.AppSources) != 0 {
		err = initAndCheckAppInfoStatus(ctx, client, cr, &cr.Spec.AppFrameworkConfig, &cr.Status.AppContext)
		if err != nil {
			eventPublisher.Warning(ctx, "initAndCheckAppInfoStatus", fmt.Sprintf("init and check app info status failed %s", err.Error()))
			cr.Status.AppContext.IsDeploymentInProgress = false
			return result, err
		}
	}

	cr.Status.Selector = fmt.Sprintf("app.kubernetes.io/instance=splunk-%s-ingestor", cr.GetName())

	// Create or update general config resources
	_, err = ApplySplunkConfig(ctx, client, cr, cr.Spec.CommonSplunkSpec, SplunkIngestor)
	if err != nil {
		scopedLog.Error(err, "create or update general config failed", "error", err.Error())
		eventPublisher.Warning(ctx, "ApplySplunkConfig", fmt.Sprintf("create or update general config failed with error %s", err.Error()))
		return result, err
	}

	// Check if deletion has been requested
	if cr.ObjectMeta.DeletionTimestamp != nil {
		if cr.Spec.MonitoringConsoleRef.Name != "" {
			_, err = ApplyMonitoringConsoleEnvConfigMap(ctx, client, cr.GetNamespace(), cr.GetName(), cr.Spec.MonitoringConsoleRef.Name, make([]corev1.EnvVar, 0), false)
			if err != nil {
				eventPublisher.Warning(ctx, "ApplyMonitoringConsoleEnvConfigMap", fmt.Sprintf("create/update monitoring console config map failed %s", err.Error()))
				return result, err
			}
		}

		// If this is the last of its kind getting deleted,
		// remove the entry for this CR type from configMap or else
		// just decrement the refCount for this CR type
		if len(cr.Spec.AppFrameworkConfig.AppSources) != 0 {
			err = UpdateOrRemoveEntryFromConfigMapLocked(ctx, client, cr, SplunkIngestor)
			if err != nil {
				return result, err
			}
		}

		DeleteOwnerReferencesForResources(ctx, client, cr, SplunkIngestor)

		terminating, err := splctrl.CheckForDeletion(ctx, cr, client)
		if terminating && err != nil {
			cr.Status.Phase = enterpriseApi.PhaseTerminating
		} else {
			result.Requeue = false
		}
		return result, err
	}

	// Create or update a headless service for ingestor cluster
	err = splctrl.ApplyService(ctx, client, getSplunkService(ctx, cr, &cr.Spec.CommonSplunkSpec, SplunkIngestor, true))
	if err != nil {
		eventPublisher.Warning(ctx, "ApplyService", fmt.Sprintf("create/update headless service for ingestor cluster failed %s", err.Error()))
		return result, err
	}

	// Create or update a regular service for ingestor cluster
	err = splctrl.ApplyService(ctx, client, getSplunkService(ctx, cr, &cr.Spec.CommonSplunkSpec, SplunkIngestor, false))
	if err != nil {
		eventPublisher.Warning(ctx, "ApplyService", fmt.Sprintf("create/update service for ingestor cluster failed %s", err.Error()))
		return result, err
	}

	// If we are using App Framework and are scaling up, we should re-populate the
	// config map with all the appSource entries
	// This is done so that the new pods
	// that come up now will have the complete list of all the apps and then can
	// download and install all the apps
	// If we are scaling down, just update the auxPhaseInfo list
	if len(cr.Spec.AppFrameworkConfig.AppSources) != 0 && cr.Status.ReadyReplicas > 0 {
		statefulsetName := GetSplunkStatefulsetName(SplunkIngestor, cr.GetName())

		isStatefulSetScaling, err := splctrl.IsStatefulSetScalingUpOrDown(ctx, client, cr, statefulsetName, cr.Spec.Replicas)
		if err != nil {
			return result, err
		}

		appStatusContext := cr.Status.AppContext

		switch isStatefulSetScaling {
		case enterpriseApi.StatefulSetScalingUp:
			// If we are indeed scaling up, then mark the deploy status to Pending
			// for all the app sources so that we add all the app sources in config map
			cr.Status.AppContext.IsDeploymentInProgress = true

			for appSrc := range appStatusContext.AppsSrcDeployStatus {
				changeAppSrcDeployInfoStatus(ctx, appSrc, appStatusContext.AppsSrcDeployStatus, enterpriseApi.RepoStateActive, enterpriseApi.DeployStatusComplete, enterpriseApi.DeployStatusPending)
				changePhaseInfo(ctx, cr.Spec.Replicas, appSrc, appStatusContext.AppsSrcDeployStatus)
			}

		// If we are scaling down, just delete the state auxPhaseInfo entries
		case enterpriseApi.StatefulSetScalingDown:
			for appSrc := range appStatusContext.AppsSrcDeployStatus {
				removeStaleEntriesFromAuxPhaseInfo(ctx, cr.Spec.Replicas, appSrc, appStatusContext.AppsSrcDeployStatus)
			}
		}
	}

	// Resolve queue and object storage config before creating the statefulset
	qosCfg, err := ResolveQueueAndObjectStorage(ctx, client, cr, cr.Spec.QueueRef, cr.Spec.ObjectStorageRef, cr.Spec.ServiceAccount)
	if err != nil {
		scopedLog.Error(err, "Failed to resolve Queue/ObjectStorage config")
		eventPublisher.Warning(ctx, "ResolveQueueAndObjectStorage", fmt.Sprintf("resolve queue/object storage config failed %s", err.Error()))
		return result, err
	}

	// Build and apply the ingestor queue config ConfigMap
	configMapChanged, err := buildAndApplyIngestorQueueConfigMap(ctx, client, cr, qosCfg)
	if err != nil {
		scopedLog.Error(err, "Failed to build/apply ingestor queue config ConfigMap")
		eventPublisher.Warning(ctx, "buildAndApplyIngestorQueueConfigMap", fmt.Sprintf("build/apply ingestor queue config configmap failed %s", err.Error()))
		return result, err
	}

	// Create or update statefulset for the ingestors
	statefulSet, err := getIngestorStatefulSet(ctx, client, cr)
	if err != nil {
		eventPublisher.Warning(ctx, "getIngestorStatefulSet", fmt.Sprintf("get ingestor stateful set failed %s", err.Error()))
		return result, err
	}

	// Make changes to respective mc configmap when changing/removing mcRef from spec
	err = validateMonitoringConsoleRef(ctx, client, statefulSet, make([]corev1.EnvVar, 0))
	if err != nil {
		eventPublisher.Warning(ctx, "validateMonitoringConsoleRef", fmt.Sprintf("validate monitoring console reference failed %s", err.Error()))
		return result, err
	}

	mgr := splctrl.DefaultStatefulSetPodManager{}
	phase, err := mgr.Update(ctx, client, statefulSet, cr.Spec.Replicas)
	cr.Status.ReadyReplicas = statefulSet.Status.ReadyReplicas
	if err != nil {
		eventPublisher.Warning(ctx, "update", fmt.Sprintf("update stateful set failed %s", err.Error()))
		return result, err
	}
	cr.Status.Phase = phase

	// No need to requeue if everything is ready
	if cr.Status.Phase == enterpriseApi.PhaseReady {
		// Upgrade fron automated MC to MC CRD
		namespacedName := types.NamespacedName{Namespace: cr.GetNamespace(), Name: GetSplunkStatefulsetName(SplunkMonitoringConsole, cr.GetNamespace())}
		err = splctrl.DeleteReferencesToAutomatedMCIfExists(ctx, client, cr, namespacedName)
		if err != nil {
			eventPublisher.Warning(ctx, "DeleteReferencesToAutomatedMCIfExists", fmt.Sprintf("delete reference to automated MC if exists failed %s", err.Error()))
			scopedLog.Error(err, "Error in deleting automated monitoring console resource")
		}
		if cr.Spec.MonitoringConsoleRef.Name != "" {
			_, err = ApplyMonitoringConsoleEnvConfigMap(ctx, client, cr.GetNamespace(), cr.GetName(), cr.Spec.MonitoringConsoleRef.Name, make([]corev1.EnvVar, 0), true)
			if err != nil {
				eventPublisher.Warning(ctx, "ApplyMonitoringConsoleEnvConfigMap", fmt.Sprintf("apply monitoring console environment config map failed %s", err.Error()))
				return result, err
			}
		}

		// When the ConfigMap changes, the kubelet needs time to propagate the
		// update to the volume mount on each pod. Store the expected checksum
		// and requeue. On subsequent reconciles, verify the mount content
		// matches before copying files and reloading.
		if configMapChanged {
			outputsConf := generateIngestorOutputsConf(&qosCfg.Queue, &qosCfg.OS, qosCfg.AccessKey, qosCfg.SecretKey)
			defaultModeConf := generateIngestorDefaultModeConf()
			cr.Status.QueueConfigExpectedChecksum = computeIngestorConfChecksum(outputsConf, defaultModeConf)
			scopedLog.Info("ConfigMap changed, waiting for volume mount propagation")
			result.RequeueAfter = 5 * time.Second
			return result, nil
		}
		if cr.Status.QueueConfigExpectedChecksum != "" {
			mountReady, err := isIngestorQueueMountCurrent(ctx, client, cr, cr.Status.QueueConfigExpectedChecksum)
			if err != nil {
				scopedLog.Error(err, "Failed to check queue config mount status")
				result.RequeueAfter = 5 * time.Second
				return result, nil
			}
			if !mountReady {
				scopedLog.Info("Volume mount not yet propagated, requeuing")
				result.RequeueAfter = 5 * time.Second
				return result, nil
			}
			err = reloadIngestorApp(ctx, client, cr)
			if err != nil {
				eventPublisher.Warning(ctx, "reloadIngestorApp", fmt.Sprintf("reload ingestor app failed %s", err.Error()))
				return result, err
			}
			cr.Status.QueueConfigExpectedChecksum = ""
		}

		finalResult := handleAppFrameworkActivity(ctx, client, cr, &cr.Status.AppContext, &cr.Spec.AppFrameworkConfig)
		result = *finalResult

		// Add a splunk operator telemetry app
		if cr.Spec.EtcVolumeStorageConfig.EphemeralStorage || !cr.Status.TelAppInstalled {
			podExecClient := splutil.GetPodExecClient(client, cr, "")
			err = addTelApp(ctx, podExecClient, cr.Spec.Replicas, cr)
			if err != nil {
				return result, err
			}

			// Mark telemetry app as installed
			cr.Status.TelAppInstalled = true
		}
	}

	// RequeueAfter if greater than 0, tells the Controller to requeue the reconcile key after the Duration.
	// Implies that Requeue is true, there is no need to set Requeue to true at the same time as RequeueAfter.
	if !result.Requeue {
		result.RequeueAfter = 0
	}

	return result, nil
}

// validateIngestorClusterSpec checks validity and makes default updates to a IngestorClusterSpec and returns error if something is wrong
func validateIngestorClusterSpec(ctx context.Context, c splcommon.ControllerClient, cr *enterpriseApi.IngestorCluster) error {
	// We cannot have 0 replicas in IngestorCluster spec since this refers to number of ingestion pods in the ingestor cluster
	if cr.Spec.Replicas < 1 {
		cr.Spec.Replicas = 1
	}

	if !reflect.DeepEqual(cr.Status.AppContext.AppFrameworkConfig, cr.Spec.AppFrameworkConfig) {
		err := ValidateAppFrameworkSpec(ctx, &cr.Spec.AppFrameworkConfig, &cr.Status.AppContext, true, cr.GetObjectKind().GroupVersionKind().Kind)
		if err != nil {
			return err
		}
	}

	return validateCommonSplunkSpec(ctx, c, &cr.Spec.CommonSplunkSpec, cr)
}

// getIngestorStatefulSet returns a Kubernetes StatefulSet object for Splunk Enterprise ingestors
func getIngestorStatefulSet(ctx context.Context, client splcommon.ControllerClient, cr *enterpriseApi.IngestorCluster) (*appsv1.StatefulSet, error) {
	ss, err := getSplunkStatefulSet(ctx, client, cr, &cr.Spec.CommonSplunkSpec, SplunkIngestor, cr.Spec.Replicas, []corev1.EnvVar{})
	if err != nil {
		return nil, err
	}

	// Mount the ingestor queue config ConfigMap volume.
	// ConfigMap is mounted directly (non-subPath), so Kubernetes propagates
	// content updates in-place via atomic symlink swap — no pod roll needed.
	configMapName := GetIngestorQueueConfigMapName(cr.GetName())
	configMapVolDefaultMode := corev1.ConfigMapVolumeSourceDefaultMode
	addSplunkVolumeToTemplate(&ss.Spec.Template, "mnt-splunk-queue-config", ingestorQueueConfigMountPath, corev1.VolumeSource{
		ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: configMapName,
			},
			DefaultMode: &configMapVolDefaultMode,
		},
	})

	// Add init container to create app directory structure and symlink config files
	setupIngestorInitContainer(&ss.Spec.Template, cr.Spec.Image, cr.Spec.ImagePullPolicy, commandForIngestorQueueConfig, cr.Spec.EtcVolumeStorageConfig.EphemeralStorage)

	// Setup App framework staging volume for apps
	setupAppsStagingVolume(ctx, client, cr, &ss.Spec.Template, &cr.Spec.AppFrameworkConfig)

	return ss, nil
}

// getPipelineInputsForConfFile returns a list of pipeline inputs for conf file
func getPipelineInputsForConfFile(isIndexer bool) (config [][]string) {
	config = append(config,
		[]string{"pipeline:remotequeueruleset", "disabled", "false"},
		[]string{"pipeline:ruleset", "disabled", "true"},
		[]string{"pipeline:remotequeuetyping", "disabled", "false"},
		[]string{"pipeline:remotequeueoutput", "disabled", "false"},
		[]string{"pipeline:typing", "disabled", "true"},
	)
	if !isIndexer {
		config = append(config, []string{"pipeline:indexerPipe", "disabled", "true"})
	}

	return
}

// getQueueAndObjectStorageInputsForIngestorConfFiles returns a list of queue and object storage inputs for conf files.
// All optional fields are only emitted when explicitly set on the CRD.
func getQueueAndObjectStorageInputsForIngestorConfFiles(queue *enterpriseApi.QueueSpec, os *enterpriseApi.ObjectStorageSpec, accessKey, secretKey string) (config [][]string) {
	queueProvider := ""
	authRegion := ""
	endpoint := ""
	dlq := ""
	if queue.Provider == "sqs" {
		queueProvider = "sqs_smartbus"
	} else if queue.Provider == "sqs_cp" {
		queueProvider = "sqs_smartbus_cp"
	}
	if queue.Provider == "sqs" || queue.Provider == "sqs_cp" {
		authRegion = queue.SQS.AuthRegion
		endpoint = queue.SQS.Endpoint
		dlq = queue.SQS.DLQ
	}

	path := ""
	osEndpoint := ""
	osProvider := ""
	if os.Provider == "s3" {
		if queueProvider == "sqs_smartbus" {
			osProvider = "sqs_smartbus"
		} else if queueProvider == "sqs_smartbus_cp" {
			osProvider = "sqs_smartbus_cp"
		}
		osEndpoint = os.S3.Endpoint
		path = os.S3.Path
		if !strings.HasPrefix(path, "s3://") {
			path = "s3://" + path
		}
	}

	// Always-emitted fields
	config = append(config,
		[]string{"remote_queue.type", queueProvider},
		[]string{fmt.Sprintf("remote_queue.%s.auth_region", queueProvider), authRegion},
		[]string{fmt.Sprintf("remote_queue.%s.endpoint", queueProvider), endpoint},
		[]string{fmt.Sprintf("remote_queue.%s.large_message_store.endpoint", osProvider), osEndpoint},
		[]string{fmt.Sprintf("remote_queue.%s.large_message_store.path", osProvider), path},
		[]string{fmt.Sprintf("remote_queue.%s.dead_letter_queue.name", queueProvider), dlq},
	)

	// Optional SQS fields — only emitted when set
	if queue.SQS.EncodingFormat != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.encoding_format", queueProvider), queue.SQS.EncodingFormat})
	}
	if queue.SQS.MaxRetriesPerPart != nil {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.max_count.max_retries_per_part", queueProvider), fmt.Sprintf("%d", *queue.SQS.MaxRetriesPerPart)})
	}
	if queue.SQS.RetryPolicy != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.retry_policy", queueProvider), queue.SQS.RetryPolicy})
	}
	if queue.SQS.SendInterval != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.send_interval", queueProvider), queue.SQS.SendInterval})
	}
	if queue.SQS.MaxConnections != nil {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.max_connections", queueProvider), fmt.Sprintf("%d", *queue.SQS.MaxConnections)})
	}
	if queue.SQS.MessageGroupID != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.message_group_id", queueProvider), queue.SQS.MessageGroupID})
	}
	if queue.SQS.TimeoutConnect != nil {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.timeout.connect", queueProvider), fmt.Sprintf("%d", *queue.SQS.TimeoutConnect)})
	}
	if queue.SQS.TimeoutRead != nil {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.timeout.read", queueProvider), fmt.Sprintf("%d", *queue.SQS.TimeoutRead)})
	}
	if queue.SQS.TimeoutWrite != nil {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.timeout.write", queueProvider), fmt.Sprintf("%d", *queue.SQS.TimeoutWrite)})
	}
	if queue.SQS.TimeoutReceiveMessage != nil {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.timeout.receive_message", queueProvider), fmt.Sprintf("%d", *queue.SQS.TimeoutReceiveMessage)})
	}
	if queue.SQS.TimeoutVisibility != nil {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.timeout.visibility", queueProvider), fmt.Sprintf("%d", *queue.SQS.TimeoutVisibility)})
	}
	if queue.SQS.BufferVisibility != nil {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.buffer.visibility", queueProvider), fmt.Sprintf("%d", *queue.SQS.BufferVisibility)})
	}
	if queue.SQS.ExecutorMaxWorkersCount != nil {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.executor_max_workers_count", queueProvider), fmt.Sprintf("%d", *queue.SQS.ExecutorMaxWorkersCount)})
	}
	if queue.SQS.MinPendingMessages != nil {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.min_pending_messages", queueProvider), fmt.Sprintf("%d", *queue.SQS.MinPendingMessages)})
	}
	if queue.SQS.RenewRetries != nil {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.renew_retries", queueProvider), fmt.Sprintf("%d", *queue.SQS.RenewRetries)})
	}
	if queue.SQS.DLQProcessInterval != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.dead_letter_queue.process_interval", queueProvider), queue.SQS.DLQProcessInterval})
	}
	if queue.SQS.EnableSharedReceipts != nil {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.enable_shared_receipts", queueProvider), fmt.Sprintf("%t", *queue.SQS.EnableSharedReceipts)})
	}

	// Optional S3/large_message_store fields — only emitted when set
	if os.S3.SSLVerifyServerCert != nil {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.large_message_store.sslVerifyServerCert", osProvider), fmt.Sprintf("%t", *os.S3.SSLVerifyServerCert)})
	}
	if os.S3.SSLVersions != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.large_message_store.sslVersions", osProvider), os.S3.SSLVersions})
	}
	if os.S3.SSLCommonNameToCheck != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.large_message_store.sslCommonNameToCheck", osProvider), os.S3.SSLCommonNameToCheck})
	}
	if os.S3.SSLAltNameToCheck != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.large_message_store.sslAltNameToCheck", osProvider), os.S3.SSLAltNameToCheck})
	}
	if os.S3.SSLRootCAPath != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.large_message_store.sslRootCAPath", osProvider), os.S3.SSLRootCAPath})
	}
	if os.S3.CipherSuite != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.large_message_store.cipherSuite", osProvider), os.S3.CipherSuite})
	}
	if os.S3.ECDHCurves != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.large_message_store.ecdhCurves", osProvider), os.S3.ECDHCurves})
	}
	if os.S3.DHFile != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.large_message_store.dhFile", osProvider), os.S3.DHFile})
	}
	if os.S3.EncryptionScheme != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.large_message_store.encryption_scheme", osProvider), os.S3.EncryptionScheme})
	}
	if os.S3.KMSEndpoint != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.large_message_store.kms_endpoint", osProvider), os.S3.KMSEndpoint})
	}
	if os.S3.KeyID != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.large_message_store.key_id", osProvider), os.S3.KeyID})
	}
	if os.S3.KeyRefreshInterval != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.large_message_store.key_refresh_interval", osProvider), os.S3.KeyRefreshInterval})
	}

	// Credentials
	if accessKey != "" && secretKey != "" {
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.access_key", queueProvider), accessKey})
		config = append(config, []string{fmt.Sprintf("remote_queue.%s.secret_key", queueProvider), secretKey})
	}

	return
}

// generateIngestorAppConf returns the app.conf content for the ingestor queue config app
func generateIngestorAppConf() string {
	return `[install]
state = enabled
allows_disable = false

[package]
check_for_updates = false

[ui]
is_visible = false
is_manageable = false
label = Splunk Operator Ingestor Queue Config
description = Operator-managed queue and pipeline configuration for IngestorCluster
`
}

// generateIngestorLocalMeta returns the local.meta content for the ingestor queue config app.
// The confChecksum is embedded as install_source_checksum so Splunk detects content changes on reload.
func generateIngestorLocalMeta(confChecksum string) string {
	return fmt.Sprintf(`[]
access = read : [ * ], write : [ admin ]
export = system

[app/install/install_source_checksum]
data = %s
`, confChecksum)
}

// computeIngestorConfChecksum returns a deterministic SHA-256 hex digest of the conf content.
// It changes only when the actual conf file content changes.
func computeIngestorConfChecksum(outputsConf, defaultModeConf string) string {
	h := sha256.New()
	h.Write([]byte(outputsConf))
	h.Write([]byte(defaultModeConf))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// generateIngestorOutputsConf converts queue/OS specs to INI format outputs.conf string
func generateIngestorOutputsConf(queue *enterpriseApi.QueueSpec, os *enterpriseApi.ObjectStorageSpec, accessKey, secretKey string) string {
	kvPairs := getQueueAndObjectStorageInputsForIngestorConfFiles(queue, os, accessKey, secretKey)

	var b strings.Builder
	fmt.Fprintf(&b, "[remote_queue:%s]\n", queue.SQS.Name)
	for _, kv := range kvPairs {
		fmt.Fprintf(&b, "%s = %s\n", kv[0], kv[1])
	}
	return b.String()
}

// generateIngestorDefaultModeConf returns the default-mode.conf content with pipeline stanzas
func generateIngestorDefaultModeConf() string {
	pipelineInputs := getPipelineInputsForConfFile(false)

	var b strings.Builder
	for _, input := range pipelineInputs {
		// input is [stanza, key, value]
		fmt.Fprintf(&b, "[%s]\n", input[0])
		fmt.Fprintf(&b, "%s = %s\n\n", input[1], input[2])
	}
	return b.String()
}

// buildAndApplyIngestorQueueConfigMap builds and applies the ConfigMap containing ingestor queue config app files.
// Returns (true, nil) when ConfigMap data changed, (false, nil) when unchanged.
func buildAndApplyIngestorQueueConfigMap(ctx context.Context, client splcommon.ControllerClient, cr *enterpriseApi.IngestorCluster, qosCfg *QueueOSConfig) (bool, error) {
	configMapName := GetIngestorQueueConfigMapName(cr.GetName())

	outputsConf := generateIngestorOutputsConf(&qosCfg.Queue, &qosCfg.OS, qosCfg.AccessKey, qosCfg.SecretKey)
	defaultModeConf := generateIngestorDefaultModeConf()
	confChecksum := computeIngestorConfChecksum(outputsConf, defaultModeConf)

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: cr.GetNamespace(),
			OwnerReferences: []metav1.OwnerReference{
				splcommon.AsOwner(cr, true),
			},
		},
		Data: map[string]string{
			"app.conf":          generateIngestorAppConf(),
			"local.meta":        generateIngestorLocalMeta(confChecksum),
			"outputs.conf":      outputsConf,
			"default-mode.conf": defaultModeConf,
		},
	}

	return splctrl.ApplyConfigMap(ctx, client, configMap)
}

// isIngestorQueueMountCurrent checks whether the kubelet has propagated the
// updated ConfigMap to the volume mount on at least one pod. It reads the
// mounted local.meta and checks whether the expected checksum is present.
func isIngestorQueueMountCurrent(ctx context.Context, client splcommon.ControllerClient, cr *enterpriseApi.IngestorCluster, expectedChecksum string) (bool, error) {
	podName := GetSplunkStatefulsetPodName(SplunkIngestor, cr.GetName(), 0)
	podExecClient := splutil.GetPodExecClient(client, cr, podName)
	command := fmt.Sprintf("cat %s/local.meta", ingestorQueueConfigMountPath)
	streamOptions := splutil.NewStreamOptionsObject(command)
	stdout, _, err := podExecClient.RunPodExecCommand(ctx, streamOptions, []string{"/bin/sh"})
	if err != nil {
		return false, err
	}
	return strings.Contains(stdout, expectedChecksum), nil
}

// reloadIngestorApp triggers a Splunk app reload on every ingestor pod so that
// updated conf files (via ConfigMap) are picked up without a full pod restart.
// Two separate exec calls are used (matching the addTelApp pattern) because
// runCustomCommandOnSplunkPods pipes the command to /bin/sh via stdin.
var reloadIngestorApp = func(ctx context.Context, client splcommon.ControllerClient, cr *enterpriseApi.IngestorCluster) error {
	reqLogger := log.FromContext(ctx)
	scopedLog := reqLogger.WithName("reloadIngestorApp").WithValues(
		"name", cr.GetName(),
		"namespace", cr.GetNamespace())

	podExecClient := splutil.GetPodExecClient(client, cr, "")

	// Step 1: Copy all conf files from ConfigMap mount into the app directory.
	// Files are always copied (not symlinked) because Splunk replaces symlinks
	// with regular files when it modifies content (e.g. encrypting credentials).
	err := runCustomCommandOnSplunkPods(ctx, cr, cr.Spec.Replicas, ingestorQueueConfigCopyConfString, podExecClient)
	if err != nil {
		scopedLog.Error(err, "Failed to copy conf files on ingestor pods")
		return err
	}

	// Step 2: POST to _reload endpoint
	err = runCustomCommandOnSplunkPods(ctx, cr, cr.Spec.Replicas, ingestorQueueConfigReloadString, podExecClient)
	if err != nil {
		scopedLog.Error(err, "Failed to reload ingestor app on pods")
		return err
	}

	scopedLog.Info("Successfully triggered app reload on ingestor pods")
	return nil
}

// setupIngestorInitContainer adds an init container with both the etc volume and
// the queue config ConfigMap volume mounted, so it can create the app directory
// structure and symlink the config files.
func setupIngestorInitContainer(podTemplateSpec *corev1.PodTemplateSpec, image string, imagePullPolicy string, commandOnContainer string, isEtcVolEph bool) {
	var etcVolMntName string

	if isEtcVolEph {
		etcVolMntName = fmt.Sprintf(splcommon.SplunkMountNamePrefix, splcommon.EtcVolumeStorage)
	} else {
		etcVolMntName = fmt.Sprintf(splcommon.PvcNamePrefix, splcommon.EtcVolumeStorage)
	}

	runAsUser := int64(41812)
	runAsNonRoot := true
	privileged := false
	containerSpec := corev1.Container{
		Image:           image,
		ImagePullPolicy: corev1.PullPolicy(imagePullPolicy),
		Name:            "init-ingestor-queue-config",
		Command:         []string{"bash", "-c", commandOnContainer},
		VolumeMounts: []corev1.VolumeMount{
			{Name: etcVolMntName, MountPath: "/opt/splk/etc"},
			{Name: "mnt-splunk-queue-config", MountPath: ingestorQueueConfigMountPath},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("0.25"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsUser:                &runAsUser,
			RunAsNonRoot:             &runAsNonRoot,
			AllowPrivilegeEscalation: &[]bool{false}[0],
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{
					"ALL",
				},
				Add: []corev1.Capability{
					"NET_BIND_SERVICE",
				},
			},
			Privileged: &privileged,
			SeccompProfile: &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeRuntimeDefault,
			},
		},
	}
	podTemplateSpec.Spec.InitContainers = append(podTemplateSpec.Spec.InitContainers, containerSpec)
}
