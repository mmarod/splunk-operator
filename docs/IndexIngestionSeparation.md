---
title: Index and Ingestion Separation
parent: Deploy & Configure
nav_order: 6
---

# Background

Separation between ingestion and indexing services within Splunk Operator for Kubernetes enables the operator to independently manage the ingestion service while maintaining seamless integration with the indexing service.

This separation enables:
- Independent scaling: Match resource allocation to ingestion or indexing workload.
- Data durability: Off-load buffer management and retry logic to a durable message queue.
- Operational clarity: Separate monitoring dashboards for ingestion throughput vs indexing latency.

## Splunk Support

These features are supported for Splunk 10.2 and above versions.

# Important Note

> [!WARNING]
> **For customers deploying SmartBus on CMP, the Splunk Operator for Kubernetes (SOK) manages the configuration and lifecycle of the ingestor tier. The following SOK guide provides implementation details for setting up ingestion separation and integrating with existing indexers. This reference is primarily intended for CMP users leveraging SOK-managed ingestors.**

# Document Variables

- SPLUNK_IMAGE_VERSION: Splunk Enterprise Docker Image version

# Queue

Queue is introduced to store message queue information to be shared among IngestorCluster and IndexerCluster.

## Spec

Queue inputs can be found in the table below. As of now, only SQS provider of message queue is supported.

| Key        | Type    | Description                                       |
| ---------- | ------- | ------------------------------------------------- |
| provider   | string | [Required] Provider of message queue (Allowed values: sqs, sqs_cp) |
| sqs   | SQS | [Required if provider=sqs or provider=sqs_cp] SQS message queue inputs  |

### SQS Spec

| Key        | Type    | Splunk conf key | Description                                       |
| ---------- | ------- | --------------- | ------------------------------------------------- |
| name   | string | stanza name | [Required] Name of the queue |
| authRegion   | string | `auth_region` | [Required] Region where the queue is located  |
| endpoint   | string | `endpoint` | [Optional] AWS SQS Service endpoint |
| dlq   | string | `dead_letter_queue.name` | [Required] Name of the dead letter queue |
| volumes | []VolumeSpec | N/A | [Optional] List of remote storage volumes used to mount the credentials for queue and bucket access (must contain s3_access_key and s3_secret_key) |
| maxConnections | *int32 | `max_connections` | [Optional] Maximum number of connections to the SQS service |
| messageGroupID | string | `message_group_id` | [Optional] Message group ID for FIFO queues |
| retryPolicy | string | `retry_policy` | [Optional] Retry policy for failed messages (Allowed values: max_count, none) |
| maxRetriesPerPart | *int32 | `max_count.max_retries_per_part` | [Optional] Maximum retries per part when retryPolicy is max_count |
| timeoutConnect | *int32 | `timeout.connect` | [Optional] Connection timeout in seconds |
| timeoutRead | *int32 | `timeout.read` | [Optional] Read timeout in seconds |
| timeoutWrite | *int32 | `timeout.write` | [Optional] Write timeout in seconds |
| timeoutReceiveMessage | *int32 | `timeout.receive_message` | [Optional] Receive message timeout in seconds |
| timeoutVisibility | *int32 | `timeout.visibility` | [Optional] Visibility timeout in seconds |
| bufferVisibility | *int32 | `buffer.visibility` | [Optional] Buffer visibility in seconds |
| executorMaxWorkersCount | *int32 | `executor_max_workers_count` | [Optional] Maximum number of executor worker threads |
| minPendingMessages | *int32 | `min_pending_messages` | [Optional] Minimum number of pending messages before sending |
| renewRetries | *int32 | `renew_retries` | [Optional] Number of retries for renewing message visibility |
| encodingFormat | string | `encoding_format` | [Optional] Encoding format for messages (e.g. "s2s") |
| sendInterval | string | `send_interval` | [Optional] Interval between send operations (e.g. "5s") |
| dlqProcessInterval | string | `dead_letter_queue.process_interval` | [Optional] Dead letter queue process interval (e.g. "1d") |
| enableSharedReceipts | *bool | `enable_shared_receipts` | [Optional] Whether to enable shared receipts |

All fields are mutable. Optional fields are only written to conf files when explicitly set on the CR. Fields that are not set are omitted entirely from the generated configuration, allowing Splunk defaults or values from other configuration layers (e.g. defaults.yaml) to take effect.

## Example

```yaml
apiVersion: enterprise.splunk.com/v4
kind: Queue
metadata:
  name: queue
spec:
  provider: sqs
  sqs:
    name: sqs-test
    authRegion: us-west-2
    endpoint: https://sqs.us-west-2.amazonaws.com
    dlq: sqs-dlq-test
    volumes:
      - name: s3-sqs-volume
        secretRef: s3-secret
```

Example with optional tuning fields:

```yaml
apiVersion: enterprise.splunk.com/v4
kind: Queue
metadata:
  name: queue
spec:
  provider: sqs
  sqs:
    name: sqs-test
    authRegion: us-west-2
    endpoint: https://sqs.us-west-2.amazonaws.com
    dlq: sqs-dlq-test
    retryPolicy: max_count
    maxRetriesPerPart: 4
    encodingFormat: s2s
    sendInterval: "5s"
    enableSharedReceipts: true
    volumes:
      - name: s3-sqs-volume
        secretRef: s3-secret
```

# ObjectStorage

ObjectStorage is introduced to store large messages (messages that exceed the size of messages that can be stored in SQS) to be shared among IngestorCluster and IndexerCluster.

## Spec

ObjectStorage inputs can be found in the table below. As of now, only S3 provider of object storage is supported.

| Key        | Type    | Description                                       |
| ---------- | ------- | ------------------------------------------------- |
| provider   | string | [Required] Provider of object storage (Allowed values: s3) |
| s3   | S3 | [Required if provider=s3] S3 object storage inputs  |

### S3 Spec

| Key        | Type    | Splunk conf key suffix | Description                                       |
| ---------- | ------- | ---------------------- | ------------------------------------------------- |
| path   | string | `large_message_store.path` | [Required] Remote storage location for messages that are larger than the underlying maximum message size  |
| endpoint   | string | `large_message_store.endpoint` | [Optional] S3-compatible service endpoint |
| sslVerifyServerCert | *bool | `large_message_store.sslVerifyServerCert` | [Optional] Whether to verify the server's SSL certificate |
| sslVersions | string | `large_message_store.sslVersions` | [Optional] Comma-separated list of SSL versions to support |
| sslCommonNameToCheck | string | `large_message_store.sslCommonNameToCheck` | [Optional] Common name to check in the server's SSL certificate |
| sslAltNameToCheck | string | `large_message_store.sslAltNameToCheck` | [Optional] Alternate name to check in the server's SSL certificate |
| sslRootCAPath | string | `large_message_store.sslRootCAPath` | [Optional] Path to the root CA certificate file inside the Splunk container (must be mounted via volumes) |
| cipherSuite | string | `large_message_store.cipherSuite` | [Optional] Cipher suite string for SSL connections |
| ecdhCurves | string | `large_message_store.ecdhCurves` | [Optional] ECDH curves for SSL connections |
| dhFile | string | `large_message_store.dhFile` | [Optional] Path to the Diffie-Hellman parameter file inside the Splunk container (must be mounted via volumes) |
| encryptionScheme | string | `large_message_store.encryption_scheme` | [Optional] Encryption scheme for data at rest (e.g. "SSE-S3", "SSE-KMS") |
| kmsEndpoint | string | `large_message_store.kms_endpoint` | [Optional] KMS endpoint URL for encryption key management |
| keyID | string | `large_message_store.key_id` | [Optional] KMS key ID for encryption |
| keyRefreshInterval | string | `large_message_store.key_refresh_interval` | [Optional] Interval for refreshing the encryption key (e.g. "1d") |

All fields are mutable. Optional fields are only written to conf files when explicitly set on the CR.

## Example

```yaml
apiVersion: enterprise.splunk.com/v4
kind: ObjectStorage
metadata:
  name: os
spec:
  provider: s3
  s3:
    path: ingestion/smartbus-test
    endpoint: https://s3.us-west-2.amazonaws.com
```

# IngestorCluster

IngestorCluster is introduced for high-throughput data ingestion into a durable message queue. Its Splunk pods are configured to receive events (outputs.conf) and publish them to a message queue.

## Spec

In addition to common spec inputs, the IngestorCluster resource provides the following Spec configuration parameters.

| Key        | Type    | Description                                       |
| ---------- | ------- | ------------------------------------------------- |
| replicas   | integer | The number of replicas (defaults to 3) |
| queueRef   | corev1.ObjectReference | Message queue reference |
| objectStorageRef   | corev1.ObjectReference | Object storage reference |

All fields are mutable. Changing queueRef or objectStorageRef triggers an in-place reload without pod restart.

**First provisioning or scaling up the number of replicas requires Ingestor Cluster Splunkd restart, but this restart is implemented automatically and done by SOK.**

## Example

The example presented below configures IngestorCluster named ingestor with Splunk ${SPLUNK_IMAGE_VERSION} image that resides in a default namespace and is scaled to 3 replicas that serve the ingestion traffic. This IngestorCluster custom resource is set up with the s3-secret credentials allowing it to perform SQS and S3 operations. Queue and ObjectStorage references allow the user to specify queue and bucket settings for the ingestion process.

In this case, the setup uses the SQS and S3 based configuration where the messages are stored in sqs-test queue in us-west-2 region with dead letter queue set to sqs-dlq-test queue. The object storage is set to ingestion bucket in smartbus-test directory. Based on these inputs, default-mode.conf and outputs.conf files are configured accordingly.

```yaml
apiVersion: enterprise.splunk.com/v4
kind: IngestorCluster
metadata:
  name: ingestor
  finalizers:
    - enterprise.splunk.com/delete-pvc
spec:
  serviceAccount: ingestor-sa
  replicas: 3
  image: splunk/splunk:${SPLUNK_IMAGE_VERSION}
  queueRef:
    name: queue
  objectStorageRef:
    name: os
```

## Configuration Management

The operator manages IngestorCluster Splunk configuration through a Kubernetes ConfigMap and a Splunk app called `100-sok-ingestorcluster`. This section describes how configuration is delivered to pods and how changes are applied without restarting them.

### App structure

The operator creates a ConfigMap named `splunk-<name>-ingestor-queue-config` containing four files that form a Splunk app:

```
100-sok-ingestorcluster/
  local/
    app.conf            # App metadata (enabled, non-visible)
    outputs.conf        # Queue and object storage settings from Queue/ObjectStorage CRs
    default-mode.conf   # Pipeline stanzas (remote queue routing, typing, indexerPipe)
  metadata/
    local.meta          # ACLs + install_source_checksum for reload detection
```

An init container runs on each pod at startup to create the app directory structure under `/opt/splunk/etc/apps/`. All four conf files are copied (not symlinked) from the ConfigMap mount at `/mnt/splunk-queue-config/` into the app directory. Copies are used instead of symlinks because Splunk replaces symlinks with regular files when it modifies content (e.g. encrypting credentials in outputs.conf, writing metadata stanzas in local.meta).

### Reload on change

When Queue or ObjectStorage configuration changes, the operator updates the ConfigMap and triggers an in-place reload using a deferred checksum-based mechanism:

1. The operator computes a SHA-256 checksum of `outputs.conf` + `default-mode.conf` and embeds it in `local.meta` as an `install_source_checksum` stanza.
2. `ApplyConfigMap` compares the new ConfigMap data against the existing data in etcd and returns whether anything changed.
3. If the data changed and the IngestorCluster is in `PhaseReady`, the operator stores the expected checksum in `cr.Status.QueueConfigExpectedChecksum` and requeues after 5 seconds. This delay allows the kubelet to propagate the ConfigMap update to the volume mount on each pod.
4. On subsequent reconciles, the operator execs into pod 0 and reads the mounted `local.meta` to verify the expected checksum is present. If the mount has not yet propagated, the operator requeues again (indefinitely, every 5 seconds) until the content matches.
5. Once the mount is current, the operator executes two commands on each pod via `kubectl exec`:
   - Copies all conf files from the ConfigMap mount into the app directory.
   - POSTs to `/services/apps/local/_reload` on `localhost:8089`, which causes Splunk to detect the changed `install_source_checksum` and reload the app's conf files.
6. The operator clears `QueueConfigExpectedChecksum` from the status to indicate the reload is complete.

This avoids a full StatefulSet rolling restart for configuration-only changes. Pod restarts are only needed when the StatefulSet spec itself changes (image, resources, volumes, etc.).

### Observability

To verify the reload mechanism is working:

- **Operator logs**: Look for `Successfully triggered app reload on ingestor pods` after a Queue/ObjectStorage change.
- **Operator logs (propagation)**: Look for `ConfigMap changed, waiting for volume mount propagation` followed by `Volume mount not yet propagated, requeuing` (zero or more times) until propagation completes.
- **Pod age**: Should remain unchanged after a config update (no restart).
- **ConfigMap content**: `kubectl get configmap splunk-<name>-ingestor-queue-config -o jsonpath='{.data.local\.meta}'` should show the `install_source_checksum` stanza.
- **Splunk access log**: Should show a `POST /services/apps/local/_reload` entry with a `200` response.

# IndexerCluster

IndexerCluster is enhanced to support index-only mode enabling independent scaling, loss-safe buffering, and simplified day-0/day-n management via Kubernetes CRDs. Its Splunk pods are configured to pull events from the queue (inputs.conf) and index them.

## Spec

In addition to common spec inputs, the IndexerCluster resource provides the following Spec configuration parameters.

| Key        | Type    | Description                                       |
| ---------- | ------- | ------------------------------------------------- |
| replicas   | integer | The number of replicas (defaults to 3) |
| queueRef   | corev1.ObjectReference | Message queue reference |
| objectStorageRef   | corev1.ObjectReference | Object storage reference |

All fields are mutable.

**First provisioning or scaling up the number of replicas requires Indexer Cluster Splunkd restart, but this restart is implemented automatically and done by SOK.**

## Example

The example presented below configures IndexerCluster named indexer with Splunk ${SPLUNK_IMAGE_VERSION} image that resides in a default namespace and is scaled to 3 replicas that serve the indexing traffic. This IndexerCluster custom resource is set up with the s3-secret credentials allowing it to perform SQS and S3 operations. Queue and ObjectStorage references allow the user to specify queue and bucket settings for the indexing process.

In this case, the setup uses the SQS and S3 based configuration where the messages are stored in and retrieved from sqs-test queue in us-west-2 region with dead letter queue set to sqs-dlq-test queue. The object storage is set to ingestion bucket in smartbus-test directory. Based on these inputs, default-mode.conf, inputs.conf and outputs.conf files are configured accordingly.

```yaml
apiVersion: enterprise.splunk.com/v4
kind: ClusterManager
metadata:
  name: cm
  finalizers:
    - enterprise.splunk.com/delete-pvc
spec:
  serviceAccount: ingestor-sa
  image: splunk/splunk:${SPLUNK_IMAGE_VERSION}
---
apiVersion: enterprise.splunk.com/v4
kind: IndexerCluster
metadata:
  name: indexer
  finalizers:
    - enterprise.splunk.com/delete-pvc
spec:
  clusterManagerRef:
    name: cm
  serviceAccount: ingestor-sa
  replicas: 3
  image: splunk/splunk:${SPLUNK_IMAGE_VERSION}
  queueRef:
    name: queue
  objectStorageRef:
    name: os
```

# Common Spec

Common spec values for all SOK Custom Resources can be found in [CustomResources doc](CustomResources.md).

# Helm Charts

Queue, ObjectStorage and IngestorCluster have been added to the splunk/splunk-enterprise Helm chart. IndexerCluster has also been enhanced to support new inputs.

## Example

Below examples describe how to define values for Queue, ObjectStorage, IngestorCluster and IndexerCluster similarly to the above yaml files specifications.

```yaml
queue:
  enabled: true
  name: queue
  provider: sqs
  sqs:
    name: sqs-test
    authRegion: us-west-2
    endpoint: https://sqs.us-west-2.amazonaws.com
    dlq: sqs-dlq-test
    volumes:
        - name: s3-sqs-volume
          secretRef: s3-secret
```

```yaml
objectStorage:
  enabled: true
  name: os
  provider: s3
  s3:
    endpoint: https://s3.us-west-2.amazonaws.com
    path: ingestion/smartbus-test
```

```yaml
ingestorCluster:
  enabled: true
  name: ingestor
  replicaCount: 3
  serviceAccount: ingestor-sa
  queueRef:
    name: queue
  objectStorageRef:
    name: os
```

```yaml
clusterManager:
  enabled: true
  name: cm
  replicaCount: 1
  serviceAccount: ingestor-sa

indexerCluster:
  enabled: true
  name: indexer
  replicaCount: 3
  serviceAccount: ingestor-sa
  clusterManagerRef:
    name: cm
  queueRef:
    name: queue
  objectStorageRef:
    name: os
```

# Service Account

To be able to configure ingestion and indexing resources correctly in a secure manner, it is required to provide these resources with the service account that is configured with a minimum set of permissions to complete required operations. With this provided, the right credentials are used by Splunk to peform its tasks.

## Example

The example presented below configures the ingestor-sa service account by using eksctl utility. It sets up the service account for cluster-name cluster in region us-west-2 with AmazonS3FullAccess and AmazonSQSFullAccess access policies.

```
eksctl create iamserviceaccount \
  --name ingestor-sa \
  --cluster ind-ing-sep-demo \
  --region us-west-2 \
  --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess \
  --attach-policy-arn arn:aws:iam::aws:policy/AmazonSQSFullAccess \
  --approve \
  --override-existing-serviceaccounts
```

## Documentation References

- [IAM Roles for Service Accounts on eksctl Docs](https://eksctl.io/usage/iamserviceaccounts/)

# Horizontal Pod Autoscaler

To automatically adjust the number of replicas to serve the ingestion traffic effectively, it is recommended to use Horizontal Pod Autoscaler which scales the workload based on the actual demand. It enables the user to provide the metrics which are used to make decisions on removing unwanted replicas if there is not too much traffic or setting up the new ones if the traffic is too big to be handled by currently running resources.

## Example

The example presented below configures HorizontalPodAutoscaler named ingestor-hpa that resides in a default namespace (same namespace as resources it is managing) to scale IngestorCluster custom resource named ingestor. With average utilization set to 50, the HorizontalPodAutoscaler resource will try to keep the average utilization of the pods in the scaling target at 50%. It will be able to scale the replicas starting from the minimum number of 3 with the maximum number of 10 replicas.

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: ingestor-hpa
spec:
  scaleTargetRef:
    apiVersion: enterprise.splunk.com/v4
    kind: IngestorCluster
    name: ingestor
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 50
```

## Documentation References

- [Horizontal Pod Autoscaling on Kubernetes Docs](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)

# App Installation for Ingestor Cluster Instances

Application installation is supported for Ingestor Cluster instances. However, as of now, applications are installed using local scope and if any application requires Splunk restart, there is no automated way to detect it and trigger automatically via Splunk Operator.

Therefore, to be able to enforce Splunk restart for each of the Ingestor Cluster pods, it is recommended to add/update IngestorCluster CR annotations/labels and apply the new configuration which will trigger the rolling restart of Splunk pods for Ingestor Cluster.

# Example

1. Install CRDs and Splunk Operator for Kubernetes.

- SOK_IMAGE_VERSION: version of the image for Splunk Operator for Kubernetes

```
$ make install
```

```
$ kubectl apply -f ${SOK_IMAGE_VERSION}/splunk-operator-cluster.yaml --server-side
```

```
$ kubectl get po -n splunk-operator
NAME                                                  READY   STATUS    RESTARTS   AGE
splunk-operator-controller-manager-785b89d45c-dwfkd   2/2     Running   0          4d3h
```

2. Create a service account.

```
$ eksctl create iamserviceaccount \
  --name ingestor-sa \
  --cluster ind-ing-sep-demo \
  --region us-west-2 \
  --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess \
  --attach-policy-arn arn:aws:iam::aws:policy/AmazonSQSFullAccess \
  --approve \
  --override-existing-serviceaccounts
```

3. Install Queue resource.

```
$ cat queue.yaml
apiVersion: enterprise.splunk.com/v4
kind: Queue
metadata:
  name: queue
  finalizers:
    - enterprise.splunk.com/delete-pvc
spec:
  provider: sqs
  sqs:
    name: sqs-test
    authRegion: us-west-2
    endpoint: https://sqs.us-west-2.amazonaws.com
    dlq: sqs-dlq-test
```

```
$ kubectl apply -f queue.yaml
```

```
$ kubectl get queue
NAME   PHASE   AGE   MESSAGE
queue  Ready   20s
```

4. Install ObjectStorage resource.

```
$ cat os.yaml
apiVersion: enterprise.splunk.com/v4
kind: ObjectStorage
metadata:
  name: os
  finalizers:
    - enterprise.splunk.com/delete-pvc
spec:
  provider: s3
  s3:
    endpoint: https://s3.us-west-2.amazonaws.com
    path: ingestion/smartbus-test
```

```
$ kubectl apply -f os.yaml
```

```
$ kubectl get os
NAME   PHASE   AGE   MESSAGE
os    Ready   20s
```

5. Install IngestorCluster resource.

```
$ cat ingestor.yaml
apiVersion: enterprise.splunk.com/v4
kind: IngestorCluster
metadata:
  name: ingestor
  finalizers:
    - enterprise.splunk.com/delete-pvc
spec:
  serviceAccount: ingestor-sa
  replicas: 3
  image: splunk/splunk:${SPLUNK_IMAGE_VERSION}
  queueRef:
    name: queue
  objectStorageRef:
    name: os
```

```
$ kubectl apply -f ingestor.yaml
```

```
$ kubectl get po
NAME                         READY   STATUS    RESTARTS   AGE
splunk-ingestor-ingestor-0   1/1     Running   0          2m12s
splunk-ingestor-ingestor-1   1/1     Running   0          2m12s
splunk-ingestor-ingestor-2   1/1     Running   0          2m12s
```

Verify the configuration on the ingestor pods. The conf files are in the `100-sok-ingestorcluster` app directory:

```
$ kubectl exec -it splunk-ingestor-ingestor-0 -- cat /opt/splunk/etc/apps/100-sok-ingestorcluster/local/outputs.conf
[remote_queue:sqs-test]
remote_queue.type = sqs_smartbus
remote_queue.sqs_smartbus.auth_region = us-west-2
remote_queue.sqs_smartbus.endpoint = https://sqs.us-west-2.amazonaws.com
remote_queue.sqs_smartbus.large_message_store.endpoint = https://s3.us-west-2.amazonaws.com
remote_queue.sqs_smartbus.large_message_store.path = s3://ingestion/smartbus-test
remote_queue.sqs_smartbus.dead_letter_queue.name = sqs-dlq-test
```

```
$ kubectl exec -it splunk-ingestor-ingestor-0 -- cat /opt/splunk/etc/apps/100-sok-ingestorcluster/local/default-mode.conf
[pipeline:remotequeueruleset]
disabled = false

[pipeline:ruleset]
disabled = true

[pipeline:remotequeuetyping]
disabled = false

[pipeline:remotequeueoutput]
disabled = false

[pipeline:typing]
disabled = true

[pipeline:indexerPipe]
disabled = true
```

6. Update Queue configuration without pod restart.

After initial provisioning, you can update Queue fields and the operator will apply them in-place:

```
$ kubectl patch queue queue --type merge -p '{"spec":{"sqs":{"enableSharedReceipts":true,"encodingFormat":"s2s","sendInterval":"5s"}}}'
```

Verify the updated configuration appears on the pod (the operator will copy and reload within a few reconcile cycles):

```
$ kubectl exec -it splunk-ingestor-ingestor-0 -- cat /opt/splunk/etc/apps/100-sok-ingestorcluster/local/outputs.conf
[remote_queue:sqs-test]
remote_queue.type = sqs_smartbus
remote_queue.sqs_smartbus.auth_region = us-west-2
remote_queue.sqs_smartbus.endpoint = https://sqs.us-west-2.amazonaws.com
remote_queue.sqs_smartbus.large_message_store.endpoint = https://s3.us-west-2.amazonaws.com
remote_queue.sqs_smartbus.large_message_store.path = s3://ingestion/smartbus-test
remote_queue.sqs_smartbus.dead_letter_queue.name = sqs-dlq-test
remote_queue.sqs_smartbus.encoding_format = s2s
remote_queue.sqs_smartbus.send_interval = 5s
remote_queue.sqs_smartbus.enable_shared_receipts = true
```

7. Install IndexerCluster resource.

```
$ cat idxc.yaml
apiVersion: enterprise.splunk.com/v4
kind: ClusterManager
metadata:
  name: cm
  finalizers:
    - enterprise.splunk.com/delete-pvc
spec:
  image: splunk/splunk:${SPLUNK_IMAGE_VERSION}
  serviceAccount: ingestor-sa
---
apiVersion: enterprise.splunk.com/v4
kind: IndexerCluster
metadata:
  name: indexer
  finalizers:
    - enterprise.splunk.com/delete-pvc
spec:
  image: splunk/splunk:${SPLUNK_IMAGE_VERSION}
  replicas: 3
  clusterManagerRef:
    name: cm
  serviceAccount: ingestor-sa
  queueRef:
    name: queue
  objectStorageRef:
    name: os
```

```
$ kubectl apply -f idxc.yaml
```

```
$ kubectl get po
NAME                          READY   STATUS    RESTARTS   AGE
splunk-cm-cluster-manager-0   1/1     Running   0          15m
splunk-indexer-indexer-0      1/1     Running   0          12m
splunk-indexer-indexer-1      1/1     Running   0          12m
splunk-indexer-indexer-2      1/1     Running   0          12m
splunk-ingestor-ingestor-0    1/1     Running   0          27m
splunk-ingestor-ingestor-1    1/1     Running   0          29m
splunk-ingestor-ingestor-2    1/1     Running   0          31m
```

8. Install Horizontal Pod Autoscaler for IngestorCluster.

```
$ cat hpa-ing.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: ing-hpa
spec:
  scaleTargetRef:
    apiVersion: enterprise.splunk.com/v4
    kind: IngestorCluster
    name: ingestor
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 50
```

```
$ kubectl apply -f hpa-ing.yaml
```

```
$ kubectl get hpa
NAME      REFERENCE                  TARGETS              MINPODS   MAXPODS   REPLICAS   AGE
ing-hpa   IngestorCluster/ingestor   cpu: <unknown>/50%   3         10        0          10s
```

## Documentation References

- [Horizontal Pod Autoscaling on Kubernetes Docs](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- [kube-prometheus-stack](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack)
