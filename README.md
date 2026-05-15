# LogPilot

LogPilot is a Kubernetes operator for collecting workload logs, Kubernetes
Events, and Kubernetes object state snapshots. It deploys and manages:

- `log-pilot-api`: admission webhook and runtime API service.
- `log-pilot-agent`: DaemonSet that runs on each node and ships collected data.
- `logpilot-controller-manager`: operator controller that reconciles LogPilot
  resources into the runtime components.

## What LogPilot Collects

- Container stdout, stderr, and application file logs through
  `LogPilotPolicy`.
- Kubernetes Events through cluster-scoped `ClusterLogPilotPolicy` resources.
- Kubernetes object state snapshots through cluster-scoped
  `ClusterLogPilotPolicy` resources.

Object state collection currently covers Pods, Nodes, Deployments, StatefulSets,
DaemonSets, and Jobs. The emitted state includes fields useful for correlating
logs with runtime failures, such as Pod phase, conditions, container statuses,
restart counts, last termination state, exit code, node pressure conditions,
capacity, allocatable resources, workload replica status, and rollout
conditions.

## Requirements

- Go
- Docker
- kubectl
- kind
- kubebuilder/controller-runtime toolchain installed through the project
  `Makefile` targets

## Quick Start With kind

Create a local cluster and load the LogPilot image into it:

```bash
kind create cluster --name logpilot-demo
make docker-build IMG=logpilot:dev
kind load docker-image logpilot:dev --name logpilot-demo
```

Deploy the operator and CRDs:

```bash
make deploy IMG=logpilot:dev API_IMG=logpilot:dev AGENT_IMG=logpilot:dev
kubectl -n logpilot-system rollout status deployment/logpilot-controller-manager
```

Create the LogPilot runtime:

```bash
kubectl apply -f config/samples/logpilot_v1alpha1_logpilot.yaml
kubectl -n logpilot-system rollout status deployment/log-pilot-api
kubectl -n logpilot-system rollout status daemonset/log-pilot-agent
```

The sample runtime uses the images passed through `API_IMG` and `AGENT_IMG`
during deployment.

## Collect Container Logs

Use `LogPilotPolicy` for namespace-scoped workload log collection. A policy
selects Pods, declares which containers and paths to collect, and configures
where records are sent.

```yaml
apiVersion: logpilot.logpilot.jimyag.com/v1alpha1
kind: LogPilotPolicy
metadata:
  name: app-logs
  namespace: default
spec:
  selector:
    matchLabels:
      app: demo
  containerSelector:
    names:
      - app
  volume:
    name: app-logs
    mountPath: /var/log/app
  paths:
    - /var/log/app/*.log
  transforms:
    - type: json
  outputs:
    - name: collector
      type: http
      url: http://log-collector.default.svc.cluster.local:8080/ingest
```

Apply the included sample:

```bash
kubectl apply -f config/samples/logpilot_v1alpha1_logpilotpolicy.yaml
```

Pods created after a matching policy exists are mutated by the runtime API so
the agent can collect the configured log files reliably.

## Collect Kubernetes Events

Use a cluster-scoped `ClusterLogPilotPolicy` with the `k8sEvent` input:

```yaml
apiVersion: logpilot.logpilot.jimyag.com/v1alpha1
kind: ClusterLogPilotPolicy
metadata:
  name: k8s-events
spec:
  input:
    type: k8sEvent
    config:
      namespaces:
        - default
  outputs:
    - name: collector
      type: http
      url: http://log-collector.default.svc.cluster.local:8080/ingest
```

Apply the included sample:

```bash
kubectl apply -f config/samples/logpilot_v1alpha1_clusterlogpilotpolicy.yaml
```

The agent performs an initial list and then watches Events from the Kubernetes
API. Only one agent instance actively runs a cluster-scoped collector at a time.

## Collect Kubernetes Object State

Use a cluster-scoped `ClusterLogPilotPolicy` with the `k8sObjectState` input:

```yaml
apiVersion: logpilot.logpilot.jimyag.com/v1alpha1
kind: ClusterLogPilotPolicy
metadata:
  name: k8s-object-state
spec:
  input:
    type: k8sObjectState
    config:
      resources:
        - pods
        - nodes
        - deployments
        - statefulsets
        - daemonsets
        - jobs
      namespaces:
        - default
        - logpilot-system
  outputs:
    - name: collector
      type: http
      url: http://log-collector.default.svc.cluster.local:8080/ingest
```

Apply the included sample:

```bash
kubectl apply -f config/samples/logpilot_v1alpha1_clusterlogpilotpolicy_state.yaml
```

Object state records include the object kind, namespace, name, event action, and
a normalized state payload. This makes it possible to correlate logs with
signals such as `OOMKilled`, container restarts, node pressure, rollout
progress, unavailable replicas, and failed Jobs.

## Outputs And Transforms

Supported output types:

- `http`: POST records to an HTTP endpoint.
- `file`: append records to a file path on the agent.

Supported transform types:

- `json`: parse JSON log lines into structured fields.
- `label`: add static labels to records.
- `drop`: drop records matching the transform configuration.

## Check The Deployment

Inspect runtime Pods:

```bash
kubectl -n logpilot-system get pods
kubectl -n logpilot-system get deployment log-pilot-api
kubectl -n logpilot-system get daemonset log-pilot-agent
```

Check policy status:

```bash
kubectl get logpilotpolicies -A
kubectl get clusterlogpilotpolicies
kubectl describe clusterlogpilotpolicy k8s-events
kubectl describe clusterlogpilotpolicy k8s-object-state
```

Inspect agent logs:

```bash
kubectl -n logpilot-system logs daemonset/log-pilot-agent -f
```

For local performance and end-to-end validation, use the dedicated kind helper:

```bash
PERF_CLUSTER_NAME=logpilot-perf PERF_KEEP_CLUSTER=false bash hack/perf-kind.sh
```

## Cleanup

Delete sample resources:

```bash
kubectl delete -f config/samples/logpilot_v1alpha1_clusterlogpilotpolicy_state.yaml --ignore-not-found
kubectl delete -f config/samples/logpilot_v1alpha1_clusterlogpilotpolicy.yaml --ignore-not-found
kubectl delete -f config/samples/logpilot_v1alpha1_logpilotpolicy.yaml --ignore-not-found
kubectl delete -f config/samples/logpilot_v1alpha1_logpilot.yaml --ignore-not-found
```

Remove the operator and CRDs:

```bash
make undeploy
make uninstall
kind delete cluster --name logpilot-demo
```

## Development

Regenerate manifests and generated code after API or marker changes:

```bash
make manifests
make generate
```

Run local checks:

```bash
make test
make build
CERT_MANAGER_INSTALL_SKIP=true KIND_CLUSTER=logpilot-e2e-state make test-e2e
```

E2E tests require an isolated kind cluster. Do not run them against a shared
development or production cluster.

## License

[Apache 2.0](LICENSE)
