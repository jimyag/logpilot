#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${PERF_CLUSTER_NAME:-logpilot-perf}"
NODE_IMAGE="${PERF_NODE_IMAGE:-kindest/node:v1.35.1}"
LOGPILOT_IMAGE="${PERF_LOGPILOT_IMAGE:-logpilot:perf}"
COLLECTOR_IMAGE="${PERF_COLLECTOR_IMAGE:-logpilot-perf-collector:latest}"
WRITER_IMAGE="${PERF_WRITER_IMAGE:-logpilot-perf-writer:latest}"
NAMESPACE="${PERF_NAMESPACE:-logpilot-perf}"
REPLICAS="${PERF_REPLICAS:-6}"
LINES="${PERF_LINES:-20000}"
LINE_BYTES="${PERF_LINE_BYTES:-200}"
TIMEOUT_SECONDS="${PERF_TIMEOUT_SECONDS:-300}"
KEEP_CLUSTER="${PERF_KEEP_CLUSTER:-true}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
PF_PID=""

cleanup() {
  if [[ -n "${PF_PID}" ]]; then
    kill "${PF_PID}" >/dev/null 2>&1 || true
  fi
  rm -rf "${TMP_DIR}"
  if [[ "${KEEP_CLUSTER}" == "false" ]]; then
    kind delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require kind
require docker
require kubectl
require go
require curl
require python3

if kind get clusters | grep -qx "${CLUSTER_NAME}"; then
  kind delete cluster --name "${CLUSTER_NAME}"
fi

cat >"${TMP_DIR}/kind.yaml" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  image: ${NODE_IMAGE}
- role: worker
  image: ${NODE_IMAGE}
- role: worker
  image: ${NODE_IMAGE}
EOF

kind create cluster --name "${CLUSTER_NAME}" --config "${TMP_DIR}/kind.yaml"
kind export kubeconfig --name "${CLUSTER_NAME}"

build_logpilot_image() {
  (
    cd "${ROOT_DIR}"
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/log-pilot-operator ./cmd/log-pilot-operator
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/log-pilot-agent ./cmd/log-pilot-agent
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/log-pilot-api ./cmd/log-pilot-api
  )
  mkdir -p "${TMP_DIR}/logpilot-image"
  cp "${ROOT_DIR}/bin/log-pilot-operator" "${ROOT_DIR}/bin/log-pilot-agent" "${ROOT_DIR}/bin/log-pilot-api" "${TMP_DIR}/logpilot-image/"
  docker build -t "${LOGPILOT_IMAGE}" -f- "${TMP_DIR}/logpilot-image" <<'EOF'
FROM scratch
COPY log-pilot-operator /usr/local/bin/log-pilot-operator
COPY log-pilot-agent /usr/local/bin/log-pilot-agent
COPY log-pilot-api /usr/local/bin/log-pilot-api
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/log-pilot-operator"]
EOF
}

build_collector_image() {
  cat >"${TMP_DIR}/collector.go" <<'EOF'
package main

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type stats struct {
	Requests   int64   `json:"requests"`
	Records    int64   `json:"records"`
	Unique     int     `json:"unique"`
	Duplicates int64   `json:"duplicates"`
	Bytes      int64   `json:"bytes"`
	StartedAt  string  `json:"startedAt"`
	ElapsedSec float64 `json:"elapsedSec"`
}

var (
	startedAt = time.Now()
	requests  int64
	records   int64
	dupes     int64
	bytesIn   int64
	mu        sync.Mutex
	seen      = map[string]struct{}{}
)

func main() {
	http.HandleFunc("/ingest", ingest)
	http.HandleFunc("/stats", handleStats)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}

func ingest(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	atomic.AddInt64(&requests, 1)
	atomic.AddInt64(&bytesIn, int64(len(body)))

	var entries []map[string]interface{}
	if err := json.Unmarshal(body, &entries); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	atomic.AddInt64(&records, int64(len(entries)))

	mu.Lock()
	for _, entry := range entries {
		key := ""
		if pod, ok := entry["pod"].(string); ok {
			key += pod
		}
		key += "/"
		if seq, ok := entry["seq"].(string); ok {
			key += seq
		}
		if key == "/" {
			continue
		}
		if _, exists := seen[key]; exists {
			atomic.AddInt64(&dupes, 1)
			continue
		}
		seen[key] = struct{}{}
	}
	mu.Unlock()
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func handleStats(w http.ResponseWriter, _ *http.Request) {
	mu.Lock()
	unique := len(seen)
	mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats{
		Requests:   atomic.LoadInt64(&requests),
		Records:    atomic.LoadInt64(&records),
		Unique:     unique,
		Duplicates: atomic.LoadInt64(&dupes),
		Bytes:      atomic.LoadInt64(&bytesIn),
		StartedAt:  startedAt.Format(time.RFC3339),
		ElapsedSec: time.Since(startedAt).Seconds(),
	})
}
EOF
  mkdir -p "${TMP_DIR}/collector-image"
  CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o "${TMP_DIR}/collector-image/collector" "${TMP_DIR}/collector.go"
  docker build -t "${COLLECTOR_IMAGE}" -f- "${TMP_DIR}/collector-image" <<'EOF'
FROM scratch
COPY collector /collector
USER 65532:65532
ENTRYPOINT ["/collector"]
EOF
}

build_writer_image() {
  cat >"${TMP_DIR}/writer.go" <<'EOF'
package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func envInt(key string, def int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return v
}

func main() {
	lines := envInt("LINES", 10000)
	lineBytes := envInt("LINE_BYTES", 200)
	path := os.Getenv("LOG_PATH")
	if path == "" {
		path = "/app/logs/app.log"
	}
	pod := os.Getenv("POD_NAME")
	if pod == "" {
		pod = "unknown"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		panic(err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1024*1024)
	payload := strings.Repeat("x", lineBytes)
	start := time.Now()
	for i := 0; i < lines; i++ {
		_, _ = fmt.Fprintf(w, "{\"pod\":\"%s\",\"seq\":\"%d\",\"payload\":\"%s\"}\n", pod, i, payload)
		if i%1000 == 0 {
			_ = w.Flush()
		}
	}
	_ = w.Flush()
	fmt.Printf("wrote %d lines in %s\n", lines, time.Since(start))
	time.Sleep(10 * time.Minute)
}
EOF
  mkdir -p "${TMP_DIR}/writer-image"
  CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o "${TMP_DIR}/writer-image/writer" "${TMP_DIR}/writer.go"
  docker build -t "${WRITER_IMAGE}" -f- "${TMP_DIR}/writer-image" <<'EOF'
FROM scratch
COPY writer /writer
ENTRYPOINT ["/writer"]
EOF
}

deploy_logpilot() {
  (
    cd "${ROOT_DIR}"
    make manifests kustomize
  )
  cp -R "${ROOT_DIR}/config" "${TMP_DIR}/config"
  (
    cd "${TMP_DIR}/config/manager"
    "${ROOT_DIR}/bin/kustomize" edit set image "controller=${LOGPILOT_IMAGE}"
  )
  "${ROOT_DIR}/bin/kustomize" build "${TMP_DIR}/config/default" | kubectl apply -f -
  kubectl -n logpilot-system set env deployment/logpilot-controller-manager \
    "LOG_PILOT_API_IMAGE=${LOGPILOT_IMAGE}" "LOG_PILOT_AGENT_IMAGE=${LOGPILOT_IMAGE}"
  kubectl -n logpilot-system rollout status deployment/logpilot-controller-manager --timeout=180s
  kubectl -n logpilot-system apply -f - <<EOF
apiVersion: logpilot.logpilot.jimyag.com/v1alpha1
kind: LogPilot
metadata:
  name: logpilot
spec:
  api:
    replicas: 1
  agent: {}
EOF
  for _ in $(seq 1 90); do
    if kubectl -n logpilot-system get deployment/log-pilot-api daemonset/log-pilot-agent >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
  kubectl -n logpilot-system rollout status deployment/log-pilot-api --timeout=240s
  kubectl -n logpilot-system rollout status daemonset/log-pilot-agent --timeout=240s
}

run_benchmark() {
  kubectl create namespace "${NAMESPACE}"
  kubectl -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: collector
spec:
  replicas: 1
  selector:
    matchLabels:
      app: collector
  template:
    metadata:
      labels:
        app: collector
    spec:
      containers:
      - name: collector
        image: ${COLLECTOR_IMAGE}
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: collector
spec:
  selector:
    app: collector
  ports:
  - port: 8080
    targetPort: 8080
EOF
  kubectl -n "${NAMESPACE}" rollout status deployment/collector --timeout=120s
  kubectl -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: logpilot.logpilot.jimyag.com/v1alpha1
kind: LogPilotPolicy
metadata:
  name: perf-writer
spec:
  selector:
    matchLabels:
      app: perf-writer
  containers:
  - name: app
    logType: applog
    path: /app/logs
    collector: host
    delivery: guaranteed
    batchLen: 1000
    transforms:
    - type: json
    output:
      type: http
      config:
        url: http://collector.${NAMESPACE}.svc.cluster.local:8080/ingest
    clean:
      strategy: never
EOF

  expected=$((REPLICAS * LINES))
  start_epoch="$(date +%s)"
  kubectl -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: perf-writer
spec:
  replicas: ${REPLICAS}
  selector:
    matchLabels:
      app: perf-writer
  template:
    metadata:
      labels:
        app: perf-writer
    spec:
      topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: kubernetes.io/hostname
        whenUnsatisfiable: ScheduleAnyway
        labelSelector:
          matchLabels:
            app: perf-writer
      containers:
      - name: app
        image: ${WRITER_IMAGE}
        imagePullPolicy: IfNotPresent
        env:
        - name: LINES
          value: "${LINES}"
        - name: LINE_BYTES
          value: "${LINE_BYTES}"
EOF
  kubectl -n "${NAMESPACE}" rollout status deployment/perf-writer --timeout=180s

  kubectl -n "${NAMESPACE}" port-forward svc/collector 18080:8080 >"${TMP_DIR}/collector-port-forward.log" 2>&1 &
  PF_PID="$!"
  sleep 2

  echo "expected_records=${expected}"
  while true; do
    stats="$(curl -fsS http://127.0.0.1:18080/stats)"
    unique="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["unique"])' <<<"${stats}")"
    elapsed=$(( $(date +%s) - start_epoch ))
    echo "elapsed=${elapsed}s unique=${unique}"
    if (( unique >= expected )); then
      break
    fi
    if (( elapsed >= TIMEOUT_SECONDS )); then
      echo "timed out waiting for expected records" >&2
      break
    fi
    sleep 5
  done

  final_stats="$(curl -fsS http://127.0.0.1:18080/stats)"
  printf '%s' "${final_stats}" | python3 -c '
import json
import sys
import time

expected = int(sys.argv[1])
start = int(sys.argv[2])
stats = json.load(sys.stdin)
elapsed = max(time.time() - start, 0.001)
unique = int(stats["unique"])
records = int(stats["records"])
dupes = int(stats["duplicates"])
missing = max(expected - unique, 0)
loss = (missing / expected * 100) if expected else 0.0
rate = unique / elapsed
print("summary")
print(f"  expected: {expected}")
print(f"  records_received: {records}")
print(f"  unique_received: {unique}")
print(f"  duplicates: {dupes}")
print(f"  missing: {missing}")
print(f"  loss_percent: {loss:.4f}")
print(f"  elapsed_seconds: {elapsed:.2f}")
print(f"  unique_records_per_second: {rate:.2f}")
print("  ingest_requests: {}".format(stats["requests"]))
print("  ingest_bytes: {}".format(stats["bytes"]))
' "${expected}" "${start_epoch}"

  echo "writer pod distribution"
  kubectl -n "${NAMESPACE}" get pods -l app=perf-writer -o wide
  echo "agent status"
  for pod in $(kubectl -n logpilot-system get pods -l app.kubernetes.io/name=log-pilot-agent -o name); do
    kubectl -n logpilot-system port-forward "${pod}" 19090:9090 >"${TMP_DIR}/agent-port-forward.log" 2>&1 &
    agent_pf="$!"
    sleep 2
    printf '%s ' "${pod}"
    curl -fsS http://127.0.0.1:19090/status || true
    kill "${agent_pf}" >/dev/null 2>&1 || true
    wait "${agent_pf}" >/dev/null 2>&1 || true
  done
}

build_logpilot_image
build_collector_image
build_writer_image
kind load docker-image "${LOGPILOT_IMAGE}" --name "${CLUSTER_NAME}"
kind load docker-image "${COLLECTOR_IMAGE}" --name "${CLUSTER_NAME}"
kind load docker-image "${WRITER_IMAGE}" --name "${CLUSTER_NAME}"
deploy_logpilot
run_benchmark

echo "cluster=${CLUSTER_NAME}"
echo "keep_cluster=${KEEP_CLUSTER}"
