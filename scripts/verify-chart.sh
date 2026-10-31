#!/usr/bin/env bash
# Renders the chart the way an install would and checks what the runner
# depends on is in it.
set -euo pipefail

chart_dir="charts/k8s-microvm-runner"
rendered="$(mktemp)"
trap 'rm -f "$rendered"' EXIT

helm lint "$chart_dir" >/dev/null

helm template k8s-microvm-runner "$chart_dir" \
  --set snapshotStore.endpoint=store.example:9000 \
  --set snapshotStore.bucket=agyn-snapshots \
  --set snapshotStore.existingSecret=store-credentials \
  --set devicePlugin.enabled=true \
  >"$rendered"

assert_contains() {
  if ! grep -Fq -- "$1" "$rendered"; then
    echo "expected rendered chart to contain: $1" >&2
    exit 1
  fi
}

assert_not_contains() {
  if grep -Fq -- "$1" "$rendered"; then
    echo "expected rendered chart not to contain: $1" >&2
    exit 1
  fi
}

assert_contains 'kind: Deployment'
assert_contains 'kind: ServiceAccount'
assert_contains 'kind: Role'
assert_contains 'kind: RoleBinding'
assert_contains 'kind: ConfigMap'
assert_contains 'kind: PriorityClass'
assert_contains 'kind: DaemonSet'
assert_contains 'name: SLOT_IMAGE'
assert_contains 'name: SNAPSHOT_STORE_ENDPOINT'
assert_contains 'name: SNAPSHOT_STORE_SECRET_KEY'
assert_contains 'name: store-credentials'
assert_contains 'squat.ai/kvm'
assert_contains 'squat.ai/tun'
assert_contains 'name: "agyn-workload"'
assert_contains 'catalog.yaml: |'
# The runner's Role is exactly pods get/list/watch/create/delete and events
# get/list/watch: no exec, no secrets, no claims.
assert_not_contains 'pods/exec'
assert_not_contains 'persistentvolumeclaims'
assert_not_contains '"secrets"'

helm template k8s-microvm-runner "$chart_dir" >"$rendered"
assert_not_contains 'kind: DaemonSet'
echo "chart ok"
