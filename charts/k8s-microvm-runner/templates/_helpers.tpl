{{/*
Names of what this chart puts in the workload namespace, derived from the
release's fullname so a rename cannot leave the runner pointing at the wrong
object.
*/}}
{{- define "k8s-microvm-runner.catalogName" -}}
{{ include "service-base.fullname" . }}-catalog
{{- end -}}

{{- define "k8s-microvm-runner.proxyCAName" -}}
{{ include "service-base.fullname" . }}-proxy-ca
{{- end -}}

{{- define "k8s-microvm-runner.storeSecretName" -}}
{{- if .Values.snapshotStore.existingSecret -}}
{{ .Values.snapshotStore.existingSecret }}
{{- else -}}
{{ include "service-base.fullname" . }}-snapshot-store
{{- end -}}
{{- end -}}

{{- define "k8s-microvm-runner.slotImage" -}}
{{- $registry := .Values.slot.image.registry | default .Values.global.imageRegistry -}}
{{- $tag := .Values.slot.image.tag | default .Chart.AppVersion -}}
{{- if $registry -}}
{{ printf "%s/%s:%s" $registry .Values.slot.image.repository $tag }}
{{- else -}}
{{ printf "%s:%s" .Values.slot.image.repository $tag }}
{{- end -}}
{{- end -}}

{{/*
Everything the runner reads from its environment that this chart derives
from values, appended to extraEnvVars so a plain env entry can still
override it.
*/}}
{{- define "k8s-microvm-runner.configure" -}}
{{- $env := .Values.extraEnvVars | default (list) -}}
{{- $env = append $env (dict "name" "CATALOG_PATH" "value" .Values.catalogPath) -}}
{{- $env = append $env (dict "name" "KUBE_NAMESPACE" "value" (default .Release.Namespace .Values.workloadNamespace)) -}}
{{- $env = append $env (dict "name" "RUNNER_NAME" "value" (include "service-base.fullname" .)) -}}
{{- $env = append $env (dict "name" "SLOT_IMAGE" "value" (include "k8s-microvm-runner.slotImage" .)) -}}
{{- $env = append $env (dict "name" "SLOT_IMAGE_PULL_POLICY" "value" .Values.slot.image.pullPolicy) -}}
{{- $env = append $env (dict "name" "SLOT_OVERHEAD_MEMORY" "value" (toString .Values.slot.overheadMemory)) -}}
{{- $env = append $env (dict "name" "SLOT_CACHE_SIZE" "value" (toString .Values.slot.cacheSize)) -}}
{{- $env = append $env (dict "name" "SLOT_CPU_LIMIT_FACTOR" "value" (toString .Values.slot.cpuLimitFactor)) -}}
{{- $env = append $env (dict "name" "SLOT_HOST_USERS" "value" (toString .Values.slot.hostUsers)) -}}
{{- $env = append $env (dict "name" "SLOT_KVM_RESOURCE" "value" .Values.slot.kvmResource) -}}
{{- $env = append $env (dict "name" "SLOT_TUN_RESOURCE" "value" .Values.slot.tunResource) -}}
{{- $env = append $env (dict "name" "GUEST_IDLE_POLL" "value" (toString .Values.slot.guestIdlePoll)) -}}
{{- $env = append $env (dict "name" "SLOT_SNAPSHOT_CACHE_TTL" "value" (toString .Values.slot.snapshotCacheTTL)) -}}
{{- $env = append $env (dict "name" "SLOT_MAX_BINDS" "value" (toString .Values.slot.maxBinds)) -}}
{{- $env = append $env (dict "name" "SLOT_WARMUP_TIMEOUT" "value" (toString .Values.slot.warmupTimeout)) -}}
{{- with .Values.workloadPriorityClass.name -}}
{{- $env = append $env (dict "name" "WORKLOAD_PRIORITY_CLASS" "value" .) -}}
{{- end -}}
{{- if .Values.imageProxy.ca -}}
{{- $env = append $env (dict "name" "SLOT_PROXY_CA_CONFIGMAP" "value" (include "k8s-microvm-runner.proxyCAName" .)) -}}
{{- end -}}
{{- if .Values.snapshotStore.endpoint -}}
{{- $env = append $env (dict "name" "SNAPSHOT_STORE_ENDPOINT" "value" .Values.snapshotStore.endpoint) -}}
{{- $env = append $env (dict "name" "SNAPSHOT_STORE_BUCKET" "value" .Values.snapshotStore.bucket) -}}
{{- $env = append $env (dict "name" "SNAPSHOT_STORE_REGION" "value" .Values.snapshotStore.region) -}}
{{- $env = append $env (dict "name" "SNAPSHOT_STORE_TLS" "value" (toString .Values.snapshotStore.tls)) -}}
{{- $env = append $env (dict "name" "SNAPSHOT_STORE_PATH_STYLE" "value" (toString .Values.snapshotStore.pathStyle)) -}}
{{- $env = append $env (dict "name" "SNAPSHOT_STORE_SNAPSHOTS_PREFIX" "value" .Values.snapshotStore.snapshotsPrefix) -}}
{{- $env = append $env (dict "name" "SNAPSHOT_STORE_VOLUMES_PREFIX" "value" .Values.snapshotStore.volumesPrefix) -}}
{{- $env = append $env (dict "name" "SNAPSHOT_STORE_TEMPLATES_PREFIX" "value" .Values.snapshotStore.templatesPrefix) -}}
{{- $secret := include "k8s-microvm-runner.storeSecretName" . -}}
{{- $env = append $env (dict "name" "SNAPSHOT_STORE_ACCESS_KEY" "valueFrom" (dict "secretKeyRef" (dict "name" $secret "key" "SNAPSHOT_STORE_ACCESS_KEY"))) -}}
{{- $env = append $env (dict "name" "SNAPSHOT_STORE_SECRET_KEY" "valueFrom" (dict "secretKeyRef" (dict "name" $secret "key" "SNAPSHOT_STORE_SECRET_KEY"))) -}}
{{- end -}}
{{- $_ := set .Values "extraEnvVars" $env -}}
{{- $mounts := append (.Values.extraVolumeMounts | default (list)) (dict "name" "runner-catalog" "mountPath" (dir .Values.catalogPath) "readOnly" true) -}}
{{- $volumes := append (.Values.extraVolumes | default (list)) (dict "name" "runner-catalog" "configMap" (dict "name" (include "k8s-microvm-runner.catalogName" .))) -}}
{{- $_ := set .Values "extraVolumeMounts" $mounts -}}
{{- $_ := set .Values "extraVolumes" $volumes -}}
{{- $annotations := merge (dict "agyn.io/catalog-checksum" (toYaml .Values.catalog | sha256sum)) (.Values.podAnnotations | default (dict)) -}}
{{- $_ := set .Values "podAnnotations" $annotations -}}
{{- end -}}
