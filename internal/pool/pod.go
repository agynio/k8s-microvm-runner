package pool

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

// Environment the runner injects into a slot Pod.
const (
	TokenEnv      = "SLOT_TOKEN"
	SupervisorCmd = "/opt/agyn/bin/slot-supervisor"
	CacheMount    = "/var/lib/agyn/slot"
	ProxyCAMount  = "/etc/agyn/proxy-ca"
	ProxyCAFile   = "ca.crt"
	ContainerName = "supervisor"
)

// PodTemplate is what every slot Pod is made from besides its flavor.
type PodTemplate struct {
	Image           string
	ImagePullPolicy corev1.PullPolicy
	// OverheadMemory is the supervisor's fixed share of the Pod's memory
	// limit on top of the guest's.
	OverheadMemory resource.Quantity
	// CacheSize sizes the emptyDir cache.
	CacheSize resource.Quantity
	// CPULimit multiplies the vCPU count into a CPU limit; zero sets none.
	CPULimitFactor float64
	// HostUsers false runs the Pod in a user namespace, where the capability
	// set is the namespace's rather than the node's and userfaultfd comes
	// from the sysctl rather than SYS_PTRACE.
	HostUsers bool
	// KVMResource and TUNResource are the device plugin's extended resources.
	KVMResource string
	TUNResource string
	// PriorityClass is the workload PriorityClass.
	PriorityClass string
	// ProxyCAConfigMap names a ConfigMap holding the image proxy's CA; empty
	// mounts none.
	ProxyCAConfigMap string
	// IdlePoll boots guests with idle=poll.
	IdlePoll bool
	// SnapshotCacheTTL is how long a slot keeps a hibernated workload's
	// snapshot.
	SnapshotCacheTTL string
	LogLevel         string
	// ExtraEnv is appended to the supervisor's environment.
	ExtraEnv []corev1.EnvVar
}

// Capabilities a slot keeps; everything else is dropped. SYS_PTRACE is what
// the kernel demands of a process that serves page faults for another
// address space, as the VMM does for a guest whose memory it restores on
// demand; in a user namespace it cannot be granted and the node sysctl
// vm.unprivileged_userfaultfd=1 stands in for it.
var (
	capabilities = []corev1.Capability{
		"NET_ADMIN", "NET_RAW", "SYS_ADMIN", "SYS_PTRACE",
		"CHOWN", "DAC_OVERRIDE", "DAC_READ_SEARCH", "FOWNER", "FSETID", "MKNOD", "SETUID", "SETGID", "SETFCAP", "SETPCAP",
	}
	capabilitiesInUserNamespace = []corev1.Capability{
		"NET_ADMIN", "NET_RAW", "SYS_ADMIN",
		"CHOWN", "DAC_OVERRIDE", "DAC_READ_SEARCH", "FOWNER", "FSETID", "MKNOD", "SETUID", "SETGID", "SETFCAP", "SETPCAP",
	}
)

// Pod renders the slot Pod for a flavor.
func (t PodTemplate) Pod(name, namespace, flavor string, spec FlavorSpec, token string, labels map[string]string) *corev1.Pod {
	memory := resource.NewQuantity(spec.MemoryMiB<<20, resource.BinarySI)
	limitMemory := memory.DeepCopy()
	limitMemory.Add(t.OverheadMemory)
	cpu := resource.NewQuantity(int64(spec.VCPUs), resource.DecimalSI)
	limits := corev1.ResourceList{corev1.ResourceMemory: limitMemory}
	if t.CPULimitFactor > 0 {
		limits[corev1.ResourceCPU] = resource.MustParse(strconv.FormatFloat(float64(spec.VCPUs)*t.CPULimitFactor, 'f', 2, 64))
	}
	if t.KVMResource != "" {
		limits[corev1.ResourceName(t.KVMResource)] = resource.MustParse("1")
	}
	if t.TUNResource != "" {
		limits[corev1.ResourceName(t.TUNResource)] = resource.MustParse("1")
	}
	requests := corev1.ResourceList{corev1.ResourceCPU: *cpu, corev1.ResourceMemory: limitMemory}
	for k, v := range limits {
		if k == corev1.ResourceCPU || k == corev1.ResourceMemory {
			continue
		}
		requests[k] = v
	}

	caps := capabilities
	if !t.HostUsers {
		caps = capabilitiesInUserNamespace
	}
	env := []corev1.EnvVar{
		{Name: "SLOT_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: TokenEnv, Value: token},
		{Name: "SLOT_FLAVOR", Value: flavor},
		{Name: "SLOT_VCPUS", Value: strconv.Itoa(spec.VCPUs)},
		{Name: "SLOT_MEMORY_MIB", Value: strconv.FormatInt(spec.MemoryMiB, 10)},
		{Name: "SLOT_DIR", Value: CacheMount},
		{Name: "GUEST_IDLE_POLL", Value: strconv.FormatBool(t.IdlePoll)},
		{Name: "SLOT_LISTEN_ADDR", Value: fmt.Sprintf(":%d", 7700)},
	}
	if t.SnapshotCacheTTL != "" {
		env = append(env, corev1.EnvVar{Name: "SLOT_SNAPSHOT_CACHE_TTL", Value: t.SnapshotCacheTTL})
	}
	if t.LogLevel != "" {
		env = append(env, corev1.EnvVar{Name: "LOG_LEVEL", Value: t.LogLevel})
	}
	volumes := []corev1.Volume{{
		Name:         "cache",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr.To(t.CacheSize)}},
	}}
	mounts := []corev1.VolumeMount{{Name: "cache", MountPath: CacheMount}}
	if t.ProxyCAConfigMap != "" {
		volumes = append(volumes, corev1.Volume{
			Name:         "proxy-ca",
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: t.ProxyCAConfigMap}}},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: "proxy-ca", MountPath: ProxyCAMount, ReadOnly: true})
		env = append(env, corev1.EnvVar{Name: "IMAGE_PROXY_CA_FILE", Value: ProxyCAMount + "/" + ProxyCAFile})
	}
	env = append(env, t.ExtraEnv...)

	pullPolicy := t.ImagePullPolicy
	if pullPolicy == "" {
		pullPolicy = corev1.PullIfNotPresent
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
			Annotations: map[string]string{
				// The runtime's default profile denies mount, and the share
				// is made of mounts. Field below for kubelets that read it,
				// annotation for those that still read this.
				"container.apparmor.security.beta.kubernetes.io/" + ContainerName: "unconfined",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: ptr.To(int64(10)),
			AutomountServiceAccountToken:  ptr.To(false),
			PriorityClassName:             t.PriorityClass,
			SecurityContext: &corev1.PodSecurityContext{
				Sysctls: []corev1.Sysctl{{Name: "net.ipv4.ip_forward", Value: "1"}},
			},
			Containers: []corev1.Container{{
				Name:            ContainerName,
				Image:           t.Image,
				ImagePullPolicy: pullPolicy,
				Command:         []string{SupervisorCmd},
				Env:             env,
				Ports:           []corev1.ContainerPort{{Name: "control", ContainerPort: 7700, Protocol: corev1.ProtocolTCP}},
				SecurityContext: &corev1.SecurityContext{
					Privileged:               ptr.To(false),
					AllowPrivilegeEscalation: ptr.To(true),
					AppArmorProfile:          &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeUnconfined},
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}, Add: caps},
				},
				Resources:    corev1.ResourceRequirements{Requests: requests, Limits: limits},
				VolumeMounts: mounts,
				ReadinessProbe: &corev1.Probe{
					ProbeHandler:     corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(7700)}},
					PeriodSeconds:    2,
					FailureThreshold: 3,
				},
			}},
			Volumes: volumes,
		},
	}
	if !t.HostUsers {
		pod.Spec.HostUsers = ptr.To(false)
	}
	return pod
}
