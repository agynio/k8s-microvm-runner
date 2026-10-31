# k8s-microvm-runner chart

Installs the k8s-microvm-runner: a single-replica Deployment that enrolls with
the platform, keeps a pool of slot Pods in the workload namespace, and runs
every workload as a Cloud Hypervisor microVM in one of them. The chart also
renders the runner's Role in the workload namespace, the catalog ConfigMap,
the workload PriorityClass, the snapshot store Secret (or a reference to an
existing one), the image proxy CA ConfigMap, and, when enabled, a generic
device plugin DaemonSet and a NetworkPolicy for the slots.

## Values that must be set

| Value | Meaning |
|---|---|
| `env` (`ZITI_ENABLED`, `SERVICE_TOKEN`, `GATEWAY_ADDRESS`) | Enrollment with the platform, as for the k8s-runner |
| `workloadNamespace` | Where the slot Pods are created; the k8s-runner's workload namespace |
| `catalog` | The flavors (VM sizes with `warm` and `max`), storage classes and capabilities this runner offers |
| `slot.image` | The slot image: the supervisor with the pinned Cloud Hypervisor, virtiofsd, guest kernel and image |
| `snapshotStore` | The S3-compatible bucket for templates, snapshots and volume deltas; credentials inline or in `existingSecret` |
| `imageProxy.ca` | The image proxy's CA when its certificate is not publicly trusted |

## Install-time requirements

Beyond what the k8s-runner needs, the cluster must provide:

- **Devices**: a device plugin advertising `/dev/kvm` and `/dev/net/tun` as
  extended resources (`slot.kvmResource`, `slot.tunResource`). The chart can
  install one with `devicePlugin.enabled: true`.
- **KVM on the nodes**: bare metal or nested virtualization enabled.
- **Sysctl**: the kubelet's `allowed-unsafe-sysctls` must include
  `net.ipv4.ip_forward`; every slot Pod sets it.
- **Pod Security**: the workload namespace must admit the slot Pod's security
  context: the capability set (`NET_ADMIN`, `NET_RAW`, `SYS_ADMIN`,
  `SYS_PTRACE`, `CHOWN`, `DAC_OVERRIDE`, `DAC_READ_SEARCH`, `FOWNER`, `FSETID`,
  `MKNOD`, `SETUID`, `SETGID`, `SETFCAP`, `SETPCAP`) and an unconfined
  AppArmor profile. With `slot.hostUsers: false` the Pod runs in a user
  namespace, `SYS_PTRACE` is not granted, and the nodes need
  `vm.unprivileged_userfaultfd=1`.
- **Kernel**: overlayfs the slot Pod may mount (in a user namespace, 5.11 or
  later), and transparent huge pages allowed for shared memory
  (`/sys/kernel/mm/transparent_hugepage/shmem_enabled` at least `advise`).
- **Guest idle behaviour**: `slot.guestIdlePoll: true` under nested
  virtualization, where a halted vCPU wakes tens of milliseconds late; off on
  hardware KVM.
- **Snapshot store**: an S3-compatible bucket with server-side encryption,
  readable only with the runner's credentials.
- **Network**: two egress rules for the slot Pods beyond the workload egress
  policy: the image proxy as a Pod reaches it, and the snapshot store. Either
  add them to the k8s-runner chart's workload egress policy or enable
  `slotEgressNetworkPolicy`. The image proxy's hostname must resolve from a Pod
  to an address the policy admits (the local bundle rewrites it in cluster DNS).
- **PriorityClass**: `agyn-workload`, which the k8s-runner's chart also
  creates; set `workloadPriorityClass.create: false` where it already exists.
