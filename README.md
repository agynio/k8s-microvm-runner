# k8s-microvm-runner

k8s-microvm-runner is the second implementation of the agyn Runner gRPC API. It runs every workload as its own Cloud Hypervisor microVM inside a pool of prewarmed, unprivileged slot Pods on Kubernetes, drives the stock Kata guest agent inside each microVM through a slot supervisor of its own, and can hibernate a workload — its memory, its containers' writable layers and its volumes — to an S3-compatible store and wake it later in any slot of the same flavor with its processes exactly where they were. It is built on [runner-core](https://github.com/agynio/runner-core), which carries what every runner shares: enrollment with the Gateway, the catalog report, the Runner API server and workload state reporting.

Architecture: [k8s-microvm-runner](https://github.com/agynio/architecture/blob/main/architecture/k8s-microvm-runner.md)

## Layout

| Path | Holds |
|---|---|
| `cmd/k8s-microvm-runner` | The runner: runner-core's front, the slot pool, the workload and volume index, the snapshot store client |
| `cmd/slot-supervisor` | One per slot Pod: the VMM, virtiofsd, the share, the guest, the tap and NAT, the local cache |
| `proto/agynio/microvm/slot/v1` | The control protocol between the two, generated into `internal/.gen` with `buf generate` |
| `internal/pool` | Slot Pods: creation, the `warming → idle → bound → idle → retired` lifecycle, adoption by label |
| `internal/runner` | The Runner API behaviour over the pool, the index and the store |
| `internal/guest` | The runtime path inside a slot: cold boot, containers, exec, logs, hibernate, wake, stop |
| `internal/ch`, `internal/kata`, `internal/image`, `internal/share`, `internal/snapshot`, `internal/store` | Cloud Hypervisor, the guest agent, image unpacking, the share and its deltas, snapshot packing, the S3 client |
| `charts/k8s-microvm-runner` | The Helm chart; its README lists the install-time requirements |

## Building

```bash
buf generate
go build ./...
go test ./...
```

The `Dockerfile` has two targets: `runner` and `slot`. The slot bundles Cloud Hypervisor v52.0 and virtiofsd, the guest kernel and the guest image from kata-static 4.3.1, pinned together and recorded in `/opt/agyn/versions.json`, which every snapshot carries and every restore checks.

```bash
docker buildx build --target runner -t k8s-microvm-runner .
docker buildx build --target slot -t k8s-microvm-slot .
```
