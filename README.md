# k8s-microvm-runner

k8s-microvm-runner is the second implementation of the agyn Runner gRPC API. It runs every workload as its own Cloud Hypervisor microVM inside a pool of prewarmed, unprivileged slot Pods on Kubernetes, drives the stock Kata guest agent inside each microVM through a slot supervisor of its own, and can hibernate a workload — its memory, its containers' writable layers and its volumes — to an S3-compatible store and wake it later in any slot of the same flavor with its processes exactly where they were. It is built on [runner-core](https://github.com/agynio/runner-core), which carries what every runner shares: enrollment with the Gateway, the catalog report, the Runner API server and workload state reporting.

Architecture: [k8s-microvm-runner](https://github.com/agynio/architecture/blob/main/architecture/k8s-microvm-runner.md)
