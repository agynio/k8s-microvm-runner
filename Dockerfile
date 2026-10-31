# syntax=docker/dockerfile:1
ARG GO_VERSION=1.25
ARG BUF_VERSION=1.64.0
# The slot bundles a pinned set: Cloud Hypervisor v52.0 (v53 prefaults every
# page of an on-demand restore, which makes the next snapshot full-sized),
# and virtiofsd, the guest kernel and the guest image from kata-static 4.3.1.
ARG CLOUD_HYPERVISOR_VERSION=v52.0
ARG KATA_VERSION=4.3.1
ARG KATA_STATIC_URL=
ARG KATA_KERNEL=vmlinux-6.18.56-208
ARG KATA_IMAGE=kata-ubuntu-resolute.image

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS buf
ARG BUF_VERSION
RUN apk add --no-cache curl
RUN curl -sSL \
      "https://github.com/bufbuild/buf/releases/download/v${BUF_VERSION}/buf-$(uname -s)-$(uname -m)" \
      -o /usr/local/bin/buf && \
    chmod +x /usr/local/bin/buf

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY --from=buf /usr/local/bin/buf /usr/local/bin/buf
COPY go.mod go.sum ./
RUN go mod download
COPY buf.yaml buf.gen.yaml buf.lock ./
COPY proto ./proto
RUN buf generate
COPY . .
ARG TARGETOS TARGETARCH
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH
RUN go build -o /out/k8s-microvm-runner ./cmd/k8s-microvm-runner && \
    go build -o /out/slot-supervisor ./cmd/slot-supervisor

# The runner image: runner-core's front and the pool, nothing else.
FROM alpine:3.21 AS runner
# A numeric user, so a Pod that asks for runAsNonRoot can verify it.
RUN addgroup -S -g 10001 appgroup && adduser -S -u 10001 appuser -G appgroup
WORKDIR /app
COPY --from=build /out/k8s-microvm-runner /app/k8s-microvm-runner
USER 10001:10001
ENTRYPOINT ["/app/k8s-microvm-runner"]

# The guest assets: fetched once per architecture and version, not per
# build of the Go code.
FROM alpine:3.21 AS assets
ARG TARGETARCH
ARG CLOUD_HYPERVISOR_VERSION
ARG KATA_VERSION
ARG KATA_STATIC_URL
ARG KATA_KERNEL
ARG KATA_IMAGE
RUN apk add --no-cache curl zstd tar
WORKDIR /assets
RUN set -eux; \
    case "$TARGETARCH" in \
      arm64) CH_ASSET=cloud-hypervisor-static-aarch64; KATA_ARCH=arm64 ;; \
      amd64) CH_ASSET=cloud-hypervisor-static; KATA_ARCH=amd64 ;; \
      *) echo "unsupported architecture $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    curl -sSL -o cloud-hypervisor "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/${CLOUD_HYPERVISOR_VERSION}/${CH_ASSET}"; \
    chmod +x cloud-hypervisor; \
    url="${KATA_STATIC_URL:-https://github.com/kata-containers/kata-containers/releases/download/${KATA_VERSION}/kata-static-${KATA_VERSION}-${KATA_ARCH}.tar.zst}"; \
    curl -sSL -o kata-static.tar.zst "$url"; \
    tar --zstd -xf kata-static.tar.zst \
        ./opt/kata/libexec/virtiofsd \
        "./opt/kata/share/kata-containers/${KATA_KERNEL}" \
        "./opt/kata/share/kata-containers/${KATA_IMAGE}"; \
    rm kata-static.tar.zst; \
    mkdir -p bin guest; \
    mv cloud-hypervisor bin/cloud-hypervisor; \
    mv opt/kata/libexec/virtiofsd bin/virtiofsd; \
    mv "opt/kata/share/kata-containers/${KATA_KERNEL}" guest/vmlinux; \
    mv "opt/kata/share/kata-containers/${KATA_IMAGE}" guest/image; \
    rm -rf opt; \
    printf '{"cloud_hypervisor":"%s","virtiofsd":"kata-static-%s","guest_kernel":"kata-static-%s/%s","guest_image":"kata-static-%s/%s"}\n' \
        "$CLOUD_HYPERVISOR_VERSION" "$KATA_VERSION" "$KATA_VERSION" "$KATA_KERNEL" "$KATA_VERSION" "$KATA_IMAGE" > versions.json

# The slot image: the supervisor with what it drives, pinned together. The
# supervisor runs as root inside an unprivileged Pod; the capability set is
# the Pod's, granted by the runner at creation.
FROM alpine:3.21 AS slot
RUN apk add --no-cache iproute2 iptables ca-certificates
COPY --from=assets /assets/bin/cloud-hypervisor /opt/agyn/bin/cloud-hypervisor
COPY --from=assets /assets/bin/virtiofsd /opt/agyn/bin/virtiofsd
COPY --from=assets /assets/guest/vmlinux /opt/agyn/guest/vmlinux
COPY --from=assets /assets/guest/image /opt/agyn/guest/image
COPY --from=assets /assets/versions.json /opt/agyn/versions.json
COPY --from=build /out/slot-supervisor /opt/agyn/bin/slot-supervisor
ENTRYPOINT ["/opt/agyn/bin/slot-supervisor"]
