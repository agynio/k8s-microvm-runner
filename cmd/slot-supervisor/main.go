// The slot supervisor: one per slot Pod, started as its command. It owns
// everything inside the slot -- the VMM, virtiofsd, the share, the guest,
// the tap, the cache -- and serves the runner's control channel.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/agynio/runner-core/config"
	"github.com/agynio/runner-core/logging"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	slotv1 "github.com/agynio/k8s-microvm-runner/internal/.gen/agynio/microvm/slot/v1"
	"github.com/agynio/k8s-microvm-runner/internal/guest"
	"github.com/agynio/k8s-microvm-runner/internal/share"
	"github.com/agynio/k8s-microvm-runner/internal/supervisor"
	"github.com/agynio/k8s-microvm-runner/internal/versions"
)

// Where the slot image puts what it bundles.
const (
	defaultCloudHypervisor = "/opt/agyn/bin/cloud-hypervisor"
	defaultVirtiofsd       = "/opt/agyn/bin/virtiofsd"
	defaultKernel          = "/opt/agyn/guest/vmlinux"
	defaultGuestImage      = "/opt/agyn/guest/image"
	defaultVersions        = "/opt/agyn/versions.json"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "slot-supervisor failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger, err := logging.New(config.ReadEnv("LOG_LEVEL", "info"))
	if err != nil {
		return err
	}
	defer func() { _ = logger.Sync() }()

	token := config.ReadEnv("SLOT_TOKEN", "")
	if token == "" {
		return fmt.Errorf("SLOT_TOKEN is required")
	}
	name := config.ReadEnv("SLOT_NAME", "slot")
	set, err := versions.Load(config.ReadEnv("SLOT_VERSIONS_FILE", defaultVersions))
	if err != nil {
		return err
	}
	idlePoll, err := config.ReadBool("GUEST_IDLE_POLL", false)
	if err != nil {
		return err
	}
	cacheTTL, err := config.ReadDuration("SLOT_SNAPSHOT_CACHE_TTL", time.Hour)
	if err != nil {
		return err
	}
	bootTimeout, err := config.ReadDuration("GUEST_BOOT_TIMEOUT", 90*time.Second)
	if err != nil {
		return err
	}
	guestDebug, err := config.ReadBool("GUEST_DEBUG", false)
	if err != nil {
		return err
	}

	service := (*supervisor.Service)(nil)
	manager, err := guest.New(guest.Config{
		Layout:           share.Layout{Root: config.ReadEnv("SLOT_DIR", "/var/lib/agyn/slot")},
		CloudHypervisor:  config.ReadEnv("CLOUD_HYPERVISOR_BIN", defaultCloudHypervisor),
		Virtiofsd:        config.ReadEnv("VIRTIOFSD_BIN", defaultVirtiofsd),
		VirtiofsdSandbox: config.ReadEnv("VIRTIOFSD_SANDBOX", "namespace"),
		Kernel:           config.ReadEnv("GUEST_KERNEL", defaultKernel),
		GuestImage:       config.ReadEnv("GUEST_IMAGE", defaultGuestImage),
		Versions:         set,
		IdlePoll:         idlePoll,
		Egress:           config.ReadEnv("EGRESS_IFACE", "eth0"),
		CAFile:           config.ReadEnv("IMAGE_PROXY_CA_FILE", ""),
		RestoreMode:      config.ReadEnv("RESTORE_MODE", "OnDemand"),
		AgentBootTimeout: bootTimeout,
		SnapshotCacheTTL: cacheTTL,
		GuestDebug:       guestDebug,
		Log:              logger,
		OnChange: func(v guest.View) {
			if service != nil {
				service.OnChange(v)
			}
		},
	})
	if err != nil {
		return err
	}
	service = supervisor.New(name, token, manager, logger)

	addr := config.ReadEnv("SLOT_LISTEN_ADDR", ":"+strconv.Itoa(7700))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(service.UnaryAuth),
		grpc.StreamInterceptor(service.StreamAuth),
		grpc.MaxRecvMsgSize(16<<20),
		grpc.MaxSendMsgSize(16<<20),
	)
	slotv1.RegisterSlotSupervisorServiceServer(server, service)

	go manager.RunJanitor(ctx)
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(listener) }()
	logger.Info("slot supervisor serving", zap.String("slot", name), zap.String("addr", listener.Addr().String()),
		zap.String("versions", set.String()), zap.Bool("idle_poll", idlePoll))
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutting down")
		server.GracefulStop()
		return nil
	}
}
