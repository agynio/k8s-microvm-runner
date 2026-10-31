// The k8s-microvm-runner: runner-core's front over a pool of slot Pods, an
// index of workloads and volumes, and the snapshot store.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	runnercore "github.com/agynio/runner-core"
	"github.com/agynio/runner-core/logging"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/agynio/k8s-microvm-runner/internal/config"
	"github.com/agynio/k8s-microvm-runner/internal/index"
	"github.com/agynio/k8s-microvm-runner/internal/pool"
	"github.com/agynio/k8s-microvm-runner/internal/runner"
	"github.com/agynio/k8s-microvm-runner/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "k8s-microvm-runner failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger, err := logging.New(cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("kubernetes config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}

	var snapshotStore *store.Client
	if cfg.Store.Configured() {
		snapshotStore, err = store.New(cfg.Store)
		if err != nil {
			return err
		}
		pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := snapshotStore.Ping(pingCtx)
		cancel()
		if err != nil {
			// Not fatal: starts and stops work without the store; a
			// hibernation says so when asked.
			logger.Error("snapshot store unreachable; hibernation will fail until it is", zap.Error(err))
		}
	} else {
		logger.Warn("no snapshot store configured; workloads cannot hibernate and persistent volumes are not persisted")
	}

	flavors := map[string]pool.FlavorSpec{}
	for _, f := range cfg.Catalog.Flavors {
		memoryMiB, err := f.MemoryMiB()
		if err != nil {
			return err
		}
		flavors[f.Name] = pool.FlavorSpec{VCPUs: f.VCPUs, MemoryMiB: memoryMiB, Warm: f.Warm, Max: f.Max}
	}
	slots := pool.New(pool.Config{
		Namespace: cfg.Namespace,
		Flavors:   flavors,
		Template: pool.PodTemplate{
			Image: cfg.Slot.Image, ImagePullPolicy: cfg.Slot.ImagePullPolicy, OverheadMemory: cfg.Slot.OverheadMemory, CacheSize: cfg.Slot.CacheSize,
			CPULimitFactor: cfg.Slot.CPULimitFactor, HostUsers: cfg.Slot.HostUsers, KVMResource: cfg.Slot.KVMResource, TUNResource: cfg.Slot.TUNResource,
			PriorityClass: cfg.Slot.PriorityClass, ProxyCAConfigMap: cfg.Slot.ProxyCAConfigMap, IdlePoll: cfg.Slot.IdlePoll,
			SnapshotCacheTTL: cfg.Slot.SnapshotCacheTTL.String(), LogLevel: cfg.LogLevel,
		},
		MaxBinds:      cfg.Slot.MaxBinds,
		WarmupTimeout: cfg.Slot.WarmupTimeout,
		Versions:      cfg.Slot.VersionsDigest,
		RunnerName:    cfg.Slot.RunnerName,
	}, clientset, nil, logger)

	workloads := index.New()
	backend := runner.New(runner.Options{Index: workloads, Pool: slots, Store: snapshotStore, Catalog: cfg.Catalog, Logger: logger})

	// What this runner held before it restarted: slots are adopted by
	// label, hibernated workloads and volumes are listed from the store.
	rebuildCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	err = backend.Rebuild(rebuildCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("rebuild index: %w", err)
	}

	poolCtx, cancelPool := context.WithCancel(ctx)
	defer cancelPool()
	go func() {
		if err := slots.Run(poolCtx); err != nil && poolCtx.Err() == nil {
			logger.Error("slot pool stopped", zap.Error(err))
		}
	}()

	// Everything that is not slots -- enrollment, the catalog report, the
	// Runner API server, state reporting -- is runner-core's.
	return runnercore.Run(ctx, runnercore.Options{
		Config:       cfg.Config,
		Logger:       logger,
		Backend:      backend,
		Catalog:      cfg.Catalog.Reported(),
		Capabilities: []string{runner.CapabilityDocker},
	})
}
