// Package config reads the runner's configuration: what every runner shares,
// read by runner-core, and what is this runner's own -- the workload
// namespace, the slot Pod template, the snapshot store and the catalog.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	coreconfig "github.com/agynio/runner-core/config"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/agynio/k8s-microvm-runner/internal/store"
)

// Config is the runner's configuration.
type Config struct {
	coreconfig.Config
	Namespace string
	Catalog   Catalog
	Store     store.Config
	Slot      SlotConfig
}

// SlotConfig is the slot Pod template and pool limits.
type SlotConfig struct {
	Image            string
	ImagePullPolicy  corev1.PullPolicy
	OverheadMemory   resource.Quantity
	CacheSize        resource.Quantity
	CPULimitFactor   float64
	HostUsers        bool
	KVMResource      string
	TUNResource      string
	PriorityClass    string
	ProxyCAConfigMap string
	IdlePoll         bool
	SnapshotCacheTTL time.Duration
	MaxBinds         int
	WarmupTimeout    time.Duration
	// VersionsDigest is the version set the slot image runs, so a slot
	// found running another is retired; empty trusts every slot.
	VersionsDigest string
	RunnerName     string
}

// Load reads the configuration from the environment.
func Load() (Config, error) {
	var cfg Config
	shared, err := coreconfig.Load()
	if err != nil {
		return Config{}, err
	}
	cfg.Config = shared

	cfg.Namespace = coreconfig.ReadEnv("KUBE_NAMESPACE", "")
	if cfg.Namespace == "" {
		return Config{}, fmt.Errorf("KUBE_NAMESPACE is required")
	}
	cfg.Catalog, err = LoadCatalog(cfg.CatalogPath)
	if err != nil {
		return Config{}, err
	}

	cfg.Store = store.Config{
		Endpoint:        coreconfig.ReadEnv("SNAPSHOT_STORE_ENDPOINT", ""),
		Bucket:          coreconfig.ReadEnv("SNAPSHOT_STORE_BUCKET", ""),
		Region:          coreconfig.ReadEnv("SNAPSHOT_STORE_REGION", ""),
		AccessKey:       coreconfig.ReadEnv("SNAPSHOT_STORE_ACCESS_KEY", ""),
		SecretKey:       strings.TrimSpace(os.Getenv("SNAPSHOT_STORE_SECRET_KEY")),
		SnapshotsPrefix: coreconfig.ReadEnv("SNAPSHOT_STORE_SNAPSHOTS_PREFIX", store.DefaultSnapshotsPrefix),
		VolumesPrefix:   coreconfig.ReadEnv("SNAPSHOT_STORE_VOLUMES_PREFIX", store.DefaultVolumesPrefix),
		TemplatesPrefix: coreconfig.ReadEnv("SNAPSHOT_STORE_TEMPLATES_PREFIX", store.DefaultTemplatesPrefix),
	}
	if cfg.Store.UseTLS, err = coreconfig.ReadBool("SNAPSHOT_STORE_TLS", true); err != nil {
		return Config{}, err
	}
	if cfg.Store.PathStyle, err = coreconfig.ReadBool("SNAPSHOT_STORE_PATH_STYLE", true); err != nil {
		return Config{}, err
	}

	slot := SlotConfig{
		Image:            coreconfig.ReadEnv("SLOT_IMAGE", ""),
		ImagePullPolicy:  corev1.PullPolicy(coreconfig.ReadEnv("SLOT_IMAGE_PULL_POLICY", string(corev1.PullIfNotPresent))),
		KVMResource:      coreconfig.ReadEnv("SLOT_KVM_RESOURCE", "squat.ai/kvm"),
		TUNResource:      coreconfig.ReadEnv("SLOT_TUN_RESOURCE", "squat.ai/tun"),
		PriorityClass:    coreconfig.ReadEnv("WORKLOAD_PRIORITY_CLASS", ""),
		ProxyCAConfigMap: coreconfig.ReadEnv("SLOT_PROXY_CA_CONFIGMAP", ""),
		VersionsDigest:   coreconfig.ReadEnv("SLOT_VERSIONS_DIGEST", ""),
		RunnerName:       coreconfig.ReadEnv("RUNNER_NAME", ""),
	}
	if slot.Image == "" {
		return Config{}, fmt.Errorf("SLOT_IMAGE is required")
	}
	if slot.OverheadMemory, err = readQuantity("SLOT_OVERHEAD_MEMORY", "1Gi"); err != nil {
		return Config{}, err
	}
	if slot.CacheSize, err = readQuantity("SLOT_CACHE_SIZE", "20Gi"); err != nil {
		return Config{}, err
	}
	if slot.CPULimitFactor, err = readFloat("SLOT_CPU_LIMIT_FACTOR", 0); err != nil {
		return Config{}, err
	}
	if slot.HostUsers, err = coreconfig.ReadBool("SLOT_HOST_USERS", true); err != nil {
		return Config{}, err
	}
	if slot.IdlePoll, err = coreconfig.ReadBool("GUEST_IDLE_POLL", false); err != nil {
		return Config{}, err
	}
	if slot.SnapshotCacheTTL, err = coreconfig.ReadDuration("SLOT_SNAPSHOT_CACHE_TTL", time.Hour); err != nil {
		return Config{}, err
	}
	if slot.WarmupTimeout, err = coreconfig.ReadDuration("SLOT_WARMUP_TIMEOUT", 5*time.Minute); err != nil {
		return Config{}, err
	}
	if slot.MaxBinds, err = readInt("SLOT_MAX_BINDS", 50); err != nil {
		return Config{}, err
	}
	cfg.Slot = slot
	return cfg, nil
}

func readQuantity(key, def string) (resource.Quantity, error) {
	value := coreconfig.ReadEnv(key, def)
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("invalid %s: %w", key, err)
	}
	return q, nil
}

func readInt(key string, def int) (int, error) {
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return def, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return n, nil
}

func readFloat(key string, def float64) (float64, error) {
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return f, nil
}
