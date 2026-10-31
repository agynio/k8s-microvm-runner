// Package guest runs one workload as a Cloud Hypervisor microVM driven
// through the Kata guest agent: the guest image on virtio-pmem, every
// container rootfs and volume on one virtio-fs share assembled on the host,
// memory snapshots for hibernation. One Manager per slot, one workload at a
// time.
package guest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"go.uber.org/zap"

	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"

	"github.com/agynio/k8s-microvm-runner/internal/ch"
	"github.com/agynio/k8s-microvm-runner/internal/image"
	"github.com/agynio/k8s-microvm-runner/internal/kata"
	"github.com/agynio/k8s-microvm-runner/internal/share"
	"github.com/agynio/k8s-microvm-runner/internal/snapshot"
	"github.com/agynio/k8s-microvm-runner/internal/store"
	"github.com/agynio/k8s-microvm-runner/internal/versions"
)

// Config is what a slot runs guests with.
type Config struct {
	Layout          share.Layout
	CloudHypervisor string
	Virtiofsd       string
	// VirtiofsdSandbox is virtiofsd's --sandbox: namespace, chroot or none.
	VirtiofsdSandbox string
	Kernel           string
	GuestImage       string
	Versions         versions.Set
	// IdlePoll boots guests with idle=poll, for nodes under nested
	// virtualization where a halted vCPU wakes tens of milliseconds late.
	IdlePoll bool
	// Egress is the Pod interface the guest's traffic leaves through.
	Egress string
	// CAFile adds the image proxy's CA to the system roots.
	CAFile string
	// RestoreMode is ch.RestoreOnDemand or ch.RestoreCopy.
	RestoreMode string
	// AgentBootTimeout bounds a cold boot's wait for the guest agent.
	AgentBootTimeout time.Duration
	// SnapshotCacheTTL bounds how long a hibernated workload's snapshot is
	// kept in the slot after it leaves.
	SnapshotCacheTTL time.Duration
	// GuestDebug routes the serial console to a file, for boot diagnosis.
	GuestDebug bool
	Log        *zap.Logger
	// OnChange is told of every change to the held workload's view.
	OnChange func(View)
}

// Flavor is the VM size a workload runs at.
type Flavor struct {
	Name      string
	VCPUs     int
	MemoryMiB int64
}

// Status is a workload's runtime status as the slot observes it.
type Status string

// Statuses, in the vocabulary of the Runner API.
const (
	StatusStarting    Status = "starting"
	StatusRunning     Status = "running"
	StatusHibernating Status = "hibernating"
	StatusHibernated  Status = "hibernated"
	StatusWaking      Status = "waking"
	StatusStopping    Status = "stopping"
	StatusStopped     Status = "stopped"
	StatusFailed      Status = "failed"
)

// ContainerRole is a container's place in the workload.
type ContainerRole string

// Roles.
const (
	RoleMain    ContainerRole = "main"
	RoleSidecar ContainerRole = "sidecar"
	RoleInit    ContainerRole = "init"
)

// ContainerStatus is a container's state inside the guest.
type ContainerStatus string

// Container statuses.
const (
	ContainerWaiting    ContainerStatus = "waiting"
	ContainerRunning    ContainerStatus = "running"
	ContainerTerminated ContainerStatus = "terminated"
)

// Container is one of the workload's containers.
type Container struct {
	Name        string
	Role        ContainerRole
	Image       string
	ImageDigest string
	Status      ContainerStatus
	ExitCode    *int32
	Reason      string
	Message     string
	StartedAt   time.Time
	FinishedAt  time.Time
	Mounts      []snapshot.Mount

	uid, gid uint32
	cwd      string
	// nativeSidecar is an init container with restart_policy Always: it
	// starts with the init containers and stays running.
	nativeSidecar bool
	spec          *runnerv1.ContainerSpec
	oci           *specs.Spec
}

// VolumeStore says where a persistent volume's delta lives in the store.
type VolumeStore struct {
	Name      string
	VolumeKey string
	Bucket    string
	Prefix    string
}

// Volume is one of the workload's volumes.
type Volume struct {
	Name       string
	Persistent bool
	VolumeKey  string
	InstanceID string
	SizeBytes  int64
	Store      VolumeStore
}

// Workload is the one guest a slot hosts.
type Workload struct {
	ID            string
	Key           string
	Labels        map[string]string
	Flavor        Flavor
	Status        Status
	Failure       string
	Hostname      string
	DNS           []string
	Containers    []*Container
	Volumes       []Volume
	StartedAt     time.Time
	HibernatedAt  time.Time
	SnapshotBytes int64
	Generation    int

	vm       *ch.VM
	vfsd     *ch.Virtiofsd
	agent    *kata.Client
	onDemand bool
	mu       sync.Mutex
}

// ContainerView is a container as the supervisor reports it.
type ContainerView struct {
	Name        string
	Role        ContainerRole
	Image       string
	ImageDigest string
	Status      ContainerStatus
	ExitCode    *int32
	Reason      string
	Message     string
	StartedAt   time.Time
	FinishedAt  time.Time
}

// View is a workload as the supervisor reports it: a copy, safe to read
// after the workload moved on.
type View struct {
	ID            string
	Key           string
	Labels        map[string]string
	Flavor        string
	Status        Status
	Failure       string
	Containers    []ContainerView
	SnapshotBytes int64
	StartedAt     time.Time
	HibernatedAt  time.Time
	// Mounts are the main container's volume mounts.
	Mounts []snapshot.Mount
}

// Manager hosts at most one workload at a time.
type Manager struct {
	cfg    Config
	images *image.Store
	log    *zap.Logger
	net    netConfig

	mu    sync.Mutex
	w     *Workload
	binds int
}

// New readies a manager over the slot's cache volume.
func New(cfg Config) (*Manager, error) {
	if cfg.Log == nil {
		cfg.Log = zap.NewNop()
	}
	if cfg.RestoreMode == "" {
		cfg.RestoreMode = ch.RestoreOnDemand
	}
	if cfg.AgentBootTimeout == 0 {
		cfg.AgentBootTimeout = 90 * time.Second
	}
	if cfg.SnapshotCacheTTL == 0 {
		cfg.SnapshotCacheTTL = time.Hour
	}
	if cfg.Egress == "" {
		cfg.Egress = "eth0"
	}
	for _, d := range []string{cfg.Layout.Images(), cfg.Layout.SnapshotCache()} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	images, err := image.NewStore(cfg.Layout.Images(), cfg.CAFile, cfg.Log)
	if err != nil {
		return nil, err
	}
	m := &Manager{cfg: cfg, images: images, log: cfg.Log, net: defaultNet()}
	// A workload directory left by a previous supervisor process in this
	// Pod is no workload: nothing in it is running.
	m.resetWorkloadDir()
	return m, nil
}

// Binds is how many workloads this slot has hosted since it started.
func (m *Manager) Binds() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.binds
}

// Versions is the set this slot runs.
func (m *Manager) Versions() versions.Set { return m.cfg.Versions }

// Current is the held workload, if any.
func (m *Manager) Current() *Workload {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.w
}

// View is the held workload's view, if any.
func (m *Manager) View() (View, bool) {
	w := m.Current()
	if w == nil {
		return View{}, false
	}
	return w.View(), true
}

// View copies the workload's state.
func (w *Workload) View() View {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.viewLocked()
}

func (w *Workload) viewLocked() View {
	v := View{
		ID: w.ID, Key: w.Key, Flavor: w.Flavor.Name, Status: w.Status, Failure: w.Failure,
		SnapshotBytes: w.SnapshotBytes, StartedAt: w.StartedAt, HibernatedAt: w.HibernatedAt,
		Labels: map[string]string{},
	}
	for k, val := range w.Labels {
		v.Labels[k] = val
	}
	for _, c := range w.Containers {
		cv := ContainerView{
			Name: c.Name, Role: c.Role, Image: c.Image, ImageDigest: c.ImageDigest, Status: c.Status,
			Reason: c.Reason, Message: c.Message, StartedAt: c.StartedAt, FinishedAt: c.FinishedAt,
		}
		if c.ExitCode != nil {
			code := *c.ExitCode
			cv.ExitCode = &code
		}
		v.Containers = append(v.Containers, cv)
		if c.Role == RoleMain {
			v.Mounts = append([]snapshot.Mount(nil), c.Mounts...)
		}
	}
	return v
}

// update applies fn under the workload's lock and tells the listener.
func (m *Manager) update(w *Workload, fn func()) {
	w.mu.Lock()
	fn()
	v := w.viewLocked()
	w.mu.Unlock()
	if m.cfg.OnChange != nil {
		m.cfg.OnChange(v)
	}
}

func (m *Manager) fail(w *Workload, err error) {
	m.log.Error("workload failed", zap.String("workload", w.ID), zap.Error(err))
	m.update(w, func() {
		w.Status = StatusFailed
		w.Failure = err.Error()
	})
}

// resetWorkloadDir removes whatever is below the workload directory.
func (m *Manager) resetWorkloadDir() {
	dir := m.cfg.Layout.Workload()
	share.UnmountUnder(dir)
	_ = os.RemoveAll(dir)
}

func (m *Manager) prepareWorkloadDir() error {
	m.resetWorkloadDir()
	l := m.cfg.Layout
	for _, d := range []string{l.Share(), l.Upper(), l.Volumes(), l.Logs(), l.Scratch(), l.InlineDir()} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	return nil
}

// bootArgs is the guest command line: the image on virtio-pmem mounted with
// DAX, the agent as init, a virtio console, and idle=poll where nesting
// makes a halted vCPU too slow to wake.
func (m *Manager) bootArgs() string {
	args := "root=/dev/pmem0p1 rootflags=dax,data=ordered,errors=remount-ro ro rootfstype=ext4 panic=1 console=hvc0 init=/usr/bin/kata-agent cgroup_no_v1=all systemd.unified_cgroup_hierarchy=1 agent.log=info"
	if m.cfg.IdlePoll {
		args += " idle=poll"
	}
	return args
}

func (m *Manager) vmConfig(w *Workload) ch.VmConfig {
	l := m.cfg.Layout
	cmdline := m.bootArgs()
	serial := &ch.ConsoleConfig{Mode: "Off"}
	if m.cfg.GuestDebug {
		serial = &ch.ConsoleConfig{Mode: "File", File: filepath.Join(l.Workload(), "serial.log")}
	}
	return ch.VmConfig{
		Cpus:    ch.CpusConfig{BootVcpus: w.Flavor.VCPUs, MaxVcpus: w.Flavor.VCPUs},
		Memory:  ch.MemoryConfig{Size: w.Flavor.MemoryMiB << 20, Shared: true, Thp: true},
		Payload: ch.PayloadConfig{Kernel: m.cfg.Kernel, Cmdline: cmdline},
		Pmem:    []ch.PmemConfig{{ID: "image", File: m.cfg.GuestImage, DiscardWrites: true}},
		Fs:      []ch.FsConfig{{Tag: share.Tag, Socket: m.vfsdSocket(), NumQueues: 1, QueueSize: 1024}},
		Net:     []ch.NetConfig{{Tap: m.net.Tap, Mac: m.net.MAC}},
		Balloon: &ch.BalloonConfig{Size: 0, FreePageReporting: true},
		Rng:     &ch.RngConfig{Src: "/dev/urandom"},
		Serial:  serial,
		Console: &ch.ConsoleConfig{Mode: "File", File: filepath.Join(l.Workload(), "console.log")},
		Vsock:   &ch.VsockConfig{Cid: 3, Socket: m.vsockPath()},
	}
}

func (m *Manager) vsockPath() string  { return filepath.Join(m.cfg.Layout.Workload(), "vsock.sock") }
func (m *Manager) vfsdSocket() string { return filepath.Join(m.cfg.Layout.Workload(), "vfsd.sock") }

func (m *Manager) startVirtiofsd(ctx context.Context) (*ch.Virtiofsd, error) {
	v := &ch.Virtiofsd{
		Bin: m.cfg.Virtiofsd, Socket: m.vfsdSocket(), SharedDir: m.cfg.Layout.Share(),
		LogPath: filepath.Join(m.cfg.Layout.Workload(), "vfsd.log"), Sandbox: m.cfg.VirtiofsdSandbox,
		InodeFileHandles: "prefer",
	}
	err := v.Start(ctx)
	if err != nil && v.Sandbox != "none" {
		// The namespace sandbox needs more than some Pods grant; serve
		// unsandboxed rather than not at all, and say so.
		m.log.Warn("virtiofsd sandbox unavailable, retrying without", zap.String("sandbox", v.Sandbox), zap.Error(err))
		v.Sandbox = "none"
		err = v.Start(ctx)
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

// teardownGuest kills whatever of the guest is running and unmounts the
// share. The workload directory stays.
func (m *Manager) teardownGuest(w *Workload) {
	w.mu.Lock()
	agent, vm, vfsd := w.agent, w.vm, w.vfsd
	w.agent, w.vm, w.vfsd = nil, nil, nil
	w.mu.Unlock()
	if agent != nil {
		agent.Close()
	}
	if vm != nil {
		vm.Kill()
	}
	if vfsd != nil {
		vfsd.Kill()
	}
	share.UnmountUnder(m.cfg.Layout.Share())
	_ = os.Remove(m.vsockPath())
}

// clear forgets the workload and resets the slot's network.
func (m *Manager) clear(w *Workload) {
	m.mu.Lock()
	if m.w == w {
		m.w = nil
	}
	m.mu.Unlock()
	m.teardownNet()
}

// storeClient opens the store a request carries; nil when it carries none.
func storeClient(cfg store.Config) (*store.Client, error) {
	if !cfg.Configured() {
		return nil, nil
	}
	return store.New(cfg)
}

func errorf(format string, args ...any) error { return fmt.Errorf(format, args...) }
