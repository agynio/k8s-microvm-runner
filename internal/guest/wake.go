package guest

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"

	"github.com/agynio/k8s-microvm-runner/internal/ch"
	"github.com/agynio/k8s-microvm-runner/internal/image"
	"github.com/agynio/k8s-microvm-runner/internal/kata"
	"github.com/agynio/k8s-microvm-runner/internal/share"
	"github.com/agynio/k8s-microvm-runner/internal/snapshot"
	"github.com/agynio/k8s-microvm-runner/internal/store"
)

// WakeSpec restores a hibernated workload into this slot.
type WakeSpec struct {
	WorkloadID  string
	WorkloadKey string
	Flavor      Flavor
	Store       store.Config
	Volumes     []VolumeStore
	Credentials []*runnerv1.ImagePullCredential
}

const sigCONT = 18

// Wake fetches the workload's snapshot (from the slot's cache when it is
// still there), rebuilds the share at the same paths, restores the guest
// with its memory mapped on demand, resumes it, reseeds its random pool,
// sets its clock and signals its containers. It returns once the guest
// agent answers again.
func (m *Manager) Wake(ctx context.Context, spec WakeSpec) (View, error) {
	st, err := storeClient(spec.Store)
	if err != nil {
		return View{}, err
	}
	if st == nil {
		return View{}, errors.New("a wake needs the snapshot store")
	}
	if spec.WorkloadKey == "" {
		return View{}, errors.New("workload key required")
	}
	prefix := snapshot.Prefix(spec.Store, spec.WorkloadKey)
	data, err := st.GetBytes(ctx, "", store.Key(prefix, snapshot.ManifestObject))
	if err != nil {
		return View{}, fmt.Errorf("snapshot manifest: %w", err)
	}
	man, err := snapshot.ParseManifest(data)
	if err != nil {
		return View{}, err
	}
	if man.Versions.Digest() != m.cfg.Versions.Digest() {
		return View{}, fmt.Errorf("snapshot was taken with %s; this slot runs %s", man.Versions, m.cfg.Versions)
	}
	flavor := spec.Flavor
	if flavor.VCPUs <= 0 || flavor.MemoryMiB <= 0 {
		flavor = Flavor{Name: man.Flavor, VCPUs: man.VCPUs, MemoryMiB: man.MemoryMiB}
	}
	if flavor.MemoryMiB != man.MemoryMiB || flavor.VCPUs != man.VCPUs {
		return View{}, fmt.Errorf("snapshot is a %d vCPU, %d MiB guest; the slot offers %d vCPUs, %d MiB", man.VCPUs, man.MemoryMiB, flavor.VCPUs, flavor.MemoryMiB)
	}

	w := workloadFromManifest(man, flavor)
	w.Status = StatusWaking
	m.applyVolumeRefsUnlocked(w, spec.Volumes)
	m.mu.Lock()
	if m.w != nil {
		m.mu.Unlock()
		return View{}, ErrBusy
	}
	m.w = w
	m.binds++
	m.mu.Unlock()
	if m.cfg.OnChange != nil {
		m.cfg.OnChange(w.View())
	}

	if err := m.restore(ctx, w, man, st, spec, false); err != nil {
		m.fail(w, err)
		m.teardownGuest(w)
		m.resetWorkloadDir()
		m.clear(w)
		return View{}, err
	}
	return w.View(), nil
}

func (m *Manager) applyVolumeRefsUnlocked(w *Workload, refs []VolumeStore) {
	byName := map[string]VolumeStore{}
	for _, r := range refs {
		byName[r.Name] = r
	}
	for i := range w.Volumes {
		if r, ok := byName[w.Volumes[i].Name]; ok {
			w.Volumes[i].Store = r
		}
	}
}

func workloadFromManifest(man *snapshot.Manifest, flavor Flavor) *Workload {
	w := &Workload{
		ID: man.WorkloadID, Key: man.WorkloadKey, Labels: man.Labels, Flavor: flavor, Hostname: man.Hostname,
		StartedAt: man.StartedAt, HibernatedAt: man.HibernatedAt, Generation: man.Generation, SnapshotBytes: man.TotalBytes,
	}
	if w.Labels == nil {
		w.Labels = map[string]string{}
	}
	for _, c := range man.Containers {
		cc := &Container{Name: c.Name, Role: ContainerRole(c.Role), Image: c.Image, ImageDigest: c.ImageDigest, Status: ContainerStatus(c.Status), StartedAt: c.StartedAt, FinishedAt: c.FinishedAt, uid: c.UID, gid: c.GID, Mounts: c.Mounts}
		if c.ExitCode != nil {
			code := *c.ExitCode
			cc.ExitCode = &code
		}
		w.Containers = append(w.Containers, cc)
	}
	for _, v := range man.Volumes {
		w.Volumes = append(w.Volumes, Volume{Name: v.Name, Persistent: v.Persistent, VolumeKey: v.VolumeKey, InstanceID: v.InstanceID, SizeBytes: v.SizeBytes})
	}
	return w
}

// restore brings a guest back from a snapshot: the one in the workload
// directory when local, the slot's cached copy when it has one of the
// manifest's generation, and the store's otherwise.
func (m *Manager) restore(ctx context.Context, w *Workload, man *snapshot.Manifest, st *store.Client, spec WakeSpec, local bool) error {
	t0 := time.Now()
	l := m.cfg.Layout
	cached := local || m.takeCached(w.Key, man.Generation)
	if !cached {
		if err := m.prepareWorkloadDir(); err != nil {
			return err
		}
		if err := m.fetchSnapshot(ctx, w, man, st, spec.Store); err != nil {
			return err
		}
	} else {
		// The cached copy has the deltas in place; the mounts are rebuilt
		// below.
		for _, d := range []string{l.Share(), l.Scratch(), l.Logs()} {
			if err := os.MkdirAll(d, 0o755); err != nil {
				return err
			}
		}
	}
	tFetch := time.Since(t0)

	if err := m.setupNet(ctx); err != nil {
		return fmt.Errorf("network: %w", err)
	}
	creds := credentialsOf(spec.Credentials)
	for _, c := range w.Containers {
		meta, err := m.images.ByDigest(ctx, c.Image, c.ImageDigest, creds)
		if err != nil {
			return err
		}
		if err := share.MountOverlay(l.ContainerRootfs(c.Name), meta.Dir, l.ContainerUpper(c.Name), l.ContainerWork(c.Name), share.InUserNamespace()); err != nil {
			return err
		}
		if c.cwd == "" {
			c.cwd = meta.WorkingDir
		}
	}
	for i := range w.Volumes {
		v := &w.Volumes[i]
		if err := os.MkdirAll(l.VolumeBacking(v.Name), 0o755); err != nil {
			return err
		}
		if v.Persistent {
			m.applyQuota(v, l.VolumeBacking(v.Name))
		}
	}
	if _, err := m.mountVolumes(w); err != nil {
		return err
	}
	tShare := time.Since(t0) - tFetch

	vfsd, err := m.startVirtiofsd(ctx)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.vfsd = vfsd
	w.mu.Unlock()
	_ = os.Remove(m.vsockPath())
	vm := ch.New(m.cfg.CloudHypervisor, l.Workload())
	if err := vm.Spawn(ctx); err != nil {
		return err
	}
	w.mu.Lock()
	w.vm = vm
	w.mu.Unlock()
	tr := time.Now()
	if err := vm.Restore(ctx, l.Snap(), m.cfg.RestoreMode); err != nil {
		return fmt.Errorf("vm.restore: %w (%s)", err, vm.TailLog())
	}
	w.mu.Lock()
	w.onDemand = m.cfg.RestoreMode == ch.RestoreOnDemand
	w.mu.Unlock()
	tRestore := time.Since(tr)
	if err := vm.Resume(ctx); err != nil {
		return fmt.Errorf("vm.resume: %w", err)
	}
	tResume := time.Since(tr)
	agent, err := kata.WaitReady(ctx, m.vsockPath(), 30*time.Second)
	if err != nil {
		return fmt.Errorf("guest agent after restore: %w", err)
	}
	w.mu.Lock()
	w.agent = agent
	w.mu.Unlock()
	tAgent := time.Since(tr)

	// Reseeded before anything in the guest is signalled, so two restores
	// of one image never hand out the same keys or nonces.
	seed := make([]byte, 32)
	_, _ = rand.Read(seed)
	if err := agent.Reseed(ctx, seed); err != nil {
		m.log.Warn("reseed guest random pool", zap.Error(err))
	}
	if err := agent.SetTime(ctx, time.Now()); err != nil {
		m.log.Warn("set guest clock", zap.Error(err))
	}
	m.watchGuest(w, vm)
	for _, c := range w.Containers {
		if c.Status != ContainerRunning {
			continue
		}
		go m.await(w, c)
		m.captureLogs(w, c)
		// A signal sent in the first half second after resume was observed
		// to be lost; one a second later lands. Sent twice, since the
		// receiver's reaction is idempotent.
		go func(name string) {
			for _, delay := range []time.Duration{time.Second, 3 * time.Second} {
				time.Sleep(delay)
				cl, err := kata.Dial(m.vsockPath())
				if err != nil {
					return
				}
				if err := cl.Signal(context.Background(), name, name, sigCONT); err != nil {
					m.log.Debug("wake signal", zap.String("container", name), zap.Error(err))
				}
				cl.Close()
			}
		}(c.Name)
	}
	m.update(w, func() {
		w.Status = StatusRunning
		w.SnapshotBytes = 0
	})
	m.log.Info("workload woken", zap.String("workload", w.ID), zap.Bool("cached", cached), zap.Duration("fetch", tFetch.Round(time.Millisecond)),
		zap.Duration("share", tShare.Round(time.Millisecond)), zap.Duration("restore", tRestore.Round(time.Millisecond)),
		zap.Duration("resume", tResume.Round(time.Millisecond)), zap.Duration("agent", tAgent.Round(time.Millisecond)),
		zap.Duration("total", time.Since(t0).Round(time.Millisecond)))
	return nil
}

// fetchSnapshot downloads a snapshot into a fresh workload directory.
func (m *Manager) fetchSnapshot(ctx context.Context, w *Workload, man *snapshot.Manifest, st *store.Client, cfg store.Config) error {
	l := m.cfg.Layout
	prefix := snapshot.Prefix(cfg, w.Key)
	if err := os.MkdirAll(l.Snap(), 0o755); err != nil {
		return err
	}
	packed := filepath.Join(l.Scratch(), snapshot.MemoryObject)
	if err := st.GetFile(ctx, "", store.Key(prefix, snapshot.MemoryObject), packed); err != nil {
		return err
	}
	if err := snapshot.UnpackMemoryFromFile(packed, filepath.Join(l.Snap(), ch.MemoryFile)); err != nil {
		return fmt.Errorf("unpack memory: %w", err)
	}
	_ = os.Remove(packed)
	for _, f := range ch.SnapshotFiles {
		if err := st.GetFile(ctx, "", store.Key(prefix, f), filepath.Join(l.Snap(), f)); err != nil {
			return err
		}
	}
	deltas := filepath.Join(l.Scratch(), snapshot.DeltasObject)
	if err := st.GetFile(ctx, "", store.Key(prefix, snapshot.DeltasObject), deltas); err != nil {
		return err
	}
	if err := m.unpackDeltas(deltas); err != nil {
		return fmt.Errorf("unpack deltas: %w", err)
	}
	_ = os.Remove(deltas)
	if err := snapshot.WriteManifest(filepath.Join(l.Workload(), snapshot.ManifestObject), man); err != nil {
		return err
	}
	return m.fetchVolumes(ctx, w, st, cfg)
}

var _ = image.Credential{}
