package guest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/agynio/k8s-microvm-runner/internal/ch"
	"github.com/agynio/k8s-microvm-runner/internal/share"
	"github.com/agynio/k8s-microvm-runner/internal/snapshot"
	"github.com/agynio/k8s-microvm-runner/internal/store"
)

// Hibernate pauses the guest, snapshots its memory and VM state, archives
// the share's deltas, uploads everything under the workload's key, tears the
// guest down and keeps a cached copy. It returns the snapshot's size in the
// store once it is durable.
func (m *Manager) Hibernate(ctx context.Context, cfg store.Config, volumes []VolumeStore) (int64, error) {
	w := m.Current()
	if w == nil {
		return 0, ErrIdle
	}
	st, err := storeClient(cfg)
	if err != nil {
		return 0, err
	}
	if st == nil {
		return 0, fmt.Errorf("hibernation needs a snapshot store")
	}
	w.mu.Lock()
	if w.Status != StatusRunning || w.vm == nil {
		status := w.Status
		w.mu.Unlock()
		return 0, fmt.Errorf("workload is %s", status)
	}
	vm := w.vm
	w.mu.Unlock()
	m.update(w, func() { w.Status = StatusHibernating })
	m.applyVolumeRefs(w, volumes)

	t0 := time.Now()
	if err := vm.Pause(ctx); err != nil {
		m.update(w, func() { w.Status = StatusRunning })
		return 0, fmt.Errorf("pause: %w", err)
	}
	tPause := time.Since(t0)
	l := m.cfg.Layout
	// A VM restored on demand still pages from snap/, so the new image goes
	// beside it and is folded in afterwards.
	dest := l.Snap()
	if _, err := os.Stat(dest); err == nil {
		dest = l.Snap() + ".new"
	}
	_ = os.RemoveAll(dest)
	if err := vm.Snapshot(ctx, dest); err != nil {
		_ = vm.Resume(ctx)
		m.update(w, func() { w.Status = StatusRunning })
		return 0, fmt.Errorf("snapshot: %w", err)
	}
	tSnapshot := time.Since(t0)

	// From here the guest is gone; a failure fails the workload.
	m.teardownGuest(w)
	var tMerge time.Duration
	if dest != l.Snap() {
		tm := time.Now()
		if w.onDemand {
			if err := ch.MergeDelta(l.Snap(), dest); err != nil {
				m.fail(w, fmt.Errorf("merge snapshot: %w", err))
				return 0, err
			}
		} else {
			_ = os.RemoveAll(l.Snap())
			if err := os.Rename(dest, l.Snap()); err != nil {
				m.fail(w, err)
				return 0, err
			}
		}
		tMerge = time.Since(tm)
	}
	w.mu.Lock()
	w.onDemand = false
	w.Generation++
	w.HibernatedAt = time.Now()
	w.mu.Unlock()

	tu := time.Now()
	size, err := m.uploadSnapshot(ctx, w, st, cfg)
	if err == nil {
		err = m.persistVolumes(ctx, w, st, cfg)
	}
	if err != nil {
		// Whatever reached the store is not a snapshot without its manifest;
		// it is removed rather than left for nobody.
		if derr := st.DeletePrefix(ctx, "", snapshot.Prefix(cfg, w.Key)+"/"); derr != nil {
			m.log.Warn("partial snapshot not removed from the store", zap.Error(derr))
		}
		// The snapshot is whole on the slot's disk; only the store failed.
		// The guest is brought back from it, so a store outage costs the
		// workload a few seconds rather than its life, and the caller is
		// told the hibernation did not happen.
		m.log.Error("snapshot not stored; restoring the guest from the local snapshot", zap.String("workload", w.ID), zap.Error(err))
		if rerr := m.restore(ctx, w, w.manifest(snapshotVersions{set: m.cfg.Versions}), nil, WakeSpec{}, true); rerr != nil {
			m.fail(w, fmt.Errorf("store snapshot: %v; restore the guest: %w", err, rerr))
			m.teardownGuest(w)
			return 0, err
		}
		return 0, fmt.Errorf("store snapshot: %w (the workload is running again)", err)
	}
	tUpload := time.Since(tu)

	if err := m.cacheWorkload(w); err != nil {
		m.log.Warn("snapshot not cached in the slot", zap.Error(err))
		m.resetWorkloadDir()
	}
	m.update(w, func() {
		w.Status = StatusHibernated
		w.SnapshotBytes = size
	})
	m.clear(w)
	m.log.Info("workload hibernated", zap.String("workload", w.ID), zap.Duration("pause", tPause.Round(time.Millisecond)),
		zap.Duration("snapshot", tSnapshot.Round(time.Millisecond)), zap.Duration("merge", tMerge.Round(time.Millisecond)),
		zap.Duration("upload", tUpload.Round(time.Millisecond)), zap.Int64("bytes", size), zap.Duration("total", time.Since(t0).Round(time.Millisecond)))
	return size, nil
}

func (m *Manager) applyVolumeRefs(w *Workload, refs []VolumeStore) {
	if len(refs) == 0 {
		return
	}
	byName := map[string]VolumeStore{}
	for _, r := range refs {
		byName[r.Name] = r
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range w.Volumes {
		if r, ok := byName[w.Volumes[i].Name]; ok {
			w.Volumes[i].Store = r
			if w.Volumes[i].VolumeKey == "" {
				w.Volumes[i].VolumeKey = r.VolumeKey
			}
		}
	}
}

// manifest describes the workload as it is now.
func (w *Workload) manifest(set snapshotVersions) *snapshot.Manifest {
	w.mu.Lock()
	defer w.mu.Unlock()
	man := &snapshot.Manifest{
		WorkloadID: w.ID, WorkloadKey: w.Key, Labels: w.Labels, Flavor: w.Flavor.Name, VCPUs: w.Flavor.VCPUs, MemoryMiB: w.Flavor.MemoryMiB,
		Hostname: w.Hostname, Versions: set.set, HibernatedAt: w.HibernatedAt, StartedAt: w.StartedAt, Generation: w.Generation,
	}
	for _, c := range w.Containers {
		mc := snapshot.Container{Name: c.Name, Role: string(c.Role), Image: c.Image, ImageDigest: c.ImageDigest, Status: string(c.Status), StartedAt: c.StartedAt, FinishedAt: c.FinishedAt, UID: c.uid, GID: c.gid, Mounts: c.Mounts}
		if c.ExitCode != nil {
			code := *c.ExitCode
			mc.ExitCode = &code
		}
		man.Containers = append(man.Containers, mc)
	}
	for _, v := range w.Volumes {
		man.Volumes = append(man.Volumes, snapshot.Volume{Name: v.Name, Persistent: v.Persistent, VolumeKey: v.VolumeKey, InstanceID: v.InstanceID, SizeBytes: v.SizeBytes})
	}
	return man
}

type snapshotVersions struct{ set versionsSet }

// uploadSnapshot packs and uploads the memory, VM state and deltas, then
// the manifest last: the manifest is what the runner lists, so a snapshot
// is visible only once it is whole.
func (m *Manager) uploadSnapshot(ctx context.Context, w *Workload, st *store.Client, cfg store.Config) (int64, error) {
	l := m.cfg.Layout
	prefix := snapshot.Prefix(cfg, w.Key)
	sizes := map[string]int64{}

	memoryPacked := filepath.Join(l.Scratch(), snapshot.MemoryObject)
	if _, err := snapshot.PackMemoryToFile(memoryPacked, filepath.Join(l.Snap(), ch.MemoryFile)); err != nil {
		return 0, fmt.Errorf("pack memory: %w", err)
	}
	n, err := st.PutFile(ctx, "", store.Key(prefix, snapshot.MemoryObject), memoryPacked)
	if err != nil {
		return 0, err
	}
	sizes[snapshot.MemoryObject] = n
	_ = os.Remove(memoryPacked)

	for _, f := range ch.SnapshotFiles {
		n, err := st.PutFile(ctx, "", store.Key(prefix, f), filepath.Join(l.Snap(), f))
		if err != nil {
			return 0, err
		}
		sizes[f] = n
	}

	deltas := filepath.Join(l.Scratch(), snapshot.DeltasObject)
	if err := m.packDeltas(deltas); err != nil {
		return 0, fmt.Errorf("pack deltas: %w", err)
	}
	n, err = st.PutFile(ctx, "", store.Key(prefix, snapshot.DeltasObject), deltas)
	if err != nil {
		return 0, err
	}
	sizes[snapshot.DeltasObject] = n
	_ = os.Remove(deltas)

	man := w.manifest(snapshotVersions{set: m.cfg.Versions})
	man.ObjectBytes = sizes
	for _, s := range sizes {
		man.TotalBytes += s
	}
	manifestPath := filepath.Join(l.Workload(), snapshot.ManifestObject)
	if err := snapshot.WriteManifest(manifestPath, man); err != nil {
		return 0, err
	}
	data, _ := os.ReadFile(manifestPath)
	if _, err := st.PutBytes(ctx, "", store.Key(prefix, snapshot.ManifestObject), data, "application/json"); err != nil {
		return 0, err
	}
	return man.TotalBytes, nil
}

// deltaDirs are the parts of the workload directory that are the
// workload's own and travel with its snapshot: every container's writable
// layer, the scratch volumes, the sandbox files and the captured logs.
// Persistent volumes travel under their own keys.
func (m *Manager) packDeltas(dst string) error {
	l := m.cfg.Layout
	staging := filepath.Join(l.Scratch(), "deltas")
	_ = os.RemoveAll(staging)
	defer os.RemoveAll(staging)
	if err := os.MkdirAll(staging, 0o755); err != nil {
		return err
	}
	// Bind rather than copy: the archive walks the staged tree, and a bind
	// costs nothing.
	entries := map[string]string{
		"upper":   l.Upper(),
		"sandbox": l.SandboxDir(),
		"logs":    l.Logs(),
	}
	w := m.Current()
	if w != nil {
		for _, v := range w.Volumes {
			if !v.Persistent {
				entries["volumes/"+v.Name] = l.VolumeBacking(v.Name)
			}
		}
	}
	for name, src := range entries {
		if _, err := os.Stat(src); err != nil {
			continue
		}
		if err := share.BindMount(src, filepath.Join(staging, name)); err != nil {
			return err
		}
	}
	defer share.UnmountUnder(staging)
	if _, err := share.PackTreeToFile(dst, staging); err != nil {
		return err
	}
	return nil
}

// unpackDeltas restores a deltas archive into the workload directory.
func (m *Manager) unpackDeltas(src string) error {
	l := m.cfg.Layout
	staging := filepath.Join(l.Scratch(), "deltas")
	_ = os.RemoveAll(staging)
	if err := share.UnpackTreeFromFile(src, staging); err != nil {
		return err
	}
	moves := map[string]string{
		"upper":   l.Upper(),
		"sandbox": l.SandboxDir(),
		"logs":    l.Logs(),
	}
	for name, dst := range moves {
		from := filepath.Join(staging, name)
		if _, err := os.Stat(from); err != nil {
			continue
		}
		_ = os.RemoveAll(dst)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := os.Rename(from, dst); err != nil {
			return err
		}
	}
	volumes, _ := os.ReadDir(filepath.Join(staging, "volumes"))
	for _, entry := range volumes {
		dst := l.VolumeBacking(entry.Name())
		_ = os.RemoveAll(dst)
		if err := os.Rename(filepath.Join(staging, "volumes", entry.Name()), dst); err != nil {
			return err
		}
	}
	return os.RemoveAll(staging)
}
