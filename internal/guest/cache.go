package guest

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/agynio/k8s-microvm-runner/internal/share"
	"github.com/agynio/k8s-microvm-runner/internal/snapshot"
	"github.com/agynio/k8s-microvm-runner/internal/versions"
)

type versionsSet = versions.Set

// The slot keeps the snapshot of a workload it hibernated for a while: the
// whole workload directory, moved aside. A wake back into this slot moves
// it back and fetches nothing, which is why the runner prefers the slot a
// workload left.

func (m *Manager) cacheEntry(key string) string {
	return filepath.Join(m.cfg.Layout.SnapshotCache(), key)
}

// cacheWorkload moves the workload directory into the cache.
func (m *Manager) cacheWorkload(w *Workload) error {
	entry := m.cacheEntry(w.Key)
	_ = os.RemoveAll(entry)
	share.UnmountUnder(m.cfg.Layout.Workload())
	// Nothing a wake would not want again: sockets and logs of the VMM.
	for _, f := range []string{"api.sock", "vsock.sock", "vfsd.sock", "vfsd.sock.pid", "ch.log", "vfsd.log", "console.log", "serial.log"} {
		_ = os.Remove(filepath.Join(m.cfg.Layout.Workload(), f))
	}
	_ = os.RemoveAll(m.cfg.Layout.Scratch())
	_ = os.RemoveAll(m.cfg.Layout.Share())
	return os.Rename(m.cfg.Layout.Workload(), entry)
}

// takeCached moves a cached snapshot of the given generation back into
// place, reporting whether it had one.
func (m *Manager) takeCached(key string, generation int) bool {
	entry := m.cacheEntry(key)
	man, err := snapshot.ReadManifest(filepath.Join(entry, snapshot.ManifestObject))
	if err != nil {
		return false
	}
	if man.Generation != generation {
		_ = os.RemoveAll(entry)
		return false
	}
	m.resetWorkloadDir()
	if err := os.Rename(entry, m.cfg.Layout.Workload()); err != nil {
		m.log.Warn("cached snapshot unusable", zap.String("workload_key", key), zap.Error(err))
		_ = os.RemoveAll(entry)
		return false
	}
	return true
}

// CachedSnapshots lists the workload keys this slot caches, newest first.
func (m *Manager) CachedSnapshots() []string {
	entries, err := os.ReadDir(m.cfg.Layout.SnapshotCache())
	if err != nil {
		return nil
	}
	type cached struct {
		key string
		at  time.Time
	}
	var out []cached
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		man, err := snapshot.ReadManifest(filepath.Join(m.cacheEntry(e.Name()), snapshot.ManifestObject))
		if err != nil {
			continue
		}
		out = append(out, cached{key: e.Name(), at: man.HibernatedAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].at.After(out[j].at) })
	keys := make([]string, 0, len(out))
	for _, c := range out {
		keys = append(keys, c.key)
	}
	return keys
}

// DiscardCache drops a cached snapshot.
func (m *Manager) DiscardCache(key string) error {
	if key == "" {
		return nil
	}
	return os.RemoveAll(m.cacheEntry(key))
}

// RunJanitor drops cached snapshots older than the configured TTL until ctx
// ends.
func (m *Manager) RunJanitor(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		entries, err := os.ReadDir(m.cfg.Layout.SnapshotCache())
		if err != nil {
			continue
		}
		for _, e := range entries {
			man, err := snapshot.ReadManifest(filepath.Join(m.cacheEntry(e.Name()), snapshot.ManifestObject))
			if err != nil {
				_ = os.RemoveAll(m.cacheEntry(e.Name()))
				continue
			}
			if time.Since(man.HibernatedAt) > m.cfg.SnapshotCacheTTL {
				m.log.Info("cached snapshot expired", zap.String("workload_key", e.Name()))
				_ = os.RemoveAll(m.cacheEntry(e.Name()))
			}
		}
	}
}
