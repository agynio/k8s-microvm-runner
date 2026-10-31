package guest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/agynio/k8s-microvm-runner/internal/share"
	"github.com/agynio/k8s-microvm-runner/internal/snapshot"
	"github.com/agynio/k8s-microvm-runner/internal/store"
)

func sortStrings(s []string) { sort.Strings(s) }

// fetchVolumes brings every persistent volume's delta from the store into
// its backing directory, and creates the others empty.
func (m *Manager) fetchVolumes(ctx context.Context, w *Workload, st *store.Client, cfg store.Config) error {
	l := m.cfg.Layout
	for i := range w.Volumes {
		v := &w.Volumes[i]
		backing := l.VolumeBacking(v.Name)
		if err := os.MkdirAll(backing, 0o755); err != nil {
			return err
		}
		if !v.Persistent {
			continue
		}
		if st != nil && v.VolumeKey != "" {
			key := store.Key(snapshot.VolumePrefix(cfg, v.Store.Prefix, v.VolumeKey), snapshot.VolumeDeltaObject)
			local := filepath.Join(l.Scratch(), "volume-"+v.Name+".tar.zst")
			t0 := time.Now()
			err := st.GetFile(ctx, v.Store.Bucket, key, local)
			switch {
			case err == nil:
				if err := share.UnpackTreeFromFile(local, backing); err != nil {
					return fmt.Errorf("volume %s: restore delta: %w", v.Name, err)
				}
				_ = os.Remove(local)
				m.log.Info("volume delta restored", zap.String("volume", v.Name), zap.Duration("took", time.Since(t0).Round(time.Millisecond)))
			case isNotFound(err):
				// Provisioned and never written, or provisioned before the
				// store knew it: an empty volume.
			default:
				return fmt.Errorf("volume %s: fetch delta: %w", v.Name, err)
			}
		}
		m.applyQuota(v, backing)
	}
	return nil
}

func isNotFound(err error) bool {
	return errors.Is(err, store.ErrNotFound) || (err != nil && containsNotFound(err.Error()))
}

func containsNotFound(msg string) bool {
	for _, needle := range []string{"NoSuchKey", "does not exist", "not found", "404"} {
		if contains(msg, needle) {
			return true
		}
	}
	return false
}

func contains(s, needle string) bool {
	return len(needle) > 0 && len(s) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(s); i++ {
			if s[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}

// applyQuota puts the declared size on the directory where the filesystem
// can enforce it, and says once when it cannot.
func (m *Manager) applyQuota(v *Volume, backing string) {
	if v.SizeBytes <= 0 {
		return
	}
	key := v.VolumeKey
	if key == "" {
		key = v.InstanceID
	}
	if err := share.ApplyQuota(backing, share.ProjectID(key), v.SizeBytes); err != nil {
		if errors.Is(err, share.ErrQuotaUnsupported) {
			m.log.Warn("volume size is advisory: the cache filesystem cannot enforce a project quota",
				zap.String("volume", v.Name), zap.Int64("size_bytes", v.SizeBytes), zap.Error(err))
			return
		}
		m.log.Warn("volume quota not applied", zap.String("volume", v.Name), zap.Error(err))
	}
}

// mountVolumes binds every volume's backing directory into the share and
// returns the guest path of each.
func (m *Manager) mountVolumes(w *Workload) (map[string]string, error) {
	l := m.cfg.Layout
	guest := map[string]string{}
	for _, v := range w.Volumes {
		if err := share.BindMount(l.VolumeBacking(v.Name), l.VolumeShare(v.Name)); err != nil {
			return nil, err
		}
		guest[v.Name] = share.GuestVolume(v.Name)
	}
	return guest, nil
}

// persistVolumes archives every persistent volume's directory and uploads
// it under the volume's key. The delta is the volume: a stop or a
// hibernation without it would lose what the workload wrote.
func (m *Manager) persistVolumes(ctx context.Context, w *Workload, st *store.Client, cfg store.Config) error {
	if st == nil {
		return nil
	}
	l := m.cfg.Layout
	for _, v := range w.Volumes {
		if !v.Persistent || v.VolumeKey == "" {
			continue
		}
		backing := l.VolumeBacking(v.Name)
		if _, err := os.Stat(backing); err != nil {
			continue
		}
		t0 := time.Now()
		local := filepath.Join(l.Scratch(), "volume-"+v.Name+".tar.zst")
		size, err := share.PackTreeToFile(local, backing)
		if err != nil {
			return fmt.Errorf("volume %s: archive: %w", v.Name, err)
		}
		prefix := snapshot.VolumePrefix(cfg, v.Store.Prefix, v.VolumeKey)
		if _, err := st.PutFile(ctx, v.Store.Bucket, store.Key(prefix, snapshot.VolumeDeltaObject), local); err != nil {
			return fmt.Errorf("volume %s: upload: %w", v.Name, err)
		}
		_ = os.Remove(local)
		manifest := snapshot.VolumeManifest{VolumeKey: v.VolumeKey, InstanceID: v.InstanceID, SizeBytes: v.SizeBytes, DeltaBytes: size, UpdatedAt: time.Now()}
		if existing, err := st.GetBytes(ctx, v.Store.Bucket, store.Key(prefix, snapshot.VolumeManifestObject)); err == nil {
			var old snapshot.VolumeManifest
			if json.Unmarshal(existing, &old) == nil {
				manifest.Labels, manifest.StorageClass, manifest.CreatedAt = old.Labels, old.StorageClass, old.CreatedAt
			}
		}
		if manifest.CreatedAt.IsZero() {
			manifest.CreatedAt = manifest.UpdatedAt
		}
		data, _ := json.Marshal(manifest)
		if _, err := st.PutBytes(ctx, v.Store.Bucket, store.Key(prefix, snapshot.VolumeManifestObject), data, "application/json"); err != nil {
			return fmt.Errorf("volume %s: manifest: %w", v.Name, err)
		}
		m.log.Info("volume delta stored", zap.String("volume", v.Name), zap.Int64("bytes", size), zap.Duration("took", time.Since(t0).Round(time.Millisecond)))
	}
	return nil
}
