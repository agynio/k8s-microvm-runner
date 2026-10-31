// Package snapshot is what hibernation stores and what a wake restores: a
// manifest, the guest's sparse memory image, its VM state and the share's
// deltas, keyed in the store by the workload's key. Volume deltas are
// separate objects keyed by the volume's key; they are the volume.
package snapshot

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/agynio/k8s-microvm-runner/internal/ch"
	"github.com/agynio/k8s-microvm-runner/internal/store"
	"github.com/agynio/k8s-microvm-runner/internal/versions"
)

// Object names below a snapshot's prefix.
const (
	ManifestObject = "manifest.json"
	MemoryObject   = "memory.sparse.zst"
	ConfigObject   = "config.json"
	StateObject    = "state.json"
	DeltasObject   = "deltas.tar.zst"
)

// Volume object names below a volume's prefix.
const (
	VolumeManifestObject = "manifest.json"
	VolumeDeltaObject    = "delta.tar.zst"
)

// Manifest describes a hibernated workload well enough to wake it in any
// slot: what to pull, how to rebuild the share, and what it was.
type Manifest struct {
	WorkloadID   string            `json:"workload_id"`
	WorkloadKey  string            `json:"workload_key"`
	Labels       map[string]string `json:"labels"`
	Flavor       string            `json:"flavor"`
	VCPUs        int               `json:"vcpus"`
	MemoryMiB    int64             `json:"memory_mib"`
	Hostname     string            `json:"hostname"`
	Containers   []Container       `json:"containers"`
	Volumes      []Volume          `json:"volumes"`
	Versions     versions.Set      `json:"versions"`
	HibernatedAt time.Time         `json:"hibernated_at"`
	StartedAt    time.Time         `json:"started_at"`
	// Generation counts the hibernations of this workload, so a cached copy
	// can be told from a newer one in the store.
	Generation int `json:"generation"`
	// Sizes of the objects as stored, and their total.
	ObjectBytes map[string]int64 `json:"object_bytes"`
	TotalBytes  int64            `json:"total_bytes"`
}

// Container is one container as it was when the guest was paused.
type Container struct {
	Name        string    `json:"name"`
	Role        string    `json:"role"`
	Image       string    `json:"image"`
	ImageDigest string    `json:"image_digest"`
	Status      string    `json:"status"`
	ExitCode    *int32    `json:"exit_code,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	FinishedAt  time.Time `json:"finished_at,omitempty"`
	UID         uint32    `json:"uid"`
	GID         uint32    `json:"gid"`
	Mounts      []Mount   `json:"mounts"`
}

// Mount is one of a container's volume mounts, kept for inspect.
type Mount struct {
	Volume      string `json:"volume"`
	Destination string `json:"destination"`
	ReadOnly    bool   `json:"read_only"`
}

// Volume is one of the workload's volumes.
type Volume struct {
	Name       string `json:"name"`
	Persistent bool   `json:"persistent"`
	VolumeKey  string `json:"volume_key,omitempty"`
	InstanceID string `json:"instance_id,omitempty"`
	SizeBytes  int64  `json:"size_bytes,omitempty"`
}

// VolumeManifest describes a persistent volume's delta in the store.
type VolumeManifest struct {
	VolumeKey  string            `json:"volume_key"`
	InstanceID string            `json:"instance_id"`
	Labels     map[string]string `json:"labels"`
	SizeBytes  int64             `json:"size_bytes"`
	// DeltaBytes is the archived delta's size; zero for an empty volume.
	DeltaBytes   int64     `json:"delta_bytes"`
	StorageClass string    `json:"storage_class"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Prefix is where a workload's snapshot objects live.
func Prefix(cfg store.Config, workloadKey string) string {
	return store.Key(cfg.SnapshotsPrefix, workloadKey)
}

// VolumePrefix is where a volume's objects live.
func VolumePrefix(cfg store.Config, prefixOverride, volumeKey string) string {
	prefix := cfg.VolumesPrefix
	if prefixOverride != "" {
		prefix = prefixOverride
	}
	return store.Key(prefix, volumeKey)
}

// WriteManifest writes a manifest file.
func WriteManifest(path string, m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// ReadManifest reads a manifest file.
func ReadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseManifest(data)
}

// ParseManifest decodes a manifest.
func ParseManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse snapshot manifest: %w", err)
	}
	if m.WorkloadID == "" || m.WorkloadKey == "" {
		return nil, errors.New("snapshot manifest names no workload")
	}
	return &m, nil
}

// The sparse memory stream: a header with the file's size, then one record
// per populated extent, all zstd-compressed. Holes are not stored and come
// back as holes, so the restored file is as sparse as the original.
const memoryMagic = "AGYNMEM1"

// PackMemory writes the populated extents of a sparse memory file to w.
func PackMemory(w io.Writer, memoryPath string) (int64, error) {
	f, err := os.Open(memoryPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	counter := &countingWriter{w: w}
	zw, err := zstd.NewWriter(counter, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(2))
	if err != nil {
		return 0, err
	}
	hdr := make([]byte, 16)
	copy(hdr, memoryMagic)
	binary.LittleEndian.PutUint64(hdr[8:], uint64(st.Size()))
	if _, err := zw.Write(hdr); err != nil {
		return 0, err
	}
	buf := make([]byte, 1<<20)
	rec := make([]byte, 16)
	err = ch.WalkDataExtents(f, func(start, end int64) error {
		binary.LittleEndian.PutUint64(rec[0:], uint64(start))
		binary.LittleEndian.PutUint64(rec[8:], uint64(end-start))
		if _, err := zw.Write(rec); err != nil {
			return err
		}
		pos := start
		for pos < end {
			n := int64(len(buf))
			if end-pos < n {
				n = end - pos
			}
			r, err := f.ReadAt(buf[:n], pos)
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			if r == 0 {
				return fmt.Errorf("memory file shrank while packing at %d", pos)
			}
			if _, err := zw.Write(buf[:r]); err != nil {
				return err
			}
			pos += int64(r)
		}
		return nil
	})
	if err != nil {
		zw.Close()
		return 0, err
	}
	if err := zw.Close(); err != nil {
		return 0, err
	}
	return counter.n, nil
}

// UnpackMemory restores a PackMemory stream into a sparse file at
// memoryPath.
func UnpackMemory(r io.Reader, memoryPath string) error {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return err
	}
	defer zr.Close()
	hdr := make([]byte, 16)
	if _, err := io.ReadFull(zr, hdr); err != nil {
		return fmt.Errorf("memory stream header: %w", err)
	}
	if string(hdr[:8]) != memoryMagic {
		return errors.New("memory stream: bad magic")
	}
	size := int64(binary.LittleEndian.Uint64(hdr[8:]))
	if err := os.MkdirAll(filepath.Dir(memoryPath), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(memoryPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	rec := make([]byte, 16)
	buf := make([]byte, 1<<20)
	for {
		if _, err := io.ReadFull(zr, rec); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("memory stream record: %w", err)
		}
		start := int64(binary.LittleEndian.Uint64(rec[0:]))
		length := int64(binary.LittleEndian.Uint64(rec[8:]))
		if start < 0 || length < 0 || start+length > size {
			return fmt.Errorf("memory stream: extent %d+%d outside %d", start, length, size)
		}
		pos := start
		for pos < start+length {
			n := int64(len(buf))
			if start+length-pos < n {
				n = start + length - pos
			}
			if _, err := io.ReadFull(zr, buf[:n]); err != nil {
				return fmt.Errorf("memory stream data: %w", err)
			}
			if _, err := f.WriteAt(buf[:n], pos); err != nil {
				return err
			}
			pos += n
		}
	}
	return f.Sync()
}

// PackMemoryToFile is PackMemory into a file, returning its size.
func PackMemoryToFile(dst, memoryPath string) (int64, error) {
	f, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	n, err := PackMemory(f, memoryPath)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return n, err
}

// UnpackMemoryFromFile is UnpackMemory from a file.
func UnpackMemoryFromFile(src, memoryPath string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	return UnpackMemory(f, memoryPath)
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
