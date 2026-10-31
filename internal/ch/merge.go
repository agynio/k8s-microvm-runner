package ch

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// MemoryFile is the guest RAM image inside a snapshot directory.
const MemoryFile = "memory-ranges"

// SnapshotFiles are the files a Cloud Hypervisor snapshot directory holds
// besides the memory.
var SnapshotFiles = []string{"config.json", "state.json"}

// MergeDelta folds a snapshot taken after an on-demand restore into the
// snapshot it was restored from: only the pages the guest touched since the
// restore are populated in the delta's memory file, so its data extents are
// copied over the base in place, and the delta's config and state replace the
// base's. The delta directory is removed. The result is one snapshot, so a
// wake never replays a chain.
func MergeDelta(baseDir, deltaDir string) error {
	base, err := os.OpenFile(filepath.Join(baseDir, MemoryFile), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer base.Close()
	delta, err := os.Open(filepath.Join(deltaDir, MemoryFile))
	if err != nil {
		return err
	}
	defer delta.Close()
	bs, err := base.Stat()
	if err != nil {
		return err
	}
	ds, err := delta.Stat()
	if err != nil {
		return err
	}
	if bs.Size() != ds.Size() {
		return fmt.Errorf("memory files differ in size: base %d, delta %d", bs.Size(), ds.Size())
	}
	if _, err := CopyDataExtents(delta, base); err != nil {
		return err
	}
	if err := base.Sync(); err != nil {
		return err
	}
	for _, f := range SnapshotFiles {
		if err := os.Rename(filepath.Join(deltaDir, f), filepath.Join(baseDir, f)); err != nil {
			return err
		}
	}
	return os.RemoveAll(deltaDir)
}

// CopyDataExtents writes every populated extent of src at the same offset in
// dst and returns how many bytes it copied.
func CopyDataExtents(src, dst *os.File) (int64, error) {
	var copied int64
	buf := make([]byte, 1<<20)
	err := WalkDataExtents(src, func(start, end int64) error {
		pos := start
		for pos < end {
			n := int64(len(buf))
			if end-pos < n {
				n = end - pos
			}
			r, err := src.ReadAt(buf[:n], pos)
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			if r == 0 {
				return nil
			}
			if _, err := dst.WriteAt(buf[:r], pos); err != nil {
				return err
			}
			pos += int64(r)
			copied += int64(r)
		}
		return nil
	})
	return copied, err
}

// WalkDataExtents calls fn for every populated extent of a sparse file, in
// order.
func WalkDataExtents(f *os.File, fn func(start, end int64) error) error {
	fd := int(f.Fd())
	off := int64(0)
	for {
		start, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				return nil
			}
			return fmt.Errorf("SEEK_DATA: %w", err)
		}
		end, err := unix.Seek(fd, start, unix.SEEK_HOLE)
		if err != nil {
			return fmt.Errorf("SEEK_HOLE: %w", err)
		}
		if err := fn(start, end); err != nil {
			return err
		}
		off = end
	}
}

// AllocatedBytes is the disk space a (sparse) file really occupies.
func AllocatedBytes(path string) int64 {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return 0
	}
	return st.Blocks * 512
}
