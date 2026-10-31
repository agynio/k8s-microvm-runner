// Package share assembles the one directory tree a guest sees over
// virtio-fs and moves its deltas in and out of archives: for every
// container an overlay whose lower layer is the unpacked image and whose
// upper layer is the workload's writable layer, and for every volume a
// directory. The upper layer is the delta, so a hibernation archives upper
// layers and volume directories, never images.
package share

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"
)

const xattrPAXPrefix = "SCHILY.xattr."

// ExtractTar unpacks a tar stream below root, keeping what a container
// filesystem needs kept: ownership, modes, timestamps, symlinks and hard
// links, device nodes (an overlay whiteout is one), and extended attributes
// (file capabilities, overlay opaque markers). An entry that would escape
// root is refused.
func ExtractTar(r io.Reader, root string) error {
	root = filepath.Clean(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	tr := tar.NewReader(r)
	type dirFix struct {
		path  string
		mode  os.FileMode
		mtime time.Time
	}
	var dirs []dirFix
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar: %w", err)
		}
		rel, ok := safeRelPath(hdr.Name)
		if !ok {
			return fmt.Errorf("tar entry %q escapes the root", hdr.Name)
		}
		if rel == "" {
			continue
		}
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := insideRoot(root, filepath.Dir(path)); err != nil {
			return fmt.Errorf("tar entry %q: %w", hdr.Name, err)
		}
		mode := hdr.FileInfo().Mode()
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0o755); err != nil {
				return err
			}
			dirs = append(dirs, dirFix{path: path, mode: mode.Perm() | (mode & (os.ModeSetuid | os.ModeSetgid | os.ModeSticky)), mtime: hdr.ModTime})
		case tar.TypeReg, tar.TypeRegA:
			_ = os.RemoveAll(path)
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode.Perm())
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return fmt.Errorf("write %s: %w", rel, err)
			}
			f.Close()
		case tar.TypeSymlink:
			_ = os.RemoveAll(path)
			if err := os.Symlink(hdr.Linkname, path); err != nil {
				return err
			}
		case tar.TypeLink:
			targetRel, ok := safeRelPath(hdr.Linkname)
			if !ok {
				return fmt.Errorf("tar link %q escapes the root", hdr.Linkname)
			}
			_ = os.RemoveAll(path)
			if err := os.Link(filepath.Join(root, targetRel), path); err != nil {
				return err
			}
			// A hard link shares its target's metadata; nothing more to set.
			continue
		case tar.TypeChar, tar.TypeBlock, tar.TypeFifo:
			_ = os.RemoveAll(path)
			m := uint32(mode.Perm())
			switch hdr.Typeflag {
			case tar.TypeChar:
				m |= unix.S_IFCHR
			case tar.TypeBlock:
				m |= unix.S_IFBLK
			default:
				m |= unix.S_IFIFO
			}
			if err := unix.Mknod(path, m, int(unix.Mkdev(uint32(hdr.Devmajor), uint32(hdr.Devminor)))); err != nil {
				return fmt.Errorf("mknod %s: %w", rel, err)
			}
		default:
			continue
		}
		if err := applyMetadata(path, hdr, mode); err != nil {
			return err
		}
	}
	// Directories last and deepest first: a read-only directory would
	// otherwise refuse its own children.
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i].path) > len(dirs[j].path) })
	for _, d := range dirs {
		if err := os.Chmod(d.path, d.mode); err != nil {
			return err
		}
		_ = os.Chtimes(d.path, d.mtime, d.mtime)
	}
	return nil
}

func applyMetadata(path string, hdr *tar.Header, mode os.FileMode) error {
	if err := os.Lchown(path, hdr.Uid, hdr.Gid); err != nil && !errors.Is(err, syscall.EPERM) {
		return fmt.Errorf("chown %s: %w", hdr.Name, err)
	}
	if hdr.Typeflag == tar.TypeDir {
		return applyXattrs(path, hdr)
	}
	if hdr.Typeflag != tar.TypeSymlink {
		// Chown clears setuid/setgid, so the mode goes on afterwards.
		if err := os.Chmod(path, mode.Perm()|(mode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky))); err != nil {
			return fmt.Errorf("chmod %s: %w", hdr.Name, err)
		}
		_ = os.Chtimes(path, hdr.ModTime, hdr.ModTime)
	}
	return applyXattrs(path, hdr)
}

func applyXattrs(path string, hdr *tar.Header) error {
	for key, value := range hdr.PAXRecords {
		if !strings.HasPrefix(key, xattrPAXPrefix) {
			continue
		}
		name := strings.TrimPrefix(key, xattrPAXPrefix)
		if err := unix.Lsetxattr(path, name, []byte(value), 0); err != nil {
			if errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.EPERM) {
				continue
			}
			return fmt.Errorf("setxattr %s on %s: %w", name, hdr.Name, err)
		}
	}
	return nil
}

// insideRoot refuses a parent directory that resolves, through a symlink
// the archive itself may have planted, to somewhere outside root.
func insideRoot(root, dir string) error {
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return err
	}
	if resolved != resolvedRoot && !strings.HasPrefix(resolved, resolvedRoot+string(filepath.Separator)) {
		return fmt.Errorf("parent %s resolves outside the root", dir)
	}
	return nil
}

func safeRelPath(name string) (string, bool) {
	cleaned := filepath.Clean("/" + name)
	rel := strings.TrimPrefix(cleaned, "/")
	if rel == "." {
		rel = ""
	}
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return "", false
	}
	return rel, true
}

// ArchiveTree writes everything below root as a tar stream, with the same
// fidelity ExtractTar restores. Hard links within the tree are written as
// links; the stream is relative to root.
func ArchiveTree(w io.Writer, root string) error {
	root = filepath.Clean(root)
	tw := tar.NewWriter(w)
	seen := map[[2]uint64]string{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if rel == "." {
			return nil
		}
		if info.Mode()&os.ModeSocket != 0 {
			// A unix socket is its process's, not the filesystem's: the
			// process that owns it recreates it, and tar cannot carry it.
			return nil
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("stat %s: no inode information", path)
		}
		var link string
		if info.Mode()&os.ModeSymlink != 0 {
			link, err = os.Readlink(path)
			if err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = rel
		if info.IsDir() {
			hdr.Name += "/"
		}
		hdr.Uid, hdr.Gid = int(st.Uid), int(st.Gid)
		hdr.Uname, hdr.Gname = "", ""
		hdr.Format = tar.FormatPAX
		if info.Mode()&(os.ModeDevice|os.ModeCharDevice) != 0 {
			hdr.Devmajor = int64(unix.Major(uint64(st.Rdev)))
			hdr.Devminor = int64(unix.Minor(uint64(st.Rdev)))
		}
		if info.Mode().IsRegular() && st.Nlink > 1 {
			key := [2]uint64{uint64(st.Dev), uint64(st.Ino)}
			if target, dup := seen[key]; dup {
				hdr.Typeflag = tar.TypeLink
				hdr.Linkname = target
				hdr.Size = 0
			} else {
				seen[key] = rel
			}
		}
		if info.Mode()&os.ModeSymlink == 0 {
			xattrs, err := readXattrs(path)
			if err != nil {
				return err
			}
			if len(xattrs) > 0 {
				hdr.PAXRecords = map[string]string{}
				for name, value := range xattrs {
					hdr.PAXRecords[xattrPAXPrefix+name] = value
				}
			}
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if hdr.Typeflag == tar.TypeReg {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			_, err = io.CopyN(tw, f, hdr.Size)
			f.Close()
			if err != nil {
				return fmt.Errorf("archive %s: %w", rel, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return tw.Close()
}

func readXattrs(path string) (map[string]string, error) {
	buf := make([]byte, 4096)
	n, err := unix.Llistxattr(path, buf)
	if err != nil {
		if errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.EOPNOTSUPP) {
			return nil, nil
		}
		if errors.Is(err, syscall.ERANGE) {
			buf = make([]byte, 1<<16)
			n, err = unix.Llistxattr(path, buf)
		}
		if err != nil {
			return nil, fmt.Errorf("listxattr %s: %w", path, err)
		}
	}
	out := map[string]string{}
	for _, name := range strings.Split(strings.TrimRight(string(buf[:n]), "\x00"), "\x00") {
		if name == "" {
			continue
		}
		value := make([]byte, 4096)
		vn, err := unix.Lgetxattr(path, name, value)
		if err != nil {
			if errors.Is(err, syscall.ERANGE) {
				value = make([]byte, 1<<16)
				vn, err = unix.Lgetxattr(path, name, value)
			}
			if err != nil {
				if errors.Is(err, syscall.ENODATA) || errors.Is(err, syscall.EPERM) {
					continue
				}
				return nil, fmt.Errorf("getxattr %s on %s: %w", name, path, err)
			}
		}
		out[name] = string(value[:vn])
	}
	return out, nil
}

// PackTree writes a zstd-compressed tar of root to w.
func PackTree(w io.Writer, root string) error {
	zw, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(2))
	if err != nil {
		return err
	}
	if err := ArchiveTree(zw, root); err != nil {
		zw.Close()
		return err
	}
	return zw.Close()
}

// UnpackTree restores a PackTree archive below root.
func UnpackTree(r io.Reader, root string) error {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return err
	}
	defer zr.Close()
	return ExtractTar(zr, root)
}

// PackTreeToFile is PackTree into a new file at path, returning its size.
func PackTreeToFile(path, root string) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	if err := PackTree(f, root); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	st, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// UnpackTreeFromFile is UnpackTree from a file.
func UnpackTreeFromFile(path, root string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return UnpackTree(f, root)
}

// TreeBytes is the apparent size of everything below root, counting a
// hard-linked file once.
func TreeBytes(root string) int64 {
	var total int64
	seen := map[[2]uint64]struct{}{}
	_ = filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil || !info.Mode().IsRegular() {
			return nil
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			key := [2]uint64{uint64(st.Dev), uint64(st.Ino)}
			if _, dup := seen[key]; dup {
				return nil
			}
			seen[key] = struct{}{}
		}
		total += info.Size()
		return nil
	})
	return total
}
