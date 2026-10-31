//go:build linux

package share

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

// MountOverlay assembles a container rootfs on the host: the unpacked image
// below, the workload's own upper on top. metacopy and index stay off so the
// upper never refers to lower inodes and can be re-mounted over a fresh
// unpack of the same image in another slot. In a user namespace overlayfs
// keeps its markers in user.* xattrs (userxattr), which is what a slot Pod
// with hostUsers: false can write.
func MountOverlay(dst, lower, upper, work string, userxattr bool) error {
	for _, d := range []string{dst, upper, work} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	opts := "lowerdir=" + lower + ",upperdir=" + upper + ",workdir=" + work + ",metacopy=off,index=off"
	if userxattr {
		opts += ",userxattr"
	}
	if err := unix.Mount("overlay", dst, "overlay", unix.MS_NOSUID|unix.MS_NODEV, opts); err != nil {
		return fmt.Errorf("mount overlay at %s: %w", dst, err)
	}
	return nil
}

// BindMount makes src visible at dst, inside the share so virtiofsd serves
// it: the agent accepts bind sources only from inside the share.
func BindMount(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	if err := unix.Mount(src, dst, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
		return fmt.Errorf("bind %s at %s: %w", src, dst, err)
	}
	return nil
}

// UnmountUnder detaches every mount at or below root, deepest first.
func UnmountUnder(root string) {
	for _, mp := range MountsUnder(root) {
		_ = unix.Unmount(mp, unix.MNT_DETACH)
	}
}

// MountsUnder lists the mount points at or below root, deepest first.
func MountsUnder(root string) []string {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil
	}
	defer f.Close()
	var points []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 5 {
			continue
		}
		mp := strings.ReplaceAll(fields[4], `\040`, " ")
		if mp == root || strings.HasPrefix(mp, root+"/") {
			points = append(points, mp)
		}
	}
	sort.Slice(points, func(i, j int) bool { return len(points[i]) > len(points[j]) })
	return points
}

// InUserNamespace reports whether the process runs in a user namespace that
// is not the initial one, which is what a slot Pod with hostUsers: false
// gets.
func InUserNamespace() bool {
	data, err := os.ReadFile("/proc/self/uid_map")
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(data)) != "0 0 4294967295"
}
