//go:build linux

package share

import (
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// A persistent volume's declared size is a quota on its directory. The one
// mechanism an unprivileged Pod can use for that is a project quota, which
// the filesystem under the slot's cache must support (ext4 or XFS mounted
// with prjquota). Where it does not, the size is recorded and the quota is
// advisory: the volume still works, and the supervisor says so once.

const (
	fsIOCGetXattr      = 0x801c581f // FS_IOC_FSGETXATTR
	fsIOCSetXattr      = 0x401c5820 // FS_IOC_FSSETXATTR
	fsXflagProjInherit = 0x00000200
	qSetQuota          = 0x800008
	prjQuota           = 2
	qifBLimits         = 1
	quotaBlock         = 1024
	projectIDBase      = 100000
	projectIDSpan      = 1000000
)

type fsxattr struct {
	Xflags     uint32
	Extsize    uint32
	Nextents   uint32
	Projid     uint32
	Cowextsize uint32
	Pad        [8]byte
}

type dqblk struct {
	BHardLimit uint64
	BSoftLimit uint64
	CurSpace   uint64
	IHardLimit uint64
	ISoftLimit uint64
	CurInodes  uint64
	BTime      uint64
	ITime      uint64
	Valid      uint32
	_          uint32
}

// ErrQuotaUnsupported says the filesystem under dir cannot enforce a
// project quota, so the declared size is advisory.
var ErrQuotaUnsupported = errors.New("project quotas are not available on this filesystem")

// ProjectID derives a stable project id from a volume's key, so the same
// volume lands on the same project in every slot.
func ProjectID(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return projectIDBase + h.Sum32()%projectIDSpan
}

// ApplyQuota puts dir, and everything created below it, under a project
// limited to bytes. ErrQuotaUnsupported means the filesystem cannot do it.
func ApplyQuota(dir string, projectID uint32, bytes int64) error {
	if bytes <= 0 {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	var attr fsxattr
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), fsIOCGetXattr, uintptr(unsafe.Pointer(&attr))); errno != 0 {
		return fmt.Errorf("%w: %v", ErrQuotaUnsupported, errno)
	}
	attr.Projid = projectID
	attr.Xflags |= fsXflagProjInherit
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, f.Fd(), fsIOCSetXattr, uintptr(unsafe.Pointer(&attr))); errno != 0 {
		return fmt.Errorf("%w: %v", ErrQuotaUnsupported, errno)
	}
	limit := dqblk{BHardLimit: uint64((bytes + quotaBlock - 1) / quotaBlock), Valid: qifBLimits}
	cmd := uintptr(qSetQuota<<8 | prjQuota)
	if _, _, errno := unix.Syscall6(unix.SYS_QUOTACTL_FD, f.Fd(), cmd, uintptr(projectID), uintptr(unsafe.Pointer(&limit)), 0, 0); errno != 0 {
		return fmt.Errorf("%w: %v", ErrQuotaUnsupported, errno)
	}
	return nil
}
