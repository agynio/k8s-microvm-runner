package share

import "path/filepath"

// The share has the same layout on every slot, and the guest sees it at the
// same mount point, so a woken guest's open files are re-opened by path by
// a fresh virtiofsd in whichever slot it lands.
const (
	// GuestContainerBase is where the agent keeps container state.
	GuestContainerBase = "/run/kata-containers"
	// GuestShare is where the agent mounts the share; container rootfs
	// trees and volumes are reached below it by the same relative paths the
	// host assembled them at.
	GuestShare = GuestContainerBase + "/shared/containers"
	// Tag is the virtio-fs tag the guest mounts.
	Tag = "kataShared"

	sandboxDirName = "sandbox"
	inlineDirName  = "inline"
	volumesDirName = "vol"
)

// Layout is where everything of a slot lives on its disk. Root is the slot's
// cache volume; the workload directory is the same path for every workload
// a slot hosts, because the share's host paths must be the same in every
// slot for a restore to find them.
type Layout struct {
	Root string
}

// Images is where unpacked images live, one directory per digest.
func (l Layout) Images() string { return filepath.Join(l.Root, "images") }

// Workload is the directory of whatever the slot hosts right now.
func (l Layout) Workload() string { return filepath.Join(l.Root, "workload") }

// Share is the directory virtiofsd serves.
func (l Layout) Share() string { return filepath.Join(l.Workload(), "share") }

// Upper holds every container's writable layer and overlay work directory.
func (l Layout) Upper() string { return filepath.Join(l.Workload(), "upper") }

// Volumes holds the backing directory of every volume, bound into the share.
func (l Layout) Volumes() string { return filepath.Join(l.Workload(), "volumes") }

// Snap is where the guest's memory and VM state are written.
func (l Layout) Snap() string { return filepath.Join(l.Workload(), "snap") }

// Logs holds the captured stdio of every container.
func (l Layout) Logs() string { return filepath.Join(l.Workload(), "logs") }

// Scratch is where archives are staged on their way to and from the store.
func (l Layout) Scratch() string { return filepath.Join(l.Workload(), "scratch") }

// SnapshotCache is where the snapshot of a hibernated workload is kept for a
// while after it leaves, so a wake back into this slot fetches nothing.
func (l Layout) SnapshotCache() string { return filepath.Join(l.Root, "snapshots") }

// ContainerRootfs is a container's merged root filesystem in the share.
func (l Layout) ContainerRootfs(name string) string {
	return filepath.Join(l.Share(), name, "rootfs")
}

// ContainerUpper is a container's writable layer.
func (l Layout) ContainerUpper(name string) string { return filepath.Join(l.Upper(), name, "fs") }

// ContainerWork is a container's overlay work directory.
func (l Layout) ContainerWork(name string) string { return filepath.Join(l.Upper(), name, "work") }

// VolumeShare is where a volume appears in the share.
func (l Layout) VolumeShare(name string) string {
	return filepath.Join(l.Share(), volumesDirName, name)
}

// VolumeBacking is a volume's backing directory.
func (l Layout) VolumeBacking(name string) string { return filepath.Join(l.Volumes(), name) }

// SandboxDir holds the hosts, hostname and resolv.conf every container
// binds, and the inline files.
func (l Layout) SandboxDir() string { return filepath.Join(l.Share(), sandboxDirName) }

// InlineDir holds the inline files.
func (l Layout) InlineDir() string { return filepath.Join(l.SandboxDir(), inlineDirName) }

// GuestContainerRootfs is where the guest sees a container's rootfs.
func GuestContainerRootfs(name string) string { return GuestShare + "/" + name + "/rootfs" }

// GuestVolume is where the guest sees a volume.
func GuestVolume(name string) string { return GuestShare + "/" + volumesDirName + "/" + name }

// GuestSandboxDir is where the guest sees the sandbox files.
func GuestSandboxDir() string { return GuestShare + "/" + sandboxDirName }

// GuestInlineFile is where the guest sees an inline file.
func GuestInlineFile(name string) string { return GuestSandboxDir() + "/" + inlineDirName + "/" + name }

// ImageDirName is the directory an image digest unpacks into, the same on
// every slot.
func ImageDirName(digest string) string {
	return "sha256-" + trimDigest(digest)
}

func trimDigest(digest string) string {
	if len(digest) > 7 && digest[:7] == "sha256:" {
		return digest[7:]
	}
	return digest
}
