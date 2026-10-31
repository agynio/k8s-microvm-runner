package share

import (
	"strings"
	"testing"
)

// The share's host layout and the guest's view of it must not depend on
// the slot, the workload or the image reference: a woken guest's open
// files are re-opened by path.
func TestLayoutIsTheSameInEverySlot(t *testing.T) {
	a := Layout{Root: "/var/lib/agyn/slot"}
	b := Layout{Root: "/var/lib/agyn/slot"}
	if a.ContainerRootfs("agent") != b.ContainerRootfs("agent") {
		t.Fatal("rootfs paths differ between slots")
	}
	if a.ContainerRootfs("agent") != "/var/lib/agyn/slot/workload/share/agent/rootfs" {
		t.Fatalf("unexpected rootfs path %s", a.ContainerRootfs("agent"))
	}
	if a.VolumeShare("agyn") != "/var/lib/agyn/slot/workload/share/vol/agyn" {
		t.Fatalf("unexpected volume path %s", a.VolumeShare("agyn"))
	}
	if !strings.HasPrefix(a.ContainerUpper("agent"), a.Upper()) || strings.HasPrefix(a.ContainerUpper("agent"), a.Share()) {
		t.Fatal("the upper layer must live outside the share")
	}
	if GuestContainerRootfs("agent") != GuestShare+"/agent/rootfs" {
		t.Fatalf("unexpected guest rootfs %s", GuestContainerRootfs("agent"))
	}
	if GuestVolume("agyn") != GuestShare+"/vol/agyn" {
		t.Fatalf("unexpected guest volume %s", GuestVolume("agyn"))
	}
	if !strings.HasPrefix(GuestInlineFile("f0"), GuestSandboxDir()) {
		t.Fatal("inline files live under the sandbox directory")
	}
}

// An image unpacks to a name derived from its digest, so the same image
// lands at the same path on every slot whatever reference pulled it.
func TestImageDirNameDependsOnTheDigestOnly(t *testing.T) {
	a := ImageDirName("sha256:0123abcd")
	b := ImageDirName("0123abcd")
	if a != b || a != "sha256-0123abcd" {
		t.Fatalf("unexpected image dir names %s %s", a, b)
	}
}
