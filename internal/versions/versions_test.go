package versions

import "testing"

// A snapshot is restored only by a slot running the same set: the digest
// changes with any member.
func TestDigestChangesWithAnyMember(t *testing.T) {
	base := Set{CloudHypervisor: "v52.0", Virtiofsd: "1.14.0", GuestKernel: "vmlinux-6.18.56-208", GuestImage: "kata-ubuntu-resolute.image"}
	same := base
	if base.Digest() != same.Digest() {
		t.Fatal("expected equal sets to digest equally")
	}
	bumped := base
	bumped.CloudHypervisor = "v53.0"
	if base.Digest() == bumped.Digest() {
		t.Fatal("expected a different VMM to change the digest")
	}
	if base.IsZero() || (Set{}).IsZero() == false {
		t.Fatal("IsZero is wrong")
	}
}
