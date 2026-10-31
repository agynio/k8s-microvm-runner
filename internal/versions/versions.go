// Package versions names what a slot runs a guest with. A snapshot records
// the set it was taken with and is only restored by a slot running the same:
// a VM state file from one Cloud Hypervisor is not a VM state file for
// another, and a guest kernel's memory image is not another kernel's.
package versions

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Set is the pinned combination a slot image ships. It is written into the
// image at build time and read by the supervisor at start.
type Set struct {
	CloudHypervisor string `json:"cloud_hypervisor"`
	Virtiofsd       string `json:"virtiofsd"`
	GuestKernel     string `json:"guest_kernel"`
	GuestImage      string `json:"guest_image"`
}

// Load reads the set the slot image records.
func Load(path string) (Set, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Set{}, fmt.Errorf("read version set %s: %w", path, err)
	}
	var set Set
	if err := json.Unmarshal(data, &set); err != nil {
		return Set{}, fmt.Errorf("parse version set %s: %w", path, err)
	}
	if set.IsZero() {
		return Set{}, fmt.Errorf("version set %s names nothing", path)
	}
	return set, nil
}

// IsZero reports whether nothing is pinned.
func (s Set) IsZero() bool {
	return s.CloudHypervisor == "" && s.Virtiofsd == "" && s.GuestKernel == "" && s.GuestImage == ""
}

// Digest identifies the set in one string, which is what a snapshot
// manifest and a template key carry.
func (s Set) Digest() string {
	sum := sha256.Sum256([]byte(strings.Join([]string{s.CloudHypervisor, s.Virtiofsd, s.GuestKernel, s.GuestImage}, "\n")))
	return hex.EncodeToString(sum[:16])
}

// String is the set in one line, for logs.
func (s Set) String() string {
	return fmt.Sprintf("cloud-hypervisor=%s virtiofsd=%s kernel=%s image=%s", s.CloudHypervisor, s.Virtiofsd, s.GuestKernel, s.GuestImage)
}
