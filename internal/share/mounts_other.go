//go:build !linux

package share

import "errors"

var errLinuxOnly = errors.New("the share is assembled with Linux mounts")

// MountOverlay is Linux-only.
func MountOverlay(dst, lower, upper, work string, userxattr bool) error { return errLinuxOnly }

// BindMount is Linux-only.
func BindMount(src, dst string) error { return errLinuxOnly }

// UnmountUnder is Linux-only.
func UnmountUnder(root string) {}

// MountsUnder is Linux-only.
func MountsUnder(root string) []string { return nil }

// InUserNamespace is Linux-only.
func InUserNamespace() bool { return false }
