//go:build !linux

package share

import (
	"errors"
	"hash/fnv"
)

// ErrQuotaUnsupported says the filesystem under dir cannot enforce a
// project quota, so the declared size is advisory.
var ErrQuotaUnsupported = errors.New("project quotas are not available on this filesystem")

// ProjectID derives a stable project id from a volume's key.
func ProjectID(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return 100000 + h.Sum32()%1000000
}

// ApplyQuota is Linux-only.
func ApplyQuota(dir string, projectID uint32, bytes int64) error { return ErrQuotaUnsupported }
