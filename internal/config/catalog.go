package config

import (
	"fmt"
	"strings"

	"github.com/agynio/runner-core/catalog"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Catalog is what this runner offers: VM sizes, the store tiers volumes go
// to, and the capabilities it implements. It is declared here, next to the
// implementation, because a flavor is only real if a slot of that size can
// be created and a storage class only if a bucket backs it.
type Catalog struct {
	Flavors        []FlavorEntry       `yaml:"flavors"`
	StorageClasses []StorageClassEntry `yaml:"storageClasses"`
	Capabilities   []string            `yaml:"capabilities"`
}

// FlavorEntry is a VM size and the pool kept for it.
type FlavorEntry struct {
	Name       string `yaml:"name"`
	Default    bool   `yaml:"default"`
	Deprecated bool   `yaml:"deprecated"`
	VCPUs      int    `yaml:"vcpus"`
	Memory     string `yaml:"memory"`
	// Warm is how many idle slots to keep ready; Max how many slots this
	// flavor may occupy at once (0 is unlimited).
	Warm int `yaml:"warm"`
	Max  int `yaml:"max"`
}

// MemoryMiB is the flavor's memory in MiB.
func (f FlavorEntry) MemoryMiB() (int64, error) {
	q, err := resource.ParseQuantity(strings.TrimSpace(f.Memory))
	if err != nil {
		return 0, err
	}
	mib := q.Value() >> 20
	if mib <= 0 {
		return 0, fmt.Errorf("memory %q is below 1Mi", f.Memory)
	}
	return mib, nil
}

// StorageClassEntry names a store tier: the bucket and prefix deltas of
// volumes in that class are written to. Empty means the store's defaults.
type StorageClassEntry struct {
	Name       string `yaml:"name"`
	Default    bool   `yaml:"default"`
	Deprecated bool   `yaml:"deprecated"`
	Bucket     string `yaml:"bucket"`
	Prefix     string `yaml:"prefix"`
}

// LoadCatalog reads the catalog from path. A missing path is not an error.
func LoadCatalog(path string) (Catalog, error) {
	var declared Catalog
	if err := catalog.LoadFile(path, &declared); err != nil {
		return Catalog{}, err
	}
	if err := declared.Validate(); err != nil {
		return Catalog{}, fmt.Errorf("catalog %s: %w", strings.TrimSpace(path), err)
	}
	return declared, nil
}

// Validate catches what the platform would reject and what the pool could
// not honour.
func (c Catalog) Validate() error {
	for _, f := range c.Flavors {
		name := strings.TrimSpace(f.Name)
		if f.VCPUs <= 0 {
			return fmt.Errorf("flavor %q: vcpus must be at least 1", name)
		}
		if _, err := f.MemoryMiB(); err != nil {
			return fmt.Errorf("flavor %q: %w", name, err)
		}
		if f.Warm < 0 || f.Max < 0 {
			return fmt.Errorf("flavor %q: warm and max must not be negative", name)
		}
		if f.Max > 0 && f.Warm > f.Max {
			return fmt.Errorf("flavor %q: warm (%d) exceeds max (%d)", name, f.Warm, f.Max)
		}
	}
	return c.Reported().Validate()
}

// Reported is the platform-facing view: every flavor's size as requests and
// limits, derived from its vCPUs and memory, since the platform requires
// both on every entry.
func (c Catalog) Reported() catalog.Catalog {
	flavors := make([]catalog.Flavor, 0, len(c.Flavors))
	for _, f := range c.Flavors {
		cpu := fmt.Sprint(f.VCPUs)
		memory := strings.TrimSpace(f.Memory)
		flavors = append(flavors, catalog.Flavor{
			Name: f.Name, Default: f.Default, Deprecated: f.Deprecated,
			Resources: catalog.ComputeResources{RequestsCPU: cpu, RequestsMemory: memory, LimitsCPU: cpu, LimitsMemory: memory},
		})
	}
	classes := make([]catalog.StorageClass, 0, len(c.StorageClasses))
	for _, s := range c.StorageClasses {
		classes = append(classes, catalog.StorageClass{Name: s.Name, Default: s.Default, Deprecated: s.Deprecated})
	}
	return catalog.Catalog{Flavors: flavors, StorageClasses: classes, Capabilities: c.Capabilities}
}

// FlavorFor resolves a name; an empty name resolves to the default flavor.
func (c Catalog) FlavorFor(name string) (FlavorEntry, bool) {
	name = strings.TrimSpace(name)
	for _, f := range c.Flavors {
		if (name == "" && f.Default) || f.Name == name {
			return f, true
		}
	}
	return FlavorEntry{}, false
}

// StorageClassFor resolves a name; an empty name resolves to the default
// class, or to an unnamed default tier when none is declared.
func (c Catalog) StorageClassFor(name string) (StorageClassEntry, bool) {
	name = strings.TrimSpace(name)
	for _, s := range c.StorageClasses {
		if (name == "" && s.Default) || s.Name == name {
			return s, true
		}
	}
	if name == "" {
		return StorageClassEntry{}, true
	}
	return StorageClassEntry{}, false
}
