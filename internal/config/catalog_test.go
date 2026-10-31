package config

import (
	"os"
	"path/filepath"
	"testing"
)

const sampleCatalog = `
flavors:
  - name: small
    default: true
    vcpus: 1
    memory: "2Gi"
    warm: 4
    max: 50
  - name: large
    vcpus: 4
    memory: "16Gi"
    warm: 1
    max: 10
storageClasses:
  - name: standard
    default: true
  - name: cold
    bucket: archive
    prefix: deltas
capabilities: [docker, hibernate]
`

func writeCatalog(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// The platform requires requests and limits on every flavor; a VM size
// reports its vCPUs and memory as both.
func TestReportedDerivesResourcesFromTheVMSize(t *testing.T) {
	c, err := LoadCatalog(writeCatalog(t, sampleCatalog))
	if err != nil {
		t.Fatal(err)
	}
	reported := c.Reported()
	if len(reported.Flavors) != 2 {
		t.Fatalf("expected two flavors, got %d", len(reported.Flavors))
	}
	small := reported.Flavors[0]
	if small.Resources.RequestsCPU != "1" || small.Resources.LimitsCPU != "1" || small.Resources.RequestsMemory != "2Gi" || small.Resources.LimitsMemory != "2Gi" {
		t.Fatalf("unexpected resources %+v", small.Resources)
	}
	if !small.Default {
		t.Fatal("expected small to be the default")
	}
	if err := reported.Validate(); err != nil {
		t.Fatalf("reported catalog should validate: %v", err)
	}
	if len(reported.StorageClasses) != 2 || reported.StorageClasses[1].Name != "cold" {
		t.Fatalf("unexpected storage classes %+v", reported.StorageClasses)
	}
}

// warm and max stay the runner's own; the platform never sees them, and a
// pool larger than its ceiling is a typo.
func TestValidateRefusesWarmAboveMax(t *testing.T) {
	_, err := LoadCatalog(writeCatalog(t, `
flavors:
  - name: small
    vcpus: 1
    memory: "2Gi"
    warm: 5
    max: 2
`))
	if err == nil {
		t.Fatal("expected warm above max to be refused")
	}
}

func TestValidateRefusesAnUnsizedFlavor(t *testing.T) {
	_, err := LoadCatalog(writeCatalog(t, "flavors:\n  - name: small\n    memory: \"2Gi\"\n"))
	if err == nil {
		t.Fatal("expected a flavor without vcpus to be refused")
	}
}

func TestFlavorForResolvesTheDefaultForAnEmptyName(t *testing.T) {
	c, err := LoadCatalog(writeCatalog(t, sampleCatalog))
	if err != nil {
		t.Fatal(err)
	}
	f, ok := c.FlavorFor("")
	if !ok || f.Name != "small" {
		t.Fatalf("expected the default flavor, got %+v %v", f, ok)
	}
	if _, ok := c.FlavorFor("huge"); ok {
		t.Fatal("expected an unknown flavor not to resolve")
	}
	mib, err := f.MemoryMiB()
	if err != nil || mib != 2048 {
		t.Fatalf("expected 2048 MiB, got %d %v", mib, err)
	}
}

func TestStorageClassForMapsToATier(t *testing.T) {
	c, err := LoadCatalog(writeCatalog(t, sampleCatalog))
	if err != nil {
		t.Fatal(err)
	}
	cold, ok := c.StorageClassFor("cold")
	if !ok || cold.Bucket != "archive" || cold.Prefix != "deltas" {
		t.Fatalf("unexpected class %+v %v", cold, ok)
	}
	def, ok := c.StorageClassFor("")
	if !ok || def.Name != "standard" {
		t.Fatalf("expected the default class, got %+v", def)
	}
	if _, ok := c.StorageClassFor("missing"); ok {
		t.Fatal("expected an unknown class not to resolve")
	}
}
