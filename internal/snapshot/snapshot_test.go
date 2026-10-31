package snapshot

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/agynio/k8s-microvm-runner/internal/ch"
	"github.com/agynio/k8s-microvm-runner/internal/store"
)

func writeSparse(t *testing.T, path string, size int64, extents map[int64][]byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	for off, data := range extents {
		if _, err := f.WriteAt(data, off); err != nil {
			t.Fatal(err)
		}
	}
}

// The memory file is sparse: the stream carries populated extents only and
// the restored file has holes where the original had them.
func TestPackAndUnpackMemoryKeepsTheHoles(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "memory-ranges")
	page := bytes.Repeat([]byte{0xAB}, 8192)
	writeSparse(t, src, 64<<20, map[int64][]byte{0: page, 32 << 20: page})

	var buf bytes.Buffer
	if _, err := PackMemory(&buf, src); err != nil {
		t.Fatal(err)
	}
	if buf.Len() > 64<<10 {
		t.Fatalf("expected the stream to carry the populated extents only, got %d bytes", buf.Len())
	}
	dst := filepath.Join(dir, "restored")
	if err := UnpackMemory(&buf, dst); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(dst)
	if err != nil || st.Size() != 64<<20 {
		t.Fatalf("restored file has size %d, want %d (%v)", st.Size(), 64<<20, err)
	}
	f, err := os.Open(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got := make([]byte, 8192)
	if _, err := f.ReadAt(got, 32<<20); err != nil || !bytes.Equal(got, page) {
		t.Fatalf("extent not restored: %v", err)
	}
	if _, err := f.ReadAt(got, 16<<20); err != nil || !bytes.Equal(got, make([]byte, 8192)) {
		t.Fatalf("hole not kept as zeros: %v", err)
	}
	if ch.AllocatedBytes(dst) >= 64<<20 {
		t.Fatalf("restored file is not sparse: %d bytes allocated", ch.AllocatedBytes(dst))
	}
}

// A snapshot taken after an on-demand restore holds only the pages touched
// since; merged over the one it woke from, the result is one snapshot and a
// wake never replays a chain.
func TestMergeDeltaFoldsTouchedPagesOverTheBase(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("SEEK_DATA granularity is a page only on Linux filesystems")
	}
	dir := t.TempDir()
	base := filepath.Join(dir, "snap")
	delta := filepath.Join(dir, "snap.new")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(delta, 0o755); err != nil {
		t.Fatal(err)
	}
	old := bytes.Repeat([]byte{1}, 4096)
	touched := bytes.Repeat([]byte{2}, 4096)
	writeSparse(t, filepath.Join(base, ch.MemoryFile), 1<<20, map[int64][]byte{0: old, 4096: old})
	writeSparse(t, filepath.Join(delta, ch.MemoryFile), 1<<20, map[int64][]byte{4096: touched})
	for _, f := range ch.SnapshotFiles {
		if err := os.WriteFile(filepath.Join(base, f), []byte("old"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(delta, f), []byte("new"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := ch.MergeDelta(base, delta); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(base, ch.MemoryFile))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got := make([]byte, 4096)
	if _, err := f.ReadAt(got, 0); err != nil || !bytes.Equal(got, old) {
		t.Fatal("untouched page changed")
	}
	if _, err := f.ReadAt(got, 4096); err != nil || !bytes.Equal(got, touched) {
		t.Fatal("touched page not merged")
	}
	for _, name := range ch.SnapshotFiles {
		data, _ := os.ReadFile(filepath.Join(base, name))
		if string(data) != "new" {
			t.Fatalf("%s not replaced by the delta's", name)
		}
	}
	if _, err := os.Stat(delta); !os.IsNotExist(err) {
		t.Fatal("delta directory should be gone")
	}
}

// Snapshots are keyed by workload_key and volume deltas by volume_key,
// under separate prefixes, with a storage class able to move a volume to
// another prefix.
func TestKeys(t *testing.T) {
	cfg := store.Config{}.WithDefaults()
	if got := Prefix(cfg, "wk-1"); got != "snapshots/wk-1" {
		t.Fatalf("unexpected snapshot prefix %s", got)
	}
	if got := VolumePrefix(cfg, "", "vk-1"); got != "volumes/vk-1" {
		t.Fatalf("unexpected volume prefix %s", got)
	}
	if got := VolumePrefix(cfg, "cold", "vk-1"); got != "cold/vk-1" {
		t.Fatalf("unexpected tiered volume prefix %s", got)
	}
	if got := store.Key(Prefix(cfg, "wk-1"), ManifestObject); got != "snapshots/wk-1/manifest.json" {
		t.Fatalf("unexpected manifest key %s", got)
	}
}

func TestManifestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	m := &Manifest{WorkloadID: "w", WorkloadKey: "k", Flavor: "small", VCPUs: 1, MemoryMiB: 2048, Generation: 2}
	if err := WriteManifest(path, m); err != nil {
		t.Fatal(err)
	}
	got, err := ReadManifest(path)
	if err != nil || got.WorkloadKey != "k" || got.Generation != 2 {
		t.Fatalf("manifest did not round-trip: %+v %v", got, err)
	}
	if _, err := ParseManifest([]byte(`{"workload_id":""}`)); err == nil {
		t.Fatal("expected a manifest naming no workload to be refused")
	}
}
