package share

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"
)

// A delta archived in one slot and restored in another must carry what an
// overlay upper layer and a volume are made of: files with their modes,
// directories, symlinks and hard links.
func TestPackAndUnpackTreeRoundTrip(t *testing.T) {
	src := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "dir", "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "dir", "sub", "file"), []byte("hello"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "exec"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("dir/sub/file", filepath.Join(src, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(src, "exec"), filepath.Join(src, "hard")); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := PackTree(&buf, src); err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	if err := UnpackTree(&buf, dst); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dst, "dir", "sub", "file"))
	if err != nil || string(data) != "hello" {
		t.Fatalf("file not restored: %v %q", err, data)
	}
	st, err := os.Stat(filepath.Join(dst, "exec"))
	if err != nil || st.Mode().Perm() != 0o755 {
		t.Fatalf("mode not restored: %v %v", err, st)
	}
	target, err := os.Readlink(filepath.Join(dst, "link"))
	if err != nil || target != "dir/sub/file" {
		t.Fatalf("symlink not restored: %v %q", err, target)
	}
	a, _ := os.Stat(filepath.Join(dst, "exec"))
	b, _ := os.Stat(filepath.Join(dst, "hard"))
	if !os.SameFile(a, b) {
		t.Fatal("hard link not restored as a link")
	}
	if TreeBytes(dst) != int64(len("hello")+len("#!/bin/sh\n")) { // the hard link counts once
		t.Fatalf("unexpected tree size %d", TreeBytes(dst))
	}
}

// A writable layer holds the sockets its processes bound; they are left
// out, as tar would refuse them, and everything else is archived.
func TestPackTreeSkipsSockets(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "file"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(src, "agent.sock"))
	if err != nil {
		t.Skip("unix sockets unavailable:", err)
	}
	defer listener.Close()
	var buf bytes.Buffer
	if err := PackTree(&buf, src); err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	if err := UnpackTree(&buf, dst); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "file")); err != nil {
		t.Fatal("file not restored")
	}
	if _, err := os.Lstat(filepath.Join(dst, "agent.sock")); err == nil {
		t.Fatal("socket should not be in the archive")
	}
}

// An archive cannot reach outside its root: a leading .. is dropped, and
// an entry below a symlink the archive planted to the outside is refused.
func TestExtractKeepsEntriesInsideTheRoot(t *testing.T) {
	var buf bytes.Buffer
	newTarWithEntry(t, &buf, "../escape", "x")
	root := t.TempDir()
	if err := ExtractTar(&buf, root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "escape")); err != nil {
		t.Fatal("expected the entry sanitized into the root")
	}

	outside := t.TempDir()
	root = t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "out")); err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	newTarWithEntry(t, &buf, "out/file", "x")
	if err := ExtractTar(&buf, root); err == nil {
		t.Fatal("expected an entry below a symlink to the outside to be refused")
	}
	if _, err := os.Stat(filepath.Join(outside, "file")); err == nil {
		t.Fatal("the archive wrote outside the root")
	}
}
