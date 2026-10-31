package share

import (
	"archive/tar"
	"io"
	"testing"
)

func newTarWithEntry(t *testing.T, w io.Writer, name, body string) *tar.Writer {
	t.Helper()
	tw := tar.NewWriter(w)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return tw
}
