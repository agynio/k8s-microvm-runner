package guest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/agynio/k8s-microvm-runner/internal/kata"
)

// Captured output is kept the way a container runtime keeps it: one line per
// write, with its time and stream, so a reader can tail by line and filter
// by time without the stream carrying timestamps itself.
//
//	2026-10-31T00:00:00.000000000Z stdout F the line
//	2026-10-31T00:00:00.000000000Z stdout P a partial line without its newline yet

type logWriter struct {
	mu      sync.Mutex
	f       *os.File
	partial map[string][]byte
}

func newLogWriter(path string) (*logWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &logWriter{f: f, partial: map[string][]byte{}}, nil
}

func (l *logWriter) write(stream string, data []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	buf := append(l.partial[stream], data...)
	for {
		i := bytes.IndexByte(buf, '\n')
		if i < 0 {
			break
		}
		fmt.Fprintf(l.f, "%s %s F %s\n", time.Now().UTC().Format(time.RFC3339Nano), stream, buf[:i])
		buf = buf[i+1:]
	}
	l.partial[stream] = buf
}

func (l *logWriter) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for stream, buf := range l.partial {
		if len(buf) > 0 {
			fmt.Fprintf(l.f, "%s %s P %s\n", time.Now().UTC().Format(time.RFC3339Nano), stream, buf)
		}
	}
	l.partial = map[string][]byte{}
	_ = l.f.Close()
}

func (m *Manager) logPath(container string) string {
	return filepath.Join(m.cfg.Layout.Logs(), container+".log")
}

// captureLogs follows a container's stdout and stderr into its log file for
// as long as the guest answers.
func (m *Manager) captureLogs(w *Workload, c *Container) {
	lw, err := newLogWriter(m.logPath(c.Name))
	if err != nil {
		m.log.Warn("open container log", zapErr(err))
		return
	}
	var wg sync.WaitGroup
	for _, stream := range []string{"stdout", "stderr"} {
		wg.Add(1)
		go func(stream string) {
			defer wg.Done()
			cl, err := kata.Dial(m.vsockPath())
			if err != nil {
				return
			}
			defer cl.Close()
			for {
				var data []byte
				var err error
				if stream == "stdout" {
					data, err = cl.ReadStdout(context.Background(), c.Name, c.Name)
				} else {
					data, err = cl.ReadStderr(context.Background(), c.Name, c.Name)
				}
				if err != nil {
					return
				}
				if len(data) > 0 {
					lw.write(stream, data)
				}
			}
		}(stream)
	}
	go func() {
		wg.Wait()
		lw.close()
	}()
}

// LogOptions select what a log reader returns.
type LogOptions struct {
	TailLines  int64
	Since      time.Time
	Follow     bool
	Timestamps bool
}

// OpenLogs reads a container's captured output. With Follow the reader
// keeps returning new lines until the container terminates or the workload
// leaves; without it, it ends at the end of what was captured.
func (m *Manager) OpenLogs(ctx context.Context, w *Workload, container string, opts LogOptions) (io.ReadCloser, error) {
	var c *Container
	w.mu.Lock()
	for _, candidate := range w.Containers {
		if candidate.Name == container {
			c = candidate
		}
	}
	w.mu.Unlock()
	if c == nil {
		return nil, ErrNoContainer
	}
	path := m.logPath(container)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Listed but not started yet: nothing captured.
			return io.NopCloser(strings.NewReader("")), nil
		}
		return nil, err
	}
	r := &logReader{m: m, w: w, c: c, f: f, opts: opts}
	if err := r.prime(); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

// ErrNoContainer says the workload has no container by that name.
var ErrNoContainer = errors.New("container not found")

type logReader struct {
	m    *Manager
	w    *Workload
	c    *Container
	f    *os.File
	opts LogOptions

	buf    bytes.Buffer
	offset int64
	done   bool
}

// prime reads what was captured so far, applies tail and since, and leaves
// the reader positioned at the end of the file.
func (r *logReader) prime() error {
	data, err := io.ReadAll(r.f)
	if err != nil {
		return err
	}
	r.offset = int64(len(data))
	lines := splitLines(data)
	if !r.opts.Since.IsZero() {
		kept := lines[:0]
		for _, line := range lines {
			if ts, _, _, ok := parseLogLine(line); ok && !ts.Before(r.opts.Since) {
				kept = append(kept, line)
			}
		}
		lines = kept
	}
	if r.opts.TailLines > 0 && int64(len(lines)) > r.opts.TailLines {
		lines = lines[int64(len(lines))-r.opts.TailLines:]
	}
	for _, line := range lines {
		r.emit(line)
	}
	return nil
}

func (r *logReader) emit(line []byte) {
	ts, _, text, ok := parseLogLine(line)
	if !ok {
		return
	}
	if r.opts.Timestamps {
		r.buf.WriteString(ts.Format(time.RFC3339Nano))
		r.buf.WriteByte(' ')
	}
	r.buf.Write(text)
	r.buf.WriteByte('\n')
}

func (r *logReader) Read(p []byte) (int, error) {
	for r.buf.Len() == 0 {
		if r.done {
			return 0, io.EOF
		}
		if !r.opts.Follow {
			r.done = true
			continue
		}
		if err := r.more(); err != nil {
			return 0, err
		}
	}
	return r.buf.Read(p)
}

// more waits for new lines, or for the container to end.
func (r *logReader) more() error {
	for {
		st, err := r.f.Stat()
		if err != nil {
			return err
		}
		if st.Size() > r.offset {
			chunk := make([]byte, st.Size()-r.offset)
			n, err := r.f.ReadAt(chunk, r.offset)
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			chunk = chunk[:n]
			// Only whole lines; a line still being written waits.
			if i := bytes.LastIndexByte(chunk, '\n'); i >= 0 {
				for _, line := range splitLines(chunk[:i+1]) {
					if ts, _, _, ok := parseLogLine(line); ok && !r.opts.Since.IsZero() && ts.Before(r.opts.Since) {
						continue
					}
					r.emit(line)
				}
				r.offset += int64(i + 1)
				if r.buf.Len() > 0 {
					return nil
				}
			}
		}
		r.w.mu.Lock()
		ended := r.c.Status == ContainerTerminated || r.w.Status == StatusStopped || r.w.Status == StatusFailed || r.w.Status == StatusHibernated || r.w.agent == nil
		r.w.mu.Unlock()
		if ended {
			r.done = true
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (r *logReader) Close() error { return r.f.Close() }

func splitLines(data []byte) [][]byte {
	var out [][]byte
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 1<<16), 1<<24)
	for sc.Scan() {
		out = append(out, append([]byte(nil), sc.Bytes()...))
	}
	return out
}

func parseLogLine(line []byte) (time.Time, string, []byte, bool) {
	parts := bytes.SplitN(line, []byte(" "), 4)
	if len(parts) < 3 {
		return time.Time{}, "", nil, false
	}
	ts, err := time.Parse(time.RFC3339Nano, string(parts[0]))
	if err != nil {
		return time.Time{}, "", nil, false
	}
	var text []byte
	if len(parts) == 4 {
		text = parts[3]
	}
	return ts, string(parts[1]), text, true
}
