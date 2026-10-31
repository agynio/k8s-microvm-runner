package guest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/agynio/k8s-microvm-runner/internal/kata"
)

// ExecStreams is what an exec is wired to.
type ExecStreams struct {
	Stdin  io.Reader
	Stdout io.Writer
	// Stderr is nil in TTY mode.
	Stderr io.Writer
	// Resize delivers terminal sizes as rows, cols.
	Resize <-chan [2]uint32
}

// ExitError is a command that ran and exited non-zero.
type ExitError struct{ Code int }

func (e *ExitError) Error() string { return fmt.Sprintf("exit status %d", e.Code) }

// ExitStatus is what runner-core reads the exit code off.
func (e *ExitError) ExitStatus() int { return e.Code }

// Exec runs a command inside one of the workload's containers through the
// guest agent, with a TTY when asked, and returns when it exits. A non-zero
// exit is an *ExitError.
func (m *Manager) Exec(ctx context.Context, w *Workload, container string, command []string, tty bool, streams ExecStreams) error {
	w.mu.Lock()
	var c *Container
	for _, candidate := range w.Containers {
		if candidate.Name == container || (container == "" && candidate.Role == RoleMain) {
			c = candidate
		}
	}
	running := w.Status == StatusRunning && w.agent != nil
	w.mu.Unlock()
	if c == nil {
		return ErrNoContainer
	}
	if !running {
		return fmt.Errorf("workload is %s", w.Status)
	}
	if c.Status != ContainerRunning {
		return fmt.Errorf("container %s is %s", c.Name, c.Status)
	}
	if len(command) == 0 {
		return errors.New("command required")
	}

	t0 := time.Now()
	ctl, err := kata.Dial(m.vsockPath())
	if err != nil {
		return err
	}
	defer ctl.Close()
	execID := "exec-" + uuid.NewString()
	defer func() {
		m.log.Debug("exec finished", zap.String("exec", execID), zap.Duration("took", time.Since(t0).Round(time.Millisecond)))
	}()
	env := []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOSTNAME=" + w.Hostname}
	if tty {
		env = append(env, "TERM=xterm")
	}
	cwd := c.cwd
	if cwd == "" {
		cwd = "/"
	}
	if err := ctl.Exec(ctx, c.Name, execID, kata.ExecOptions{Args: command, Env: env, Cwd: cwd, UID: c.uid, GID: c.gid, Terminal: tty}); err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	m.log.Debug("exec started", zap.String("exec", execID), zap.Duration("after", time.Since(t0).Round(time.Millisecond)))

	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The agent answers a read with an empty chunk when nothing is buffered
	// yet, and again once the process is gone, so each stream is drained
	// until the wait returns and one read after it.
	exited := make(chan struct{})
	var wg sync.WaitGroup
	pump := func(read func(*kata.Client, context.Context, string, string) ([]byte, error), out io.Writer) {
		defer wg.Done()
		cl, err := kata.Dial(m.vsockPath())
		if err != nil {
			return
		}
		defer cl.Close()
		done := false
		for {
			data, err := read(cl, execCtx, c.Name, execID)
			if err != nil {
				return
			}
			if len(data) > 0 {
				if out != nil {
					if _, err := out.Write(data); err != nil {
						return
					}
				}
				continue
			}
			if done {
				return
			}
			select {
			case <-exited:
				done = true // one more read drains what was written before the exit
			case <-execCtx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	wg.Add(1)
	go pump((*kata.Client).ReadStdout, streams.Stdout)
	if !tty {
		wg.Add(1)
		go pump((*kata.Client).ReadStderr, streams.Stderr)
	}
	if streams.Stdin != nil {
		go func() {
			buf := make([]byte, 32*1024)
			for {
				n, err := streams.Stdin.Read(buf)
				if n > 0 {
					if werr := ctl.WriteStdin(execCtx, c.Name, execID, buf[:n]); werr != nil {
						return
					}
				}
				if err != nil {
					_ = ctl.CloseStdin(execCtx, c.Name, execID)
					return
				}
			}
		}()
	}
	if streams.Resize != nil {
		go func() {
			for size := range streams.Resize {
				if err := ctl.Resize(execCtx, c.Name, execID, size[0], size[1]); err != nil {
					m.log.Debug("exec resize", zap.Error(err))
				}
			}
		}()
	}

	waiter, err := kata.Dial(m.vsockPath())
	if err != nil {
		cancel()
		return err
	}
	defer waiter.Close()
	code, werr := waiter.Wait(execCtx, c.Name, execID)
	close(exited)
	m.log.Debug("exec exited", zap.String("exec", execID), zap.Int32("code", code), zap.Duration("after", time.Since(t0).Round(time.Millisecond)))
	if werr != nil {
		if ctx.Err() != nil {
			// Cancelled: the process is killed so the guest does not keep it.
			_ = ctl.Signal(context.Background(), c.Name, execID, 9)
			return ctx.Err()
		}
		return fmt.Errorf("wait: %w", werr)
	}
	wg.Wait()
	if code != 0 {
		return &ExitError{Code: int(code)}
	}
	return nil
}

func zapErr(err error) zap.Field { return zap.Error(err) }
