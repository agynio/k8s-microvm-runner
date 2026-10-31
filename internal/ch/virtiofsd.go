package ch

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// Virtiofsd is one virtiofsd process serving a directory tree to a guest.
type Virtiofsd struct {
	Bin       string
	Socket    string
	SharedDir string
	LogPath   string
	// Sandbox is virtiofsd's --sandbox: "none", "chroot" or "namespace".
	Sandbox string
	// InodeFileHandles is virtiofsd's --inode-file-handles: "never",
	// "prefer" or "mandatory". Referring to inodes by file handle needs
	// DAC_READ_SEARCH, which the slot Pod grants; "prefer" falls back to
	// open descriptors where it cannot.
	InodeFileHandles string

	cmd  *exec.Cmd
	done chan struct{}
}

// Start launches virtiofsd in find-paths migration mode, so a Cloud
// Hypervisor restore can hand it the guest's open files by path, and waits
// for its socket.
func (v *Virtiofsd) Start(ctx context.Context) error {
	_ = os.Remove(v.Socket)
	_ = os.Remove(v.Socket + ".pid")
	args := []string{
		"--socket-path=" + v.Socket,
		"--shared-dir=" + v.SharedDir,
		"--cache=auto",
		"--thread-pool-size=1",
		"--announce-submounts",
		"--migration-mode", "find-paths",
		"--migration-on-error", "guest-error",
	}
	if v.Sandbox != "" {
		args = append(args, "--sandbox", v.Sandbox)
	}
	if v.InodeFileHandles != "" {
		// virtiofsd insists on the equals sign for this one.
		args = append(args, "--inode-file-handles="+v.InodeFileHandles)
	}
	logf, err := os.OpenFile(v.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(v.Bin, args...)
	cmd.Stdout = logf
	cmd.Stderr = logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		logf.Close()
		return fmt.Errorf("start virtiofsd: %w", err)
	}
	logf.Close()
	v.cmd = cmd
	v.done = make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(v.done)
	}()
	// virtiofsd binds its socket before it sandboxes itself and drops
	// capabilities, so the socket alone is not readiness: give an early exit
	// a moment to show.
	deadline := time.Now().Add(10 * time.Second)
	var seen time.Time
	for {
		if _, err := os.Stat(v.Socket); err == nil {
			if seen.IsZero() {
				seen = time.Now()
			} else if time.Since(seen) > 150*time.Millisecond && !v.Exited() {
				return nil
			}
		}
		if v.Exited() {
			data, _ := os.ReadFile(v.LogPath)
			return fmt.Errorf("virtiofsd exited: %s", string(data))
		}
		if time.Now().After(deadline) {
			v.Kill()
			return fmt.Errorf("virtiofsd socket %s did not appear", v.Socket)
		}
		select {
		case <-ctx.Done():
			v.Kill()
			return ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// Exited reports whether the process is gone.
func (v *Virtiofsd) Exited() bool {
	if v.done == nil {
		return true
	}
	select {
	case <-v.done:
		return true
	default:
		return false
	}
}

// Kill stops the process and removes its socket.
func (v *Virtiofsd) Kill() {
	if v.cmd != nil && v.cmd.Process != nil && !v.Exited() {
		_ = v.cmd.Process.Kill()
		<-v.done
	}
	_ = os.Remove(v.Socket)
	_ = os.Remove(v.Socket + ".pid")
}
