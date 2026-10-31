// Package ch drives one Cloud Hypervisor process over its REST API socket:
// create and boot a VM, pause it, write a snapshot, and relaunch a bare VMM
// that restores one.
package ch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// VmConfig is the body of vm.create: the subset of Cloud Hypervisor's
// VmConfig a slot uses.
type VmConfig struct {
	Cpus    CpusConfig     `json:"cpus"`
	Memory  MemoryConfig   `json:"memory"`
	Payload PayloadConfig  `json:"payload"`
	Pmem    []PmemConfig   `json:"pmem,omitempty"`
	Fs      []FsConfig     `json:"fs,omitempty"`
	Net     []NetConfig    `json:"net,omitempty"`
	Balloon *BalloonConfig `json:"balloon,omitempty"`
	Rng     *RngConfig     `json:"rng,omitempty"`
	Serial  *ConsoleConfig `json:"serial,omitempty"`
	Console *ConsoleConfig `json:"console,omitempty"`
	Vsock   *VsockConfig   `json:"vsock,omitempty"`
}

type CpusConfig struct {
	BootVcpus int `json:"boot_vcpus"`
	MaxVcpus  int `json:"max_vcpus"`
}

// MemoryConfig: Shared backs guest RAM with a memfd, which is what makes
// vm.snapshot write a sparse memory file (holes where the guest never
// touched or has released a page). Thp asks for transparent huge pages on
// it, so a first touch of guest memory is a fault per 2 MB rather than per
// 4 KB.
type MemoryConfig struct {
	Size   int64 `json:"size"`
	Shared bool  `json:"shared"`
	Thp    bool  `json:"thp"`
}

type PayloadConfig struct {
	Kernel  string `json:"kernel"`
	Cmdline string `json:"cmdline"`
}

// PmemConfig is a virtio-pmem device; DiscardWrites maps the file
// privately so the guest image stays pristine.
type PmemConfig struct {
	ID            string `json:"id,omitempty"`
	File          string `json:"file"`
	DiscardWrites bool   `json:"discard_writes,omitempty"`
}

// FsConfig is a virtio-fs device served by a virtiofsd on Socket.
type FsConfig struct {
	Tag       string `json:"tag"`
	Socket    string `json:"socket"`
	NumQueues int    `json:"num_queues,omitempty"`
	QueueSize int    `json:"queue_size,omitempty"`
}

// NetConfig is a virtio-net device on an existing tap, opened by name.
type NetConfig struct {
	ID        string `json:"id,omitempty"`
	Tap       string `json:"tap,omitempty"`
	Mac       string `json:"mac,omitempty"`
	NumQueues int    `json:"num_queues,omitempty"`
	QueueSize int    `json:"queue_size,omitempty"`
}

// BalloonConfig with FreePageReporting lets the guest hand freed pages back,
// so they are holes in the next snapshot rather than stale data.
type BalloonConfig struct {
	Size              int64 `json:"size"`
	FreePageReporting bool  `json:"free_page_reporting,omitempty"`
}

type RngConfig struct {
	Src string `json:"src"`
}

// ConsoleConfig: Mode is "Off", "File" (with File) or "Tty".
type ConsoleConfig struct {
	Mode string `json:"mode"`
	File string `json:"file,omitempty"`
}

// VsockConfig is the hybrid vsock: the guest CID and the host unix socket the
// guest's ports are reached through ("CONNECT <port>").
type VsockConfig struct {
	Cid    int64  `json:"cid"`
	Socket string `json:"socket"`
}

// Info is what vmm.ping reports.
type Info struct {
	Version      string   `json:"version"`
	BuildVersion string   `json:"build_version"`
	Features     []string `json:"features"`
}

// Memory restore modes accepted by vm.restore.
const (
	// RestoreOnDemand maps the memory file through userfaultfd: pages come
	// in as the guest touches them, and the next snapshot holds only those.
	RestoreOnDemand = "OnDemand"
	// RestoreCopy reads every populated extent of the memory file up front.
	RestoreCopy = "Copy"
)

// VM is one Cloud Hypervisor process.
type VM struct {
	Bin     string
	Dir     string
	APISock string
	LogPath string

	cmd  *exec.Cmd
	done chan struct{}
	http *http.Client
	info Info
}

// New prepares a VMM whose API socket and log live in dir.
func New(bin, dir string) *VM {
	sock := filepath.Join(dir, "api.sock")
	return &VM{
		Bin: bin, Dir: dir, APISock: sock, LogPath: filepath.Join(dir, "ch.log"),
		http: &http.Client{
			Timeout: 0,
			Transport: &http.Transport{
				// A kept-alive connection to the API server was observed to
				// block on its next request; a fresh connection per request
				// is cheap.
				DisableKeepAlives: true,
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", sock)
				},
			},
		},
	}
}

// Spawn starts a bare VMM listening on the API socket and waits until it
// answers.
func (v *VM) Spawn(ctx context.Context) error {
	_ = os.Remove(v.APISock)
	logf, err := os.OpenFile(v.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(v.Bin, "--api-socket", v.APISock)
	cmd.Stdout = logf
	cmd.Stderr = logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		logf.Close()
		return fmt.Errorf("start cloud-hypervisor: %w", err)
	}
	logf.Close()
	v.cmd = cmd
	v.done = make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(v.done)
	}()
	deadline := time.Now().Add(15 * time.Second)
	for {
		info, err := v.Ping(ctx)
		if err == nil {
			v.info = info
			return nil
		}
		if v.Exited() {
			return fmt.Errorf("cloud-hypervisor exited before answering: %s", v.tailLog())
		}
		if time.Now().After(deadline) {
			v.Kill()
			return fmt.Errorf("cloud-hypervisor api socket not ready: %w", err)
		}
		select {
		case <-ctx.Done():
			v.Kill()
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Exited reports whether the VMM process is gone.
func (v *VM) Exited() bool {
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

// Done is closed when the VMM process exits.
func (v *VM) Done() <-chan struct{} { return v.done }

// Kill stops the VMM process and reaps it.
func (v *VM) Kill() {
	if v.cmd != nil && v.cmd.Process != nil && !v.Exited() {
		_ = v.cmd.Process.Kill()
		<-v.done
	}
	_ = os.Remove(v.APISock)
}

// TailLog is the end of the VMM's log, for a failure message.
func (v *VM) TailLog() string { return v.tailLog() }

func (v *VM) tailLog() string {
	data, err := os.ReadFile(v.LogPath)
	if err != nil {
		return ""
	}
	if len(data) > 1500 {
		data = data[len(data)-1500:]
	}
	return string(bytes.TrimSpace(data))
}

func (v *VM) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost"+path, nil)
	if err != nil {
		return err
	}
	resp, err := v.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s: %d: %s", path, resp.StatusCode, bytes.TrimSpace(body))
	}
	if out != nil && len(body) > 0 {
		return json.Unmarshal(body, out)
	}
	return nil
}

func (v *VM) put(ctx context.Context, path string, body any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://localhost"+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := v.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("PUT %s: %d: %s", path, resp.StatusCode, bytes.TrimSpace(msg))
	}
	return nil
}

// Ping asks the VMM whether it is there and which version it is.
func (v *VM) Ping(ctx context.Context) (Info, error) {
	var info Info
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err := v.get(cctx, "/api/v1/vmm.ping", &info)
	return info, err
}

// Info is what the VMM reported at Spawn.
func (v *VM) Info() Info { return v.info }

// State is the VM state from vm.info: "Created", "Running", "Paused",
// "Shutdown".
func (v *VM) State(ctx context.Context) (string, error) {
	var out struct {
		State string `json:"state"`
	}
	if err := v.get(ctx, "/api/v1/vm.info", &out); err != nil {
		return "", err
	}
	return out.State, nil
}

// Create configures the VM.
func (v *VM) Create(ctx context.Context, cfg VmConfig) error {
	return v.put(ctx, "/api/v1/vm.create", cfg)
}

// Boot starts a created VM.
func (v *VM) Boot(ctx context.Context) error { return v.put(ctx, "/api/v1/vm.boot", nil) }

// Pause freezes every vCPU and device. Already paused is not an error.
func (v *VM) Pause(ctx context.Context) error {
	if st, err := v.State(ctx); err == nil && st == "Paused" {
		return nil
	}
	return v.put(ctx, "/api/v1/vm.pause", nil)
}

// Resume continues a paused VM.
func (v *VM) Resume(ctx context.Context) error { return v.put(ctx, "/api/v1/vm.resume", nil) }

// Snapshot writes the paused VM (config.json, state.json, memory-ranges)
// into dir.
func (v *VM) Snapshot(ctx context.Context, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return v.put(ctx, "/api/v1/vm.snapshot", map[string]string{"destination_url": "file://" + dir})
}

// Restore loads a snapshot into a bare VMM; the VM comes back paused.
func (v *VM) Restore(ctx context.Context, dir, memoryMode string) error {
	body := map[string]any{"source_url": "file://" + dir}
	if memoryMode != "" {
		body["memory_restore_mode"] = memoryMode
	}
	return v.put(ctx, "/api/v1/vm.restore", body)
}

// Shutdown asks the VMM to shut the VM down without killing the process.
func (v *VM) Shutdown(ctx context.Context) error { return v.put(ctx, "/api/v1/vm.shutdown", nil) }

// ErrExited says the VMM process is gone.
var ErrExited = errors.New("cloud-hypervisor exited")
