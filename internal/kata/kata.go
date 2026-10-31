// Package kata talks to the stock Kata guest agent over the VMM's hybrid
// vsock socket. The agent is the guest's PID 1; this is the only way in.
package kata

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/containerd/ttrpc"
	specs "github.com/opencontainers/runtime-spec/specs-go"

	"github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/agent/protocols"
	agentgrpc "github.com/kata-containers/kata-containers/src/runtime/virtcontainers/pkg/agent/protocols/grpc"
)

const agentPort = 1024

// Client is one ttrpc connection to the agent.
type Client struct {
	conn   *ttrpc.Client
	Agent  agentgrpc.AgentServiceService
	Health agentgrpc.HealthService
}

func dialHybrid(uds string) (net.Conn, error) {
	conn, err := net.DialTimeout("unix", uds, 3*time.Second)
	if err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", agentPort); err != nil {
		conn.Close()
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("vsock handshake: %w", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	if !strings.Contains(resp, "OK") {
		conn.Close()
		return nil, fmt.Errorf("vsock handshake refused: %q", strings.TrimSpace(resp))
	}
	return conn, nil
}

// Dial opens one ttrpc connection. Each long-lived stream (log readers,
// waits) uses its own connection so a blocking read does not hold the
// control channel.
func Dial(uds string) (*Client, error) {
	conn, err := dialHybrid(uds)
	if err != nil {
		return nil, err
	}
	c := ttrpc.NewClient(conn)
	return &Client{conn: c, Agent: agentgrpc.NewAgentServiceClient(c), Health: agentgrpc.NewHealthClient(c)}, nil
}

// Close ends the connection.
func (c *Client) Close() { _ = c.conn.Close() }

// WaitReady dials until the agent answers a health check.
func WaitReady(ctx context.Context, uds string, timeout time.Duration) (*Client, error) {
	deadline := time.Now().Add(timeout)
	var last error
	for {
		c, err := Dial(uds)
		if err == nil {
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, err = c.Health.Check(cctx, &agentgrpc.CheckRequest{})
			cancel()
			if err == nil {
				return c, nil
			}
			c.Close()
		}
		last = err
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("agent not ready after %s: %w", timeout, last)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// Check is one health check.
func (c *Client) Check(ctx context.Context) error {
	_, err := c.Health.Check(ctx, &agentgrpc.CheckRequest{})
	return err
}

// CreateSandbox creates the guest sandbox with the share mounted.
func (c *Client) CreateSandbox(ctx context.Context, id, hostname string, dns []string, storages []*agentgrpc.Storage) error {
	_, err := c.Agent.CreateSandbox(ctx, &agentgrpc.CreateSandboxRequest{
		Hostname:  hostname,
		SandboxId: id,
		Dns:       dns,
		Storages:  storages,
	})
	return err
}

// ConfigureNetwork gives the guest's NIC an address and a default route.
func (c *Client) ConfigureNetwork(ctx context.Context, device, mac, ip string, prefix int, gateway string, mtu int) error {
	_, err := c.Agent.UpdateInterface(ctx, &agentgrpc.UpdateInterfaceRequest{Interface: &protocols.Interface{
		Device: device,
		Name:   device,
		HwAddr: mac,
		Mtu:    uint64(mtu),
		IPAddresses: []*protocols.IPAddress{{
			Family:  protocols.IPFamily_v4,
			Address: ip,
			Mask:    fmt.Sprint(prefix),
		}},
	}})
	if err != nil {
		return fmt.Errorf("update interface: %w", err)
	}
	_, err = c.Agent.UpdateRoutes(ctx, &agentgrpc.UpdateRoutesRequest{Routes: &agentgrpc.Routes{Routes: []*protocols.Route{{
		Dest:    "",
		Gateway: gateway,
		Device:  device,
		Family:  protocols.IPFamily_v4,
	}}}})
	if err != nil {
		return fmt.Errorf("update routes: %w", err)
	}
	return nil
}

// CreateContainer creates a container from an OCI spec whose root filesystem
// is already in the share.
func (c *Client) CreateContainer(ctx context.Context, cid string, spec *specs.Spec) error {
	oci, err := agentgrpc.OCItoGRPC(spec)
	if err != nil {
		return fmt.Errorf("oci to grpc: %w", err)
	}
	_, err = c.Agent.CreateContainer(ctx, &agentgrpc.CreateContainerRequest{
		ContainerId: cid,
		ExecId:      cid,
		OCI:         oci,
	})
	return err
}

// StartContainer runs a created container's process.
func (c *Client) StartContainer(ctx context.Context, cid string) error {
	_, err := c.Agent.StartContainer(ctx, &agentgrpc.StartContainerRequest{ContainerId: cid})
	return err
}

// Wait blocks until the process exits and returns its status.
func (c *Client) Wait(ctx context.Context, cid, execID string) (int32, error) {
	w, err := c.Agent.WaitProcess(ctx, &agentgrpc.WaitProcessRequest{ContainerId: cid, ExecId: execID})
	if err != nil {
		return -1, err
	}
	return w.Status, nil
}

// ReadStdout returns the next chunk of a process's stdout; an error ends the
// stream. The agent answers with an empty chunk when nothing is buffered.
func (c *Client) ReadStdout(ctx context.Context, cid, execID string) ([]byte, error) {
	r, err := c.Agent.ReadStdout(ctx, &agentgrpc.ReadStreamRequest{ContainerId: cid, ExecId: execID, Len: 65536})
	if err != nil {
		return nil, err
	}
	return r.Data, nil
}

// ReadStderr is ReadStdout for stderr.
func (c *Client) ReadStderr(ctx context.Context, cid, execID string) ([]byte, error) {
	r, err := c.Agent.ReadStderr(ctx, &agentgrpc.ReadStreamRequest{ContainerId: cid, ExecId: execID, Len: 65536})
	if err != nil {
		return nil, err
	}
	return r.Data, nil
}

// WriteStdin hands a process bytes on its stdin.
func (c *Client) WriteStdin(ctx context.Context, cid, execID string, data []byte) error {
	for len(data) > 0 {
		r, err := c.Agent.WriteStdin(ctx, &agentgrpc.WriteStreamRequest{ContainerId: cid, ExecId: execID, Data: data})
		if err != nil {
			return err
		}
		n := int(r.Len)
		if n <= 0 || n > len(data) {
			n = len(data)
		}
		data = data[n:]
	}
	return nil
}

// CloseStdin ends a process's stdin.
func (c *Client) CloseStdin(ctx context.Context, cid, execID string) error {
	_, err := c.Agent.CloseStdin(ctx, &agentgrpc.CloseStdinRequest{ContainerId: cid, ExecId: execID})
	return err
}

// Resize sets a process's terminal size.
func (c *Client) Resize(ctx context.Context, cid, execID string, rows, cols uint32) error {
	_, err := c.Agent.TtyWinResize(ctx, &agentgrpc.TtyWinResizeRequest{ContainerId: cid, ExecId: execID, Row: rows, Column: cols})
	return err
}

// ExecOptions describe a process to run inside a container.
type ExecOptions struct {
	Args     []string
	Env      []string
	Cwd      string
	UID, GID uint32
	Terminal bool
}

// Exec starts a process inside a running container.
func (c *Client) Exec(ctx context.Context, cid, execID string, opts ExecOptions) error {
	_, err := c.Agent.ExecProcess(ctx, &agentgrpc.ExecProcessRequest{
		ContainerId: cid,
		ExecId:      execID,
		Process: &agentgrpc.Process{
			Terminal: opts.Terminal,
			User:     &agentgrpc.User{UID: opts.UID, GID: opts.GID},
			Args:     opts.Args,
			Env:      opts.Env,
			Cwd:      opts.Cwd,
		},
	})
	return err
}

// Signal sends a signal to a process.
func (c *Client) Signal(ctx context.Context, cid, execID string, sig uint32) error {
	_, err := c.Agent.SignalProcess(ctx, &agentgrpc.SignalProcessRequest{ContainerId: cid, ExecId: execID, Signal: sig})
	return err
}

// SetTime sets the guest's wall clock.
func (c *Client) SetTime(ctx context.Context, now time.Time) error {
	_, err := c.Agent.SetGuestDateTime(ctx, &agentgrpc.SetGuestDateTimeRequest{Sec: now.Unix(), Usec: int64(now.Nanosecond() / 1000)})
	return err
}

// Reseed mixes fresh entropy into the guest kernel's random pool, so a guest
// restored from a snapshot stops sharing its random stream with every other
// restore of it.
func (c *Client) Reseed(ctx context.Context, data []byte) error {
	_, err := c.Agent.ReseedRandomDev(ctx, &agentgrpc.ReseedRandomDevRequest{Data: data})
	return err
}

// Storage is the agent's storage descriptor, re-exported so callers need not
// import the agent protocol.
type Storage = agentgrpc.Storage
