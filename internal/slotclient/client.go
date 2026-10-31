// Package slotclient is the runner's side of the slot control protocol: a
// gRPC client per slot, authenticated with the slot's token.
package slotclient

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	slotv1 "github.com/agynio/k8s-microvm-runner/internal/.gen/agynio/microvm/slot/v1"
	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"
)

// DefaultPort is where a supervisor listens inside its Pod.
const DefaultPort = 7700

// Client talks to one supervisor.
type Client struct {
	conn  *grpc.ClientConn
	token string
	api   slotv1.SlotSupervisorServiceClient
}

// Dial connects to a supervisor at address with its token. The connection is
// lazy; a dead supervisor shows on the first call.
func Dial(address, token string) (*Client, error) {
	conn, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20), grpc.MaxCallSendMsgSize(16<<20)),
	)
	if err != nil {
		return nil, fmt.Errorf("dial slot %s: %w", address, err)
	}
	return &Client{conn: conn, token: token, api: slotv1.NewSlotSupervisorServiceClient(conn)}, nil
}

// Close ends the connection.
func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) ctx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
}

// Describe asks the slot what it runs and holds.
func (c *Client) Describe(ctx context.Context, timeout time.Duration) (*slotv1.DescribeResponse, error) {
	ctx, cancel := context.WithTimeout(c.ctx(ctx), timeout)
	defer cancel()
	return c.api.Describe(ctx, &slotv1.DescribeRequest{})
}

// Bind starts a workload in the slot.
func (c *Client) Bind(ctx context.Context, req *slotv1.BindRequest) (*slotv1.BindResponse, error) {
	return c.api.Bind(c.ctx(ctx), req)
}

// Inspect is the slot's view of its workload.
func (c *Client) Inspect(ctx context.Context, workloadID string) (*slotv1.WorkloadView, error) {
	resp, err := c.api.Inspect(c.ctx(ctx), &slotv1.InspectRequest{WorkloadId: workloadID})
	if err != nil {
		return nil, err
	}
	return resp.GetWorkload(), nil
}

// Stop ends the slot's workload.
func (c *Client) Stop(ctx context.Context, req *slotv1.StopRequest) error {
	_, err := c.api.Stop(c.ctx(ctx), req)
	return err
}

// Hibernate snapshots the slot's workload and clears the slot.
func (c *Client) Hibernate(ctx context.Context, req *slotv1.HibernateRequest) (int64, error) {
	resp, err := c.api.Hibernate(c.ctx(ctx), req)
	if err != nil {
		return 0, err
	}
	return resp.GetSnapshotSizeBytes(), nil
}

// Wake restores a workload into the slot.
func (c *Client) Wake(ctx context.Context, req *slotv1.WakeRequest) (*slotv1.WorkloadView, error) {
	resp, err := c.api.Wake(c.ctx(ctx), req)
	if err != nil {
		return nil, err
	}
	return resp.GetWorkload(), nil
}

// DiscardCache drops a cached snapshot.
func (c *Client) DiscardCache(ctx context.Context, workloadKey string) error {
	_, err := c.api.DiscardCache(c.ctx(ctx), &slotv1.DiscardCacheRequest{WorkloadKey: workloadKey})
	return err
}

// WatchState opens the slot's state stream.
func (c *Client) WatchState(ctx context.Context) (slotv1.SlotSupervisorService_WatchStateClient, error) {
	return c.api.WatchState(c.ctx(ctx), &slotv1.WatchStateRequest{})
}

// Logs opens a container's log stream as a reader.
func (c *Client) Logs(ctx context.Context, req *slotv1.LogsRequest) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(c.ctx(ctx))
	stream, err := c.api.Logs(ctx, req)
	if err != nil {
		cancel()
		return nil, err
	}
	return &logStream{stream: stream, cancel: cancel}, nil
}

type logStream struct {
	stream slotv1.SlotSupervisorService_LogsClient
	cancel context.CancelFunc
	buf    []byte
}

func (l *logStream) Read(p []byte) (int, error) {
	for len(l.buf) == 0 {
		msg, err := l.stream.Recv()
		if err != nil {
			return 0, err
		}
		l.buf = msg.GetData()
	}
	n := copy(p, l.buf)
	l.buf = l.buf[n:]
	return n, nil
}

func (l *logStream) Close() error {
	l.cancel()
	return nil
}

// ExecStreams is what an exec is wired to on the runner's side.
type ExecStreams struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	Resize <-chan [2]uint32
}

// ExitError is a command that exited non-zero.
type ExitError struct{ Code int }

func (e *ExitError) Error() string   { return fmt.Sprintf("exit status %d", e.Code) }
func (e *ExitError) ExitStatus() int { return e.Code }

// Exec runs a command in the slot's workload and returns when it exits.
func (c *Client) Exec(ctx context.Context, workloadID, container string, command []string, tty bool, streams ExecStreams) error {
	ctx, cancel := context.WithCancel(c.ctx(ctx))
	defer cancel()
	stream, err := c.api.Exec(ctx)
	if err != nil {
		return err
	}
	// Stdin and resizes are sent from their own goroutines; a stream takes
	// one sender at a time.
	var sendMu sync.Mutex
	send := func(req *slotv1.ExecRequest) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(req)
	}
	if err := send(&slotv1.ExecRequest{Event: &slotv1.ExecRequest_Start{Start: &slotv1.ExecStart{WorkloadId: workloadID, Container: container, Command: command, Tty: tty}}}); err != nil {
		return err
	}
	if streams.Stdin == nil {
		// No stdin at all is stdin at EOF: a command that reads it must not
		// wait for a caller that will never write.
		if err := send(&slotv1.ExecRequest{Event: &slotv1.ExecRequest_StdinEof{StdinEof: true}}); err != nil {
			return err
		}
	} else {
		go func() {
			buf := make([]byte, 32*1024)
			for {
				n, err := streams.Stdin.Read(buf)
				if n > 0 {
					if serr := send(&slotv1.ExecRequest{Event: &slotv1.ExecRequest_Stdin{Stdin: append([]byte(nil), buf[:n]...)}}); serr != nil {
						return
					}
				}
				if err != nil {
					_ = send(&slotv1.ExecRequest{Event: &slotv1.ExecRequest_StdinEof{StdinEof: true}})
					return
				}
			}
		}()
	}
	if streams.Resize != nil {
		go func() {
			for size := range streams.Resize {
				if err := send(&slotv1.ExecRequest{Event: &slotv1.ExecRequest_Resize{Resize: &slotv1.ExecResize{Rows: size[0], Cols: size[1]}}}); err != nil {
					return
				}
			}
		}()
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		switch ev := msg.GetEvent().(type) {
		case *slotv1.ExecResponse_Stdout:
			if streams.Stdout != nil {
				if _, err := streams.Stdout.Write(ev.Stdout); err != nil {
					return err
				}
			}
		case *slotv1.ExecResponse_Stderr:
			if streams.Stderr != nil {
				if _, err := streams.Stderr.Write(ev.Stderr); err != nil {
					return err
				}
			}
		case *slotv1.ExecResponse_Exit:
			if ev.Exit.GetError() != "" {
				return fmt.Errorf("%s", ev.Exit.GetError())
			}
			if ev.Exit.GetCode() != 0 {
				return &ExitError{Code: int(ev.Exit.GetCode())}
			}
			return nil
		}
	}
}

// Credential helpers for requests.
func Credentials(creds []*runnerv1.ImagePullCredential) []*runnerv1.ImagePullCredential { return creds }

// API is what the runner uses of a slot's control connection; *Client
// implements it and tests fake it.
type API interface {
	Describe(ctx context.Context, timeout time.Duration) (*slotv1.DescribeResponse, error)
	Bind(ctx context.Context, req *slotv1.BindRequest) (*slotv1.BindResponse, error)
	Inspect(ctx context.Context, workloadID string) (*slotv1.WorkloadView, error)
	Stop(ctx context.Context, req *slotv1.StopRequest) error
	Hibernate(ctx context.Context, req *slotv1.HibernateRequest) (int64, error)
	Wake(ctx context.Context, req *slotv1.WakeRequest) (*slotv1.WorkloadView, error)
	DiscardCache(ctx context.Context, workloadKey string) error
	WatchState(ctx context.Context) (slotv1.SlotSupervisorService_WatchStateClient, error)
	Logs(ctx context.Context, req *slotv1.LogsRequest) (io.ReadCloser, error)
	Exec(ctx context.Context, workloadID, container string, command []string, tty bool, streams ExecStreams) error
	Close() error
}

var _ API = (*Client)(nil)

// FromConn wraps an existing connection; tests use it with an in-memory
// listener.
func FromConn(conn *grpc.ClientConn, token string) *Client {
	return &Client{conn: conn, token: token, api: slotv1.NewSlotSupervisorServiceClient(conn)}
}

// Conn is the underlying connection.
func (c *Client) Conn() *grpc.ClientConn { return c.conn }
