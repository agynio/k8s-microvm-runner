package supervisor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"

	slotv1 "github.com/agynio/k8s-microvm-runner/internal/.gen/agynio/microvm/slot/v1"
	"github.com/agynio/k8s-microvm-runner/internal/guest"
	"github.com/agynio/k8s-microvm-runner/internal/slotclient"
	"github.com/agynio/k8s-microvm-runner/internal/store"
	"github.com/agynio/k8s-microvm-runner/internal/versions"
)

// fakeGuest stands in for the VMM, virtiofsd and the guest agent: it holds
// one workload and moves it through the statuses a real one would.
type fakeGuest struct {
	mu       sync.Mutex
	w        *guest.Workload
	onChange func(guest.View)
	binds    int
	cached   []string
	execOut  string
	stdin    bytes.Buffer
	resizes  [][2]uint32
	logs     string
}

func (g *fakeGuest) Versions() versions.Set { return versions.Set{CloudHypervisor: "v52.0"} }
func (g *fakeGuest) Binds() int             { return g.binds }
func (g *fakeGuest) Current() *guest.Workload {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.w
}
func (g *fakeGuest) View() (guest.View, bool) {
	w := g.Current()
	if w == nil {
		return guest.View{}, false
	}
	return w.View(), true
}
func (g *fakeGuest) set(w *guest.Workload, st guest.Status) {
	w.Status = st
	if g.onChange != nil {
		g.onChange(w.View())
	}
}
func (g *fakeGuest) Bind(ctx context.Context, spec guest.BindSpec) (guest.View, error) {
	g.mu.Lock()
	if g.w != nil {
		g.mu.Unlock()
		return guest.View{}, guest.ErrBusy
	}
	w := &guest.Workload{ID: spec.Spec.GetWorkloadId(), Key: spec.Key, Flavor: spec.Flavor, Status: guest.StatusStarting, Labels: map[string]string{}}
	w.Containers = []*guest.Container{{Name: "agent", Role: guest.RoleMain, Image: spec.Spec.GetMain().GetImage(), Status: guest.ContainerWaiting}}
	g.w = w
	g.binds++
	g.mu.Unlock()
	go func() {
		time.Sleep(20 * time.Millisecond)
		w.Containers[0].Status = guest.ContainerRunning
		g.set(w, guest.StatusRunning)
	}()
	return w.View(), nil
}
func (g *fakeGuest) Stop(ctx context.Context, timeout time.Duration, cfg store.Config, volumes []guest.VolumeStore) error {
	w := g.Current()
	if w == nil {
		return guest.ErrIdle
	}
	g.set(w, guest.StatusStopped)
	g.mu.Lock()
	g.w = nil
	g.mu.Unlock()
	return nil
}
func (g *fakeGuest) Hibernate(ctx context.Context, cfg store.Config, volumes []guest.VolumeStore) (int64, error) {
	w := g.Current()
	if w == nil {
		return 0, guest.ErrIdle
	}
	if !cfg.Configured() {
		return 0, errors.New("hibernation needs a snapshot store")
	}
	g.set(w, guest.StatusHibernating)
	w.SnapshotBytes = 1234
	g.set(w, guest.StatusHibernated)
	g.mu.Lock()
	g.cached = append(g.cached, w.Key)
	g.w = nil
	g.mu.Unlock()
	return 1234, nil
}
func (g *fakeGuest) Wake(ctx context.Context, spec guest.WakeSpec) (guest.View, error) {
	g.mu.Lock()
	if g.w != nil {
		g.mu.Unlock()
		return guest.View{}, guest.ErrBusy
	}
	w := &guest.Workload{ID: spec.WorkloadID, Key: spec.WorkloadKey, Status: guest.StatusRunning, Labels: map[string]string{}}
	g.w = w
	g.binds++
	g.mu.Unlock()
	return w.View(), nil
}
func (g *fakeGuest) Exec(ctx context.Context, w *guest.Workload, container string, command []string, tty bool, streams guest.ExecStreams) error {
	// As the real one: resizes are taken as they come, for as long as the
	// session lasts, without holding the command up.
	gotResize := make(chan struct{}, 1)
	if streams.Resize != nil {
		go func() {
			for size := range streams.Resize {
				g.mu.Lock()
				g.resizes = append(g.resizes, size)
				g.mu.Unlock()
				select {
				case gotResize <- struct{}{}:
				default:
				}
			}
		}()
	}
	if streams.Stdin != nil {
		_, _ = io.Copy(&g.stdin, streams.Stdin)
		select {
		case <-gotResize:
		case <-time.After(time.Second):
		}
	}
	_, _ = streams.Stdout.Write([]byte(g.execOut))
	if streams.Stderr != nil {
		_, _ = streams.Stderr.Write([]byte("warn\n"))
	}
	if strings.Join(command, " ") == "false" {
		return &guest.ExitError{Code: 1}
	}
	return nil
}
func (g *fakeGuest) OpenLogs(ctx context.Context, w *guest.Workload, container string, opts guest.LogOptions) (io.ReadCloser, error) {
	if container != "agent" {
		return nil, guest.ErrNoContainer
	}
	return io.NopCloser(strings.NewReader(g.logs)), nil
}
func (g *fakeGuest) CachedSnapshots() []string { return g.cached }
func (g *fakeGuest) DiscardCache(key string) error {
	g.cached = nil
	return nil
}

func startService(t *testing.T, g *fakeGuest) *slotclient.Client {
	t.Helper()
	service := New("slot-test", "secret", g, zap.NewNop())
	g.onChange = service.OnChange
	server := grpc.NewServer(grpc.UnaryInterceptor(service.UnaryAuth), grpc.StreamInterceptor(service.StreamAuth))
	slotv1.RegisterSlotSupervisorServiceServer(server, service)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return slotclient.FromConn(conn, "secret")
}

func startRequest(id string) *runnerv1.StartWorkloadRequest {
	return &runnerv1.StartWorkloadRequest{WorkloadId: id, Main: &runnerv1.ContainerSpec{Name: "agent", Image: "registry.example/agent:1"}}
}

// Nothing but the runner may call the supervisor: a call without the slot's
// token is refused before it reaches anything.
func TestCallsWithoutTheTokenAreRefused(t *testing.T) {
	g := &fakeGuest{}
	client := startService(t, g)
	wrong := slotclient.FromConn(client.Conn(), "guess")
	if _, err := wrong.Describe(context.Background(), time.Second); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %v", err)
	}
	if _, err := client.Describe(context.Background(), time.Second); err != nil {
		t.Fatalf("expected the right token to pass, got %v", err)
	}
}

// Bind returns at once with the workload starting; WatchState then carries
// it to running; Describe shows it held; Hibernate clears the slot and
// reports the size; the cached snapshot shows on Describe until discarded.
func TestBindWatchHibernateWake(t *testing.T) {
	g := &fakeGuest{}
	client := startService(t, g)
	ctx := context.Background()

	resp, err := client.Bind(ctx, &slotv1.BindRequest{Spec: startRequest("w1"), WorkloadKey: "k1", Flavor: &slotv1.Flavor{Name: "small", Vcpus: 1, MemoryMib: 2048}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetWorkload().GetStatus() != slotv1.WorkloadStatus_WORKLOAD_STATUS_STARTING || len(resp.GetWorkload().GetContainers()) != 1 {
		t.Fatalf("unexpected bind response %+v", resp.GetWorkload())
	}
	if _, err := client.Bind(ctx, &slotv1.BindRequest{Spec: startRequest("w2")}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected a second bind refused, got %v", err)
	}
	stream, err := client.WatchState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var seen []slotv1.WorkloadStatus
	deadline := time.After(5 * time.Second)
	for len(seen) == 0 || seen[len(seen)-1] != slotv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		select {
		case <-deadline:
			t.Fatalf("never saw running, saw %v", seen)
		default:
		}
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("stream ended: %v (saw %v)", err, seen)
		}
		seen = append(seen, msg.GetWorkload().GetStatus())
	}
	desc, err := client.Describe(ctx, time.Second)
	if err != nil || desc.GetState() != slotv1.SlotState_SLOT_STATE_BOUND || desc.GetWorkload().GetWorkloadId() != "w1" {
		t.Fatalf("expected the slot bound to w1, got %+v %v", desc, err)
	}

	if _, err := client.Hibernate(ctx, &slotv1.HibernateRequest{WorkloadId: "w1"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected hibernation without a store refused, got %v", err)
	}
	size, err := client.Hibernate(ctx, &slotv1.HibernateRequest{WorkloadId: "w1", Store: &slotv1.StoreConfig{Endpoint: "store:9000", Bucket: "b"}})
	if err != nil || size != 1234 {
		t.Fatalf("expected the snapshot size, got %d %v", size, err)
	}
	msg, err := stream.Recv()
	for err == nil && msg.GetWorkload().GetStatus() != slotv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED {
		msg, err = stream.Recv()
	}
	if err != nil || msg.GetWorkload().GetSnapshotSizeBytes() != 1234 {
		t.Fatalf("expected the stream to end on hibernated with the size, got %v %v", msg, err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("expected the stream to end once the slot cleared")
	}
	desc, _ = client.Describe(ctx, time.Second)
	if desc.GetState() != slotv1.SlotState_SLOT_STATE_IDLE || len(desc.GetCachedSnapshots()) != 1 || desc.GetCachedSnapshots()[0] != "k1" {
		t.Fatalf("expected an idle slot caching k1, got %+v", desc)
	}
	view, err := client.Wake(ctx, &slotv1.WakeRequest{WorkloadId: "w1", WorkloadKey: "k1", Store: &slotv1.StoreConfig{Endpoint: "store:9000", Bucket: "b"}})
	if err != nil || view.GetStatus() != slotv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		t.Fatalf("expected the wake to return running, got %+v %v", view, err)
	}
	if err := client.DiscardCache(ctx, "k1"); err != nil {
		t.Fatal(err)
	}
	if desc, _ := client.Describe(ctx, time.Second); len(desc.GetCachedSnapshots()) != 0 || desc.GetBinds() != 2 {
		t.Fatalf("expected no cached snapshot and two binds, got %+v", desc)
	}
	if err := client.Stop(ctx, &slotv1.StopRequest{WorkloadId: "w1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Inspect(ctx, "w1"); status.Code(err) != codes.NotFound {
		t.Fatalf("expected the stopped workload gone, got %v", err)
	}
}

// Exec carries stdin, stdout, stderr, resizes and the exit code across the
// stream; a non-zero exit is an error exposing its status.
func TestExecStreams(t *testing.T) {
	g := &fakeGuest{execOut: "out\n"}
	client := startService(t, g)
	ctx := context.Background()
	if _, err := client.Bind(ctx, &slotv1.BindRequest{Spec: startRequest("w1"), Flavor: &slotv1.Flavor{Vcpus: 1, MemoryMib: 1}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	var stdout, stderr bytes.Buffer
	resize := make(chan [2]uint32, 1)
	resize <- [2]uint32{40, 100}
	close(resize)
	err := client.Exec(ctx, "w1", "", []string{"cat"}, false, slotclient.ExecStreams{Stdin: strings.NewReader("input"), Stdout: &stdout, Stderr: &stderr, Resize: resize})
	if err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "out\n" || stderr.String() != "warn\n" || g.stdin.String() != "input" {
		t.Fatalf("streams not carried: stdout %q stderr %q stdin %q", stdout.String(), stderr.String(), g.stdin.String())
	}
	g.mu.Lock()
	resizes := append([][2]uint32(nil), g.resizes...)
	g.mu.Unlock()
	if len(resizes) != 1 || resizes[0] != [2]uint32{40, 100} {
		t.Fatalf("resize not carried: %v", resizes)
	}
	err = client.Exec(ctx, "w1", "", []string{"false"}, true, slotclient.ExecStreams{Stdout: &stdout})
	var exit *slotclient.ExitError
	if !errors.As(err, &exit) || exit.ExitStatus() != 1 {
		t.Fatalf("expected exit status 1, got %v", err)
	}
	g.logs = "line1\nline2\n"
	rc, err := client.Logs(ctx, &slotv1.LogsRequest{WorkloadId: "w1", Container: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	if string(data) != "line1\nline2\n" {
		t.Fatalf("unexpected logs %q", data)
	}
	// A server stream's refusal arrives on the first read, not on the call.
	rc, err = client.Logs(ctx, &slotv1.LogsRequest{WorkloadId: "w1", Container: "nope"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(rc); status.Code(err) != codes.NotFound {
		t.Fatalf("expected an unknown container refused with NotFound, got %v", err)
	}
}
