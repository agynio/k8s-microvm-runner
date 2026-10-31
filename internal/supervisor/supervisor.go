// Package supervisor is the control surface of one slot: the gRPC service
// the runner dials over the Pod network, authenticated with the token the
// runner injected when it created the Pod, over a guest manager that owns
// everything inside the slot.
package supervisor

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	slotv1 "github.com/agynio/k8s-microvm-runner/internal/.gen/agynio/microvm/slot/v1"
	"github.com/agynio/k8s-microvm-runner/internal/guest"
	"github.com/agynio/k8s-microvm-runner/internal/snapshot"
	"github.com/agynio/k8s-microvm-runner/internal/store"
	"github.com/agynio/k8s-microvm-runner/internal/versions"
)

// Guest is what the service needs from the slot's guest manager. The real
// one is *guest.Manager; tests use a fake.
type Guest interface {
	Versions() versions.Set
	Binds() int
	Current() *guest.Workload
	View() (guest.View, bool)
	Bind(ctx context.Context, spec guest.BindSpec) (guest.View, error)
	Stop(ctx context.Context, timeout time.Duration, cfg store.Config, volumes []guest.VolumeStore) error
	Hibernate(ctx context.Context, cfg store.Config, volumes []guest.VolumeStore) (int64, error)
	Wake(ctx context.Context, spec guest.WakeSpec) (guest.View, error)
	Exec(ctx context.Context, w *guest.Workload, container string, command []string, tty bool, streams guest.ExecStreams) error
	OpenLogs(ctx context.Context, w *guest.Workload, container string, opts guest.LogOptions) (io.ReadCloser, error)
	CachedSnapshots() []string
	DiscardCache(key string) error
}

// Service serves the slot control API.
type Service struct {
	slotv1.UnimplementedSlotSupervisorServiceServer
	name  string
	token string
	guest Guest
	log   *zap.Logger

	mu       sync.Mutex
	watchers map[chan guest.View]struct{}
	last     *guest.View
}

// New builds the service. OnChange is what the guest manager must be given
// so every change reaches WatchState.
func New(name, token string, g Guest, log *zap.Logger) *Service {
	if log == nil {
		log = zap.NewNop()
	}
	return &Service{name: name, token: token, guest: g, log: log, watchers: map[chan guest.View]struct{}{}}
}

// OnChange is the guest manager's change listener: it fans a view out to
// every WatchState stream.
func (s *Service) OnChange(v guest.View) {
	s.mu.Lock()
	s.last = &v
	for ch := range s.watchers {
		select {
		case ch <- v:
		default:
			// A slow watcher is dropped a view; the next one carries the
			// whole state again.
		}
	}
	s.mu.Unlock()
}

// UnaryAuth refuses calls without the slot's token.
func (s *Service) UnaryAuth(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := s.authenticate(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// StreamAuth is UnaryAuth for streams.
func (s *Service) StreamAuth(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.authenticate(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

// MetadataKey carries the token.
const MetadataKey = "authorization"

func (s *Service) authenticate(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	for _, value := range md.Get(MetadataKey) {
		token := strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
		if subtle.ConstantTimeCompare([]byte(token), []byte(s.token)) == 1 {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "slot token required")
}

func (s *Service) Describe(ctx context.Context, _ *slotv1.DescribeRequest) (*slotv1.DescribeResponse, error) {
	set := s.guest.Versions()
	resp := &slotv1.DescribeResponse{
		SlotName: s.name,
		Versions: &slotv1.VersionSet{CloudHypervisor: set.CloudHypervisor, Virtiofsd: set.Virtiofsd, GuestKernel: set.GuestKernel, GuestImage: set.GuestImage, Digest: set.Digest()},
		State:    slotv1.SlotState_SLOT_STATE_IDLE,
		Binds:    int32(s.guest.Binds()),
	}
	if v, ok := s.guest.View(); ok {
		resp.State = slotv1.SlotState_SLOT_STATE_BOUND
		resp.Workload = ViewToProto(v)
	}
	resp.CachedSnapshots = s.guest.CachedSnapshots()
	return resp, nil
}

func (s *Service) Bind(ctx context.Context, req *slotv1.BindRequest) (*slotv1.BindResponse, error) {
	if req.GetSpec() == nil {
		return nil, status.Error(codes.InvalidArgument, "spec required")
	}
	v, err := s.guest.Bind(ctx, guest.BindSpec{
		Spec:    req.GetSpec(),
		Key:     req.GetWorkloadKey(),
		Flavor:  flavorFromProto(req.GetFlavor()),
		Store:   storeFromProto(req.GetStore()),
		Volumes: volumesFromProto(req.GetVolumes()),
	})
	if err != nil {
		return nil, bindError(err)
	}
	return &slotv1.BindResponse{Workload: ViewToProto(v)}, nil
}

func bindError(err error) error {
	switch {
	case errors.Is(err, guest.ErrBusy):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, guest.ErrIdle):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.InvalidArgument, err.Error())
	}
}

func (s *Service) held(workloadID string) (*guest.Workload, error) {
	w := s.guest.Current()
	if w == nil || (workloadID != "" && w.ID != workloadID) {
		return nil, status.Error(codes.NotFound, "slot does not hold this workload")
	}
	return w, nil
}

func (s *Service) Inspect(ctx context.Context, req *slotv1.InspectRequest) (*slotv1.InspectResponse, error) {
	w, err := s.held(req.GetWorkloadId())
	if err != nil {
		return nil, err
	}
	return &slotv1.InspectResponse{Workload: ViewToProto(w.View())}, nil
}

func (s *Service) Stop(ctx context.Context, req *slotv1.StopRequest) (*slotv1.StopResponse, error) {
	if _, err := s.held(req.GetWorkloadId()); err != nil {
		return nil, err
	}
	if err := s.guest.Stop(ctx, time.Duration(req.GetTimeoutSec())*time.Second, storeFromProto(req.GetStore()), volumesFromProto(req.GetVolumes())); err != nil {
		return nil, bindError(err)
	}
	return &slotv1.StopResponse{}, nil
}

func (s *Service) Hibernate(ctx context.Context, req *slotv1.HibernateRequest) (*slotv1.HibernateResponse, error) {
	if _, err := s.held(req.GetWorkloadId()); err != nil {
		return nil, err
	}
	size, err := s.guest.Hibernate(ctx, storeFromProto(req.GetStore()), volumesFromProto(req.GetVolumes()))
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &slotv1.HibernateResponse{SnapshotSizeBytes: size}, nil
}

func (s *Service) Wake(ctx context.Context, req *slotv1.WakeRequest) (*slotv1.WakeResponse, error) {
	v, err := s.guest.Wake(ctx, guest.WakeSpec{
		WorkloadID:  req.GetWorkloadId(),
		WorkloadKey: req.GetWorkloadKey(),
		Flavor:      flavorFromProto(req.GetFlavor()),
		Store:       storeFromProto(req.GetStore()),
		Volumes:     volumesFromProto(req.GetVolumes()),
		Credentials: req.GetImagePullCredentials(),
	})
	if err != nil {
		if errors.Is(err, guest.ErrBusy) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &slotv1.WakeResponse{Workload: ViewToProto(v)}, nil
}

func (s *Service) DiscardCache(ctx context.Context, req *slotv1.DiscardCacheRequest) (*slotv1.DiscardCacheResponse, error) {
	if err := s.guest.DiscardCache(req.GetWorkloadKey()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &slotv1.DiscardCacheResponse{}, nil
}

// WatchState sends the held workload's view now and on every change, and
// ends once the slot holds nothing.
func (s *Service) WatchState(_ *slotv1.WatchStateRequest, stream slotv1.SlotSupervisorService_WatchStateServer) error {
	ch := make(chan guest.View, 16)
	s.mu.Lock()
	s.watchers[ch] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.watchers, ch)
		s.mu.Unlock()
	}()
	if v, ok := s.guest.View(); ok {
		if err := stream.Send(&slotv1.WatchStateResponse{Workload: ViewToProto(v)}); err != nil {
			return err
		}
	} else {
		return nil
	}
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case v := <-ch:
			if err := stream.Send(&slotv1.WatchStateResponse{Workload: ViewToProto(v)}); err != nil {
				return err
			}
			if v.Status == guest.StatusStopped || v.Status == guest.StatusHibernated {
				return nil
			}
		}
	}
}

func (s *Service) Logs(req *slotv1.LogsRequest, stream slotv1.SlotSupervisorService_LogsServer) error {
	w, err := s.held(req.GetWorkloadId())
	if err != nil {
		return err
	}
	opts := guest.LogOptions{TailLines: req.GetTailLines(), Follow: req.GetFollow(), Timestamps: req.GetTimestamps()}
	if req.GetSince() != nil {
		opts.Since = req.GetSince().AsTime()
	}
	rc, err := s.guest.OpenLogs(stream.Context(), w, req.GetContainer(), opts)
	if err != nil {
		if errors.Is(err, guest.ErrNoContainer) {
			return status.Error(codes.NotFound, "container_not_found")
		}
		return status.Error(codes.Internal, err.Error())
	}
	defer rc.Close()
	buf := make([]byte, 32*1024)
	for {
		n, err := rc.Read(buf)
		if n > 0 {
			if err := stream.Send(&slotv1.LogsResponse{Data: append([]byte(nil), buf[:n]...)}); err != nil {
				return err
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return status.Error(codes.Internal, err.Error())
		}
	}
}

// Exec bridges the stream to the guest: the first message starts the
// command, the rest feed it stdin and resizes; stdout, stderr and the exit
// come back.
func (s *Service) Exec(stream slotv1.SlotSupervisorService_ExecServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "exec must start with a start message")
	}
	w, err := s.held(start.GetWorkloadId())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	stdinR, stdinW := io.Pipe()
	resize := make(chan [2]uint32, 1)
	go func() {
		defer stdinW.Close()
		defer close(resize)
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			switch ev := msg.GetEvent().(type) {
			case *slotv1.ExecRequest_Stdin:
				if _, err := stdinW.Write(ev.Stdin); err != nil {
					return
				}
			case *slotv1.ExecRequest_StdinEof:
				// Stdin is done; resizes keep coming for as long as the
				// session lasts.
				if ev.StdinEof {
					_ = stdinW.Close()
				}
			case *slotv1.ExecRequest_Resize:
				select {
				case resize <- [2]uint32{ev.Resize.GetRows(), ev.Resize.GetCols()}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	var sendMu sync.Mutex
	send := func(resp *slotv1.ExecResponse) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(resp)
	}
	stdout := &streamWriter{send: func(b []byte) error { return send(&slotv1.ExecResponse{Event: &slotv1.ExecResponse_Stdout{Stdout: b}}) }}
	var stderr io.Writer
	if !start.GetTty() {
		stderr = &streamWriter{send: func(b []byte) error { return send(&slotv1.ExecResponse{Event: &slotv1.ExecResponse_Stderr{Stderr: b}}) }}
	}
	runErr := s.guest.Exec(ctx, w, start.GetContainer(), start.GetCommand(), start.GetTty(), guest.ExecStreams{Stdin: stdinR, Stdout: stdout, Stderr: stderr, Resize: resize})
	exit := &slotv1.ExecExit{}
	var exitErr *guest.ExitError
	switch {
	case runErr == nil:
	case errors.As(runErr, &exitErr):
		exit.Code = int32(exitErr.Code)
	default:
		exit.Code = -1
		exit.Error = runErr.Error()
	}
	return send(&slotv1.ExecResponse{Event: &slotv1.ExecResponse_Exit{Exit: exit}})
}

type streamWriter struct {
	send func([]byte) error
}

func (w *streamWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := w.send(append([]byte(nil), p...)); err != nil {
		return 0, err
	}
	return len(p), nil
}

// ViewToProto renders a workload view.
func ViewToProto(v guest.View) *slotv1.WorkloadView {
	out := &slotv1.WorkloadView{
		WorkloadId: v.ID, WorkloadKey: v.Key, Labels: v.Labels, Flavor: v.Flavor, Status: StatusToProto(v.Status), Failure: v.Failure,
		SnapshotSizeBytes: v.SnapshotBytes,
	}
	if !v.StartedAt.IsZero() {
		out.StartedAt = timestamppb.New(v.StartedAt)
	}
	if !v.HibernatedAt.IsZero() {
		out.HibernatedAt = timestamppb.New(v.HibernatedAt)
	}
	for _, c := range v.Containers {
		cv := &slotv1.ContainerView{Name: c.Name, Role: roleToProto(c.Role), Image: c.Image, ImageDigest: c.ImageDigest, Status: containerStatusToProto(c.Status), Reason: c.Reason, Message: c.Message}
		if c.ExitCode != nil {
			code := *c.ExitCode
			cv.ExitCode = &code
		}
		if !c.StartedAt.IsZero() {
			cv.StartedAt = timestamppb.New(c.StartedAt)
		}
		if !c.FinishedAt.IsZero() {
			cv.FinishedAt = timestamppb.New(c.FinishedAt)
		}
		out.Containers = append(out.Containers, cv)
	}
	for _, mnt := range v.Mounts {
		out.Mounts = append(out.Mounts, &slotv1.MountView{Volume: mnt.Volume, Type: "share", Destination: mnt.Destination, ReadOnly: mnt.ReadOnly})
	}
	return out
}

// StatusToProto maps a guest status.
func StatusToProto(st guest.Status) slotv1.WorkloadStatus {
	switch st {
	case guest.StatusStarting:
		return slotv1.WorkloadStatus_WORKLOAD_STATUS_STARTING
	case guest.StatusRunning:
		return slotv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
	case guest.StatusHibernating:
		return slotv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING
	case guest.StatusHibernated:
		return slotv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED
	case guest.StatusWaking:
		return slotv1.WorkloadStatus_WORKLOAD_STATUS_WAKING
	case guest.StatusStopping:
		return slotv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING
	case guest.StatusStopped:
		return slotv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
	case guest.StatusFailed:
		return slotv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
	}
	return slotv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED
}

func roleToProto(r guest.ContainerRole) slotv1.ContainerRole {
	switch r {
	case guest.RoleMain:
		return slotv1.ContainerRole_CONTAINER_ROLE_MAIN
	case guest.RoleSidecar:
		return slotv1.ContainerRole_CONTAINER_ROLE_SIDECAR
	case guest.RoleInit:
		return slotv1.ContainerRole_CONTAINER_ROLE_INIT
	}
	return slotv1.ContainerRole_CONTAINER_ROLE_UNSPECIFIED
}

func containerStatusToProto(st guest.ContainerStatus) slotv1.ContainerStatus {
	switch st {
	case guest.ContainerWaiting:
		return slotv1.ContainerStatus_CONTAINER_STATUS_WAITING
	case guest.ContainerRunning:
		return slotv1.ContainerStatus_CONTAINER_STATUS_RUNNING
	case guest.ContainerTerminated:
		return slotv1.ContainerStatus_CONTAINER_STATUS_TERMINATED
	}
	return slotv1.ContainerStatus_CONTAINER_STATUS_UNSPECIFIED
}

func flavorFromProto(f *slotv1.Flavor) guest.Flavor {
	if f == nil {
		return guest.Flavor{}
	}
	return guest.Flavor{Name: f.GetName(), VCPUs: int(f.GetVcpus()), MemoryMiB: f.GetMemoryMib()}
}

// StoreFromProto reads a store configuration off the wire.
func storeFromProto(c *slotv1.StoreConfig) store.Config {
	if c == nil {
		return store.Config{}
	}
	return store.Config{
		Endpoint: c.GetEndpoint(), Bucket: c.GetBucket(), Region: c.GetRegion(), AccessKey: c.GetAccessKey(), SecretKey: c.GetSecretKey(),
		UseTLS: c.GetUseTls(), PathStyle: c.GetPathStyle(),
		SnapshotsPrefix: c.GetSnapshotsPrefix(), VolumesPrefix: c.GetVolumesPrefix(), TemplatesPrefix: c.GetTemplatesPrefix(),
	}.WithDefaults()
}

func volumesFromProto(refs []*slotv1.VolumeStoreRef) []guest.VolumeStore {
	out := make([]guest.VolumeStore, 0, len(refs))
	for _, r := range refs {
		out = append(out, guest.VolumeStore{Name: r.GetName(), VolumeKey: r.GetVolumeKey(), Bucket: r.GetBucket(), Prefix: r.GetPrefix()})
	}
	return out
}

var _ = snapshot.ManifestObject
var _ = fmt.Sprintf
