package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/agynio/runner-core/backend"
	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"

	slotv1 "github.com/agynio/k8s-microvm-runner/internal/.gen/agynio/microvm/slot/v1"
	"github.com/agynio/k8s-microvm-runner/internal/slotclient"
)

// execTargetPrefix is how the Terminal Proxy names an exec target today:
// "workload-<instance id>", the k8s-runner's Pod name for the workload. This
// runner's instance id is the workload id, so the target resolves with or
// without it, and the proxy works against this runner unchanged.
const execTargetPrefix = "workload-"

// PrepareExec readies a command to run in the workload's main container.
// The target id is the instance id a start returned, bare or in the
// Terminal Proxy's "workload-<instance id>" form.
func (b *Backend) PrepareExec(ctx context.Context, targetID string, command []string, tty bool) (backend.Execution, error) {
	w, err := b.get(strings.TrimPrefix(strings.TrimSpace(targetID), execTargetPrefix))
	if err != nil {
		return nil, err
	}
	slot, err := b.slotOf(w)
	if err != nil {
		return nil, err
	}
	return &execution{client: slot.Client(), workloadID: w.ID, command: command, tty: tty}, nil
}

type execution struct {
	client     slotclient.API
	workloadID string
	command    []string
	tty        bool
}

// Run streams the command through the slot. A non-zero exit comes back as
// an error exposing ExitStatus, which runner-core reads.
func (e *execution) Run(ctx context.Context, streams backend.ExecStreams) error {
	var resize chan [2]uint32
	if streams.Resize != nil {
		resize = make(chan [2]uint32, 1)
		go func() {
			defer close(resize)
			for size := range streams.Resize {
				select {
				case resize <- [2]uint32{uint32(size.Height), uint32(size.Width)}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	return e.client.Exec(ctx, e.workloadID, "", e.command, e.tty, slotclient.ExecStreams{Stdin: streams.Stdin, Stdout: streams.Stdout, Stderr: streams.Stderr, Resize: resize})
}

// PutArchive extracts a tar archive at a path inside the main container.
func (b *Backend) PutArchive(ctx context.Context, req *runnerv1.PutArchiveRequest) (*runnerv1.PutArchiveResponse, error) {
	path := strings.TrimSpace(req.GetPath())
	if strings.TrimSpace(req.GetWorkloadId()) == "" || path == "" {
		return nil, status.Error(codes.InvalidArgument, "workload_id_and_path_required")
	}
	w, err := b.get(req.GetWorkloadId())
	if err != nil {
		return nil, err
	}
	slot, err := b.slotOf(w)
	if err != nil {
		return nil, err
	}
	var stdout, stderr bytes.Buffer
	err = slot.Client().Exec(ctx, w.ID, "", []string{"tar", "-xf", "-", "-C", path}, false, slotclient.ExecStreams{Stdin: bytes.NewReader(req.GetTarPayload()), Stdout: &stdout, Stderr: &stderr})
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = strings.TrimSpace(stdout.String())
		}
		if message != "" {
			return nil, status.Errorf(codes.Internal, "put archive failed: %s", message)
		}
		return nil, status.Errorf(codes.Internal, "put archive failed: %v", err)
	}
	return &runnerv1.PutArchiveResponse{}, nil
}

// OpenWorkloadLogs streams what the slot captured of a container.
func (b *Backend) OpenWorkloadLogs(ctx context.Context, req *runnerv1.StreamWorkloadLogsRequest) (io.ReadCloser, error) {
	w, err := b.get(req.GetWorkloadId())
	if err != nil {
		return nil, err
	}
	container := strings.TrimSpace(req.GetContainerName())
	known := false
	for _, c := range w.Containers {
		if c.GetName() == container {
			known = true
		}
	}
	if !known {
		return nil, status.Error(codes.NotFound, "container_not_found")
	}
	slot, err := b.slotOf(w)
	if err != nil {
		return nil, err
	}
	logs := &slotv1.LogsRequest{WorkloadId: w.ID, Container: container, Follow: req.GetFollow(), Timestamps: req.GetTimestamps()}
	if req.GetTailLines() > 0 {
		logs.TailLines = int64(req.GetTailLines())
	} else if req.GetTail() > 0 {
		logs.TailLines = int64(req.GetTail())
	}
	if req.GetSinceTime() != nil {
		logs.Since = req.GetSinceTime()
	} else if req.GetSince() > 0 {
		logs.Since = timestamppb.New(time.Unix(req.GetSince(), 0))
	}
	rc, err := slot.Client().Logs(ctx, logs)
	if err != nil {
		return nil, err
	}
	return &logReadCloser{rc: rc}, nil
}

// logReadCloser turns the stream's end into io.EOF for runner-core.
type logReadCloser struct {
	rc io.ReadCloser
}

func (l *logReadCloser) Read(p []byte) (int, error) {
	n, err := l.rc.Read(p)
	if err != nil {
		if errors_Is_EOF(err) {
			return n, io.EOF
		}
		return n, err
	}
	return n, nil
}

func (l *logReadCloser) Close() error { return l.rc.Close() }

func errors_Is_EOF(err error) bool {
	if err == io.EOF {
		return true
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.OK
}

// WorkloadExists is how runner-core tells a container that finished from a
// workload that was deleted mid-stream.
func (b *Backend) WorkloadExists(ctx context.Context, workloadID string) (bool, error) {
	_, ok := b.index.Get(strings.TrimSpace(workloadID))
	return ok, nil
}

// StreamEvents streams the runner's own events: every workload transition
// the index records, as JSON, filtered by workload id when asked.
func (b *Backend) StreamEvents(req *runnerv1.StreamEventsRequest, stream runnerv1.RunnerService_StreamEventsServer) error {
	ctx := stream.Context()
	ch := b.index.Subscribe()
	wanted := map[string]struct{}{}
	for _, f := range req.GetFilters() {
		key := strings.TrimSpace(f.GetKey())
		if key == "involvedObject.name" || key == "workload_id" || key == "metadata.name" {
			for _, v := range f.GetValues() {
				wanted[v] = struct{}{}
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case o := <-ch:
			if len(wanted) > 0 {
				if _, ok := wanted[o.WorkloadID]; !ok {
					continue
				}
			}
			event := map[string]any{"workload_id": o.WorkloadID, "status": o.Status.String(), "gone": o.Gone}
			if o.SnapshotSizeBytes != nil {
				event["snapshot_size_bytes"] = *o.SnapshotSizeBytes
			}
			payload, _ := json.Marshal(event)
			if err := stream.Send(&runnerv1.StreamEventsResponse{Event: &runnerv1.StreamEventsResponse_Data{Data: &runnerv1.RunnerEventData{Json: string(payload), Ts: timestamppb.Now()}}}); err != nil {
				return err
			}
		}
	}
}
