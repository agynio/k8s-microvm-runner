package runner

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes/fake"

	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"

	slotv1 "github.com/agynio/k8s-microvm-runner/internal/.gen/agynio/microvm/slot/v1"
	"github.com/agynio/k8s-microvm-runner/internal/config"
	"github.com/agynio/k8s-microvm-runner/internal/index"
	"github.com/agynio/k8s-microvm-runner/internal/pool"
	"github.com/agynio/k8s-microvm-runner/internal/snapshot"
)

func newBackend(t *testing.T) (*Backend, *index.Index) {
	t.Helper()
	idx := index.New()
	p := pool.New(pool.Config{Namespace: "ns", Flavors: map[string]pool.FlavorSpec{"small": {VCPUs: 1, MemoryMiB: 2048, Warm: 0}}}, fake.NewSimpleClientset(), nil, zap.NewNop())
	catalog := config.Catalog{Flavors: []config.FlavorEntry{{Name: "small", Default: true, VCPUs: 1, Memory: "2Gi"}}, StorageClasses: []config.StorageClassEntry{{Name: "standard", Default: true}}}
	return New(Options{Index: idx, Pool: p, Catalog: catalog, Logger: zap.NewNop()}), idx
}

// Inspect answers from the index in the Runner API's vocabulary: the typed
// status, state_status and state_running for callers predating it, every
// container with its role and state, and the snapshot size while hibernated.
func TestInspectMapsTheIndex(t *testing.T) {
	b, idx := newBackend(t)
	code := int32(0)
	idx.Put(&index.Workload{
		ID: "w1", Key: "k1", Labels: map[string]string{"workload_key": "k1"}, MainImage: "img:1",
		Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		Containers: []*slotv1.ContainerView{
			{Name: "init-1", Role: slotv1.ContainerRole_CONTAINER_ROLE_INIT, Image: "init:1", Status: slotv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, ExitCode: &code},
			{Name: "agent", Role: slotv1.ContainerRole_CONTAINER_ROLE_MAIN, Image: "img:1", Status: slotv1.ContainerStatus_CONTAINER_STATUS_WAITING},
			{Name: "ziti", Role: slotv1.ContainerRole_CONTAINER_ROLE_SIDECAR, Image: "ziti:1", Status: slotv1.ContainerStatus_CONTAINER_STATUS_WAITING},
		},
		Mounts: []*slotv1.MountView{{Volume: "agyn", Type: "share", Destination: "/agyn"}},
	})
	resp, err := b.InspectWorkload(context.Background(), &runnerv1.InspectWorkloadRequest{WorkloadId: "w1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING || resp.GetStateStatus() != "starting" || resp.GetStateRunning() {
		t.Fatalf("unexpected status fields %+v", resp)
	}
	if len(resp.GetContainers()) != 3 {
		t.Fatalf("expected every container of the spec listed, got %d", len(resp.GetContainers()))
	}
	init := resp.GetContainers()[0]
	if init.GetRole() != runnerv1.ContainerRole_CONTAINER_ROLE_INIT || init.GetStatus() != runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED || init.ExitCode == nil || *init.ExitCode != 0 {
		t.Fatalf("unexpected init container %+v", init)
	}
	if main := resp.GetContainers()[1]; main.GetRole() != runnerv1.ContainerRole_CONTAINER_ROLE_MAIN || main.GetStatus() != runnerv1.ContainerStatus_CONTAINER_STATUS_WAITING {
		t.Fatalf("unexpected main container %+v", main)
	}
	if resp.GetImage() != "img:1" || len(resp.GetMounts()) != 1 || resp.GetMounts()[0].GetDestination() != "/agyn" {
		t.Fatalf("unexpected image or mounts %+v", resp)
	}
	if resp.SnapshotSizeBytes != nil {
		t.Fatal("no snapshot size while starting")
	}

	idx.Update("w1", func(w *index.Workload) {
		w.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED
		w.SnapshotBytes = 99
	})
	resp, _ = b.InspectWorkload(context.Background(), &runnerv1.InspectWorkloadRequest{WorkloadId: "w1"})
	if resp.GetStatus() != runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED || resp.SnapshotSizeBytes == nil || *resp.SnapshotSizeBytes != 99 || resp.GetStateStatus() != "hibernated" {
		t.Fatalf("unexpected hibernated inspect %+v", resp)
	}

	idx.Update("w1", func(w *index.Workload) {
		w.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
		w.Failure = "guest exited"
	})
	resp, _ = b.InspectWorkload(context.Background(), &runnerv1.InspectWorkloadRequest{WorkloadId: "w1"})
	main := resp.GetContainers()[1]
	if main.GetStatus() != runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED || main.GetMessage() != "guest exited" || main.ExitCode == nil {
		t.Fatalf("expected the failure on the main container, got %+v", main)
	}
	if _, err := b.InspectWorkload(context.Background(), &runnerv1.InspectWorkloadRequest{WorkloadId: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

// ListWorkloads carries every workload the runner holds, running or
// hibernated, keyed by workload_key; one without a key cannot be matched by
// the platform and is left out, as a Pod without the label is.
func TestListWorkloadsCarriesEveryKeyedWorkload(t *testing.T) {
	b, idx := newBackend(t)
	idx.Put(&index.Workload{ID: "w1", Key: "k1", Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, Slot: "slot-a"})
	idx.Put(&index.Workload{ID: "w2", Key: "k2", Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, SnapshotBytes: 7})
	idx.Put(&index.Workload{ID: "w3", Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING})
	resp, err := b.ListWorkloads(context.Background(), &runnerv1.ListWorkloadsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetWorkloads()) != 2 {
		t.Fatalf("expected two keyed workloads, got %d", len(resp.GetWorkloads()))
	}
	for _, item := range resp.GetWorkloads() {
		switch item.GetInstanceId() {
		case "w1":
			if item.GetWorkloadKey() != "k1" || item.GetStatus() != runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING || item.SnapshotSizeBytes != nil {
				t.Fatalf("unexpected item %+v", item)
			}
		case "w2":
			if item.GetStatus() != runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED || item.GetSnapshotSizeBytes() != 7 {
				t.Fatalf("unexpected item %+v", item)
			}
		default:
			t.Fatalf("unexpected item %+v", item)
		}
	}
	found, _ := b.FindWorkloadsByLabels(context.Background(), &runnerv1.FindWorkloadsByLabelsRequest{Labels: map[string]string{}})
	if len(found.GetTargetIds()) != 2 {
		t.Fatalf("expected only running workloads without all, got %v", found.GetTargetIds())
	}
	found, _ = b.FindWorkloadsByLabels(context.Background(), &runnerv1.FindWorkloadsByLabelsRequest{All: true})
	if len(found.GetTargetIds()) != 3 {
		t.Fatalf("expected every workload with all, got %v", found.GetTargetIds())
	}
}

// A hibernated workload is rebuilt from its manifest: hibernated, with its
// size, labels and the container list it had when paused.
func TestWorkloadFromManifest(t *testing.T) {
	exit := int32(0)
	man := &snapshot.Manifest{
		WorkloadID: "w1", WorkloadKey: "k1", Labels: map[string]string{"workload_key": "k1"}, Flavor: "small", TotalBytes: 123,
		Containers: []snapshot.Container{
			{Name: "init-1", Role: "init", Image: "i", Status: "terminated", ExitCode: &exit},
			{Name: "agent", Role: "main", Image: "a", Status: "running"},
		},
		Volumes: []snapshot.Volume{{Name: "ws", Persistent: true, VolumeKey: "vk", InstanceID: "ws-1"}},
	}
	w := workloadFromManifest(man)
	if w.Status != runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED || w.SnapshotBytes != 123 || w.MainImage != "a" {
		t.Fatalf("unexpected workload %+v", w)
	}
	if len(w.Containers) != 2 || w.Containers[1].GetRole() != slotv1.ContainerRole_CONTAINER_ROLE_MAIN || w.Containers[0].GetStatus() != slotv1.ContainerStatus_CONTAINER_STATUS_TERMINATED {
		t.Fatalf("unexpected containers %+v", w.Containers)
	}
	if len(w.Volumes) != 1 || w.Volumes[0].VolumeKey != "vk" || !w.Volumes[0].Persistent {
		t.Fatalf("unexpected volumes %+v", w.Volumes)
	}
}

// Hibernate and wake refuse what is not in the right state, and a stop of
// a hibernated workload with no store still forgets it.
func TestLifecyclePreconditions(t *testing.T) {
	b, idx := newBackend(t)
	ctx := context.Background()
	idx.Put(&index.Workload{ID: "w1", Key: "k1", Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING, Slot: "slot-a"})
	if _, err := b.HibernateWorkload(ctx, &runnerv1.HibernateWorkloadRequest{WorkloadId: "w1"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for a starting workload, got %v", err)
	}
	if _, err := b.WakeWorkload(ctx, &runnerv1.WakeWorkloadRequest{WorkloadId: "w1"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for a wake of a starting workload, got %v", err)
	}
	idx.Update("w1", func(w *index.Workload) { w.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING })
	if _, err := b.HibernateWorkload(ctx, &runnerv1.HibernateWorkloadRequest{WorkloadId: "w1"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition without a store, got %v", err)
	}
	if _, err := b.WakeWorkload(ctx, &runnerv1.WakeWorkloadRequest{WorkloadId: "w1"}); err != nil {
		t.Fatalf("a wake of a running workload is a no-op, got %v", err)
	}
	idx.Update("w1", func(w *index.Workload) {
		w.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED
		w.Slot = ""
	})
	if _, err := b.StopWorkload(ctx, &runnerv1.StopWorkloadRequest{WorkloadId: "w1"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := idx.Get("w1"); ok {
		t.Fatal("expected the stopped workload forgotten")
	}
	if _, err := b.StopWorkload(ctx, &runnerv1.StopWorkloadRequest{WorkloadId: "w1"}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound for a second stop, got %v", err)
	}
}

// A start that names an unknown flavor fails rather than landing unsized,
// and one that names no idle slot says to retry.
func TestStartRefusesAnUnknownFlavorAndWaitsForASlot(t *testing.T) {
	b, _ := newBackend(t)
	ctx := context.Background()
	req := &runnerv1.StartWorkloadRequest{WorkloadId: "9b8f2a3e-1b1c-4d5e-8f90-0a1b2c3d4e5f", Main: &runnerv1.ContainerSpec{Name: "agent", Image: "img:1"}, Flavor: "huge"}
	if _, err := b.StartWorkload(ctx, req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for an unknown flavor, got %v", err)
	}
	req.Flavor = ""
	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := b.StartWorkload(waitCtx, req); status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expected the start to wait for a slot and give up, got %v", err)
	}
	req.WorkloadId = "not-a-uuid"
	if _, err := b.StartWorkload(ctx, req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for a bad id, got %v", err)
	}
}

// Labels are the request's plus the label.* additional properties, which
// is where the platform puts workload_key.
func TestMergeLabels(t *testing.T) {
	labels := mergeLabels(&runnerv1.StartWorkloadRequest{
		Labels:               map[string]string{"a": "1"},
		AdditionalProperties: map[string]string{"label.workload_key": "k", "other": "x", "label.": "empty"},
	})
	if labels["a"] != "1" || labels["workload_key"] != "k" || len(labels) != 2 {
		t.Fatalf("unexpected labels %v", labels)
	}
}
