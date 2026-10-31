// Package runner is the k8s-microvm-runner's backend behind runner-core: it
// implements the Runner API over a pool of slots, an index of workloads and
// volumes, and the snapshot store. runner-core owns enrollment, the catalog
// report, the gRPC server and state reporting.
package runner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agynio/runner-core/backend"
	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runner-core/gen/agynio/api/runners/v1"

	slotv1 "github.com/agynio/k8s-microvm-runner/internal/.gen/agynio/microvm/slot/v1"
	"github.com/agynio/k8s-microvm-runner/internal/config"
	"github.com/agynio/k8s-microvm-runner/internal/index"
	"github.com/agynio/k8s-microvm-runner/internal/pool"
	"github.com/agynio/k8s-microvm-runner/internal/snapshot"
	"github.com/agynio/k8s-microvm-runner/internal/store"
)

// CapabilityDocker is implemented the way the k8s-runner's kata option
// implements it: a standard privileged DinD container, privileged relative
// to the guest kernel.
const CapabilityDocker = "docker"

// Options build a Backend.
type Options struct {
	Index   *index.Index
	Pool    *pool.Pool
	Store   *store.Client
	Catalog config.Catalog
	Logger  *zap.Logger
}

// Backend implements backend.Backend and backend.Hibernator.
type Backend struct {
	index   *index.Index
	pool    *pool.Pool
	store   *store.Client
	catalog config.Catalog
	log     *zap.Logger

	mu       sync.Mutex
	watching map[string]context.CancelFunc
}

var (
	_ backend.Backend    = (*Backend)(nil)
	_ backend.Hibernator = (*Backend)(nil)
)

// New builds the backend.
func New(opts Options) *Backend {
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}
	b := &Backend{index: opts.Index, pool: opts.Pool, store: opts.Store, catalog: opts.Catalog, log: opts.Logger, watching: map[string]context.CancelFunc{}}
	opts.Pool.OnLost = b.slotLost
	return b
}

// storeConfig is what travels to a supervisor.
func (b *Backend) storeConfig() store.Config {
	if b.store == nil {
		return store.Config{}
	}
	return b.store.Config()
}

func storeToProto(cfg store.Config) *slotv1.StoreConfig {
	if !cfg.Configured() {
		return nil
	}
	return &slotv1.StoreConfig{
		Endpoint: cfg.Endpoint, Bucket: cfg.Bucket, Region: cfg.Region, AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey,
		UseTls: cfg.UseTLS, PathStyle: cfg.PathStyle,
		SnapshotsPrefix: cfg.SnapshotsPrefix, VolumesPrefix: cfg.VolumesPrefix, TemplatesPrefix: cfg.TemplatesPrefix,
	}
}

// Rebuild restores the index from the supervisors and the store after a
// restart.
func (b *Backend) Rebuild(ctx context.Context) error {
	adopted, err := b.pool.Adopt(ctx)
	if err != nil {
		return err
	}
	for _, a := range adopted {
		w := workloadFromView(a.Workload)
		w.Slot = a.Slot.Name
		b.index.Put(w)
		b.watchSlot(a.Slot, w.ID)
	}
	if b.store == nil {
		return nil
	}
	cfg := b.store.Config()
	objects, err := b.store.List(ctx, "", cfg.SnapshotsPrefix+"/")
	if err != nil {
		return fmt.Errorf("list snapshots: %w", err)
	}
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, "/"+snapshot.ManifestObject) {
			continue
		}
		data, err := b.store.GetBytes(ctx, "", obj.Key)
		if err != nil {
			b.log.Warn("read snapshot manifest", zap.String("key", obj.Key), zap.Error(err))
			continue
		}
		man, err := snapshot.ParseManifest(data)
		if err != nil {
			b.log.Warn("parse snapshot manifest", zap.String("key", obj.Key), zap.Error(err))
			continue
		}
		if _, held := b.index.Get(man.WorkloadID); held {
			continue
		}
		b.index.Put(workloadFromManifest(man))
	}
	volumes, err := b.store.List(ctx, "", cfg.VolumesPrefix+"/")
	if err != nil {
		return fmt.Errorf("list volumes: %w", err)
	}
	for _, obj := range volumes {
		if !strings.HasSuffix(obj.Key, "/"+snapshot.VolumeManifestObject) {
			continue
		}
		data, err := b.store.GetBytes(ctx, "", obj.Key)
		if err != nil {
			continue
		}
		var man snapshot.VolumeManifest
		if err := unmarshal(data, &man); err != nil || man.VolumeKey == "" {
			continue
		}
		instance := man.InstanceID
		if instance == "" {
			instance = man.VolumeKey
		}
		b.index.PutVolume(&index.Volume{InstanceID: instance, Key: man.VolumeKey, Labels: man.Labels, SizeBytes: man.SizeBytes, StorageClass: man.StorageClass, DeltaBytes: man.DeltaBytes, CreatedAt: man.CreatedAt})
	}
	b.log.Info("index rebuilt", zap.Int("adopted", len(adopted)), zap.Int("workloads", len(b.index.List())), zap.Int("volumes", len(b.index.Volumes())))
	return nil
}

func workloadFromView(v *slotv1.WorkloadView) *index.Workload {
	w := &index.Workload{
		ID: v.GetWorkloadId(), Key: v.GetWorkloadKey(), Labels: v.GetLabels(), Flavor: v.GetFlavor(),
		Status: index.StatusFromSlot(v.GetStatus()), Failure: v.GetFailure(), Containers: v.GetContainers(), Mounts: v.GetMounts(),
		SnapshotBytes: v.GetSnapshotSizeBytes(),
	}
	if w.Labels == nil {
		w.Labels = map[string]string{}
	}
	if v.GetStartedAt() != nil {
		w.CreatedAt = v.GetStartedAt().AsTime()
	}
	for _, c := range v.GetContainers() {
		if c.GetRole() == slotv1.ContainerRole_CONTAINER_ROLE_MAIN {
			w.MainImage = c.GetImage()
		}
	}
	return w
}

func workloadFromManifest(man *snapshot.Manifest) *index.Workload {
	w := &index.Workload{
		ID: man.WorkloadID, Key: man.WorkloadKey, Labels: man.Labels, Flavor: man.Flavor,
		Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, SnapshotBytes: man.TotalBytes, CreatedAt: man.StartedAt,
	}
	if w.Labels == nil {
		w.Labels = map[string]string{}
	}
	for _, c := range man.Containers {
		cv := &slotv1.ContainerView{Name: c.Name, Image: c.Image, ImageDigest: c.ImageDigest}
		switch c.Role {
		case "main":
			cv.Role = slotv1.ContainerRole_CONTAINER_ROLE_MAIN
			w.MainImage = c.Image
		case "sidecar":
			cv.Role = slotv1.ContainerRole_CONTAINER_ROLE_SIDECAR
		case "init":
			cv.Role = slotv1.ContainerRole_CONTAINER_ROLE_INIT
		}
		switch c.Status {
		case "running":
			cv.Status = slotv1.ContainerStatus_CONTAINER_STATUS_RUNNING
		case "terminated":
			cv.Status = slotv1.ContainerStatus_CONTAINER_STATUS_TERMINATED
		default:
			cv.Status = slotv1.ContainerStatus_CONTAINER_STATUS_WAITING
		}
		if c.ExitCode != nil {
			code := *c.ExitCode
			cv.ExitCode = &code
		}
		w.Containers = append(w.Containers, cv)
	}
	for _, v := range man.Volumes {
		w.Volumes = append(w.Volumes, index.VolumeRef{Name: v.Name, Persistent: v.Persistent, InstanceID: v.InstanceID, VolumeKey: v.VolumeKey})
	}
	return w
}

// slotLost is told by the pool that a bound slot is gone: the guest in it
// died with it.
func (b *Backend) slotLost(slot *pool.Slot) {
	w, ok := b.index.BySlot(slot.Name)
	if !ok {
		return
	}
	b.stopWatching(w.ID)
	b.index.Update(w.ID, func(w *index.Workload) {
		if w.Status == runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED {
			return
		}
		w.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
		w.Failure = "slot " + slot.Name + " was lost"
		w.Slot = ""
	})
	b.log.Warn("workload lost with its slot", zap.String("workload", w.ID), zap.String("slot", slot.Name))
}

// watchSlot follows a bound slot's state stream into the index until the
// workload leaves the slot.
func (b *Backend) watchSlot(slot *pool.Slot, workloadID string) {
	ctx, cancel := context.WithCancel(context.Background())
	b.mu.Lock()
	if old, ok := b.watching[workloadID]; ok {
		old()
	}
	b.watching[workloadID] = cancel
	b.mu.Unlock()
	go func() {
		defer func() {
			b.mu.Lock()
			if b.watching[workloadID] != nil {
				delete(b.watching, workloadID)
			}
			b.mu.Unlock()
		}()
		failures := 0
		for ctx.Err() == nil {
			stream, err := slot.Client().WatchState(ctx)
			if err == nil {
				failures = 0
				for {
					msg, err := stream.Recv()
					if err != nil {
						break
					}
					v := msg.GetWorkload()
					if v.GetWorkloadId() != workloadID {
						continue
					}
					b.applyView(slot, v)
					st := v.GetStatus()
					if st == slotv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED || st == slotv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED {
						return
					}
				}
			}
			if ctx.Err() != nil {
				return
			}
			// The stream broke. A supervisor that answers Describe is
			// alive and the stream is simply reopened; one that does not,
			// three times running, is gone with its guest.
			desc, derr := slot.Client().Describe(ctx, 5*time.Second)
			if derr != nil {
				failures++
				if failures >= 3 {
					b.log.Warn("slot stopped answering; its workload is lost", zap.String("slot", slot.Name), zap.Error(derr))
					b.pool.Retire(context.Background(), slot, "supervisor unreachable")
					return
				}
			} else if desc.GetState() != slotv1.SlotState_SLOT_STATE_BOUND || desc.GetWorkload().GetWorkloadId() != workloadID {
				// The slot no longer holds it: whatever ended it already
				// updated the index through the call that did.
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}()
}

func (b *Backend) stopWatching(workloadID string) {
	b.mu.Lock()
	if cancel, ok := b.watching[workloadID]; ok {
		cancel()
		delete(b.watching, workloadID)
	}
	b.mu.Unlock()
}

// applyView records what a supervisor reports of its workload.
func (b *Backend) applyView(slot *pool.Slot, v *slotv1.WorkloadView) {
	st := index.StatusFromSlot(v.GetStatus())
	b.index.Update(v.GetWorkloadId(), func(w *index.Workload) {
		// A hibernation or stop in flight on the runner's side owns the
		// final status; the supervisor's intermediate views fill in the
		// containers.
		w.Containers = v.GetContainers()
		w.Mounts = v.GetMounts()
		for _, c := range v.GetContainers() {
			if c.GetRole() == slotv1.ContainerRole_CONTAINER_ROLE_MAIN && c.GetImage() != "" {
				w.MainImage = c.GetImage()
			}
		}
		switch st {
		case runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING:
			if w.Status == runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING || w.Status == runnerv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED {
				return
			}
			w.Status = st
			w.Slot = slot.Name
		case runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
			w.Status = st
			w.Failure = v.GetFailure()
		case runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING, runnerv1.WorkloadStatus_WORKLOAD_STATUS_WAKING:
			w.Status = st
		case runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED:
			w.Status = st
			w.SnapshotBytes = v.GetSnapshotSizeBytes()
			w.Slot = ""
		}
	})
}

func (b *Backend) slotOf(w *index.Workload) (*pool.Slot, error) {
	if w.Slot == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "workload is %s", statusName(w.Status))
	}
	slot, ok := b.pool.Get(w.Slot)
	if !ok || slot.Client() == nil {
		return nil, status.Error(codes.Unavailable, "the slot hosting the workload is gone")
	}
	return slot, nil
}

func statusName(st runnerv1.WorkloadStatus) string {
	return strings.ToLower(strings.TrimPrefix(st.String(), "WORKLOAD_STATUS_"))
}

func (b *Backend) get(workloadID string) (*index.Workload, error) {
	workloadID = strings.TrimSpace(workloadID)
	if workloadID == "" {
		return nil, status.Error(codes.InvalidArgument, "workload_id_required")
	}
	w, ok := b.index.Get(workloadID)
	if !ok {
		return nil, status.Error(codes.NotFound, "workload_not_found")
	}
	return w, nil
}

// WatchWorkloadState hands every transition the index sees to the observer:
// running, the sleeping statuses with the snapshot size, failed, and gone.
func (b *Backend) WatchWorkloadState(ctx context.Context, observer backend.Observer) error {
	ch := b.index.Subscribe()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case o := <-ch:
			if o.Gone {
				observer.Forget(o.WorkloadID)
				continue
			}
			switch o.Status {
			case runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
				runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING,
				runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED,
				runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING,
				runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
				observer.Observe(ctx, backend.Observation{WorkloadID: o.WorkloadID, Status: o.Status, SnapshotSizeBytes: o.SnapshotSizeBytes})
			}
		}
	}
}

func notImplemented(err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}
