package runner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"

	slotv1 "github.com/agynio/k8s-microvm-runner/internal/.gen/agynio/microvm/slot/v1"
	"github.com/agynio/k8s-microvm-runner/internal/index"
	"github.com/agynio/k8s-microvm-runner/internal/pool"
	"github.com/agynio/k8s-microvm-runner/internal/snapshot"
	"github.com/agynio/k8s-microvm-runner/internal/store"
)

const (
	workloadKeyLabel = "workload_key"
	volumeKeyLabel   = "volume_key"
	defaultStopGrace = 10
	// slotWaitTimeout bounds how long a start waits for a warming slot.
	slotWaitTimeout = 45 * time.Second
)

// StartWorkload resolves the flavor, provisions the persistent volumes,
// takes an idle slot of that flavor, binds the workload to it and returns
// as soon as the slot accepted it. The boot continues in the slot.
func (b *Backend) StartWorkload(ctx context.Context, req *runnerv1.StartWorkloadRequest) (*runnerv1.StartWorkloadResponse, error) {
	if req == nil || req.GetMain() == nil {
		return nil, status.Error(codes.InvalidArgument, "main_container_required")
	}
	if strings.TrimSpace(req.GetMain().GetImage()) == "" {
		return nil, status.Error(codes.InvalidArgument, "main_container_image_required")
	}
	workloadID := strings.TrimSpace(req.GetWorkloadId())
	if workloadID == "" {
		workloadID = uuid.NewString()
		req.WorkloadId = workloadID
	} else if _, err := uuid.Parse(workloadID); err != nil {
		return nil, status.Error(codes.InvalidArgument, "workload_id_invalid")
	}
	if _, exists := b.index.Get(workloadID); exists {
		return nil, status.Error(codes.AlreadyExists, "workload_exists")
	}
	flavor, ok := b.catalog.FlavorFor(req.GetFlavor())
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown_flavor: %s", strings.TrimSpace(req.GetFlavor()))
	}
	memoryMiB, err := flavor.MemoryMiB()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "flavor %s: %v", flavor.Name, err)
	}
	for _, capability := range req.GetCapabilities() {
		if strings.TrimSpace(capability) != "" && strings.TrimSpace(capability) != CapabilityDocker && !declared(b.catalog.Capabilities, capability) {
			return nil, status.Errorf(codes.InvalidArgument, "unsupported_capability: %s", capability)
		}
	}
	labels := mergeLabels(req)
	workloadKey := labels[workloadKeyLabel]
	applyCapabilities(req)

	refs, volumeRefs, err := b.provisionVolumes(ctx, req, labels)
	if err != nil {
		return nil, err
	}

	slot, err := b.acquireSlot(ctx, flavor.Name, workloadID)
	if err != nil {
		b.log.Warn("start refused: no slot", zap.String("workload", workloadID), zap.String("flavor", flavor.Name), zap.Error(err))
		return nil, slotError(err)
	}
	bind := &slotv1.BindRequest{
		Spec:        req,
		WorkloadKey: workloadKey,
		Flavor:      &slotv1.Flavor{Name: flavor.Name, Vcpus: int32(flavor.VCPUs), MemoryMib: memoryMiB},
		Store:       storeToProto(b.storeConfig()),
		Volumes:     volumeRefs,
	}
	bindCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := slot.Client().Bind(bindCtx, bind)
	if err != nil {
		b.pool.Retire(context.Background(), slot, "bind failed: "+err.Error())
		return nil, status.Errorf(codes.Internal, "bind to slot %s: %v", slot.Name, err)
	}
	w := workloadFromView(resp.GetWorkload())
	w.Slot = slot.Name
	w.Spec = req
	w.Volumes = refs
	w.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING
	w.MainImage = req.GetMain().GetImage()
	w.Labels = labels
	b.index.Put(w)
	b.watchSlot(slot, workloadID)
	b.log.Info("workload bound", zap.String("workload", workloadID), zap.String("slot", slot.Name), zap.String("flavor", flavor.Name))

	sidecars := make([]*runnerv1.SidecarInstance, 0, len(req.GetSidecars()))
	for _, c := range resp.GetWorkload().GetContainers() {
		if c.GetRole() == slotv1.ContainerRole_CONTAINER_ROLE_SIDECAR {
			sidecars = append(sidecars, &runnerv1.SidecarInstance{Name: c.GetName(), Id: workloadID + ":" + c.GetName(), Status: "starting"})
		}
	}
	return &runnerv1.StartWorkloadResponse{
		Id:         workloadID,
		Containers: &runnerv1.WorkloadContainers{Main: workloadID, Sidecars: sidecars},
		Status:     runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
	}, nil
}

// acquireSlot takes an idle slot of the flavor, waiting a little for one
// that is warming: the pool replaces a consumed slot in the background, and
// a burst of starts should queue behind that rather than fail.
func (b *Backend) acquireSlot(ctx context.Context, flavor, workloadID string) (*pool.Slot, error) {
	deadline := time.Now().Add(slotWaitTimeout)
	for {
		slot, err := b.pool.Acquire(flavor, workloadID, "")
		if err == nil {
			return slot, nil
		}
		if !errors.Is(err, pool.ErrNoIdleSlot) || time.Now().After(deadline) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func declared(capabilities []string, capability string) bool {
	for _, c := range capabilities {
		if strings.EqualFold(strings.TrimSpace(c), strings.TrimSpace(capability)) {
			return true
		}
	}
	return false
}

func slotError(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "no_idle_slot: a slot is warming; retry shortly")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, pool.ErrNoFlavor):
		return status.Error(codes.InvalidArgument, "unknown_flavor")
	case errors.Is(err, pool.ErrFlavorFull):
		return status.Error(codes.ResourceExhausted, "runner_full: every slot of this flavor is bound")
	case errors.Is(err, pool.ErrNoIdleSlot):
		return status.Error(codes.Unavailable, "no_idle_slot: a slot is warming; retry shortly")
	}
	return status.Error(codes.Internal, err.Error())
}

func mergeLabels(req *runnerv1.StartWorkloadRequest) map[string]string {
	out := map[string]string{}
	for k, v := range req.GetAdditionalProperties() {
		if name, ok := strings.CutPrefix(k, "label."); ok && name != "" {
			out[name] = v
		}
	}
	for k, v := range req.GetLabels() {
		out[k] = v
	}
	return out
}

// provisionVolumes records every persistent volume the spec names: an
// entry in the index and, when a store is configured, an empty delta
// manifest under its key, so ListVolumes returns it from the first start.
func (b *Backend) provisionVolumes(ctx context.Context, req *runnerv1.StartWorkloadRequest, labels map[string]string) ([]index.VolumeRef, []*slotv1.VolumeStoreRef, error) {
	seen := map[string]struct{}{}
	var refs []index.VolumeRef
	var storeRefs []*slotv1.VolumeStoreRef
	for _, v := range req.GetVolumes() {
		name := strings.TrimSpace(v.GetName())
		if name == "" {
			return nil, nil, status.Error(codes.InvalidArgument, "volume_name_required")
		}
		if _, dup := seen[name]; dup {
			return nil, nil, status.Errorf(codes.InvalidArgument, "duplicate_volume_name: %s", name)
		}
		seen[name] = struct{}{}
		ref := index.VolumeRef{Name: name}
		switch v.GetKind() {
		case runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL:
		case runnerv1.VolumeKind_VOLUME_KIND_NAMED:
			ref.Persistent = true
			ref.InstanceID = strings.TrimSpace(v.GetPersistentName())
			if ref.InstanceID == "" {
				ref.InstanceID = name
			}
			ref.VolumeKey = v.GetLabels()[volumeKeyLabel]
			if ref.VolumeKey == "" {
				ref.VolumeKey = ref.InstanceID
			}
			class, ok := b.catalog.StorageClassFor(v.GetStorageClass())
			if !ok {
				return nil, nil, status.Errorf(codes.InvalidArgument, "unknown_storage_class: %s", v.GetStorageClass())
			}
			vol, known := b.index.Volume(ref.InstanceID)
			if !known {
				vol = &index.Volume{InstanceID: ref.InstanceID, Key: ref.VolumeKey, Labels: volumeLabels(labels, v.GetLabels()), StorageClass: class.Name, Bucket: class.Bucket, Prefix: class.Prefix}
				if size := strings.TrimSpace(v.GetSize()); size != "" {
					q, err := parseQuantity(size)
					if err != nil {
						return nil, nil, status.Errorf(codes.InvalidArgument, "invalid_volume_size: %v", err)
					}
					vol.SizeBytes = q
				}
				if err := b.recordVolume(ctx, vol); err != nil {
					return nil, nil, status.Errorf(codes.Internal, "provision volume %s: %v", name, err)
				}
				b.index.PutVolume(vol)
			}
			storeRefs = append(storeRefs, &slotv1.VolumeStoreRef{Name: name, VolumeKey: vol.Key, Bucket: vol.Bucket, Prefix: vol.Prefix})
		default:
			return nil, nil, status.Error(codes.InvalidArgument, "volume_kind_required")
		}
		refs = append(refs, ref)
	}
	return refs, storeRefs, nil
}

func volumeLabels(workload map[string]string, own map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range workload {
		out[k] = v
	}
	for k, v := range own {
		out[k] = v
	}
	return out
}

// recordVolume writes the volume's manifest to the store: the empty delta
// that says the volume exists.
func (b *Backend) recordVolume(ctx context.Context, vol *index.Volume) error {
	if b.store == nil {
		return nil
	}
	prefix := snapshot.VolumePrefix(b.store.Config(), vol.Prefix, vol.Key)
	key := store.Key(prefix, snapshot.VolumeManifestObject)
	if _, err := b.store.Stat(ctx, vol.Bucket, key); err == nil {
		return nil
	}
	now := time.Now()
	man := snapshot.VolumeManifest{VolumeKey: vol.Key, InstanceID: vol.InstanceID, Labels: vol.Labels, SizeBytes: vol.SizeBytes, StorageClass: vol.StorageClass, CreatedAt: now, UpdatedAt: now}
	data, err := marshal(man)
	if err != nil {
		return err
	}
	_, err = b.store.PutBytes(ctx, vol.Bucket, key, data, "application/json")
	return err
}

func (b *Backend) volumeRefsOf(w *index.Workload) []*slotv1.VolumeStoreRef {
	var refs []*slotv1.VolumeStoreRef
	for _, ref := range w.Volumes {
		if !ref.Persistent {
			continue
		}
		r := &slotv1.VolumeStoreRef{Name: ref.Name, VolumeKey: ref.VolumeKey}
		if vol, ok := b.index.Volume(ref.InstanceID); ok {
			r.Bucket, r.Prefix = vol.Bucket, vol.Prefix
			if r.VolumeKey == "" {
				r.VolumeKey = vol.Key
			}
		}
		refs = append(refs, r)
	}
	return refs
}

// StopWorkload stops a running workload, discarding its writable layers,
// scratch volumes and memory; on a hibernated one it deletes the snapshot.
// Persistent volumes are untouched either way.
func (b *Backend) StopWorkload(ctx context.Context, req *runnerv1.StopWorkloadRequest) (*runnerv1.StopWorkloadResponse, error) {
	w, err := b.get(req.GetWorkloadId())
	if err != nil {
		return nil, err
	}
	grace := int64(req.GetTimeoutSec())
	if grace <= 0 {
		grace = defaultStopGrace
	}
	if err := b.stop(ctx, w, grace); err != nil {
		return nil, err
	}
	return &runnerv1.StopWorkloadResponse{}, nil
}

func (b *Backend) stop(ctx context.Context, w *index.Workload, grace int64) error {
	b.stopWatching(w.ID)
	b.index.Update(w.ID, func(w *index.Workload) { w.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED })
	switch {
	case w.Slot != "":
		slot, ok := b.pool.Get(w.Slot)
		if ok && slot.Client() != nil {
			err := slot.Client().Stop(ctx, &slotv1.StopRequest{WorkloadId: w.ID, TimeoutSec: grace, Store: storeToProto(b.storeConfig()), Volumes: b.volumeRefsOf(w)})
			if err != nil && status.Code(err) != codes.NotFound {
				b.log.Warn("stop in slot failed; retiring the slot", zap.String("workload", w.ID), zap.String("slot", slot.Name), zap.Error(err))
				b.pool.Retire(context.Background(), slot, "stop failed")
			} else {
				b.pool.Release(context.Background(), slot, "")
			}
		}
	case w.Status == runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, w.SnapshotBytes > 0:
		if err := b.discardSnapshot(ctx, w); err != nil {
			b.index.Update(w.ID, func(x *index.Workload) { x.Status = w.Status })
			return status.Errorf(codes.Internal, "discard snapshot: %v", err)
		}
	}
	b.index.Remove(w.ID)
	b.log.Info("workload stopped", zap.String("workload", w.ID))
	return nil
}

// discardSnapshot deletes a hibernated workload's objects from the store
// and tells the slots still caching it to forget it.
func (b *Backend) discardSnapshot(ctx context.Context, w *index.Workload) error {
	if b.store == nil || w.Key == "" {
		return nil
	}
	if err := b.store.DeletePrefix(ctx, "", snapshot.Prefix(b.store.Config(), w.Key)+"/"); err != nil {
		return err
	}
	for _, slot := range b.pool.Slots() {
		for _, key := range slot.Cached {
			if key == w.Key && slot.Client() != nil {
				_ = slot.Client().DiscardCache(ctx, w.Key)
			}
		}
	}
	return nil
}

// RemoveWorkload is a stop followed, when asked, by the volumes' removal.
func (b *Backend) RemoveWorkload(ctx context.Context, req *runnerv1.RemoveWorkloadRequest) (*runnerv1.RemoveWorkloadResponse, error) {
	w, err := b.get(req.GetWorkloadId())
	if err != nil {
		return nil, err
	}
	grace := int64(defaultStopGrace)
	if req.GetForce() {
		grace = 0
	}
	if err := b.stop(ctx, w, grace); err != nil {
		return nil, err
	}
	if req.GetRemoveVolumes() {
		for _, ref := range w.Volumes {
			if !ref.Persistent {
				continue
			}
			if _, err := b.RemoveVolume(ctx, &runnerv1.RemoveVolumeRequest{VolumeName: ref.InstanceID}); err != nil {
				return nil, err
			}
		}
	}
	return &runnerv1.RemoveWorkloadResponse{}, nil
}

// InspectWorkload answers from the index: every container of the spec from
// the moment the start was accepted, with what the supervisor last reported
// of each.
func (b *Backend) InspectWorkload(ctx context.Context, req *runnerv1.InspectWorkloadRequest) (*runnerv1.InspectWorkloadResponse, error) {
	w, err := b.get(req.GetWorkloadId())
	if err != nil {
		return nil, err
	}
	resp := &runnerv1.InspectWorkloadResponse{
		Id: w.ID, Name: w.ID, Image: w.MainImage, ConfigImage: w.MainImage, ConfigLabels: w.Labels,
		StateStatus: statusName(w.Status), StateRunning: w.Status == runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		Containers: containersToProto(w), Status: w.Status,
	}
	for _, mnt := range w.Mounts {
		resp.Mounts = append(resp.Mounts, &runnerv1.TargetMount{Type: mnt.GetType(), Source: mnt.GetVolume(), Destination: mnt.GetDestination(), ReadOnly: mnt.GetReadOnly()})
	}
	if w.Status == runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED {
		size := w.SnapshotBytes
		resp.SnapshotSizeBytes = &size
	}
	return resp, nil
}

func containersToProto(w *index.Workload) []*runnerv1.WorkloadContainer {
	out := make([]*runnerv1.WorkloadContainer, 0, len(w.Containers))
	for _, c := range w.Containers {
		wc := &runnerv1.WorkloadContainer{
			ContainerId: "vm://" + w.ID + "/" + c.GetName(), Name: c.GetName(), Image: c.GetImage(),
			RestartCount: c.GetRestartCount(), StartedAt: c.GetStartedAt(), FinishedAt: c.GetFinishedAt(),
		}
		switch c.GetRole() {
		case slotv1.ContainerRole_CONTAINER_ROLE_MAIN:
			wc.Role = runnerv1.ContainerRole_CONTAINER_ROLE_MAIN
		case slotv1.ContainerRole_CONTAINER_ROLE_SIDECAR:
			wc.Role = runnerv1.ContainerRole_CONTAINER_ROLE_SIDECAR
		case slotv1.ContainerRole_CONTAINER_ROLE_INIT:
			wc.Role = runnerv1.ContainerRole_CONTAINER_ROLE_INIT
		}
		switch c.GetStatus() {
		case slotv1.ContainerStatus_CONTAINER_STATUS_RUNNING:
			wc.Status = runnerv1.ContainerStatus_CONTAINER_STATUS_RUNNING
		case slotv1.ContainerStatus_CONTAINER_STATUS_TERMINATED:
			wc.Status = runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED
		default:
			wc.Status = runnerv1.ContainerStatus_CONTAINER_STATUS_WAITING
		}
		if c.ExitCode != nil {
			code := c.GetExitCode()
			wc.ExitCode = &code
		}
		if reason := c.GetReason(); reason != "" {
			wc.Reason = &reason
		}
		if message := c.GetMessage(); message != "" {
			wc.Message = &message
		}
		if w.Status == runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED && wc.Role == runnerv1.ContainerRole_CONTAINER_ROLE_MAIN && w.Failure != "" {
			// The failure is the guest's, not one container's; it is put
			// on the main container, where the platform reads a failure.
			if wc.Status != runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED {
				wc.Status = runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED
				code := int32(-1)
				wc.ExitCode = &code
			}
			msg := w.Failure
			wc.Message = &msg
			reason := "GuestFailed"
			wc.Reason = &reason
		}
		out = append(out, wc)
	}
	return out
}

// TouchWorkload records the keepalive.
func (b *Backend) TouchWorkload(ctx context.Context, req *runnerv1.TouchWorkloadRequest) (*runnerv1.TouchWorkloadResponse, error) {
	if _, err := b.get(req.GetWorkloadId()); err != nil {
		return nil, err
	}
	b.index.Update(strings.TrimSpace(req.GetWorkloadId()), func(w *index.Workload) { w.TouchedAt = time.Now() })
	return &runnerv1.TouchWorkloadResponse{}, nil
}

// HibernateWorkload pauses the workload's guest, persists it and frees its
// slot. It returns once the snapshot is durable, with the status reported
// as the supervisor observed it.
func (b *Backend) HibernateWorkload(ctx context.Context, req *runnerv1.HibernateWorkloadRequest) (*runnerv1.HibernateWorkloadResponse, error) {
	w, err := b.get(req.GetWorkloadId())
	if err != nil {
		return nil, err
	}
	if w.Status == runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED {
		return &runnerv1.HibernateWorkloadResponse{}, nil
	}
	if w.Status != runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		return nil, status.Errorf(codes.FailedPrecondition, "workload is %s", statusName(w.Status))
	}
	if b.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "no snapshot store is configured")
	}
	slot, err := b.slotOf(w)
	if err != nil {
		return nil, err
	}
	if w.Key == "" {
		return nil, status.Error(codes.FailedPrecondition, "workload has no workload_key to store its snapshot under")
	}
	b.index.Update(w.ID, func(w *index.Workload) { w.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING })
	t0 := time.Now()
	size, err := slot.Client().Hibernate(ctx, &slotv1.HibernateRequest{WorkloadId: w.ID, Store: storeToProto(b.storeConfig()), Volumes: b.volumeRefsOf(w)})
	if err != nil {
		b.index.Update(w.ID, func(x *index.Workload) {
			if x.Status == runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING {
				x.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
			}
		})
		return nil, status.Errorf(codes.Internal, "hibernate in slot %s: %v", slot.Name, err)
	}
	b.stopWatching(w.ID)
	b.index.Update(w.ID, func(x *index.Workload) {
		x.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED
		x.SnapshotBytes = size
		x.Slot = ""
	})
	b.pool.Release(context.Background(), slot, w.Key)
	b.log.Info("workload hibernated", zap.String("workload", w.ID), zap.String("slot", slot.Name), zap.Int64("snapshot_bytes", size), zap.Duration("took", time.Since(t0).Round(time.Millisecond)))
	return &runnerv1.HibernateWorkloadResponse{}, nil
}

// WakeWorkload restores a hibernated workload into a slot, preferring the
// one still caching its snapshot, and returns once it is running.
func (b *Backend) WakeWorkload(ctx context.Context, req *runnerv1.WakeWorkloadRequest) (*runnerv1.WakeWorkloadResponse, error) {
	w, err := b.get(req.GetWorkloadId())
	if err != nil {
		return nil, err
	}
	if w.Status == runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		return &runnerv1.WakeWorkloadResponse{}, nil
	}
	if w.Status != runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED {
		return nil, status.Errorf(codes.FailedPrecondition, "workload is %s", statusName(w.Status))
	}
	if b.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "no snapshot store is configured")
	}
	flavor, ok := b.catalog.FlavorFor(w.Flavor)
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "flavor %s is no longer in the catalog", w.Flavor)
	}
	memoryMiB, _ := flavor.MemoryMiB()
	prefer := b.pool.Caching(flavor.Name, w.Key)
	slot, err := b.pool.Acquire(flavor.Name, w.ID, prefer)
	if err != nil {
		return nil, slotError(err)
	}
	b.index.Update(w.ID, func(x *index.Workload) { x.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_WAKING })
	t0 := time.Now()
	view, err := slot.Client().Wake(ctx, &slotv1.WakeRequest{
		WorkloadId: w.ID, WorkloadKey: w.Key,
		Flavor:               &slotv1.Flavor{Name: flavor.Name, Vcpus: int32(flavor.VCPUs), MemoryMib: memoryMiB},
		Store:                storeToProto(b.storeConfig()),
		Volumes:              b.volumeRefsOf(w),
		ImagePullCredentials: req.GetImagePullCredentials(),
	})
	if err != nil {
		b.index.Update(w.ID, func(x *index.Workload) { x.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED })
		if status.Code(err) == codes.FailedPrecondition {
			b.pool.Release(context.Background(), slot, "")
		} else {
			b.pool.Retire(context.Background(), slot, "wake failed: "+err.Error())
		}
		return nil, status.Errorf(codes.Internal, "wake in slot %s: %v", slot.Name, err)
	}
	b.index.Update(w.ID, func(x *index.Workload) {
		x.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
		x.Slot = slot.Name
		x.SnapshotBytes = 0
		x.Containers = view.GetContainers()
		x.Mounts = view.GetMounts()
	})
	b.watchSlot(slot, w.ID)
	b.log.Info("workload woken", zap.String("workload", w.ID), zap.String("slot", slot.Name), zap.Bool("cached", slot.Name == prefer && prefer != ""), zap.Duration("took", time.Since(t0).Round(time.Millisecond)))
	return &runnerv1.WakeWorkloadResponse{}, nil
}

func parseQuantity(s string) (int64, error) {
	q, err := resourceParse(s)
	if err != nil {
		return 0, err
	}
	return q, nil
}

var _ = fmt.Sprintf
