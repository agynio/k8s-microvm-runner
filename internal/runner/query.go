package runner

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"

	"github.com/agynio/k8s-microvm-runner/internal/snapshot"
)

// GetWorkloadLabels returns the labels the start request carried.
func (b *Backend) GetWorkloadLabels(ctx context.Context, req *runnerv1.GetWorkloadLabelsRequest) (*runnerv1.GetWorkloadLabelsResponse, error) {
	w, err := b.get(req.GetWorkloadId())
	if err != nil {
		return nil, err
	}
	return &runnerv1.GetWorkloadLabelsResponse{Labels: w.Labels}, nil
}

// ListWorkloads lists every workload the runner holds -- in a slot or in
// the store -- keyed by the workload_key the platform put in its labels. A
// workload missing from this list is one the platform treats as lost.
func (b *Backend) ListWorkloads(ctx context.Context, _ *runnerv1.ListWorkloadsRequest) (*runnerv1.ListWorkloadsResponse, error) {
	resp := &runnerv1.ListWorkloadsResponse{Workloads: []*runnerv1.WorkloadListItem{}}
	for _, w := range b.index.List() {
		if w.Key == "" {
			continue
		}
		item := &runnerv1.WorkloadListItem{InstanceId: w.ID, WorkloadKey: w.Key, Status: w.Status}
		if w.Status == runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED {
			size := w.SnapshotBytes
			item.SnapshotSizeBytes = &size
		}
		resp.Workloads = append(resp.Workloads, item)
	}
	return resp, nil
}

// FindWorkloadsByLabels matches the index on every given label; without
// all, only running workloads match, as only running Pods do.
func (b *Backend) FindWorkloadsByLabels(ctx context.Context, req *runnerv1.FindWorkloadsByLabelsRequest) (*runnerv1.FindWorkloadsByLabelsResponse, error) {
	resp := &runnerv1.FindWorkloadsByLabelsResponse{TargetIds: []string{}}
	for _, w := range b.index.List() {
		if !req.GetAll() && w.Status != runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
			continue
		}
		match := true
		for k, v := range req.GetLabels() {
			if w.Labels[k] != v {
				match = false
				break
			}
		}
		if match {
			resp.TargetIds = append(resp.TargetIds, w.ID)
		}
	}
	return resp, nil
}

// ListWorkloadsByVolume finds the workloads mounting a volume.
func (b *Backend) ListWorkloadsByVolume(ctx context.Context, req *runnerv1.ListWorkloadsByVolumeRequest) (*runnerv1.ListWorkloadsByVolumeResponse, error) {
	name := strings.TrimSpace(req.GetVolumeName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_name_required")
	}
	resp := &runnerv1.ListWorkloadsByVolumeResponse{TargetIds: []string{}}
	for _, w := range b.index.List() {
		for _, ref := range w.Volumes {
			if ref.Persistent && (ref.InstanceID == name || ref.VolumeKey == name) {
				resp.TargetIds = append(resp.TargetIds, w.ID)
				break
			}
		}
	}
	return resp, nil
}

// ListVolumes lists every persistent volume the runner provisioned, with
// its key, as the k8s-runner lists PVCs.
func (b *Backend) ListVolumes(ctx context.Context, _ *runnerv1.ListVolumesRequest) (*runnerv1.ListVolumesResponse, error) {
	resp := &runnerv1.ListVolumesResponse{Volumes: []*runnerv1.VolumeListItem{}}
	for _, v := range b.index.Volumes() {
		resp.Volumes = append(resp.Volumes, &runnerv1.VolumeListItem{InstanceId: v.InstanceID, VolumeKey: v.Key})
	}
	return resp, nil
}

// RemoveVolume deletes a volume's delta from the store. Removal is
// idempotent: the caller is a reconciler that retries until the volume is
// gone.
func (b *Backend) RemoveVolume(ctx context.Context, req *runnerv1.RemoveVolumeRequest) (*runnerv1.RemoveVolumeResponse, error) {
	name := strings.TrimSpace(req.GetVolumeName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_name_required")
	}
	vol, ok := b.index.Volume(name)
	if !ok {
		if vol, ok = b.index.VolumeByKey(name); !ok {
			return &runnerv1.RemoveVolumeResponse{}, nil
		}
	}
	if b.store != nil {
		if err := b.store.DeletePrefix(ctx, vol.Bucket, snapshot.VolumePrefix(b.store.Config(), vol.Prefix, vol.Key)+"/"); err != nil {
			return nil, status.Errorf(codes.Internal, "remove volume %s: %v", name, err)
		}
	}
	b.index.RemoveVolume(vol.InstanceID)
	return &runnerv1.RemoveVolumeResponse{}, nil
}
