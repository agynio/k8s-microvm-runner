// Package index is the runner's own record of its workloads and volumes:
// what each one is, where it is, and what the runner last observed of it.
// It is rebuilt from the supervisors and the store on restart; there is no
// database.
package index

import (
	"sort"
	"sync"
	"time"

	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runner-core/gen/agynio/api/runners/v1"

	slotv1 "github.com/agynio/k8s-microvm-runner/internal/.gen/agynio/microvm/slot/v1"
)

// Status is a workload's status as the runner reports it, in the Runner
// API's vocabulary.
type Status = runnerv1.WorkloadStatus

// Workload is one workload the runner holds: in a slot, or in the store.
type Workload struct {
	ID     string
	Key    string
	Labels map[string]string
	Flavor string
	// Slot is the slot hosting it; empty while hibernated or after a
	// failure that took the slot with it.
	Slot          string
	Status        Status
	Failure       string
	Containers    []*slotv1.ContainerView
	Mounts        []*slotv1.MountView
	Volumes       []VolumeRef
	MainImage     string
	SnapshotBytes int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
	TouchedAt     time.Time
	// Spec is the start request, kept while the workload lives so a wake
	// can resolve its volumes again.
	Spec *runnerv1.StartWorkloadRequest
}

// VolumeRef is a workload's use of a volume.
type VolumeRef struct {
	Name       string
	Persistent bool
	InstanceID string
	VolumeKey  string
}

// Volume is a persistent volume the runner provisioned: a delta in the
// store under its key.
type Volume struct {
	// InstanceID is the volume's name on this runner, which the platform
	// stores as instance_id: the spec's persistent_name.
	InstanceID   string
	Key          string
	Labels       map[string]string
	SizeBytes    int64
	StorageClass string
	Bucket       string
	Prefix       string
	DeltaBytes   int64
	CreatedAt    time.Time
}

// Observation is one transition the index saw, for the state reporter.
type Observation struct {
	WorkloadID string
	Status     runnersv1.WorkloadStatus
	// SnapshotSizeBytes accompanies a hibernated status.
	SnapshotSizeBytes *int64
	// Gone says the workload left the index; the reporter forgets it.
	Gone bool
}

// Index holds workloads and volumes.
type Index struct {
	mu        sync.RWMutex
	workloads map[string]*Workload
	volumes   map[string]*Volume
	observers []chan Observation
}

// New makes an empty index.
func New() *Index {
	return &Index{workloads: map[string]*Workload{}, volumes: map[string]*Volume{}}
}

// Subscribe returns a channel of transitions. The channel is buffered;
// a slow reader loses nothing because the reporter suppresses repeats and
// re-derives state on the next observation.
func (i *Index) Subscribe() <-chan Observation {
	ch := make(chan Observation, 256)
	i.mu.Lock()
	i.observers = append(i.observers, ch)
	i.mu.Unlock()
	return ch
}

func (i *Index) notify(o Observation) {
	for _, ch := range i.observers {
		select {
		case ch <- o:
		default:
		}
	}
}

// Put records a workload as it is now.
func (i *Index) Put(w *Workload) {
	i.mu.Lock()
	defer i.mu.Unlock()
	w.UpdatedAt = time.Now()
	if w.CreatedAt.IsZero() {
		w.CreatedAt = w.UpdatedAt
	}
	i.workloads[w.ID] = w
	i.notify(observationOf(w))
}

// Update changes a workload under the lock and reports the result.
func (i *Index) Update(id string, fn func(w *Workload)) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	w, ok := i.workloads[id]
	if !ok {
		return false
	}
	fn(w)
	w.UpdatedAt = time.Now()
	i.notify(observationOf(w))
	return true
}

// Remove forgets a workload.
func (i *Index) Remove(id string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if _, ok := i.workloads[id]; !ok {
		return
	}
	delete(i.workloads, id)
	i.notify(Observation{WorkloadID: id, Gone: true})
}

// Get returns a copy of a workload.
func (i *Index) Get(id string) (*Workload, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	w, ok := i.workloads[id]
	if !ok {
		return nil, false
	}
	return copyWorkload(w), true
}

// List returns copies of every workload, by id.
func (i *Index) List() []*Workload {
	i.mu.RLock()
	defer i.mu.RUnlock()
	out := make([]*Workload, 0, len(i.workloads))
	for _, w := range i.workloads {
		out = append(out, copyWorkload(w))
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out
}

// BySlot finds the workload a slot hosts.
func (i *Index) BySlot(slot string) (*Workload, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	for _, w := range i.workloads {
		if w.Slot == slot {
			return copyWorkload(w), true
		}
	}
	return nil, false
}

// PutVolume records a provisioned volume.
func (i *Index) PutVolume(v *Volume) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now()
	}
	i.volumes[v.InstanceID] = v
}

// Volume returns a copy of a volume by instance id.
func (i *Index) Volume(instanceID string) (*Volume, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	v, ok := i.volumes[instanceID]
	if !ok {
		return nil, false
	}
	cp := *v
	return &cp, true
}

// VolumeByKey returns a copy of a volume by key.
func (i *Index) VolumeByKey(key string) (*Volume, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	for _, v := range i.volumes {
		if v.Key == key {
			cp := *v
			return &cp, true
		}
	}
	return nil, false
}

// RemoveVolume forgets a volume.
func (i *Index) RemoveVolume(instanceID string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.volumes, instanceID)
}

// Volumes returns copies of every volume, by instance id.
func (i *Index) Volumes() []*Volume {
	i.mu.RLock()
	defer i.mu.RUnlock()
	out := make([]*Volume, 0, len(i.volumes))
	for _, v := range i.volumes {
		cp := *v
		out = append(out, &cp)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].InstanceID < out[b].InstanceID })
	return out
}

func copyWorkload(w *Workload) *Workload {
	cp := *w
	cp.Labels = map[string]string{}
	for k, v := range w.Labels {
		cp.Labels[k] = v
	}
	cp.Containers = append([]*slotv1.ContainerView(nil), w.Containers...)
	cp.Mounts = append([]*slotv1.MountView(nil), w.Mounts...)
	cp.Volumes = append([]VolumeRef(nil), w.Volumes...)
	return &cp
}

// observationOf is what the platform hears of a workload: observed runtime
// state only. Starting, stopping and the intermediate sleeping statuses
// are transitions the platform wrote itself; running, hibernated, waking
// and failed are what the runner saw.
func observationOf(w *Workload) Observation {
	o := Observation{WorkloadID: w.ID, Status: ReportedStatus(w.Status)}
	if w.Status == runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED {
		size := w.SnapshotBytes
		o.SnapshotSizeBytes = &size
	}
	return o
}

// ReportedStatus maps a Runner API status to the Runners service's.
func ReportedStatus(st Status) runnersv1.WorkloadStatus {
	switch st {
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_WAKING:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
	case runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
	}
	return runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED
}

// StatusFromSlot maps a supervisor's status to the Runner API's.
func StatusFromSlot(st slotv1.WorkloadStatus) Status {
	switch st {
	case slotv1.WorkloadStatus_WORKLOAD_STATUS_STARTING:
		return runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING
	case slotv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		return runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
	case slotv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING:
		return runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING
	case slotv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED:
		return runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED
	case slotv1.WorkloadStatus_WORKLOAD_STATUS_WAKING:
		return runnerv1.WorkloadStatus_WORKLOAD_STATUS_WAKING
	case slotv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
		return runnerv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
	case slotv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED:
		return runnerv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED
	case slotv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
		return runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
	}
	return runnerv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED
}
