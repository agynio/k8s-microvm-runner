package index

import (
	"testing"

	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/runner-core/gen/agynio/api/runners/v1"
)

// Every transition reaches the subscribers, with the snapshot size on a
// hibernated one and a forget when a workload leaves.
func TestTransitionsReachSubscribers(t *testing.T) {
	i := New()
	ch := i.Subscribe()
	i.Put(&Workload{ID: "w1", Key: "k1", Status: runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING})
	i.Update("w1", func(w *Workload) { w.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING })
	i.Update("w1", func(w *Workload) {
		w.Status = runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED
		w.SnapshotBytes = 42
	})
	i.Remove("w1")

	want := []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED,
	}
	for _, status := range want {
		o := <-ch
		if o.WorkloadID != "w1" || o.Status != status || o.Gone {
			t.Fatalf("unexpected observation %+v, want %v", o, status)
		}
		if status == runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED && (o.SnapshotSizeBytes == nil || *o.SnapshotSizeBytes != 42) {
			t.Fatalf("expected the snapshot size on the hibernated observation, got %+v", o)
		}
	}
	if o := <-ch; !o.Gone || o.WorkloadID != "w1" {
		t.Fatalf("expected a gone observation, got %+v", o)
	}
	if _, ok := i.Get("w1"); ok {
		t.Fatal("expected the workload to be gone")
	}
}

// Copies out, never pointers in: a caller cannot change the index behind
// its back.
func TestGetReturnsACopy(t *testing.T) {
	i := New()
	i.Put(&Workload{ID: "w1", Labels: map[string]string{"a": "b"}, Slot: "slot-1"})
	w, _ := i.Get("w1")
	w.Labels["a"] = "changed"
	w.Slot = "other"
	again, _ := i.Get("w1")
	if again.Labels["a"] != "b" || again.Slot != "slot-1" {
		t.Fatal("the index was changed through a copy")
	}
	if found, ok := i.BySlot("slot-1"); !ok || found.ID != "w1" {
		t.Fatal("BySlot did not find the workload")
	}
}

func TestVolumesAreIndexedByInstanceAndKey(t *testing.T) {
	i := New()
	i.PutVolume(&Volume{InstanceID: "ws-thread", Key: "vk-1", SizeBytes: 10 << 30})
	if v, ok := i.Volume("ws-thread"); !ok || v.Key != "vk-1" {
		t.Fatal("volume not found by instance id")
	}
	if v, ok := i.VolumeByKey("vk-1"); !ok || v.InstanceID != "ws-thread" {
		t.Fatal("volume not found by key")
	}
	i.RemoveVolume("ws-thread")
	if len(i.Volumes()) != 0 {
		t.Fatal("volume not removed")
	}
}

func TestStatusMapping(t *testing.T) {
	if ReportedStatus(runnerv1.WorkloadStatus_WORKLOAD_STATUS_WAKING) != runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING {
		t.Fatal("waking must map to waking")
	}
	if ReportedStatus(runnerv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING) != runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING {
		t.Fatal("hibernating must map to hibernating")
	}
}
