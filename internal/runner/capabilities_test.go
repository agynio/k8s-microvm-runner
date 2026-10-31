package runner

import (
	"testing"

	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"
)

// A workload asking for docker gets the privileged DinD sidecar and reaches
// it over localhost, as on the k8s-runner's kata option; nothing else
// changes, and applying it twice changes nothing more.
func TestApplyCapabilitiesInjectsDocker(t *testing.T) {
	req := &runnerv1.StartWorkloadRequest{
		Main:         &runnerv1.ContainerSpec{Name: "agent", Image: "img:1"},
		Capabilities: []string{"docker"},
	}
	applyCapabilities(req)
	applyCapabilities(req)
	if len(req.Sidecars) != 1 || req.Sidecars[0].GetName() != dockerSidecarName || req.Sidecars[0].GetImage() != dockerImage {
		t.Fatalf("unexpected sidecars %+v", req.Sidecars)
	}
	if req.Sidecars[0].GetRequiredCapabilities()[0] != privilegedCapabilityKey {
		t.Fatal("the DinD sidecar is privileged relative to the guest")
	}
	if len(req.Volumes) != 1 || req.Volumes[0].GetKind() != runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL {
		t.Fatalf("expected one scratch volume for the daemon's data, got %+v", req.Volumes)
	}
	var host string
	for _, e := range req.Main.GetEnv() {
		if e.GetName() == dockerHostEnv {
			host = e.GetValue()
		}
	}
	if host != dockerHostValue {
		t.Fatalf("expected DOCKER_HOST on the main container, got %q", host)
	}
	plain := &runnerv1.StartWorkloadRequest{Main: &runnerv1.ContainerSpec{Name: "agent", Image: "img:1"}}
	applyCapabilities(plain)
	if len(plain.Sidecars) != 0 || len(plain.Volumes) != 0 || len(plain.Main.GetEnv()) != 0 {
		t.Fatal("a workload without the capability is left alone")
	}
}
