package runner

import (
	"strings"

	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"
)

// The docker capability is implemented the way the k8s-runner's kata option
// implements it: a standard privileged DinD sidecar next to the main
// container, privileged relative to the guest kernel rather than the host,
// with DOCKER_HOST pointing the main container at it over localhost.
const (
	dockerSidecarName       = "docker-daemon"
	dockerImage             = "docker:27-dind"
	dockerDataVolume        = "docker-data"
	dockerDataMountPath     = "/var/lib/docker"
	dockerHostEnv           = "DOCKER_HOST"
	dockerHostValue         = "tcp://localhost:2375"
	dockerTLSCertDirEnv     = "DOCKER_TLS_CERTDIR"
	privilegedCapabilityKey = "privileged"
)

// applyCapabilities rewrites the spec for the capabilities it names, as the
// k8s-runner rewrites a Pod. It is idempotent: a spec that already carries
// the sidecar is left alone.
func applyCapabilities(req *runnerv1.StartWorkloadRequest) {
	if !wantsDocker(req) {
		return
	}
	for _, c := range req.GetSidecars() {
		if c.GetName() == dockerSidecarName {
			return
		}
	}
	for _, v := range req.GetVolumes() {
		if v.GetName() == dockerDataVolume {
			return
		}
	}
	req.Volumes = append(req.Volumes, &runnerv1.VolumeSpec{Name: dockerDataVolume, Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL})
	req.Sidecars = append(req.Sidecars, &runnerv1.ContainerSpec{
		Name:                 dockerSidecarName,
		Image:                dockerImage,
		Env:                  []*runnerv1.EnvVar{{Name: dockerTLSCertDirEnv, Value: ""}},
		Mounts:               []*runnerv1.VolumeMount{{Volume: dockerDataVolume, MountPath: dockerDataMountPath}},
		RequiredCapabilities: []string{privilegedCapabilityKey},
	})
	req.Main.Env = upsertEnv(req.Main.GetEnv(), dockerHostEnv, dockerHostValue)
}

func wantsDocker(req *runnerv1.StartWorkloadRequest) bool {
	for _, capability := range req.GetCapabilities() {
		if strings.EqualFold(strings.TrimSpace(capability), CapabilityDocker) {
			return true
		}
	}
	return false
}

func upsertEnv(env []*runnerv1.EnvVar, name, value string) []*runnerv1.EnvVar {
	for _, e := range env {
		if e.GetName() == name {
			e.Value = value
			return env
		}
	}
	return append(env, &runnerv1.EnvVar{Name: name, Value: value})
}
