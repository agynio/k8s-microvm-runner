package guest

import (
	"os"
	"sort"
	"strings"

	specs "github.com/opencontainers/runtime-spec/specs-go"

	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"

	"github.com/agynio/k8s-microvm-runner/internal/image"
	"github.com/agynio/k8s-microvm-runner/internal/share"
)

var defaultCaps = []string{
	"CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FSETID", "CAP_FOWNER", "CAP_MKNOD", "CAP_NET_RAW",
	"CAP_SETGID", "CAP_SETUID", "CAP_SETFCAP", "CAP_SETPCAP", "CAP_NET_BIND_SERVICE",
	"CAP_SYS_CHROOT", "CAP_KILL", "CAP_AUDIT_WRITE",
}

// buildOCI assembles the container's runtime spec the way a kubelet would
// from a Pod container: image defaults underneath, the workload spec on top.
// Every path in it is a guest path inside the share.
func buildOCI(spec *runnerv1.ContainerSpec, meta *image.Meta, name, hostname string, volumes map[string]string, inline map[string]string) *specs.Spec {
	var args []string
	if ep := strings.TrimSpace(spec.GetEntrypoint()); ep != "" {
		args = append([]string{ep}, spec.GetCmd()...)
	} else {
		args = append(args, meta.Entrypoint...)
		if len(spec.GetCmd()) > 0 {
			args = append(args, spec.GetCmd()...)
		} else {
			args = append(args, meta.Cmd...)
		}
	}
	if len(args) == 0 {
		args = []string{"/bin/sh"}
	}

	env := map[string]string{"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOSTNAME": hostname}
	for _, e := range meta.Env {
		if k, v, ok := strings.Cut(e, "="); ok {
			env[k] = v
		}
	}
	for _, e := range spec.GetEnv() {
		env[e.GetName()] = e.GetValue()
	}
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	envList := make([]string, 0, len(keys))
	for _, k := range keys {
		envList = append(envList, k+"="+env[k])
	}

	cwd := strings.TrimSpace(spec.GetWorkingDir())
	if cwd == "" {
		cwd = meta.WorkingDir
	}
	if cwd == "" {
		cwd = "/"
	}

	caps := append([]string{}, defaultCaps...)
	privileged := false
	for _, c := range spec.GetRequiredCapabilities() {
		c = strings.ToUpper(strings.TrimSpace(c))
		if c == "" {
			continue
		}
		if c == "PRIVILEGED" {
			// Privileged relative to the guest kernel, not the host: the
			// docker capability's DinD sidecar asks for this.
			privileged = true
			continue
		}
		if !strings.HasPrefix(c, "CAP_") {
			c = "CAP_" + c
		}
		caps = append(caps, c)
	}
	if privileged {
		caps = allCaps()
	}

	sandbox := share.GuestSandboxDir()
	mounts := []specs.Mount{
		{Destination: "/proc", Type: "proc", Source: "proc"},
		{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		{Destination: "/dev/pts", Type: "devpts", Source: "devpts", Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"}},
		{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
		{Destination: "/dev/mqueue", Type: "mqueue", Source: "mqueue", Options: []string{"nosuid", "noexec", "nodev"}},
		{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: sysfsOptions(privileged)},
		{Destination: "/sys/fs/cgroup", Type: "cgroup", Source: "cgroup", Options: cgroupOptions(privileged)},
		// Writable, as a kubelet mounts them: the Ziti enrollment rewrites
		// resolv.conf, and every container sees the rewrite.
		{Destination: "/etc/resolv.conf", Type: "bind", Source: sandbox + "/resolv.conf", Options: []string{"rbind"}},
		{Destination: "/etc/hosts", Type: "bind", Source: sandbox + "/hosts", Options: []string{"rbind"}},
		{Destination: "/etc/hostname", Type: "bind", Source: sandbox + "/hostname", Options: []string{"rbind"}},
	}
	for _, vm := range spec.GetMounts() {
		src, ok := volumes[vm.GetVolume()]
		if !ok {
			continue
		}
		opts := []string{"rbind", "rw"}
		if vm.GetReadOnly() {
			opts = []string{"rbind", "ro"}
		}
		mounts = append(mounts, specs.Mount{Destination: vm.GetMountPath(), Type: "bind", Source: src, Options: opts})
	}
	for _, im := range spec.GetInlineFileMounts() {
		src, ok := inline[im.GetPath()]
		if !ok {
			continue
		}
		mounts = append(mounts, specs.Mount{Destination: im.GetPath(), Type: "bind", Source: src, Options: []string{"rbind", "ro"}})
	}

	tunMode := os.FileMode(0o666)
	linux := &specs.Linux{
		// No pid namespace entry: the Kata runtime strips it before handing
		// a spec to the agent, which creates one per container itself. An
		// entry without a path takes a different route through the agent,
		// whose pinned namespace descriptor later fails every exec with
		// ENXIO.
		Namespaces: []specs.LinuxNamespace{
			{Type: specs.IPCNamespace},
			{Type: specs.UTSNamespace},
			{Type: specs.MountNamespace},
		},
		Devices: []specs.LinuxDevice{
			{Path: "/dev/net/tun", Type: "c", Major: 10, Minor: 200, FileMode: &tunMode},
		},
		CgroupsPath: "/agyn/" + strings.ToLower(strings.ReplaceAll(name, "_", "-")),
		MaskedPaths: []string{"/proc/kcore", "/proc/keys", "/proc/timer_list", "/sys/firmware"},
	}
	if privileged {
		linux.MaskedPaths = nil
		linux.ReadonlyPaths = nil
	}
	return &specs.Spec{
		Version: specs.Version,
		Process: &specs.Process{
			Terminal: false,
			User:     specs.User{UID: meta.UID, GID: meta.GID},
			Args:     args,
			Env:      envList,
			Cwd:      cwd,
			Capabilities: &specs.LinuxCapabilities{
				Bounding: caps, Effective: caps, Permitted: caps, Inheritable: caps,
			},
			NoNewPrivileges: false,
		},
		Root:     &specs.Root{Path: share.GuestContainerRootfs(name), Readonly: false},
		Hostname: hostname,
		Mounts:   mounts,
		Linux:    linux,
	}
}

// cgroupOptions: a privileged container -- the docker daemon -- manages
// cgroups of its own, so it sees the hierarchy writable, as a privileged
// Pod container does.
func cgroupOptions(privileged bool) []string {
	if privileged {
		return []string{"nosuid", "noexec", "nodev", "relatime", "rw"}
	}
	return []string{"nosuid", "noexec", "nodev", "relatime", "ro"}
}

func sysfsOptions(privileged bool) []string {
	if privileged {
		return []string{"nosuid", "noexec", "nodev", "rw"}
	}
	return []string{"nosuid", "noexec", "nodev", "ro"}
}

func allCaps() []string {
	return []string{
		"CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_DAC_READ_SEARCH", "CAP_FOWNER", "CAP_FSETID", "CAP_KILL",
		"CAP_SETGID", "CAP_SETUID", "CAP_SETPCAP", "CAP_LINUX_IMMUTABLE", "CAP_NET_BIND_SERVICE",
		"CAP_NET_BROADCAST", "CAP_NET_ADMIN", "CAP_NET_RAW", "CAP_IPC_LOCK", "CAP_IPC_OWNER",
		"CAP_SYS_MODULE", "CAP_SYS_RAWIO", "CAP_SYS_CHROOT", "CAP_SYS_PTRACE", "CAP_SYS_PACCT",
		"CAP_SYS_ADMIN", "CAP_SYS_BOOT", "CAP_SYS_NICE", "CAP_SYS_RESOURCE", "CAP_SYS_TIME",
		"CAP_SYS_TTY_CONFIG", "CAP_MKNOD", "CAP_LEASE", "CAP_AUDIT_WRITE", "CAP_AUDIT_CONTROL",
		"CAP_SETFCAP", "CAP_MAC_OVERRIDE", "CAP_MAC_ADMIN", "CAP_SYSLOG", "CAP_WAKE_ALARM",
		"CAP_BLOCK_SUSPEND", "CAP_AUDIT_READ", "CAP_PERFMON", "CAP_BPF", "CAP_CHECKPOINT_RESTORE",
	}
}
