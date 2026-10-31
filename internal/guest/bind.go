package guest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"

	runnerv1 "github.com/agynio/runner-core/gen/agynio/api/runner/v1"

	"github.com/agynio/k8s-microvm-runner/internal/ch"
	"github.com/agynio/k8s-microvm-runner/internal/image"
	"github.com/agynio/k8s-microvm-runner/internal/kata"
	"github.com/agynio/k8s-microvm-runner/internal/share"
	"github.com/agynio/k8s-microvm-runner/internal/snapshot"
	"github.com/agynio/k8s-microvm-runner/internal/store"
)

// BindSpec is a cold start: the workload spec as the platform sent it and
// what the runner resolved around it.
type BindSpec struct {
	Spec    *runnerv1.StartWorkloadRequest
	Key     string
	Flavor  Flavor
	Store   store.Config
	Volumes []VolumeStore
}

// ErrBusy says the slot already holds a workload.
var ErrBusy = errors.New("slot already holds a workload")

// ErrIdle says the slot holds nothing.
var ErrIdle = errors.New("slot holds no workload")

// Bind accepts a workload and boots it. It returns once the start is
// accepted, with every container of the spec listed as waiting; the boot
// continues and its progress reaches OnChange.
func (m *Manager) Bind(ctx context.Context, spec BindSpec) (View, error) {
	req := spec.Spec
	if req == nil || req.GetMain() == nil || strings.TrimSpace(req.GetMain().GetImage()) == "" {
		return View{}, errors.New("main container required")
	}
	id := strings.TrimSpace(req.GetWorkloadId())
	if id == "" {
		return View{}, errors.New("workload id required")
	}
	if spec.Flavor.VCPUs <= 0 || spec.Flavor.MemoryMiB <= 0 {
		return View{}, errors.New("flavor must size the guest")
	}
	w := &Workload{
		ID: id, Key: spec.Key, Labels: mergeLabels(req), Flavor: spec.Flavor, Status: StatusStarting,
		Hostname: hostnameFor(id), StartedAt: time.Now(),
	}
	volumes, err := declareVolumes(req, spec.Volumes)
	if err != nil {
		return View{}, err
	}
	w.Volumes = volumes
	w.Containers = declareContainers(req)

	m.mu.Lock()
	if m.w != nil {
		m.mu.Unlock()
		return View{}, ErrBusy
	}
	m.w = w
	m.binds++
	m.mu.Unlock()

	if err := m.prepareWorkloadDir(); err != nil {
		m.clear(w)
		return View{}, err
	}
	if data, err := protoJSON(req); err == nil {
		_ = os.WriteFile(filepath.Join(m.cfg.Layout.Workload(), "request.json"), data, 0o600)
	}
	v := w.View()
	if m.cfg.OnChange != nil {
		m.cfg.OnChange(v)
	}
	go func() {
		if err := m.boot(context.Background(), w, req, spec); err != nil {
			m.fail(w, err)
			m.teardownGuest(w)
		}
	}()
	return v, nil
}

func hostnameFor(id string) string { return "workload-" + id }

// declareContainers lists every container of the spec from the moment the
// start is accepted, so an inspect of a starting workload already names
// them all, as a Pod's does.
func declareContainers(req *runnerv1.StartWorkloadRequest) []*Container {
	var out []*Container
	for i, spec := range req.GetInitContainers() {
		c := &Container{Name: nameOr(spec, "init", i+1), Role: RoleInit, Image: spec.GetImage(), Status: ContainerWaiting, spec: spec}
		c.nativeSidecar = spec.GetAdditionalProperties()["restart_policy"] == "Always"
		c.Mounts = mountsOf(spec)
		out = append(out, c)
	}
	main := &Container{Name: nameOr(req.GetMain(), "main", 0), Role: RoleMain, Image: req.GetMain().GetImage(), Status: ContainerWaiting, spec: req.GetMain()}
	main.Mounts = mountsOf(req.GetMain())
	out = append(out, main)
	for i, spec := range req.GetSidecars() {
		c := &Container{Name: nameOr(spec, "sidecar", i+1), Role: RoleSidecar, Image: spec.GetImage(), Status: ContainerWaiting, spec: spec}
		c.Mounts = mountsOf(spec)
		out = append(out, c)
	}
	return out
}

func mountsOf(spec *runnerv1.ContainerSpec) []snapshot.Mount {
	var out []snapshot.Mount
	for _, mnt := range spec.GetMounts() {
		out = append(out, snapshot.Mount{Volume: mnt.GetVolume(), Destination: mnt.GetMountPath(), ReadOnly: mnt.GetReadOnly()})
	}
	return out
}

func nameOr(spec *runnerv1.ContainerSpec, kind string, i int) string {
	if n := strings.TrimSpace(spec.GetName()); n != "" {
		return n
	}
	if i == 0 {
		return kind
	}
	return fmt.Sprintf("%s-%d", kind, i)
}

func declareVolumes(req *runnerv1.StartWorkloadRequest, refs []VolumeStore) ([]Volume, error) {
	byName := map[string]VolumeStore{}
	for _, r := range refs {
		byName[r.Name] = r
	}
	var out []Volume
	for _, v := range req.GetVolumes() {
		name := strings.TrimSpace(v.GetName())
		if name == "" {
			return nil, errors.New("volume name required")
		}
		vol := Volume{Name: name}
		if v.GetKind() == runnerv1.VolumeKind_VOLUME_KIND_NAMED {
			vol.Persistent = true
			vol.InstanceID = strings.TrimSpace(v.GetPersistentName())
			if vol.InstanceID == "" {
				vol.InstanceID = name
			}
			vol.VolumeKey = v.GetLabels()["volume_key"]
			if size := strings.TrimSpace(v.GetSize()); size != "" {
				q, err := resource.ParseQuantity(size)
				if err != nil {
					return nil, fmt.Errorf("volume %s: size %q: %w", name, size, err)
				}
				vol.SizeBytes = q.Value()
			}
			if ref, ok := byName[name]; ok {
				vol.Store = ref
				if vol.VolumeKey == "" {
					vol.VolumeKey = ref.VolumeKey
				}
			}
		}
		out = append(out, vol)
	}
	return out, nil
}

// mergeLabels returns the request's labels plus the "label.<name>" entries
// of its additional properties, which is where the Orchestrator puts
// workload_key and the agent and thread ids; the k8s-runner merges them the
// same way when it labels a Pod.
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

func credentialsOf(creds []*runnerv1.ImagePullCredential) []image.Credential {
	out := make([]image.Credential, 0, len(creds))
	for _, c := range creds {
		out = append(out, image.Credential{Registry: c.GetRegistry(), Username: c.GetUsername(), Password: c.GetPassword()})
	}
	return out
}

// boot is the cold path: pull, assemble the share, boot the guest, bring the
// containers up.
func (m *Manager) boot(ctx context.Context, w *Workload, req *runnerv1.StartWorkloadRequest, spec BindSpec) error {
	t0 := time.Now()
	l := m.cfg.Layout
	if err := m.setupNet(ctx); err != nil {
		return fmt.Errorf("network: %w", err)
	}
	creds := credentialsOf(req.GetImagePullCredentials())

	// Every image is pulled before the guest boots, so the boot path is not
	// waiting on registries.
	metas := map[string]*image.Meta{}
	for _, c := range w.Containers {
		if _, ok := metas[c.Image]; ok {
			continue
		}
		meta, err := m.images.Ensure(ctx, c.Image, creds)
		if err != nil {
			return err
		}
		metas[c.Image] = meta
	}
	tImages := time.Since(t0)

	st, err := storeClient(spec.Store)
	if err != nil {
		return err
	}
	if err := m.fetchVolumes(ctx, w, st, spec.Store); err != nil {
		return err
	}
	guestVolumes, err := m.mountVolumes(w)
	if err != nil {
		return err
	}
	dns := dnsOf(req)
	if err := m.writeSandboxFiles(w, dns); err != nil {
		return err
	}
	inline, err := m.writeInlineFiles(req)
	if err != nil {
		return err
	}
	for _, c := range w.Containers {
		meta := metas[c.Image]
		c.ImageDigest, c.uid, c.gid = meta.Digest, meta.UID, meta.GID
		if err := share.MountOverlay(l.ContainerRootfs(c.Name), meta.Dir, l.ContainerUpper(c.Name), l.ContainerWork(c.Name), share.InUserNamespace()); err != nil {
			return err
		}
		c.oci = buildOCI(c.spec, meta, c.Name, w.Hostname, guestVolumes, inline)
		c.cwd = c.oci.Process.Cwd
	}
	w.DNS = dns
	tShare := time.Since(t0) - tImages

	vfsd, err := m.startVirtiofsd(ctx)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.vfsd = vfsd
	w.mu.Unlock()
	vm := ch.New(m.cfg.CloudHypervisor, l.Workload())
	if err := vm.Spawn(ctx); err != nil {
		return err
	}
	w.mu.Lock()
	w.vm = vm
	w.mu.Unlock()
	if err := vm.Create(ctx, m.vmConfig(w)); err != nil {
		return fmt.Errorf("vm.create: %w", err)
	}
	tb := time.Now()
	if err := vm.Boot(ctx); err != nil {
		return fmt.Errorf("vm.boot: %w", err)
	}
	agent, err := kata.WaitReady(ctx, m.vsockPath(), m.cfg.AgentBootTimeout)
	if err != nil {
		return fmt.Errorf("guest agent: %w (%s)", err, vm.TailLog())
	}
	w.mu.Lock()
	w.agent = agent
	w.mu.Unlock()
	tAgent := time.Since(tb)

	if err := agent.ConfigureNetwork(ctx, "eth0", m.net.MAC, m.net.GuestIP, m.net.Prefix, m.net.HostIP, m.egressMTU()); err != nil {
		return err
	}
	shareStorage := []*kata.Storage{{Driver: "virtio-fs", Source: share.Tag, Fstype: "virtiofs", MountPoint: share.GuestShare + "/"}}
	if err := agent.CreateSandbox(ctx, w.ID, w.Hostname, dns, shareStorage); err != nil {
		return fmt.Errorf("create sandbox: %w", err)
	}
	m.watchGuest(w, vm)

	// Init containers run to completion in order; one marked
	// restart_policy=Always is a native sidecar and is left running.
	for _, c := range w.Containers {
		if c.Role != RoleInit {
			continue
		}
		if err := m.startContainer(ctx, w, agent, c); err != nil {
			return err
		}
		if c.nativeSidecar {
			go m.await(w, c)
			continue
		}
		code, err := agent.Wait(ctx, c.Name, c.Name)
		m.update(w, func() {
			c.Status = ContainerTerminated
			c.FinishedAt = time.Now()
			c.ExitCode = &code
		})
		if err != nil {
			return fmt.Errorf("wait init container %s: %w", c.Name, err)
		}
		if code != 0 {
			return fmt.Errorf("init container %s exited %d", c.Name, code)
		}
	}
	for _, c := range w.Containers {
		if c.Role == RoleInit {
			continue
		}
		if err := m.startContainer(ctx, w, agent, c); err != nil {
			return err
		}
		go m.await(w, c)
	}
	m.update(w, func() {
		w.Status = StatusRunning
	})
	m.log.Info("workload running", zap.String("workload", w.ID), zap.Duration("images", tImages.Round(time.Millisecond)),
		zap.Duration("share", tShare.Round(time.Millisecond)), zap.Duration("boot_to_agent", tAgent.Round(time.Millisecond)),
		zap.Duration("total", time.Since(t0).Round(time.Millisecond)))
	return nil
}

func (m *Manager) startContainer(ctx context.Context, w *Workload, agent *kata.Client, c *Container) error {
	tc := time.Now()
	if err := agent.CreateContainer(ctx, c.Name, c.oci); err != nil {
		return fmt.Errorf("create container %s: %w", c.Name, err)
	}
	if err := agent.StartContainer(ctx, c.Name); err != nil {
		return fmt.Errorf("start container %s: %w", c.Name, err)
	}
	m.update(w, func() {
		c.Status = ContainerRunning
		c.StartedAt = time.Now()
	})
	m.captureLogs(w, c)
	m.log.Info("container started", zap.String("workload", w.ID), zap.String("container", c.Name), zap.Duration("took", time.Since(tc).Round(time.Millisecond)))
	return nil
}

// await records a container's exit. A main container that exits is not a
// failure of the workload: the Orchestrator reads the exit code off inspect
// and decides, as it does for a Pod.
func (m *Manager) await(w *Workload, c *Container) {
	cl, err := kata.Dial(m.vsockPath())
	if err != nil {
		return
	}
	defer cl.Close()
	code, err := cl.Wait(context.Background(), c.Name, c.Name)
	w.mu.Lock()
	sleeping := w.Status == StatusHibernating || w.Status == StatusHibernated || w.Status == StatusWaking || w.agent == nil
	w.mu.Unlock()
	if sleeping || err != nil {
		// The wait ended because the guest went away, not the process.
		return
	}
	m.update(w, func() {
		if c.Status != ContainerRunning {
			return
		}
		c.Status = ContainerTerminated
		c.FinishedAt = time.Now()
		c.ExitCode = &code
	})
	if c.Role == RoleMain {
		m.log.Warn("main container exited", zap.String("workload", w.ID), zap.Int32("code", code))
	}
}

// watchGuest reports a guest that dies on its own as failed.
func (m *Manager) watchGuest(w *Workload, vm *ch.VM) {
	go func() {
		<-vm.Done()
		w.mu.Lock()
		current := w.vm == vm && (w.Status == StatusRunning || w.Status == StatusStarting)
		w.mu.Unlock()
		if !current {
			return
		}
		m.fail(w, fmt.Errorf("guest exited: %s", vm.TailLog()))
		m.teardownGuest(w)
	}()
}

func dnsOf(req *runnerv1.StartWorkloadRequest) []string {
	var dns []string
	if req.GetDnsConfig() != nil {
		for _, ns := range req.GetDnsConfig().GetNameservers() {
			dns = append(dns, "nameserver "+ns)
		}
		if searches := req.GetDnsConfig().GetSearches(); len(searches) > 0 {
			dns = append(dns, "search "+strings.Join(searches, " "))
		}
	}
	if len(dns) == 0 {
		dns = slotResolvConf()
	}
	return dns
}

// writeSandboxFiles writes the hosts, hostname and resolv.conf every
// container binds, as a kubelet provides them: one writable file per
// sandbox, so what one container writes every other sees.
func (m *Manager) writeSandboxFiles(w *Workload, dns []string) error {
	dir := m.cfg.Layout.SandboxDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	hosts := fmt.Sprintf("127.0.0.1 localhost\n::1 localhost\n%s %s\n", m.net.GuestIP, w.Hostname)
	files := map[string]string{"hosts": hosts, "hostname": w.Hostname + "\n", "resolv.conf": strings.Join(dns, "\n") + "\n"}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// writeInlineFiles materializes the inline files into the share and maps
// each container path to where the guest sees it.
func (m *Manager) writeInlineFiles(req *runnerv1.StartWorkloadRequest) (map[string]string, error) {
	paths := map[string]string{}
	if err := os.MkdirAll(m.cfg.Layout.InlineDir(), 0o755); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(req.GetInlineFiles()))
	for p := range req.GetInlineFiles() {
		keys = append(keys, p)
	}
	sortStrings(keys)
	for i, p := range keys {
		name := fmt.Sprintf("f%d", i)
		if err := os.WriteFile(filepath.Join(m.cfg.Layout.InlineDir(), name), req.GetInlineFiles()[p], 0o444); err != nil {
			return nil, err
		}
		paths[p] = share.GuestInlineFile(name)
	}
	return paths, nil
}
