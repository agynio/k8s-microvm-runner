// Package pool keeps the slot Pods: a pool of prewarmed, generic slots per
// flavor, created by the runner itself through the Kubernetes API, bound to
// a workload for as long as it lives in them, and replaced rather than
// repaired. The runner, not a Deployment, decides which slots are idle and
// which are bound, which is the one thing a Deployment cannot know.
package pool

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	slotv1 "github.com/agynio/k8s-microvm-runner/internal/.gen/agynio/microvm/slot/v1"
	"github.com/agynio/k8s-microvm-runner/internal/slotclient"
)

// State is where a slot is in its life.
type State string

// The lifecycle: created → warming → idle → bound → idle → … → retired.
const (
	StateCreating State = "creating"
	StateWarming  State = "warming"
	StateIdle     State = "idle"
	StateBound    State = "bound"
	StateRetired  State = "retired"
)

// Slot is one slot Pod as the runner knows it.
type Slot struct {
	Name       string
	Flavor     string
	PodIP      string
	Token      string
	State      State
	WorkloadID string
	// Cached is the workload keys whose snapshots the slot still caches.
	Cached []string
	Binds  int
	// Versions is the digest of the set the slot runs.
	Versions  string
	CreatedAt time.Time
	LastSeen  time.Time
	failures  int
	client    slotclient.API
}

// Address is where the slot's supervisor listens.
func (s *Slot) Address() string { return fmt.Sprintf("%s:%d", s.PodIP, slotclient.DefaultPort) }

// Client is the slot's control connection.
func (s *Slot) Client() slotclient.API { return s.client }

// Dialer opens a control connection to a supervisor.
type Dialer func(address, token string) (slotclient.API, error)

// FlavorSpec is one flavor's slot sizing and pool targets.
type FlavorSpec struct {
	VCPUs     int
	MemoryMiB int64
	// Warm is how many idle slots to keep ready; Max how many slots the
	// flavor may occupy at once (0 is unlimited).
	Warm int
	Max  int
}

// Config is the pool's configuration.
type Config struct {
	Namespace string
	Flavors   map[string]FlavorSpec
	Template  PodTemplate
	// MaxBinds retires a slot after this many workloads (0 never does).
	MaxBinds int
	// WarmupTimeout retires a slot that never answers.
	WarmupTimeout time.Duration
	// Versions is the digest of the set the slot image runs; a slot
	// reporting another is retired once idle.
	Versions string
	// RunnerName labels the slots as this runner's.
	RunnerName string
}

// Pool is the set of slots.
type Pool struct {
	cfg    Config
	client kubernetes.Interface
	dial   Dialer
	log    *zap.Logger

	mu    sync.Mutex
	slots map[string]*Slot
	kick  chan struct{}
	// OnLost is told when a bound slot's Pod disappeared or its supervisor
	// stopped answering: the workload in it is gone.
	OnLost func(slot *Slot)
}

// New makes a pool. A nil dialer uses the real client.
func New(cfg Config, client kubernetes.Interface, dial Dialer, log *zap.Logger) *Pool {
	if dial == nil {
		dial = func(address, token string) (slotclient.API, error) { return slotclient.Dial(address, token) }
	}
	if cfg.WarmupTimeout == 0 {
		cfg.WarmupTimeout = 5 * time.Minute
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &Pool{cfg: cfg, client: client, dial: dial, log: log, slots: map[string]*Slot{}, kick: make(chan struct{}, 1)}
}

// Errors a caller can act on.
var (
	ErrNoIdleSlot = errors.New("no idle slot of this flavor; one is warming")
	ErrFlavorFull = errors.New("flavor has no room for another slot")
	ErrNoFlavor   = errors.New("unknown flavor")
)

// Adopted is a slot found at startup that already hosts a workload.
type Adopted struct {
	Slot     *Slot
	Workload *slotv1.WorkloadView
}

// Adopt finds the slot Pods this runner created before it restarted, asks
// each supervisor what it holds, and rebuilds the pool from that. A slot
// whose supervisor cannot be reached is retired.
func (p *Pool) Adopt(ctx context.Context) ([]Adopted, error) {
	list, err := p.client.CoreV1().Pods(p.cfg.Namespace).List(ctx, metav1.ListOptions{LabelSelector: p.selector()})
	if err != nil {
		return nil, fmt.Errorf("list slot pods: %w", err)
	}
	var adopted []Adopted
	for i := range list.Items {
		pod := &list.Items[i]
		slot := p.slotFromPod(pod)
		if slot == nil {
			continue
		}
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" || pod.DeletionTimestamp != nil {
			if pod.Status.Phase == corev1.PodPending {
				slot.State = StateWarming
				p.put(slot)
				continue
			}
			p.log.Info("retiring slot that is not running", zap.String("slot", slot.Name), zap.String("phase", string(pod.Status.Phase)))
			p.deletePod(ctx, slot.Name)
			continue
		}
		slot.PodIP = pod.Status.PodIP
		desc, err := p.describe(ctx, slot)
		if err != nil {
			p.log.Warn("retiring slot whose supervisor cannot be reached", zap.String("slot", slot.Name), zap.Error(err))
			p.deletePod(ctx, slot.Name)
			continue
		}
		p.applyDescription(slot, desc)
		if desc.GetState() == slotv1.SlotState_SLOT_STATE_BOUND && desc.GetWorkload() != nil {
			slot.State = StateBound
			slot.WorkloadID = desc.GetWorkload().GetWorkloadId()
			adopted = append(adopted, Adopted{Slot: slot, Workload: desc.GetWorkload()})
		} else {
			slot.State = StateIdle
		}
		p.put(slot)
		p.log.Info("adopted slot", zap.String("slot", slot.Name), zap.String("state", string(slot.State)), zap.String("workload", slot.WorkloadID))
	}
	return adopted, nil
}

func (p *Pool) put(slot *Slot) {
	p.mu.Lock()
	p.slots[slot.Name] = slot
	p.mu.Unlock()
}

func (p *Pool) describe(ctx context.Context, slot *Slot) (*slotv1.DescribeResponse, error) {
	if slot.client == nil {
		c, err := p.dial(slot.Address(), slot.Token)
		if err != nil {
			return nil, err
		}
		slot.client = c
	}
	return slot.client.Describe(ctx, 5*time.Second)
}

func (p *Pool) applyDescription(slot *Slot, desc *slotv1.DescribeResponse) {
	slot.Cached = desc.GetCachedSnapshots()
	slot.Binds = int(desc.GetBinds())
	slot.Versions = desc.GetVersions().GetDigest()
	slot.LastSeen = time.Now()
	slot.failures = 0
}

// Run keeps the pool at its targets until ctx ends: it watches the slot
// Pods, warms new ones, and replaces what is consumed or lost.
func (p *Pool) Run(ctx context.Context) error {
	factory := informers.NewSharedInformerFactoryWithOptions(p.client, 30*time.Second,
		informers.WithNamespace(p.cfg.Namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = p.selector() }))
	pods := factory.Core().V1().Pods().Informer()
	if _, err := pods.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { p.observePod(ctx, obj) },
		UpdateFunc: func(_, obj any) { p.observePod(ctx, obj) },
		DeleteFunc: func(obj any) { p.podGone(obj) },
	}); err != nil {
		return err
	}
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		p.reconcile(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-p.kick:
		}
	}
}

// Kick asks for a reconcile now.
func (p *Pool) Kick() {
	select {
	case p.kick <- struct{}{}:
	default:
	}
}

func (p *Pool) observePod(ctx context.Context, obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	p.mu.Lock()
	slot, known := p.slots[pod.Name]
	if !known {
		if fresh := p.slotFromPod(pod); fresh != nil && pod.DeletionTimestamp == nil {
			// A Pod this runner created but does not remember: a previous
			// run's that Adopt did not see, or one created between the
			// list and the watch.
			fresh.State = StateWarming
			p.slots[pod.Name] = fresh
			slot = fresh
		}
	}
	if slot == nil {
		p.mu.Unlock()
		return
	}
	if pod.Status.PodIP != "" {
		slot.PodIP = pod.Status.PodIP
	}
	failed := pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded || pod.DeletionTimestamp != nil
	wasBound := slot.State == StateBound
	if failed && slot.State != StateRetired {
		slot.State = StateRetired
	}
	p.mu.Unlock()
	if failed {
		if wasBound && p.OnLost != nil {
			p.OnLost(slot)
		}
		if pod.DeletionTimestamp == nil {
			p.deletePod(ctx, pod.Name)
		}
		p.Kick()
	}
}

func (p *Pool) podGone(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		if tomb, isTomb := obj.(cache.DeletedFinalStateUnknown); isTomb {
			pod, ok = tomb.Obj.(*corev1.Pod)
		}
		if !ok {
			return
		}
	}
	p.mu.Lock()
	slot, known := p.slots[pod.Name]
	if known {
		delete(p.slots, pod.Name)
		if slot.client != nil {
			_ = slot.client.Close()
		}
	}
	p.mu.Unlock()
	if known && slot.State == StateBound && p.OnLost != nil {
		p.OnLost(slot)
	}
	p.Kick()
}

// reconcile warms and retires slots to meet each flavor's targets.
func (p *Pool) reconcile(ctx context.Context) {
	p.checkWarming(ctx)
	p.checkIdle(ctx)
	p.mu.Lock()
	type counts struct{ ready, total int }
	byFlavor := map[string]*counts{}
	for name := range p.cfg.Flavors {
		byFlavor[name] = &counts{}
	}
	for _, s := range p.slots {
		c, ok := byFlavor[s.Flavor]
		if !ok {
			continue
		}
		switch s.State {
		case StateCreating, StateWarming, StateIdle:
			c.ready++
			c.total++
		case StateBound:
			c.total++
		}
	}
	var toCreate []string
	for name, spec := range p.cfg.Flavors {
		c := byFlavor[name]
		for c.ready < spec.Warm && (spec.Max == 0 || c.total < spec.Max) {
			toCreate = append(toCreate, name)
			c.ready++
			c.total++
		}
	}
	p.mu.Unlock()
	for _, flavor := range toCreate {
		if err := p.createSlot(ctx, flavor); err != nil {
			p.log.Warn("create slot", zap.String("flavor", flavor), zap.Error(err))
			return
		}
	}
}

// checkWarming promotes warming slots whose supervisor answers, and retires
// those that never do.
func (p *Pool) checkWarming(ctx context.Context) {
	p.mu.Lock()
	var warming []*Slot
	for _, s := range p.slots {
		if s.State == StateWarming && s.PodIP != "" {
			warming = append(warming, s)
		}
	}
	p.mu.Unlock()
	for _, s := range warming {
		desc, err := p.describe(ctx, s)
		if err != nil {
			if time.Since(s.CreatedAt) > p.cfg.WarmupTimeout {
				p.log.Warn("retiring slot that never warmed", zap.String("slot", s.Name), zap.Error(err))
				p.Retire(ctx, s, "warmup timeout")
			}
			continue
		}
		p.mu.Lock()
		if s.State == StateWarming {
			p.applyDescription(s, desc)
			if desc.GetState() == slotv1.SlotState_SLOT_STATE_BOUND {
				// Found holding a workload the runner does not know: it
				// belongs to nobody now.
				p.mu.Unlock()
				p.Retire(ctx, s, "holds an unknown workload")
				continue
			}
			s.State = StateIdle
			p.log.Info("slot idle", zap.String("slot", s.Name), zap.String("flavor", s.Flavor), zap.Duration("warmup", time.Since(s.CreatedAt).Round(time.Second)))
		}
		p.mu.Unlock()
	}
}

// checkIdle keeps idle slots honest: one that stops answering, runs another
// version set, or hit its reuse limit is retired.
func (p *Pool) checkIdle(ctx context.Context) {
	p.mu.Lock()
	var idle []*Slot
	for _, s := range p.slots {
		if s.State == StateIdle && time.Since(s.LastSeen) > 30*time.Second {
			idle = append(idle, s)
		}
	}
	p.mu.Unlock()
	for _, s := range idle {
		desc, err := p.describe(ctx, s)
		if err != nil {
			p.mu.Lock()
			s.failures++
			failures := s.failures
			p.mu.Unlock()
			if failures >= 3 {
				p.Retire(ctx, s, "supervisor unreachable")
			}
			continue
		}
		p.mu.Lock()
		p.applyDescription(s, desc)
		p.mu.Unlock()
		if p.cfg.Versions != "" && s.Versions != "" && s.Versions != p.cfg.Versions {
			p.Retire(ctx, s, "version set rolled out")
		} else if p.cfg.MaxBinds > 0 && s.Binds >= p.cfg.MaxBinds {
			p.Retire(ctx, s, "reuse limit")
		}
	}
}

// Acquire takes an idle slot of the flavor and marks it bound to the
// workload. prefer names a slot to take when it is idle.
func (p *Pool) Acquire(flavor, workloadID, prefer string) (*Slot, error) {
	spec, ok := p.cfg.Flavors[flavor]
	if !ok {
		return nil, ErrNoFlavor
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var candidates []*Slot
	total := 0
	for _, s := range p.slots {
		if s.Flavor != flavor || s.State == StateRetired {
			continue
		}
		total++
		if s.State == StateIdle {
			candidates = append(candidates, s)
		}
	}
	if len(candidates) == 0 {
		if spec.Max > 0 && total >= spec.Max {
			return nil, ErrFlavorFull
		}
		p.Kick()
		return nil, ErrNoIdleSlot
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Name == prefer {
			return true
		}
		if candidates[j].Name == prefer {
			return false
		}
		return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
	})
	slot := candidates[0]
	slot.State = StateBound
	slot.WorkloadID = workloadID
	slot.Binds++
	p.Kick()
	return slot, nil
}

// Caching finds an idle slot of the flavor that still caches the workload's
// snapshot.
func (p *Pool) Caching(flavor, workloadKey string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range p.slots {
		if s.Flavor != flavor || s.State != StateIdle {
			continue
		}
		for _, key := range s.Cached {
			if key == workloadKey {
				return s.Name
			}
		}
	}
	return ""
}

// Release returns a bound slot to idle once its workload left, or retires
// it when it hit its reuse limit.
func (p *Pool) Release(ctx context.Context, slot *Slot, cached string) {
	p.mu.Lock()
	if slot.State == StateBound {
		slot.State = StateIdle
	}
	slot.WorkloadID = ""
	slot.LastSeen = time.Now()
	if cached != "" {
		slot.Cached = append([]string{cached}, slot.Cached...)
	}
	limit := p.cfg.MaxBinds > 0 && slot.Binds >= p.cfg.MaxBinds
	p.mu.Unlock()
	if limit {
		p.Retire(ctx, slot, "reuse limit")
	}
	p.Kick()
}

// Retire deletes a slot's Pod; a replacement is created by the next
// reconcile.
func (p *Pool) Retire(ctx context.Context, slot *Slot, reason string) {
	p.mu.Lock()
	wasBound := slot.State == StateBound
	slot.State = StateRetired
	p.mu.Unlock()
	p.log.Info("retiring slot", zap.String("slot", slot.Name), zap.String("reason", reason))
	if wasBound && p.OnLost != nil {
		p.OnLost(slot)
	}
	p.deletePod(ctx, slot.Name)
	p.Kick()
}

// Get returns a slot by name.
func (p *Pool) Get(name string) (*Slot, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.slots[name]
	return s, ok
}

// Slots lists every slot, by name.
func (p *Pool) Slots() []*Slot {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*Slot, 0, len(p.slots))
	for _, s := range p.slots {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (p *Pool) createSlot(ctx context.Context, flavor string) error {
	spec := p.cfg.Flavors[flavor]
	token, err := newToken()
	if err != nil {
		return err
	}
	name := fmt.Sprintf("slot-%s-%s", flavor, randomSuffix())
	slot := &Slot{Name: name, Flavor: flavor, Token: token, State: StateCreating, CreatedAt: time.Now()}
	p.put(slot)
	pod := p.cfg.Template.Pod(name, p.cfg.Namespace, flavor, spec, token, p.labels(flavor))
	if _, err := p.client.CoreV1().Pods(p.cfg.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		p.mu.Lock()
		delete(p.slots, name)
		p.mu.Unlock()
		return err
	}
	p.mu.Lock()
	if slot.State == StateCreating {
		slot.State = StateWarming
	}
	p.mu.Unlock()
	p.log.Info("slot created", zap.String("slot", name), zap.String("flavor", flavor))
	return nil
}

func (p *Pool) deletePod(ctx context.Context, name string) {
	grace := int64(10)
	err := p.client.CoreV1().Pods(p.cfg.Namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &grace})
	if err != nil && !apierrors.IsNotFound(err) {
		p.log.Warn("delete slot pod", zap.String("slot", name), zap.Error(err))
	}
}

// Labels every slot Pod carries.
const (
	ManagedByLabel      = "app.kubernetes.io/managed-by"
	ManagedByValue      = "k8s-microvm-runner"
	WorkloadPolicyLabel = "agyn.dev/managed-by"
	WorkloadPolicyValue = "agents-orchestrator"
	SlotLabel           = "agyn.io/slot"
	FlavorLabel         = "agyn.io/slot-flavor"
	RunnerLabel         = "agyn.io/runner"
)

func (p *Pool) labels(flavor string) map[string]string {
	labels := map[string]string{
		ManagedByLabel:      ManagedByValue,
		WorkloadPolicyLabel: WorkloadPolicyValue,
		SlotLabel:           "true",
		FlavorLabel:         flavor,
	}
	if p.cfg.RunnerName != "" {
		labels[RunnerLabel] = p.cfg.RunnerName
	}
	return labels
}

func (p *Pool) selector() string {
	sel := ManagedByLabel + "=" + ManagedByValue + "," + SlotLabel + "=true"
	if p.cfg.RunnerName != "" {
		sel += "," + RunnerLabel + "=" + p.cfg.RunnerName
	}
	return sel
}

// slotFromPod reads a slot back off its Pod: the flavor from its label and
// the token from the environment the runner injected.
func (p *Pool) slotFromPod(pod *corev1.Pod) *Slot {
	flavor := pod.Labels[FlavorLabel]
	if flavor == "" {
		return nil
	}
	slot := &Slot{Name: pod.Name, Flavor: flavor, PodIP: pod.Status.PodIP, CreatedAt: pod.CreationTimestamp.Time, State: StateWarming}
	for _, c := range pod.Spec.Containers {
		for _, env := range c.Env {
			if env.Name == TokenEnv {
				slot.Token = env.Value
			}
		}
	}
	if slot.Token == "" {
		return nil
	}
	return slot
}

func newToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func randomSuffix() string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return strings.ToLower(hex.EncodeToString(b))
}
