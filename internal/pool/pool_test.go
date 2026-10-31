package pool

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	slotv1 "github.com/agynio/k8s-microvm-runner/internal/.gen/agynio/microvm/slot/v1"
	"github.com/agynio/k8s-microvm-runner/internal/slotclient"
)

// fakeSupervisor answers Describe the way a supervisor would, from a table
// of what each address holds.
type fakeSupervisor struct {
	mu   sync.Mutex
	held map[string]*slotv1.DescribeResponse
	down map[string]bool
}

func newFakeSupervisor() *fakeSupervisor {
	return &fakeSupervisor{held: map[string]*slotv1.DescribeResponse{}, down: map[string]bool{}}
}

func (f *fakeSupervisor) dial(address, token string) (slotclient.API, error) {
	return &fakeClient{f: f, address: address, token: token}, nil
}

type fakeClient struct {
	f       *fakeSupervisor
	address string
	token   string
}

func (c *fakeClient) Describe(ctx context.Context, timeout time.Duration) (*slotv1.DescribeResponse, error) {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	if c.f.down[c.address] {
		return nil, errors.New("connection refused")
	}
	if resp, ok := c.f.held[c.address]; ok {
		return resp, nil
	}
	return &slotv1.DescribeResponse{State: slotv1.SlotState_SLOT_STATE_IDLE, Versions: &slotv1.VersionSet{Digest: "v1"}}, nil
}
func (c *fakeClient) Bind(context.Context, *slotv1.BindRequest) (*slotv1.BindResponse, error) {
	return nil, errors.New("not in this test")
}
func (c *fakeClient) Inspect(context.Context, string) (*slotv1.WorkloadView, error) {
	return nil, errors.New("not in this test")
}
func (c *fakeClient) Stop(context.Context, *slotv1.StopRequest) error { return nil }
func (c *fakeClient) Hibernate(context.Context, *slotv1.HibernateRequest) (int64, error) {
	return 0, nil
}
func (c *fakeClient) Wake(context.Context, *slotv1.WakeRequest) (*slotv1.WorkloadView, error) {
	return nil, errors.New("not in this test")
}
func (c *fakeClient) DiscardCache(context.Context, string) error { return nil }
func (c *fakeClient) WatchState(context.Context) (slotv1.SlotSupervisorService_WatchStateClient, error) {
	return nil, errors.New("not in this test")
}
func (c *fakeClient) Logs(context.Context, *slotv1.LogsRequest) (io.ReadCloser, error) {
	return nil, errors.New("not in this test")
}
func (c *fakeClient) Exec(context.Context, string, string, []string, bool, slotclient.ExecStreams) error {
	return errors.New("not in this test")
}
func (c *fakeClient) Close() error { return nil }

func testConfig() Config {
	return Config{
		Namespace: "agyn-workloads",
		Flavors:   map[string]FlavorSpec{"small": {VCPUs: 1, MemoryMiB: 2048, Warm: 2, Max: 3}},
		Template:  PodTemplate{Image: "slot:test", OverheadMemory: resource.MustParse("512Mi"), CacheSize: resource.MustParse("10Gi"), KVMResource: "squat.ai/kvm", TUNResource: "squat.ai/tun", HostUsers: true},
		MaxBinds:  2,
		Versions:  "v1",
	}
}

func runningPod(name, flavor, token, ip string) *corev1.Pod {
	pod := (PodTemplate{Image: "slot:test"}).Pod(name, "agyn-workloads", flavor, FlavorSpec{VCPUs: 1, MemoryMiB: 2048}, token, map[string]string{ManagedByLabel: ManagedByValue, SlotLabel: "true", FlavorLabel: flavor})
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = ip
	return pod
}

// A reconcile creates slot Pods until the flavor has its warm count, and
// never beyond max.
func TestReconcileWarmsToTheTargetWithinMax(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := New(testConfig(), client, newFakeSupervisor().dial, zap.NewNop())
	ctx := context.Background()
	p.reconcile(ctx)
	pods, _ := client.CoreV1().Pods("agyn-workloads").List(ctx, metav1.ListOptions{})
	if len(pods.Items) != 2 {
		t.Fatalf("expected 2 warm slots, got %d", len(pods.Items))
	}
	pod := pods.Items[0]
	if pod.Labels[FlavorLabel] != "small" || pod.Labels[WorkloadPolicyLabel] != WorkloadPolicyValue {
		t.Fatalf("unexpected labels %v", pod.Labels)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("a slot never talks to the API: no token")
	}
	c := pod.Spec.Containers[0]
	if c.Resources.Limits[corev1.ResourceMemory] != resource.MustParse("2560Mi") {
		t.Fatalf("expected the memory limit to be flavor plus overhead, got %v", c.Resources.Limits[corev1.ResourceMemory])
	}
	if c.Resources.Limits["squat.ai/kvm"] != resource.MustParse("1") || c.Resources.Limits["squat.ai/tun"] != resource.MustParse("1") {
		t.Fatal("expected the kvm and tun extended resources")
	}
	if c.SecurityContext.AppArmorProfile == nil || c.SecurityContext.AppArmorProfile.Type != corev1.AppArmorProfileTypeUnconfined {
		t.Fatal("expected an unconfined AppArmor profile")
	}
	if pod.Spec.SecurityContext == nil || len(pod.Spec.SecurityContext.Sysctls) != 1 || pod.Spec.SecurityContext.Sysctls[0].Name != "net.ipv4.ip_forward" {
		t.Fatal("expected the ip_forward sysctl")
	}
	token := ""
	for _, env := range c.Env {
		if env.Name == TokenEnv {
			token = env.Value
		}
	}
	if len(token) != 64 {
		t.Fatalf("expected a generated token, got %q", token)
	}
	// Mark every slot bound; the pool may create one more, then stops at max.
	for _, s := range p.Slots() {
		s.State = StateBound
	}
	p.reconcile(ctx)
	p.reconcile(ctx)
	pods, _ = client.CoreV1().Pods("agyn-workloads").List(ctx, metav1.ListOptions{})
	if len(pods.Items) != 3 {
		t.Fatalf("expected max 3 slots, got %d", len(pods.Items))
	}
}

// warming → idle once the supervisor answers; bound on Acquire; idle again
// on Release; retired at the reuse limit.
func TestLifecycle(t *testing.T) {
	client := fake.NewSimpleClientset()
	sup := newFakeSupervisor()
	p := New(testConfig(), client, sup.dial, zap.NewNop())
	ctx := context.Background()
	p.reconcile(ctx)
	slots := p.Slots()
	if len(slots) != 2 || slots[0].State != StateWarming {
		t.Fatalf("expected two warming slots, got %+v", slots)
	}
	if _, err := p.Acquire("small", "w1", ""); !errors.Is(err, ErrNoIdleSlot) {
		t.Fatalf("expected no idle slot while warming, got %v", err)
	}
	for _, s := range slots {
		s.PodIP = "10.0.0." + s.Name[len(s.Name)-1:]
	}
	p.checkWarming(ctx)
	for _, s := range p.Slots() {
		if s.State != StateIdle {
			t.Fatalf("expected %s idle after its supervisor answered, got %s", s.Name, s.State)
		}
	}
	first, err := p.Acquire("small", "w1", "")
	if err != nil {
		t.Fatal(err)
	}
	if first.State != StateBound || first.WorkloadID != "w1" || first.Binds != 1 {
		t.Fatalf("unexpected bound slot %+v", first)
	}
	p.Release(ctx, first, "key-1")
	if first.State != StateIdle || first.WorkloadID != "" || first.Cached[0] != "key-1" {
		t.Fatalf("unexpected released slot %+v", first)
	}
	if p.Caching("small", "key-1") != first.Name {
		t.Fatal("expected the released slot to be found by its cached snapshot")
	}
	again, _ := p.Acquire("small", "w2", first.Name)
	if again != first {
		t.Fatal("expected the preferred slot to be taken")
	}
	p.Release(ctx, again, "")
	if again.State != StateRetired {
		t.Fatalf("expected the slot retired at its reuse limit, got %s", again.State)
	}
	if _, err := client.CoreV1().Pods("agyn-workloads").Get(ctx, again.Name, metav1.GetOptions{}); err == nil {
		t.Fatal("expected the retired slot's Pod deleted")
	}
	if _, err := p.Acquire("huge", "w3", ""); !errors.Is(err, ErrNoFlavor) {
		t.Fatalf("expected an unknown flavor to be refused, got %v", err)
	}
}

// On startup the runner adopts its slots by label, asks each what it holds,
// and retires the ones it cannot reach.
func TestAdoptRebuildsFromTheSupervisors(t *testing.T) {
	sup := newFakeSupervisor()
	sup.held["10.0.0.2:7700"] = &slotv1.DescribeResponse{
		State:    slotv1.SlotState_SLOT_STATE_BOUND,
		Versions: &slotv1.VersionSet{Digest: "v1"},
		Workload: &slotv1.WorkloadView{WorkloadId: "w1", WorkloadKey: "k1", Status: slotv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
		Binds:    1,
	}
	sup.down["10.0.0.3:7700"] = true
	client := fake.NewSimpleClientset(
		runningPod("slot-small-aaa", "small", "token-a", "10.0.0.1"),
		runningPod("slot-small-bbb", "small", "token-b", "10.0.0.2"),
		runningPod("slot-small-ccc", "small", "token-c", "10.0.0.3"),
	)
	p := New(testConfig(), client, sup.dial, zap.NewNop())
	adopted, err := p.Adopt(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(adopted) != 1 || adopted[0].Workload.GetWorkloadId() != "w1" || adopted[0].Slot.Name != "slot-small-bbb" {
		t.Fatalf("expected the bound slot adopted with its workload, got %+v", adopted)
	}
	a, _ := p.Get("slot-small-aaa")
	if a.State != StateIdle || a.Token != "token-a" {
		t.Fatalf("expected the idle slot adopted with its token, got %+v", a)
	}
	if _, ok := p.Get("slot-small-ccc"); ok {
		t.Fatal("expected the unreachable slot not to be kept")
	}
	if _, err := client.CoreV1().Pods("agyn-workloads").Get(context.Background(), "slot-small-ccc", metav1.GetOptions{}); err == nil {
		t.Fatal("expected the unreachable slot's Pod deleted")
	}
}

// A slot running another version set is retired once idle, and a bound
// slot whose Pod vanished reports its workload lost.
func TestVersionRolloutAndLoss(t *testing.T) {
	sup := newFakeSupervisor()
	sup.held["10.0.0.1:7700"] = &slotv1.DescribeResponse{State: slotv1.SlotState_SLOT_STATE_IDLE, Versions: &slotv1.VersionSet{Digest: "old"}}
	client := fake.NewSimpleClientset(runningPod("slot-small-aaa", "small", "token-a", "10.0.0.1"))
	p := New(testConfig(), client, sup.dial, zap.NewNop())
	if _, err := p.Adopt(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, _ := p.Get("slot-small-aaa")
	s.LastSeen = time.Now().Add(-time.Minute)
	p.checkIdle(context.Background())
	if s.State != StateRetired {
		t.Fatalf("expected a slot on an old version set retired, got %s", s.State)
	}

	var lost []string
	p.OnLost = func(slot *Slot) { lost = append(lost, slot.Name) }
	bound := &Slot{Name: "slot-small-bbb", Flavor: "small", State: StateBound, WorkloadID: "w1"}
	p.put(bound)
	p.podGone(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "slot-small-bbb"}})
	if len(lost) != 1 || lost[0] != "slot-small-bbb" {
		t.Fatalf("expected the bound slot reported lost, got %v", lost)
	}
}

// In a user namespace SYS_PTRACE is not granted and the Pod says so.
func TestPodInUserNamespaceDropsPtrace(t *testing.T) {
	tpl := PodTemplate{Image: "slot:test", HostUsers: false}
	pod := tpl.Pod("s", "ns", "small", FlavorSpec{VCPUs: 1, MemoryMiB: 1024}, "t", nil)
	if pod.Spec.HostUsers == nil || *pod.Spec.HostUsers {
		t.Fatal("expected hostUsers false")
	}
	for _, c := range pod.Spec.Containers[0].SecurityContext.Capabilities.Add {
		if c == "SYS_PTRACE" {
			t.Fatal("SYS_PTRACE cannot be granted in a user namespace")
		}
	}
	tpl.HostUsers = true
	pod = tpl.Pod("s", "ns", "small", FlavorSpec{VCPUs: 1, MemoryMiB: 1024}, "t", nil)
	if pod.Spec.HostUsers != nil {
		t.Fatal("expected hostUsers unset by default")
	}
}
