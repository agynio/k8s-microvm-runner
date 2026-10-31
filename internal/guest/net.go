package guest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// netConfig is the slot's one tap: a fixed MAC and a fixed point-to-point
// address pair, the same in every slot, so a guest's addressing, ARP cache
// and open connections are valid in whichever slot it is restored into.
type netConfig struct {
	Tap     string
	HostIP  string
	GuestIP string
	MAC     string
	Prefix  int
}

func defaultNet() netConfig {
	return netConfig{
		Tap:     "tap0",
		HostIP:  "10.200.0.1",
		GuestIP: "10.200.0.2",
		MAC:     "06:00:0a:c8:00:02",
		Prefix:  30,
	}
}

func (n netConfig) tapMAC() string { return n.MAC[:len(n.MAC)-2] + "01" }

func (n netConfig) cidr() string { return n.HostIP + "/" + strconv.Itoa(n.Prefix) }

func run(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// setupNet creates the tap, NATs it to the Pod's interface and keeps the
// guest out of the Pod's own network namespace: forwarded traffic passes,
// traffic addressed to the Pod itself -- the supervisor's control channel
// among it -- is dropped.
func (m *Manager) setupNet(ctx context.Context) error {
	n := m.net
	m.teardownNet()
	steps := [][]string{
		{"ip", "tuntap", "add", n.Tap, "mode", "tap"},
		{"ip", "link", "set", n.Tap, "address", n.tapMAC()},
		// The tap carries the Pod interface's MTU (1450 on a VXLAN
		// overlay): a larger guest MTU leaves every full-size segment to
		// path-MTU discovery across the NAT, which stalls.
		{"ip", "link", "set", n.Tap, "mtu", strconv.Itoa(m.egressMTU())},
		{"ip", "addr", "add", n.cidr(), "dev", n.Tap},
		{"ip", "link", "set", n.Tap, "up"},
		{"iptables", "-w", "-t", "nat", "-A", "POSTROUTING", "-s", n.cidr(), "-o", m.cfg.Egress, "-j", "MASQUERADE"},
		{"iptables", "-w", "-A", "FORWARD", "-i", n.Tap, "-j", "ACCEPT"},
		{"iptables", "-w", "-A", "FORWARD", "-o", n.Tap, "-j", "ACCEPT"},
		{"iptables", "-w", "-A", "INPUT", "-i", n.Tap, "-j", "DROP"},
	}
	for _, s := range steps {
		if err := run(ctx, s[0], s[1:]...); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) teardownNet() {
	n := m.net
	ctx := context.Background()
	_ = run(ctx, "iptables", "-w", "-t", "nat", "-D", "POSTROUTING", "-s", n.cidr(), "-o", m.cfg.Egress, "-j", "MASQUERADE")
	_ = run(ctx, "iptables", "-w", "-D", "FORWARD", "-i", n.Tap, "-j", "ACCEPT")
	_ = run(ctx, "iptables", "-w", "-D", "FORWARD", "-o", n.Tap, "-j", "ACCEPT")
	_ = run(ctx, "iptables", "-w", "-D", "INPUT", "-i", n.Tap, "-j", "DROP")
	_ = run(ctx, "ip", "link", "del", n.Tap)
}

// egressMTU is the MTU of the Pod interface the guest's traffic leaves
// through; 1500 if it cannot be read.
func (m *Manager) egressMTU() int {
	b, err := os.ReadFile("/sys/class/net/" + m.cfg.Egress + "/mtu")
	if err != nil {
		return 1500
	}
	if n, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && n > 0 {
		return n
	}
	return 1500
}

// slotResolvConf is the slot Pod's own resolver configuration, which is what
// a kubelet gives a workload Pod in the same namespace.
func slotResolvConf() []string {
	data, err := os.ReadFile("/etc/resolv.conf")
	if err != nil {
		return nil
	}
	var lines []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}
