package guest

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/agynio/k8s-microvm-runner/internal/kata"
	"github.com/agynio/k8s-microvm-runner/internal/store"
)

// Stop ends the held workload: its processes get SIGTERM and timeout to
// leave, the guest is killed, its persistent volumes are persisted, and
// everything else it wrote is discarded with the slot's workload directory.
func (m *Manager) Stop(ctx context.Context, timeout time.Duration, cfg store.Config, volumes []VolumeStore) error {
	w := m.Current()
	if w == nil {
		return ErrIdle
	}
	m.applyVolumeRefs(w, volumes)
	w.mu.Lock()
	wasRunning := w.Status == StatusRunning && w.agent != nil
	w.mu.Unlock()
	m.update(w, func() { w.Status = StatusStopping })
	if wasRunning && timeout > 0 {
		m.terminateGracefully(ctx, w, timeout)
	}
	m.teardownGuest(w)
	st, err := storeClient(cfg)
	if err != nil {
		m.log.Warn("stop: snapshot store", zap.Error(err))
	} else if err := m.persistVolumes(ctx, w, st, cfg); err != nil {
		// The workload is stopped either way; the volume is what is lost.
		m.log.Error("stop: persist volumes", zap.String("workload", w.ID), zap.Error(err))
	}
	m.resetWorkloadDir()
	m.update(w, func() { w.Status = StatusStopped })
	m.clear(w)
	m.log.Info("workload stopped", zap.String("workload", w.ID))
	return nil
}

// terminateGracefully signals every running container's init and waits,
// up to timeout, for the main container to leave.
func (m *Manager) terminateGracefully(ctx context.Context, w *Workload, timeout time.Duration) {
	cl, err := kata.Dial(m.vsockPath())
	if err != nil {
		return
	}
	defer cl.Close()
	w.mu.Lock()
	var running []*Container
	for _, c := range w.Containers {
		if c.Status == ContainerRunning {
			running = append(running, c)
		}
	}
	w.mu.Unlock()
	for _, c := range running {
		_ = cl.Signal(ctx, c.Name, c.Name, 15)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		w.mu.Lock()
		left := 0
		for _, c := range running {
			if c.Status == ContainerRunning {
				left++
			}
		}
		w.mu.Unlock()
		if left == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	m.log.Info("containers did not leave in time; killing the guest", zap.String("workload", w.ID), zap.Duration("timeout", timeout))
}

// Signal sends a signal to a container's init process.
func (m *Manager) Signal(ctx context.Context, w *Workload, container string, sig uint32) error {
	cl, err := kata.Dial(m.vsockPath())
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := cl.Signal(ctx, container, container, sig); err != nil {
		return fmt.Errorf("signal %s: %w", container, err)
	}
	return nil
}
